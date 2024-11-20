from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, regexp_extract,regexp_replace, avg,count,year
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
import logging
import sys

# Configuration des logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Pipeline")

# Initialisation de SparkSession
logger.info("Initialisation de SparkSession...")

spark = SparkSession.builder \
    .appName("joinTable") \
    .master("local[*]") \
    .config("spark.jars", "/Users/monkeydziyech/Desktop/Projet-data-Integration/scripts/duckdb_jdbc-1.1.3.jar") \
    .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY") \
    .getOrCreate()

logger.info("SparkSession initialisée.")

# URL de la base de données DuckDB
jdbc_url = "jdbc:duckdb:/Users/monkeydziyech/Desktop/Projet-data-Integration/scripts/mydatabase.db"

# Fonction 1 : Récupération des métadonnées du dernier batch
def get_last_batch_metadata(spark, jdbc_url):
    try:
        controle_df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "report_layer.controle_table") \
            .option("driver", "org.duckdb.DuckDBDriver") \
            .load()

        logger.info(f"Table controle récupérée : {controle_df.count()} lignes.")
        controle_df.show()

        controle_df = controle_df.withColumn(
            "batch_number", 
            regexp_extract(col("version_id"), r"batch_(\d+)", 1).cast("int")
        )
        
        last_batch = controle_df.orderBy(col("batch_number").desc()).limit(1).collect()[0]
        
        return {
            "min_datestamp": last_batch["min_datestamp"],
            "max_datestamp": last_batch["max_datestamp"]
        }

    except Exception as e:
        logger.error(f"Erreur lors de la récupération des métadonnées : {e}")
        sys.exit(1)

# Fonction 2 : Sélection des données du batch
def get_batch_data(spark, jdbc_url, min_date, max_date):
    try:
        ltm_table_df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "report_layer.ltm_table") \
            .option("driver", "org.duckdb.DuckDBDriver") \
            .load()

        logger.info("Contenu brut de la table LTM avant filtrage :")
        ltm_table_df.show(truncate=False)
        
        filtered_data = ltm_table_df.filter(
            (col("DATE_SMP") >= min_date) & (col("DATE_SMP") <= max_date)
        )

        logger.info("Contenu de la table LTM après filtrage par date :")
        filtered_data.show(truncate=False)
        
        return filtered_data

    except Exception as e:
        logger.error(f"Erreur lors de la récupération des données du batch : {e}")
        sys.exit(1)

# Fonction 3 : Transformation de SCD2 à SCD1
def transform_scd2_to_scd1(dataframe, primary_keys, date_column):
    logger.info(f"Transformation SCD2 -> SCD1 pour les clés {primary_keys} et la colonne {date_column}")
    logger.info(f"Nombre de lignes avant transformation : {dataframe.count()}.")
    try:
        if date_column not in dataframe.columns:
            raise ValueError(f"Colonne '{date_column}' manquante. Colonnes disponibles : {dataframe.columns}")
        
        dataframe = dataframe.withColumn("date_column_casted", col(date_column).cast("date"))

        window_spec = Window.partitionBy(*primary_keys).orderBy(col("date_column_casted").desc())

        scd1_df = dataframe.withColumn("row_num", row_number().over(window_spec)) \
                   .filter(col("row_num") == 1) \
                   .drop("row_num", "date_column_casted")  

        logger.info("Contenu après transformation SCD2 -> SCD1 :")
        scd1_df.show(truncate=False)
        
        return scd1_df

    except Exception as e:
        logger.error(f"Erreur lors de la transformation SCD2 à SCD1 : {e}")
        sys.exit(1)

# Fonction 4 : Vérification des correspondances entre deux DataFrames
def check_id_correspondences(ltm_df, site_df, id_column):
    logger.info(f"Validation des correspondances pour la colonne {id_column}.")
    ltm_ids = ltm_df.select(id_column).distinct()
    site_ids = site_df.select(id_column).distinct()

    missing_in_site = ltm_ids.exceptAll(site_ids)
    missing_in_ltm = site_ids.exceptAll(ltm_ids)

    logger.info("IDs présents dans LTM mais absents dans Site_Information :")
    missing_in_site.show(truncate=False)

    logger.info("IDs présents dans Site_Information mais absents dans LTM :")
    missing_in_ltm.show(truncate=False)

# Fonction 5 : Jointure et sauvegarde avec suppression des colonnes dupliquées
def join_and_save_to_table(df_ltm_scd1, df_site_scd1, df_methode_scd1, jdbc_url, output_table_name):
    logger.info("Début de la fonction join_and_save_to_table.")
    
    try:
        df_ltm_scd1.createOrReplaceTempView("ltm_table")
        df_site_scd1.createOrReplaceTempView("site_information")
        df_methode_scd1.createOrReplaceTempView("method")
        
        query = """
            SELECT
    -- Champs de la table ltm
    ltm.SITE_ID AS SITE_ID_LTM,
    ltm.PROGRAM_ID AS PROGRAM_ID_LTM,
    ltm.DATE_SMP,
    ltm.SAMPLE_LOCATION,
    ltm.SAMPLE_TYPE,
    ltm.WATERBODY_TYPE,
    ltm.SAMPLE_DEPTH,
    ltm.TIME_SMP,
    ltm.ANC_UEQ_L,
    ltm.CA_UEQ_L,
    ltm.CHL_A_UG_L,
    ltm.CL_UEQ_L,
    ltm.COND_UM_CM,
    ltm.DOC_MG_L,
    ltm.F_UEQ_L,
    ltm.K_UEQ_L,
    ltm.MG_UEQ_L,
    ltm.NA_UEQ_L,
    ltm.NH4_UEQ_L,
    ltm.NO3_UEQ_L,
    ltm.N_TD_UEQ_L,
    ltm.PH_EQ,
    ltm.PH_FLD,
    ltm.PH_LAB,
    ltm.PH_STVL,
    ltm.P_TL_UEQ_L,
    ltm.SECCHI_M,
    ltm.SIO2_MG_L,
    ltm.SO4_UEQ_L,
    ltm.WTEMP_DEG_C,

    -- Champs de la table methode m
    m.PROGRAM_ID AS PROGRAM_ID_METHODE,
    m.PARAMETER,
    m.START_YEAR,
    m.END_YEAR,
    m.METHOD,
    m.METHOD_DESCRIPTION,

    -- Champs de la table site sI
    si.SITE_ID AS SITE_ID_SITE,
    si.PROGRAM_ID AS PROGRAM_ID_SITE,
    si.LATDD,
    si.LONDD,
    si.LATDD_CENTROID,
    si.LONDD_CENTROID,
    si.SITE_NAME,
    si.COMID,
    si.FEATUREID,
    si.COUNTY,
    si.STATE,
    si.ECOREGION_I,
    si.ECOREGION_II,
    si.ECOREGION_III,
    si.ECOREGION_IV,
    si.ECONAME_I,
    si.ECONAME_II,
    si.ECONAME_III,
    si.ECONAME_IV,
    si.LAKE_DEPTH_MAX,
    si.LAKE_DEPTH_MEAN,
    si.LAKE_RET,
    si.LAKE_AREA_HA,
    si.LAKE_AREA_NHD2,
    si.WSHD_AREA_HA,
    si.SITE_ELEV,
    si.WSHD_ELEV_AVG,
    si.WSHD_ELEV_MIN,
    si.WSHD_ELEV_MAX,
    si.WSHD_SLOPE_AVG,
    si.UPDATE_DATE

            FROM ltm_table AS ltm
            LEFT JOIN site_information AS si
                ON ltm.SITE_ID = si.SITE_ID AND ltm.PROGRAM_ID = si.PROGRAM_ID
            LEFT JOIN method AS m 
                ON si.PROGRAM_ID = m.PROGRAM_ID
        """
        joined_df = spark.sql(query)
        
        logger.info("Contenu du DataFrame final après jointure :")
        joined_df.show(truncate=False)
        
        joined_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"report_layer.{output_table_name}") \
            .option("driver", "org.duckdb.DuckDBDriver") \
            .mode("append") \
            .save()
        return joined_df

    except Exception as e:
        logger.error(f"Erreur lors de la jointure ou de l'écriture : {e}")
        raise

# Fonction 6 : Création et sauvegarde de la table des métriques
def create_and_save_metrics_table(joined_df, jdbc_url, metrics_table_name):
    logger.info("Début de la création de la table des métriques.")
    
    try:
        # Transformation des données pour les métriques
        metrics_df = joined_df.select(
            col("SITE_ID_LTM").alias("SITE_ID"),
            col("PROGRAM_ID_LTM").alias("PROGRAM_ID"),
            year(col("DATE_SMP")).alias("YEAR"),
            regexp_replace(col("PH_LAB"), ",", ".").cast(DoubleType()).alias("PH_LAB"),
            regexp_replace(col("SO4_UEQ_L"), ",", ".").cast(DoubleType()).alias("SO4_UEQ_L"),
            regexp_replace(col("NO3_UEQ_L"), ",", ".").cast(DoubleType()).alias("NO3_UEQ_L")
        ).groupBy(
            "SITE_ID", "PROGRAM_ID", "YEAR"
        ).agg(
            avg("PH_LAB").alias("AVG_PH"),
            avg("SO4_UEQ_L").alias("AVG_SO4"),
            avg("NO3_UEQ_L").alias("AVG_NO3"),
            count("*").alias("TOTAL_SAMPLES")
        )

        logger.info("Contenu du DataFrame des métriques :")
        metrics_df.show(truncate=False)

        # Sauvegarde des métriques dans un nouveau schéma "metrique"
        metrics_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"report_layer.{metrics_table_name}") \
            .option("driver", "org.duckdb.DuckDBDriver") \
            .mode("overwrite") \
            .save()

        logger.info(f"Table des métriques sauvegardée sous : report_layer.{metrics_table_name}")

    except Exception as e:
        logger.error(f"Erreur lors de la création ou de la sauvegarde de la table des métriques : {e}")
        raise


# Pipeline
try:
    last_batch_meta = get_last_batch_metadata(spark, jdbc_url)
    df_ltm = get_batch_data(spark, jdbc_url, 
                            last_batch_meta["min_datestamp"], 
                            last_batch_meta["max_datestamp"])
    
    df_ltm_scd1 = transform_scd2_to_scd1(df_ltm, ["SITE_ID"], "DATE_SMP")

    # Vérification des données source pour site_information
    df_site_information = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "report_layer.Site_Information_Cleaned") \
        .option("driver", "org.duckdb.DuckDBDriver") \
        .load()

    logger.info("Contenu brut de la table Site_Information_Cleaned :")
    df_site_information.show(truncate=False)

    df_site_scd1 = transform_scd2_to_scd1(df_site_information, ["SITE_ID", "PROGRAM_ID"], "UPDATE_DATE")

    check_id_correspondences(df_ltm_scd1, df_site_scd1, "SITE_ID")

    df_methode = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "report_layer.Methods_Cleaned") \
        .option("driver", "org.duckdb.DuckDBDriver") \
        .load()

    logger.info("Contenu brut de la table Methods_Cleaned :")
    df_methode.show(truncate=False)

    df_methode_scd1 = transform_scd2_to_scd1(df_methode, ["PROGRAM_ID"], "END_YEAR")

    joined_df=join_and_save_to_table(df_ltm_scd1, df_site_scd1, df_methode_scd1, jdbc_url, "joined_result_table")

    create_and_save_metrics_table(joined_df, jdbc_url, "metrics_table_name")

    

except Exception as e:
    logger.error(f"Erreur dans le pipeline : {e}")
finally:
    spark.stop()
