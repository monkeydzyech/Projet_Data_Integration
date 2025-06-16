from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import logging
import os
import time


# Configuration des logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("KafkaToDuckDB")
# Écriture des logs dans un fichier local
file_handler = logging.FileHandler("../logs/pipeline_monitoring.log")
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Initialisation de SparkSession
logger.info("Initialisation de SparkSession...")
spark = SparkSession.builder \
    .appName("KafkaBatchToDuckDB") \
    .master("local[*]") \
    .config("spark.jars", "/Users/monkeydziyech/Desktop/Projet-data-Integration/scripts/duckdb_jdbc-1.1.3.jar") \
    .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY") \
    .getOrCreate()

logger.info("SparkSession initialisée.")

# Définition du schéma des données
schema = StructType([
    StructField("SITE_ID", StringType(), True),
    StructField("PROGRAM_ID", StringType(), True),
    StructField("DATE_SMP", TimestampType(), True),
    StructField("SAMPLE_LOCATION", StringType(), True),
    StructField("SAMPLE_TYPE", StringType(), True),
    StructField("WATERBODY_TYPE", StringType(), True),
    StructField("SAMPLE_DEPTH", StringType(), True),
    StructField("TIME_SMP", TimestampType(), True),
    StructField("ANC_UEQ_L", StringType(), True),
    StructField("CA_UEQ_L", StringType(), True),
    StructField("CHL_A_UG_L", StringType(), True),
    StructField("CL_UEQ_L", StringType(), True),
    StructField("COND_UM_CM", StringType(), True),
    StructField("DOC_MG_L", StringType(), True),
    StructField("F_UEQ_L", StringType(), True),
    StructField("K_UEQ_L", StringType(), True),
    StructField("MG_UEQ_L", StringType(), True),
    StructField("NA_UEQ_L", StringType(), True),
    StructField("NH4_UEQ_L", StringType(), True),
    StructField("NO3_UEQ_L", StringType(), True),
    StructField("N_TD_UEQ_L", StringType(), True),
    StructField("PH_EQ", StringType(), True),
    StructField("PH_FLD", StringType(), True),
    StructField("PH_LAB", StringType(), True),
    StructField("PH_STVL", StringType(), True),
    StructField("P_TL_UEQ_L", StringType(), True),
    StructField("SECCHI_M", StringType(), True),
    StructField("SIO2_MG_L", StringType(), True),
    StructField("SO4_UEQ_L", StringType(), True),
    StructField("WTEMP_DEG_C", StringType(), True)
])

logger.info("Schéma défini.")

# Lecture des données depuis Kafka
logger.info("Lecture des données depuis Kafka...")
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stream_water") \
    .option("startingOffsets", "earliest") \
    .load()

logger.info("Lecture des données Kafka initialisée.")

parsed_df = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("DATE_SMP", to_timestamp("DATE_SMP", "yyyy-MM-dd HH:mm:ss"))

logger.info("Parsing terminé.")



checkpoint_path = "/Users/monkeydziyech/Desktop/Projet-data-Integration/checkpoints/ltm_checkpoint"


# Fonction pour créer un snapshot local par année
def create_snapshot(batch_df, batch_id):
    min_date = batch_df.agg({"DATE_SMP": "min"}).collect()[0][0]
    year = min_date.year if min_date else "unknown"
    
    snapshot_dir = f"/Users/monkeydziyech/Desktop/Projet-data-Integration/snapshots/{year}/batch_{batch_id}"
    
    if not os.path.exists(snapshot_dir):
        os.makedirs(snapshot_dir)
    
    # Écriture en Parquet avec la gestion des dates anciennes
    batch_df.write.option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
        .format("parquet") \
        .mode("overwrite") \
        .save(snapshot_dir)
    
    logger.info(f"Snapshot pour l'année {year} sauvegardé : {snapshot_dir}")
    return snapshot_dir

# Fonction pour écrire un batch dans DuckDB
def write_batch_to_duckdb(batch_df, batch_id):
    logger.info(f"Traitement du batch {batch_id}...")
    start_time = time.time()
    try:
        num_rows = batch_df.count()
        logger.info(f"Nombre de lignes dans le batch {batch_id} : {num_rows}")
        # Connexion JDBC à DuckDB
        jdbc_url = "jdbc:duckdb:/Users/monkeydziyech/Desktop/Projet-data-Integration/scripts/mydatabase.db"

        # Écriture des données dans la table principale
        batch_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "report_layer.ltm_table") \
            .option("driver", "org.duckdb.DuckDBDriver") \
            .mode("append") \
            .save()
        
        logger.info(f"Batch {batch_id} écrit avec succès dans DuckDB.")

        # Sauvegarde du snapshot et mise à jour de la table de contrôle
        snapshot_path = create_snapshot(batch_df, batch_id)

        controle_df = spark.createDataFrame([
            {"version_id": f"batch_{batch_id}",
             "min_datestamp": batch_df.agg({"DATE_SMP": "min"}).collect()[0][0],
             "max_datestamp": batch_df.agg({"DATE_SMP": "max"}).collect()[0][0],
             "description": f"Batch {batch_id}",
             "snapshot_path": snapshot_path}
        ])

        controle_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "report_layer.controle_table") \
            .option("driver", "org.duckdb.DuckDBDriver") \
            .mode("append") \
            .save()

        logger.info(f"Métadonnées du batch {batch_id} enregistrées avec le snapshot {snapshot_path}.")

    except Exception as e:
        logger.error(f"Erreur lors de l'écriture du batch {batch_id} : {e}")
    end_time = time.time()
    logger.info(f"Durée de traitement du batch {batch_id} : {end_time - start_time:.2f} secondes")  # FIN DU MONITORING


query = parsed_df.writeStream \
    .foreachBatch(write_batch_to_duckdb) \
    .option("checkpointLocation", checkpoint_path) \
    .outputMode("append") \
    .start()

query.awaitTermination()
