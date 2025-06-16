from pyspark.sql import SparkSession
import logging
from pyspark.sql.functions import col

# Configuration des logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("HDFSToDuckDB")

# Initialisation de SparkSession
logger.info("Initialisation de SparkSession...")
spark = SparkSession.builder \
    .appName("HDFS to DuckDB") \
    .master("local[*]") \
    .config("spark.jars", "/Users/monkeydziyech/Desktop/Projet-data-Integration/scripts/duckdb_jdbc-1.1.3.jar") \
    .getOrCreate()
logger.info("SparkSession initialisée.")

# Lecture des fichiers depuis HDFS avec le bon délimiteur ";"
logger.info("Lecture des fichiers depuis HDFS...")
methods_df = spark.read.option("delimiter", ";").csv(
    "hdfs://localhost:9000/data/static/methods/Methods_Cleaned.csv",

    header=True,
    inferSchema=True
)
methods_df = methods_df.withColumn("end_date", col("end_year").substr(1, 4))


site_info_df = spark.read.option("delimiter", ";").csv(
    "hdfs://localhost:9000/data/static/sites/Site_Information_Cleaned.csv",

    header=True,
    inferSchema=True
)

logger.info("Fichiers HDFS chargés avec succès.")



# Fonction pour écrire dans DuckDB
def write_to_duckdb(df, table_name):
    logger.info(f"Écriture dans la table {table_name} de DuckDB...")
    try:
        jdbc_url = "jdbc:duckdb:/Users/monkeydziyech/Desktop/Projet-data-Integration/scripts/mydatabase.db"
        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("driver", "org.duckdb.DuckDBDriver") \
            .mode("overwrite") \
            .save()
        logger.info(f"Table {table_name} écrite avec succès.")
    except Exception as e:
        logger.error(f"Erreur lors de l'écriture dans la table {table_name} : {e}")

# Écriture des données dans DuckDB
write_to_duckdb(methods_df, "report_layer.Methods_Cleaned")
write_to_duckdb(site_info_df, "report_layer.Site_Information_Cleaned")

logger.info("Transfert des données terminé.")
