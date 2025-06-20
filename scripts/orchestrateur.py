#!/usr/bin/env python3
# orchestrateur_prefect.py

import os, time, signal, subprocess, duckdb, sys, logging
from prefect import flow, task, get_run_logger

# --- Constantes ---
BASE = "/Users/monkeydziyech/Desktop/Projet-data-Integration/scripts"
JAR = f"{BASE}/duckdb_jdbc-1.1.3.jar"
DUCKDB_PATH = f"{BASE}/mydatabase.db"
SPARK_SUBMIT = "spark-submit"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
CHECK_INTERVAL = 2
TIMEOUT = 180  # secondes max pour attendre 1er batch

# --- Tasks Prefect ---

@task
def run_data_stream_and_wait():
    logger = get_run_logger()
    cmd = [
        SPARK_SUBMIT, "--master", "local[*]",
        "--packages", KAFKA_PACKAGE,
        "--jars", JAR,
        f"{BASE}/data_stream.py"
    ]
    logger.info("🚀 Lancement du streaming Kafka...")
    proc = subprocess.Popen(cmd, preexec_fn=os.setsid)

    # Vérifier l'arrivée d'un batch
    start = time.time()
    while time.time() - start < TIMEOUT:
        try:
            with duckdb.connect(DUCKDB_PATH, read_only=True) as con:
                (count,) = con.execute("SELECT COUNT(*) FROM report_layer.controle_table").fetchone()
            if count > 0:
                logger.info(f"✅ Batch détecté : {count} lignes")
                break
        except Exception:
            pass
        time.sleep(CHECK_INTERVAL)
    else:
        logger.error("❌ Timeout - Aucun batch détecté")
        os.killpg(proc.pid, signal.SIGINT)
        proc.wait()
        raise RuntimeError("Timeout sur data_stream")

    logger.info("🛑 Arrêt du streaming Kafka")
    os.killpg(proc.pid, signal.SIGINT)
    proc.wait()
    time.sleep(2)

@task
def run_spark_script(script_name: str):
    logger = get_run_logger()
    cmd = [
        SPARK_SUBMIT, "--master", "local[*]",
        "--jars", JAR,
        f"{BASE}/{script_name}"
    ]
    logger.info(f"▶️ Lancement de {script_name}")
    subprocess.run(cmd, check=True)
    logger.info(f"✅ Fin de {script_name}")

# --- Flow principal Prefect ---
@flow(name="orchestrateur_ltm_prefect")
def ltm_pipeline():
    run_data_stream_and_wait()
    run_spark_script("hdfs_to_duckdb.py")
    run_spark_script("table_joining.py")
    # run_spark_script("snapshot_archive.py")  # si tu veux l’activer

# --- Entry point ---
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        ltm_pipeline()
    except KeyboardInterrupt:
        sys.exit("⛔ Interrompu par l’utilisateur")
