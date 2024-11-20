import duckdb
import logging

# Configurer les logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def create_duckdb_schema():
    try:
        # Connexion à DuckDB (il créera la base de données si elle n'existe pas)
        conn = duckdb.connect('mydatabase.db')
        logging.info("Connexion à DuckDB établie.")

        # Création du schéma report_layer
        conn.execute("""
            CREATE SCHEMA IF NOT EXISTS report_layer;
        """)
        logging.info("Schéma 'report_layer' créé.")

        # Création de la table LTM_Data_2022_8_1 dans le schéma report_layer
        conn.execute("""
            CREATE TABLE IF NOT EXISTS report_layer.LTM_Data_2022_8_1 (
                SITE_ID VARCHAR,
                PROGRAM_ID VARCHAR,
                DATE_SMP TIMESTAMP,
                SAMPLE_LOCATION VARCHAR,
                SAMPLE_TYPE VARCHAR,
                WATERBODY_TYPE VARCHAR,
                SAMPLE_DEPTH VARCHAR,
                TIME_SMP TIMESTAMP,
                ANC_UEQ_L VARCHAR,
                CA_UEQ_L VARCHAR,
                CHL_A_UG_L VARCHAR,
                CL_UEQ_L VARCHAR,
                COND_UM_CM VARCHAR,
                DOC_MG_L VARCHAR,
                F_UEQ_L VARCHAR,
                K_UEQ_L VARCHAR,
                MG_UEQ_L VARCHAR,
                NA_UEQ_L VARCHAR,
                NH4_UEQ_L VARCHAR,
                NO3_UEQ_L VARCHAR,
                N_TD_UEQ_L VARCHAR,
                PH_EQ VARCHAR,
                PH_FLD VARCHAR,
                PH_LAB VARCHAR,
                PH_STVL VARCHAR,
                P_TL_UEQ_L VARCHAR,
                SECCHI_M VARCHAR,
                SIO2_MG_L VARCHAR,
                SO4_UEQ_L VARCHAR,
                WTEMP_DEG_C VARCHAR
            );
        """)
        logging.info("Table 'LTM_Data_2022_8_1' créée dans le schéma 'report_layer'.")

        # Fermeture de la connexion
        conn.close()
        logging.info("Connexion à DuckDB fermée.")

    except Exception as e:
        logging.error(f"Une erreur est survenue : {e}")
        raise

# Exécution de la fonction pour créer la base de données et la table
if __name__ == "__main__":
    create_duckdb_schema()
