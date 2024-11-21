
# Monitoring of Acidified Surface Waters
## Réalisé par : Elias Frik et Rafael Nakache
## Description
Ce projet vise à surveiller et analyser les données sur les eaux de surface acidifiées. Le pipeline de traitement intègre des données provenant de Kafka, les transforme et les stocke dans une base de données DuckDB. Le système génère également des métriques significatives pour l'analyse.

---

## Fonctionnalités

### 1. Production et consommation de données Kafka
- **Producteur Kafka** : Lit les données d'un fichier CSV nettoyé et les envoie par blocs de 10 lignes au topic Kafka `water_data_stream`.
- **Consommateur Kafka** : Utilise PySpark pour lire les données en streaming depuis Kafka, les analyser et les stocker dans DuckDB.

### 2. Stockage et enrichissement des données
- Les données sont enregistrées dans DuckDB sous plusieurs tables :
  - `ltm_table` : Données de chimie des eaux.
  - `Site_Information_Cleaned` : Informations géographiques des sites.
  - `Methods_Cleaned` : Méthodologie associée aux programmes d'échantillonnage.
- Une table de résultats jointes est créée en fusionnant ces sources.

### 3. Transformation des données
- Les données sont transformées pour passer du modèle SCD2 (Slowly Changing Dimensions Type 2) à SCD1, simplifiant ainsi les analyses ultérieures.
- Les jointures entre les tables sont effectuées tout en supprimant les doublons.

### 4. Calcul de métriques
- Les métriques suivantes sont calculées et sauvegardées dans une table dédiée :
  - Moyenne annuelle des valeurs de pH.
  - Moyenne annuelle des concentrations de sulfates (SO4).
  - Moyenne annuelle des concentrations de nitrates (NO3).
  - Nombre total d'échantillons par site et par année.

### 5. Gestion des versions et snapshots
- Chaque lot traité est enregistré avec des métadonnées incluant :
  - Les dates minimum et maximum des échantillons.
  - Les chemins des snapshots sauvegardés localement pour garantir une reprise possible.

---

## Prérequis
### Logiciels nécessaires
- Apache Kafka
- Apache Spark
- DuckDB (et son driver JDBC)
- HDFS pour le stockage des fichiers statiques
- Python (avec les bibliothèques `pandas`, `pyspark`, `kafka-python`, etc.)

### Installation des dépendances
Installez les dépendances nécessaires avec pip :
```bash
pip install kafka-python pandas pyspark duckdb
```

---

## Instructions d'exécution

### Étape 1 : Production des données avec Kafka
1. Lancez le serveur Kafka et Zookeper.
```bash
   bin/zookeeper-server-start.sh config/zookeeper.properties
   ```
```bash
   bin/kafka-server-start.sh config/server.properties
   ```
2.Creer un topic
```bash
bin/kafka-topics.sh --create --topic <topic_name> --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```


3. Configurez le chemin du fichier de données nettoyé dans `data_file` (par défaut : `LTM_Data_Cleaned_final.csv`).
4. Exécutez le script du producteur Kafka :
   ```bash
   python producer1.py
   ```
## Commandes Spark
### Étape 2 : Consommation et traitement des données
1. Configurez les paramètres Kafka et Spark dans le script `datastream.py`.
2. Lancez le pipeline de consommation et de traitement des données :
Pour exécuter un traitement spécifique avec Spark, utilisez la commande suivante en remplaçant `<script_name>` par le nom du fichier à exécuter :

```bash
spark-submit \\
  --master "local[*]" \\
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
  --jars /Users/monkeydziyech/Desktop/Projet-data-Integration/scripts/duckdb_jdbc-1.1.3.jar \\
  --driver-java-options "-Dlog4j.configuration=file:/path/to/log4j.properties" \\
  <script_name>
  ```



   

### Étape 3 : Chargement des fichiers statiques depuis HDFS
1. Placez les fichiers nettoyés dans HDFS (`Methods_Cleaned.csv` et `Site_Information_Cleaned.csv`).
 ```bash
hdfs dfs -put <local_file_path> <hdfs_destination_path>
   ```

3. Exécutez le script `HDFSToDuckDB.py` pour charger les données dans DuckDB.

### Étape 4 : Jointure et calcul des métriques
1. Lancez le script principal `table_joining` pour effectuer les étapes suivantes :
   - Récupération des métadonnées des lots précédents.
   - Transformation des données en modèle SCD1.
   - Jointure des tables et stockage dans `joined_result_table`.
   - Calcul des métriques et stockage dans `metrics_table_name`.

---

## Structure des données Exemple
### Table `ltm_table`
| Colonne          | Type       | Description                      |
|-------------------|------------|----------------------------------|
| SITE_ID           | String     | Identifiant unique du site.      |
| DATE_SMP          | Timestamp  | Date de l'échantillon.           |
| PH_LAB            | String     | Valeur de pH mesurée en laboratoire. |

### Table `Site_Information_Cleaned`
| Colonne          | Type       | Description                      |
|-------------------|------------|----------------------------------|
| SITE_ID           | String     | Identifiant unique du site.      |
| STATE             | String     | État où se trouve le site.       |

### Table `Methods_Cleaned`
| Colonne          | Type       | Description                      |
|-------------------|------------|----------------------------------|
| PROGRAM_ID        | String     | Identifiant du programme.        |
| METHOD            | String     | Méthodologie utilisée.           |

---

## Résultats
Les résultats sont stockés dans :
- **DuckDB** :
  - `report_layer.ltm_table`
  - `report_layer.joined_result_table`
  - `report_layer.metrics_table_name`
- **Snapshots locaux** : Répertoire `snapshots/`.

---
### Table `metrics_table` Exemple

| YEAR | PROGRAM_ID | SITE_ID | AVG_PH      | AVG_SO4    | AVG_NO3     | TOTAL_SAMPLES |
|------|------------|---------|-------------|------------|-------------|---------------|
| 2020 | Program_1  | Site_1  | 6.08        | 49.77      | 13.73       | 1             |
| 2020 | Program_2  | Site_2  | 5.54        | 47.66      | 10.36       | 1             |
| 2020 | Program_3  | Site_3  | 5.04        | 46.67      | 22.49       | 1             |
| 2020 | Program_4  | Site_4  | 6.57        | 45.65      | 9.29        | 1             |
| 1991 | Program_5  | Site_5  | 5.81        | 126.8      | 38.2        | 1             |
| 2020 | Program_6  | Site_6  | 6.14        | 21.36      | 0.00        | 1             |
| 2024 | Program_7  | Site_7  | NULL        | NULL       | NULL        | 1             |

* **YEAR** : Année de l'échantillon.
* **PROGRAM_ID** : Identifiant du programme lié aux échantillons.
* **SITE_ID** : Identifiant unique du site.
* **AVG_PH** : Moyenne du pH mesurée sur le site cette année-là.
* **AVG_SO4** : Moyenne des concentrations de sulfates (SO4) mesurées.
* **AVG_NO3** : Moyenne des concentrations de nitrates (NO3) mesurées.
* **TOTAL_SAMPLES** : Nombre total d'échantillons collectés sur le site pour cette année.





--- 
