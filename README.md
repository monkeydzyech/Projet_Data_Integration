
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
1. Lancez le serveur Kafka.
2. Configurez le chemin du fichier de données nettoyé dans `data_file` (par défaut : `LTM_Data_Cleaned_final.csv`).
3. Exécutez le script du producteur Kafka :
   ```bash
   python producer1.py
   ```

### Étape 2 : Consommation et traitement des données
1. Configurez les paramètres Kafka et Spark dans le script `datastream.py`.
2. Lancez le pipeline de consommation et de traitement des données :
   ```bash
   python datastream.py
   ```

### Étape 3 : Chargement des fichiers statiques depuis HDFS
1. Placez les fichiers nettoyés dans HDFS (`Methods_Cleaned.csv` et `Site_Information_Cleaned.csv`).
2. Exécutez le script `HDFSToDuckDB.py` pour charger les données dans DuckDB.

### Étape 4 : Jointure et calcul des métriques
1. Lancez le script principal `table_joining` pour effectuer les étapes suivantes :
   - Récupération des métadonnées des lots précédents.
   - Transformation des données en modèle SCD1.
   - Jointure des tables et stockage dans `joined_result_table`.
   - Calcul des métriques et stockage dans `metrics_table_name`.

---

## Structure des données
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




--- 
