from kafka import KafkaProducer
import pandas as pd
import json
import time

# Initialisation du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Adresse du serveur Kafka
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Sérialisation des données en JSON
)

# Lecture des données nettoyées à partir du fichier CSV
data_file = '/Users/monkeydziyech/Desktop/Projet-data-Integration/data/cleaned/LTM_Data_Cleaned_final.csv'
data = pd.read_csv(data_file)

# Nom du topic Kafka
topic_name = 'water_data_stream'

# Envoi des données par blocs de 10 lignes toutes les 10 secondes
for i in range(0, len(data), 10):
    chunk = data.iloc[i:i+10].to_dict(orient='records')  # Convertir le bloc de 10 lignes en liste de dictionnaires
    # Envoi de chaque message Kafka par bloc (10 messages sous forme JSON ligne par ligne dans un bloc)
    for record in chunk:
        producer.send(topic_name, record)
    
    print(f"Envoyé le batch {i//10 + 1} au topic {topic_name}")
    
    # Pause de 10 secondes avant d'envoyer le prochain bloc
    time.sleep(10)

# Fermer le producteur
producer.close()
print("Producteur fermé.")
