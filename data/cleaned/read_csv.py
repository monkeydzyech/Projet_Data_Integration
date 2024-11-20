import pandas as pd

# Spécifier le chemin de votre fichier CSV
data_file = '/Users/monkeydziyech/Desktop/Projet-data-Integration/data/cleaned/LTM_Data_Cleaned.csv'

# Lire le fichier CSV avec un séparateur ';'
data = pd.read_csv(data_file, sep=';')

# Afficher les premières lignes du dataframe pour vérifier
print("Premières lignes des données:")
print(data.head())

# Afficher les types de données des colonnes
print("\nTypes des colonnes:")
print(data.dtypes)

# Vérifier s'il y a des données manquantes
print("\nVérification des données manquantes:")
print(data.isnull().sum())

# Sauvegarder les données nettoyées dans un nouveau fichier CSV si nécessaire
data.to_csv('/Users/monkeydziyech/Desktop/Projet-data-Integration/data/cleaned/LTM_Data_Cleaned_final.csv', index=False)
print("\nDonnées sauvegardées dans 'LTM_Data_Cleaned_final.csv'.")
