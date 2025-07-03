import pandas as pd
import pickle
import os
from tqdm import tqdm

# 1. Charger le fichier des unités légales dans un dictionnaire (avec cache)
unite_legale_file = "StockUniteLegale_utf8.csv"
cache_file = "unite_legale_dict.pkl"

# Charger depuis le cache si disponible
if os.path.exists(cache_file):
    print("Chargement du cache des noms d'entreprises...")
    with open(cache_file, 'rb') as f:
        unite_legale_dict = pickle.load(f)
    print(f"Nombre d'entreprises chargées depuis le cache: {len(unite_legale_dict)}")
else:
    print("Chargement des noms d'entreprises depuis le CSV...")
    unite_legale_dict = {}

    # Colonnes nécessaires pour les unités légales
    cols_unite_legale = ["siren", "denominationUniteLegale", "nomUniteLegale"]
    dtypes_unite_legale = {
        "siren": "string",
        "denominationUniteLegale": "string",
        "nomUniteLegale": "string"
    }

    # Lecture du fichier avec barre de progression
    chunk_size_unite = 10000
    total_unite_legale = sum(1 for _ in open(unite_legale_file, 'r', encoding='utf-8')) - 1

    with tqdm(total=total_unite_legale, desc="Chargement des unités légales") as pbar:
        for chunk in pd.read_csv(unite_legale_file, 
                                sep=",", 
                                usecols=cols_unite_legale, 
                                dtype=dtypes_unite_legale,
                                chunksize=chunk_size_unite):
            for _, row in chunk.iterrows():
                siren = row['siren']
                # Combiner les deux colonnes de noms
                nom_entreprise = row['denominationUniteLegale'] if pd.notna(row['denominationUniteLegale']) else row['nomUniteLegale']
                if pd.isna(nom_entreprise):
                    nom_entreprise = ""
                unite_legale_dict[siren] = nom_entreprise
            pbar.update(len(chunk))

    # Sauvegarder dans un fichier cache
    with open(cache_file, 'wb') as f:
        pickle.dump(unite_legale_dict, f)
    print(f"Cache sauvegardé dans {cache_file}")
    print(f"Nombre d'entreprises chargées: {len(unite_legale_dict)}")

# 2. Traitement du fichier complet des établissements filtrés
input_file = "Donnees_Filtrees_Completes.csv"
output_file = "Donnees_Filtrees_Completes_avec_noms.csv"
chunksize = 50000  # Taille des chunks pour la lecture/écriture

# Définition des types de données pour toutes les colonnes
dtypes_etablissements = {
    "siret": "string",
    "siren": "string",
    "denominationUsuelleEtablissement": "string",
    "codePostalEtablissement": "string",
    "libelleCommuneEtablissement": "string",
    "trancheEffectifsEtablissement": "string",
    "etatAdministratifEtablissement": "string",
    "caractereEmployeurEtablissement": "string",
    "etablissementSiege": "string"  # On traite comme string pour éviter les problèmes de conversion
}

# Compter le nombre total de lignes pour la barre de progression
total_etablissements = sum(1 for _ in open(input_file, 'r', encoding='utf-8')) - 1

# Initialiser le lecteur CSV
reader = pd.read_csv(
    input_file, 
    sep=",", 
    dtype=dtypes_etablissements,
    chunksize=chunksize
)

# Écrire le header du fichier de sortie
first_chunk = True

# Traitement avec barre de progression
with tqdm(total=total_etablissements, desc="Fusion des données") as pbar:
    for chunk in reader:
        # Ajouter la colonne avec le nom de l'entreprise
        chunk['nom_entreprise'] = chunk['siren'].map(unite_legale_dict).fillna('')
        
        # Réorganiser les colonnes pour avoir le nom d'entreprise en premier
        cols = chunk.columns.tolist()
        cols = cols[-1:] + cols[:-1]  # Déplacer la nouvelle colonne en première position
        chunk = chunk[cols]
        
        # Écrire dans le fichier de sortie
        chunk.to_csv(
            output_file,
            mode='a',
            header=first_chunk,
            index=False
        )
        if first_chunk:
            first_chunk = False
        
        # Mettre à jour la barre de progression
        pbar.update(len(chunk))

print("\nTraitement terminé avec succès!")
print(f"Fichier généré: {output_file}")