import pandas as pd
from tqdm import tqdm

# Colonnes nécessaires
colonnes_utiles = [
    "siret",
    "siren",
    "denominationUsuelleEtablissement",
    "codePostalEtablissement",
    "libelleCommuneEtablissement",
    "trancheEffectifsEtablissement",
    "etatAdministratifEtablissement",
    "caractereEmployeurEtablissement",
    "etablissementSiege",
]

# Types de données pour optimisation mémoire
dtypes = {
    "siret": "string",
    "siren": "string",
    "denominationUsuelleEtablissement": "string",
    "codePostalEtablissement": "string",
    "libelleCommuneEtablissement": "string",
    "trancheEffectifsEtablissement": "string",
    "etatAdministratifEtablissement": "category",
    "caractereEmployeurEtablissement": "category",
    "etablissementSiege": "boolean"
}

# Paramètres de traitement
chunksize = 100_000
input_file = "StockEtablissement_utf8.csv"
output_file = "Donnees_Filtrees_Completes.csv"

# Liste des codes d'effectifs valides (>= 3 salariés)
effectifs_valides = ['02', '03', '11', '12', '21', '22', '31', '32', '41', '42', '51', '52', '53']

# Compteurs pour les statistiques globales
total_lignes = 0
lignes_apres_filtres = 0

# Initialisation du lecteur CSV avec barre de progression
total_rows = sum(1 for _ in open(input_file, 'r', encoding='utf-8')) - 1  # compte le nombre de lignes du fichier de base
reader = pd.read_csv(
    input_file,
    sep=",",
    usecols=colonnes_utiles,
    dtype=dtypes,
    chunksize=chunksize
)

# Fichier de sortie - écriture avec header pour le premier chunk
first_chunk = True

# Traitement par chunks avec tqdm
with tqdm(total=total_rows, desc="Traitement des chunks") as pbar:
    for chunk in reader:
        # Mise à jour de la barre de progression
        pbar.update(len(chunk))
        total_lignes += len(chunk)
        
        # 1. Filtre employeur (O)
        chunk = chunk[chunk['caractereEmployeurEtablissement'] == 'O']
        
        # 2. Filtre effectifs (>= 3 salariés ou valeurs spéciales)
        chunk = chunk[
            chunk['trancheEffectifsEtablissement'].isin(effectifs_valides) 
            | chunk['trancheEffectifsEtablissement'].isna() 
            | (chunk['trancheEffectifsEtablissement'] == 'NN')
        ]
        
        # 3. Supprimer les actifs (A) sans effectif connu
        a_supprimer = (
            (chunk['etatAdministratifEtablissement'] == 'A') & 
            (chunk['trancheEffectifsEtablissement'].isna() | (chunk['trancheEffectifsEtablissement'] == 'NN'))
        )
        chunk = chunk[~a_supprimer]
        
        # Écriture dans le fichier de sortie
        if not chunk.empty:
            lignes_apres_filtres += len(chunk)
            chunk.to_csv(
                output_file,
                mode='a',
                header=first_chunk,
                index=False
            )
            if first_chunk:
                first_chunk = False

# Statistiques finales
print(f"\nTraitement terminé !")
print(f"Total de lignes traitées : {total_lignes}")
print(f"Lignes conservées après filtres : {lignes_apres_filtres}")
print(f"Pourcentage conservé : {lignes_apres_filtres/total_lignes:.2%}")
print(f"Fichier de sortie : {output_file}")