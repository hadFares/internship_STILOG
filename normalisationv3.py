import pandas as pd
from tqdm import tqdm
import re
import unicodedata
import os

# Configuration des fichiers
input_file = "Donnees_Filtrees_Completes_avec_noms.csv"
output_file = "SIRENE_normalise.csv"
chunksize = 50000  # Taille des chunks

# Définition des types de données pour TOUTES les colonnes (toutes en string)
dtypes = {
    'nom_entreprise': 'string',
    'siret': 'string',
    'siren': 'string',
    'denominationUsuelleEtablissement': 'string',
    'codePostalEtablissement': 'string',
    'libelleCommuneEtablissement': 'string',
    'trancheEffectifsEtablissement': 'string',
    'etatAdministratifEtablissement': 'string',
    'caractereEmployeurEtablissement': 'string',
    'etablissementSiege': 'string'
}

# Pré-compilation des regex pour la performance
LEGAL_FORMS = re.compile(r"\b(sarl|sa|sas|sasu|eurl|ei|sci|eurl|sas|sarl|sas|eurl|sarl|snc|sc|sas|scop|sarl)\b", re.IGNORECASE)
MULTISPACE = re.compile(r"\s+")
PUNCTUATION = re.compile(r"[^\w\s-]")

def normalize_company_name(name: str) -> str:
    """Normalise un nom d'entreprise en 5 étapes rapides"""
    # Gestion explicite des valeurs manquantes et NaN
    if pd.isna(name):
        return ""
    
    name_str = str(name)
    if not name_str:
        return ""
    
    # 1. Minuscules et suppression des accents
    normalized = unicodedata.normalize('NFKD', name_str.lower())
    normalized = normalized.encode('ascii', 'ignore').decode('ascii')
    
    # 2. Suppression de la ponctuation (conserve les traits d'union)
    normalized = PUNCTUATION.sub('', normalized)
    
    # 3. Suppression des formes juridiques
    normalized = LEGAL_FORMS.sub('', normalized)
    
    # 4. Réduction des espaces multiples
    normalized = MULTISPACE.sub(' ', normalized).strip()
    
    return normalized

# --- ADD THIS SECTION TO DELETE THE OUTPUT FILE IF IT EXISTS ---
if os.path.exists(output_file):
    print(f"Suppression du fichier existant: {output_file}")
    os.remove(output_file)
# -------------------------------------------------------------

# Compter le nombre total de lignes pour la barre de progression
print("Calcul du nombre total de lignes...")
with open(input_file, 'r', encoding='utf-8') as f:
    total_lines = sum(1 for line in f) - 1  # Soustraction du header

print(f"Total de lignes à traiter: {total_lines:,}")

# Initialiser le lecteur CSV avec types spécifiés
reader = pd.read_csv(
    input_file, 
    sep=",", 
    dtype=dtypes,
    chunksize=chunksize
)

# Traitement avec barre de progression
first_chunk = True
total_removed = 0  # Compteur des lignes supprimées
processed_lines = 0  # Compteur des lignes traitées

with tqdm(total=total_lines, desc="Normalisation des noms") as pbar:
    for chunk in reader:
        # NOTE : La boucle de conversion explicite a été supprimée.
        # La lecture avec dtype=dtypes est suffisante et plus robuste.
        
        # Supprimer les lignes avec "[ND]"
        original_count = len(chunk)
        # Assurez-vous que la colonne est bien en string pour la comparaison
        chunk = chunk[chunk['nom_entreprise'].astype(str) != '[ND]']
        removed_count = original_count - len(chunk)
        total_removed += removed_count
        
        # Appliquer la normalisation
        chunk['nom_normalise'] = chunk['nom_entreprise'].apply(normalize_company_name).astype('string')
        
        # Réorganiser les colonnes
        cols = chunk.columns.tolist()
        if 'nom_entreprise' in cols and 'nom_normalise' in cols:
            cols.insert(1, cols.pop(cols.index('nom_normalise')))
        chunk = chunk[cols]

        # Écrire dans le fichier de sortie
        chunk.to_csv(
            output_file,
            mode='a',
            header=first_chunk,
            index=False,
            encoding='utf-8'
        )
        if first_chunk:
            first_chunk = False
        
        # Mettre à jour la barre de progression
        processed_lines += original_count
        pbar.update(original_count)

print("\nTraitement terminé avec succès!")
print(f"Fichier généré: {output_file}")
print(f"Lignes traitées: {processed_lines:,}")
print(f"Lignes supprimées (valeurs [ND]): {total_removed:,}")
print(f"Lignes conservées: {processed_lines - total_removed:,}")
print(f"Taille du fichier: {os.path.getsize(output_file)/1024**2:.2f} MB")

# Après traitement
df = pd.read_csv(output_file, dtype={'siren': 'string'}, nrows=1000)

# Vérifier la longueur des SIREN
siren_lengths = df['siren'].str.len()
print(f"SIREN avec longueur incorrecte: {(siren_lengths != 9).sum()}")
print("Exemples:")
print(df[df['siren'].str.len() != 9][['siren', 'nom_entreprise']].head())

# Vérifier les zéros initiaux
print("\nSIREN commençant par zéro:")
print(df[df['siren'].str.startswith('0')][['siren', 'nom_entreprise']].head())