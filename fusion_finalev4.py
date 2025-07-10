import pandas as pd
from pandas.errors import EmptyDataError, ParserError
import sys
import os
from tqdm import tqdm
import numpy as np

def load_csv(path: str) -> pd.DataFrame:
    """Charge un CSV avec détection automatique du séparateur et de l'encodage"""
    encodings = ['latin1', 'utf-8', 'iso-8859-15', 'cp1252']
    separators = [',', ';', '\t']
    
    for encoding in encodings:
        for sep in separators:
            try:
                df = pd.read_csv(path, sep=sep, dtype=str, encoding=encoding)
                if not df.empty:
                    print(f"Chargé '{path}' avec séparateur '{sep}' et encoding '{encoding}'")
                    return df
            except (UnicodeDecodeError, pd.errors.ParserError):
                continue
    raise ValueError(f"Impossible de charger le fichier: {path}")

def load_orion(path: str) -> pd.DataFrame:
    """
    Charge un fichier Orion qui peut être au format CSV ou Excel.
    - Pour un CSV, on utilise le séparateur ';' et l'encodage 'latin1'.
    - Pour un fichier Excel (.xls/.xlsx), on lit la première feuille.
    """
    _, ext = os.path.splitext(path)
    ext = ext.lower()
    
    try:
        if ext in ('.xls', '.xlsx'):
            df = pd.read_excel(path, sheet_name=0, dtype=str)
            print(f"Fichier Excel '{path}' chargé (sheet 0).")
        else:
            df = pd.read_csv(path, sep=';', dtype=str, encoding='latin1')
            print(f"Fichier CSV '{path}' chargé avec séparateur ';' et encoding 'latin1'.")
        
        if df.empty:
            raise EmptyDataError("Le fichier est vide ou ne contient pas de colonnes.")
        return df

    except (EmptyDataError, ParserError, UnicodeDecodeError) as e:
        raise ValueError(f"Erreur lors du chargement du fichier '{path}' : {e}")

def normalize_uid(uid_val):
    """Normalise l'UID pour une comparaison robuste"""
    if pd.isna(uid_val) or uid_val in ["", "nan", "None"]:
        return None
    s = str(uid_val).strip()
    # Gestion des UID numériques (ex: 1234.0 -> '1234')
    if '.' in s:
        s = s.rstrip('0').rstrip('.')
    return s

def fusion(orion: pd.DataFrame, crm_update: pd.DataFrame) -> pd.DataFrame:
    """
    Fusionne les données Orion avec les informations du CRM mis à jour
    Ajoute 3 colonnes : SIRET, effectif_auto et score_match
    """
    # Créer une copie de sortie avec typage explicite
    output = orion.copy()
    output['SIRET'] = pd.Series(dtype='str')
    output['effectif_auto'] = pd.Series(dtype='str')
    output['score_match'] = 0.0  # Initialisation comme float
    
    # Vérifier la présence des colonnes nécessaires
    required_columns = ['Identifiant interne (UID)', 'score_match', 'SIRET', 'effectif']
    for col in required_columns:
        if col not in crm_update.columns:
            raise ValueError(f"Colonne manquante dans CRM update: {col}")
        if col not in output.columns and col != 'effectif':
            raise ValueError(f"Colonne manquante dans Orion: {col}")

    # Conversion numérique des scores
    crm_update['score_match'] = pd.to_numeric(crm_update['score_match'], errors='coerce')
    
    # Création du dictionnaire de correspondances
    valid_matches = {}
    count_added = 0
    
    # Trier par score décroissant pour garder les meilleures correspondances
    crm_sorted = crm_update.sort_values(by='score_match', ascending=False)
    
    for i, row in tqdm(crm_sorted.iterrows(), total=len(crm_sorted), desc="Indexation CRM"):
        try:
            uid = normalize_uid(row['Identifiant interne (UID)'])
            score_val = row['score_match']
            
            # Vérifier que l'UID n'est pas vide et que le score est valide
            if uid and pd.notna(score_val) and score_val >= 130:
                # Si UID déjà présent, ne garder que le meilleur score
                if uid not in valid_matches:
                    valid_matches[uid] = {
                        'SIRET': str(row['SIRET']),
                        'effectif': str(row['effectif']),
                        'score_match': score_val
                    }
                    count_added += 1
        except Exception as e:
            print(f"Erreur lors du traitement de la ligne {i}: {e}")
            continue

    print(f"Correspondances valides trouvées: {count_added}")

    # Création d'un set des UID Orion normalisés
    uids_in_orion = set()
    for val in output['Identifiant interne (UID)']:
        normalized = normalize_uid(val)
        if normalized:
            uids_in_orion.add(normalized)
    
    # Pré-calcul des UID normalisés pour Orion
    output_uid_normalized = output['Identifiant interne (UID)'].apply(normalize_uid)
    
    # Mise à jour des données
    count_updated = 0
    for uid, match_data in tqdm(valid_matches.items(), total=len(valid_matches), desc="Application des mises à jour"):
        if uid in uids_in_orion:
            # Trouver tous les index avec cet UID (version normalisée)
            indices = output.index[output_uid_normalized == uid].tolist()
            
            for idx in indices:
                output.at[idx, 'SIRET'] = match_data['SIRET']
                output.at[idx, 'effectif_auto'] = match_data['effectif']
                output.at[idx, 'score_match'] = match_data['score_match']
                count_updated += 1

    print(f"Entreprises mises à jour: {count_updated}")
    
    # Vérification immédiate
    updated_count = (output['score_match'] >= 130).sum()
    print(f"Vérification: {updated_count} lignes avec score >= 130 dans le DataFrame")
    
    # DEBUG: Afficher les premières lignes mises à jour
    if count_updated > 0:
        updated_rows = output[output['score_match'] >= 130].head()
        print("\nExemple de lignes mises à jour:")
        print(updated_rows[['Identifiant interne (UID)', 'SIRET', 'effectif_auto', 'score_match']])
    
    return output

def main():
    output_file = 'orion_final.csv'
    
    # Supprimer l'ancien fichier de sortie s'il existe
    if os.path.exists(output_file):
        try:
            os.remove(output_file)
            print(f"Ancien fichier {output_file} supprimé.")
        except Exception as e:
            print(f"⚠️ Impossible de supprimer {output_file} : {e}")

    try:
        print("\nChargement du fichier Orion...")
        orion = load_orion('copie_orion.xlsx')
        print(f"Colonnes Orion: {orion.columns.tolist()}")
        print("\nChargement du fichier CRM mis à jour...")
        crm_update = load_csv('crm_mis_a_jour_final.csv')
        print(f"Colonnes CRM: {crm_update.columns.tolist()}")
        
        # DEBUG: Afficher des exemples d'UID
        print("\nExemple UID Orion:", orion['Identifiant interne (UID)'].iloc[0])
        print("Exemple UID CRM:", crm_update['Identifiant interne (UID)'].iloc[0])
    except Exception as e:
        print(f"\n❌ Erreur de chargement : {e}")
        sys.exit(1)

    try:
        print("\nDébut de la fusion des données...")
        orion_updated = fusion(orion, crm_update)
    except Exception as e:
        print(f"\n❌ Erreur lors de la fusion : {e}")
        sys.exit(1)

    try:
        print("\nSauvegarde du résultat...")
        orion_updated.to_csv(output_file, sep=';', index=False, encoding='utf-8-sig')
        print("\n✅ Fusion terminée avec succès!")
        print(f"Fichier généré : {output_file}")
        print(f"Entreprises totales : {len(orion_updated)}")
        
        # Vérification finale
        if os.path.exists(output_file):
            result_df = pd.read_csv(output_file, sep=';', nrows=5)
            print("\nAperçu du fichier généré:")
            print(result_df)
        
        # Statistiques des correspondances
        updated = orion_updated[orion_updated['score_match'] >= 130]
        print(f"\nCorrespondances appliquées : {len(updated)}")
        print(f"Taux de mise à jour : {len(updated)/len(orion_updated):.1%}")
    except Exception as e:
        print(f"\n❌ Erreur lors de la sauvegarde : {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()