import pandas as pd
from pandas.errors import EmptyDataError
from pandas.errors import ParserError
import re
import os
import unicodedata
from tqdm import tqdm
from fuzzywuzzy import fuzz


"""
MISE À JOUR ORION AVEC SIRENE
------------------------------
Ce script permet de faire correspondre et enrichir ORION avec les données SIRENE :
- Pour chaque enregistrement client, il complète les champs SIRET, SIREN et effectif (potentiellement d'autres infos au choix).
- Il réalise une normalisation des noms et villes, un score de similarité flou pour l'appariement.

Personnalisation :
- Modifier `crm_test_path` pour le chemin du fichier ORION source.
- Modifier `sirene_path` pour le chemin du fichier SIRENE normalisé.
- Modifier la fonction remplir_ligne_orion selon les besoins.
- Décommenter la partie de matching avec les SIRET  si elles sont présentes dans ORION (Dans la fonction update_CRM).


À noter :
- Les seuils de matching (score à partir duquel on considère le match comme sûr) sont modifiables,
  mais une valeur de 130 garantit de ne pas commettre d'erreurs sur l'entreprise
  (une erreur sur l'établissement reste possible)

"""


# --- Utilitaires ---

def load_csv(path: str) -> pd.DataFrame:
    """
    Tente de charger un CSV avec plusieurs séparateurs possibles.
    Lance une erreur si le fichier est vide ou mal formaté.
    """
    for sep in [',', ';', '\t']:
        try:
            df = pd.read_csv(path, sep=sep, dtype=str)
            if df.empty or df.columns.size == 0:
                raise EmptyDataError
            print(f"Chargé '{path}' avec séparateur '{sep}'.")
            return df
        except EmptyDataError:
            continue
    raise ValueError(f"Impossible de lire des colonnes dans le fichier : {path}")


def normalize_name(name: str) -> str:
    """
    Normalise un champ texte pour comparaison :
    - minuscules, suppression d'accents
    - suppression du contenu entre parenthèses
    - suppression de formes juridiques
    - suppression des mots « cedex » (toutes variantes)
    - collapse des espaces multiples
    """
    if pd.isna(name) or not str(name).strip():
        return ""

    # Passage en minuscules, décomposition puis suppression des accents
    s = unicodedata.normalize('NFKD', name.lower())
    s = s.encode('ascii', 'ignore').decode('ascii')

    # On enlève tout ce qui est entre parenthèses
    s = re.sub(r'\([^)]*\)', ' ', s)

    # On retire "cedex" en toute casse
    s = re.sub(r'\bcedex\b', ' ', s)

    # Suppression de la ponctuation (sauf tiret) et des formes juridiques
    s = re.sub(r"[^\w\s-]", " ", s)
    s = re.sub(r"\b(sarl|sa|sas|sasu|eurl|ei|sci|scop|snc|sc|sasf)\b", " ", s)

    # Collapse espaces multiples
    s = re.sub(r'\s+', ' ', s).strip()

    return s


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


def extract_acronym(name: str) -> str:
    safe_name = str(name) 
    return ''.join([word[0] for word in safe_name.split() if word])


def fill_effectif(sirene_record: pd.Series, sirene_head_subset: pd.DataFrame) -> str:
    """
    Détermine le champ 'effectif'.
    - Pour un établissement actif, renvoie sa tranche.
    - Si fermé et siège social, renvoie 'RADIEE'.
    - Si fermé et non-siège social, récupère l'effectif du siège social.
      Si introuvable, 'A PRECISER'.
    """
    etat = sirene_record.get('etatAdministratifEtablissement')
    effectif = sirene_record.get('trancheEffectifsEtablissement')
    est_siege = sirene_record.get('etablissementSiege', False)
    siren_code = sirene_record.get('siren')

    # Cas actif
    if etat == 'A':
        return effectif if effectif else 'A PRECISER'

    # Cas fermé
    if etat == 'F':
        if est_siege:
            return 'RADIEE'
        # Récupérer le siège social pour cette entreprise
        siège = find_headquarters(sirene_head_subset, siren_code)
        if siège is not None:
            # On appelle récursivement pour traiter selon l'état du siège
            return fill_effectif(siège, sirene_head_subset)
        return 'A PRECISER'

    # Par défaut
    return 'A PRECISER'


def match_score(client_norm: str, siren_norm: str, client_city: str = "", siren_city: str = "") -> float:
    client_norm = str(client_norm)
    siren_norm = str(siren_norm)
    client_city = str(client_city).strip().lower()
    siren_city = str(siren_city).strip().lower()

    # Similitude brute (pondération forte)
    ratio_token = fuzz.token_sort_ratio(client_norm, siren_norm)

    # Sous-chaîne exacte (pondération moyenne)
    substring_bonus = 20 if client_norm in siren_norm or siren_norm in client_norm else 0

    # Acronymes (pondération conditionnelle)
    client_acro = extract_acronym(client_norm)
    siren_acro = extract_acronym(siren_norm)

    acronym_bonus = 0
    if client_acro and siren_acro:
        if client_acro == siren_acro:
            acronym_bonus = 20
        elif client_acro in siren_norm.replace(" ", ""):
            acronym_bonus = 15

    # Bonus si même ville
    if client_city and siren_city and client_city == siren_city :
        city_bonus = 10
    else :
        city_bonus = -10

    total_score = ratio_token + substring_bonus + acronym_bonus + city_bonus
    return total_score


# --- Recherche du siège social ---

def find_headquarters(
    sirene_head_subset: pd.DataFrame,
    siren_code: str
) -> pd.Series | None:
    """
    Trouve la ligne correspondant au siège social d'une entreprise donnée.
    Args:
        sirene_head_subset: DataFrame ne contenant que les établissements siège social.
        siren_code: Code SIREN de l'entreprise recherchée.
    Returns:
        La Series du siège social si trouvée, sinon None.
    """
    # Filtrer par code SIREN
    mask = sirene_head_subset['siren'].astype(str) == str(siren_code)
    result = sirene_head_subset.loc[mask]
    if not result.empty:
        # Renvoie la première occurrence
        return result.iloc[0]
    return None

# --- Traitement SIRENE ---

def create_sirene_by_dept(sirene_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Trie les établissements actifs par code département.
    Ne garde que ceux dont 'etatAdministratifEtablissement' == 'A'.
    """
    df = sirene_df[
        sirene_df['etatAdministratifEtablissement'] == 'A'
    ].copy()
    df['codePostalEtablissement'] = df['codePostalEtablissement'].astype(str)
    df['siren'] = df['siren'].astype(str)
    df['siret'] = df['siret'].astype(str)
    df['dept'] = df['codePostalEtablissement'].str[:2]

    sirene_by_dept: dict[str, pd.DataFrame] = {}
    for dept, group in tqdm(
        df.groupby('dept'),
        desc="Traitement par département (actifs)",
        unit="dept"
    ):
        sirene_by_dept[dept] = group

    return sirene_by_dept

# --- Mise à jour du CRM ---


def remplir_ligne_orion(
    crm_updated: list[dict],
    client_index: int,
    sirene_row: pd.Series,
    sirene_head_subset: pd.DataFrame,
    matched_name: str,  # Nouveau paramètre
    match_score: float  # Nouveau paramètre 
) -> None:
    """
    Met à jour les champs SIRET, SIREN et effectif d'un client,
    en tenant compte du siège social pour l'effectif.
    """
    crm_updated[client_index]['SIRET'] = sirene_row['siret']
    crm_updated[client_index]['SIREN'] = sirene_row['siren']
    crm_updated[client_index]['nom_match_sirene'] = matched_name
    crm_updated[client_index]['score_match'] = match_score
    # MODIFIER pour potentiellement ajouter d'autres infos ( ex : NAF)

    # Utilisation de fill_effectif pour renseigner l'effectif
    crm_updated[client_index]['effectif'] = fill_effectif(sirene_row, sirene_head_subset)


def match_single_company(
    client_name: str,
    client_city: str,
    sirene_subset: pd.DataFrame,
    threshold: float = 100.0
) -> tuple[pd.Series, float] | None:
    """
    Essaie de trouver une correspondance dans le DataFrame `sirene_subset` pour `client_name`,
    basé sur un score de similarité prenant en compte nom et ville.
    Retourne la ligne correspondante et le score si trouvé.
    """
    best_match = None
    best_score = -1.0

    # Normalisation client
    normalized_client_name = normalize_name(client_name)
    normalized_client_city = normalize_name(client_city)

    for _, row in sirene_subset.iterrows():
        siren_name = row.get('nom_normalise', '')
        siren_city = row.get('libelleCommuneEtablissement', '')
        normalized_siren_city = normalize_name(siren_city)

        score = match_score(
            normalized_client_name,
            siren_name,
            normalized_client_city,
            normalized_siren_city
        )
        if score > best_score:
            best_score, best_match = score, row

    if best_score >= threshold:
        return best_match, best_score
    return None


def update_CRM(
    sirene_df: pd.DataFrame,
    crm_db: list[dict],
    output_path: str | None = None,
    save_interval: int = 100  # Nouveau paramètre pour l'intervalle de sauvegarde
) -> list[dict]:
    # Créer le sous-ensemble des sièges sociaux
    sirene_head_subset = sirene_df[sirene_df['etablissementSiege'] == True].copy()
    
    sirene_by_dept = create_sirene_by_dept(sirene_df)

    crm_updated = []
    for rec in crm_db:
        new_rec = rec.copy()
        new_rec.setdefault('SIRET', '')
        new_rec.setdefault('SIREN', '')
        new_rec.setdefault('effectif', '')
        new_rec.setdefault('nom_match_sirene', '')
        new_rec.setdefault('score_match', 0.0)
        crm_updated.append(new_rec)

    total_clients = len(crm_updated)
    # Sauvegarde initiale (écrase le fichier existant)
    if output_path:
        pd.DataFrame(crm_updated).to_csv(output_path, index=False)
        print(f"✅ Sauvegarde initiale ({len(crm_updated)} clients)")

    # Boucle sur tous les clients de ORION
    for i, client in tqdm(
        enumerate(crm_updated),
        total=len(crm_updated),
        desc="Mise à jour CRM",
        unit="client"
    ):
        # Vérification du pays
        if client.get('Pays', '').strip().upper() != 'FRANCE':
            continue


        # Passage à décommenter si les SIRET sont présentes
        #----------------------------------------------------

        # if (client.get('Siret', '')) :

        #     siret_client = client.get('SIRET', '').strip()
        #     sirene_match = sirene_df[sirene_df['siret'] == siret_client]
        #     matched_row = sirene_match.iloc[0]
        #     # Appel de remplir_ligne_orion avec le match direct
        #     remplir_ligne_orion(
        #         crm_updated,
        #         i,
        #         matched_row,
        #         sirene_head_subset,
        #         matched_row['nom_normalise'],
        #         150.0  # score max pour indiquer match certain
        #     )
        #     continue

        #-----------------------------------------------------
            

        dept = str(client.get('CP', ''))[:2]
        subset = sirene_by_dept.get(dept)
        if subset is None or subset.empty:
            continue

        client_name = client.get('Société', '')
        client_city = client.get('Ville', '')

        match_result = match_single_company(
            client_name,
            client_city,
            subset
        )
        if match_result is not None:
            match_row, match_score_value = match_result
            remplir_ligne_orion(
                crm_updated, 
                i, 
                match_row,
                sirene_head_subset,
                match_row['nom_normalise'],
                match_score_value
            )

        # Sauvegarde périodique (écrase le fichier précédent)
        if output_path and (i+1) % save_interval == 0:
            pd.DataFrame(crm_updated).to_csv(output_path, index=False)
            print(f"✅ Sauvegarde intermédiaire après {i+1}/{total_clients} clients")

    # Sauvegarde finale
    if output_path:
        pd.DataFrame(crm_updated).to_csv(output_path, index=False)
        print(f"✅ Sauvegarde finale ({total_clients} clients)")

    return crm_updated


if __name__ == "__main__":
    crm_test_path = "copie_orion_france.csv"
    sirene_path = "SIRENE_normalise.csv"

    try:
        crm_df = load_orion(crm_test_path)
        sirene_df = load_csv(sirene_path)
        sirene_df = sirene_df[sirene_df['etatAdministratifEtablissement'] == 'A']

    except Exception as e:
        print(f"Erreur de chargement : {e}")
        exit(1)

    crm_db = crm_df.to_dict(orient="records")

    update_CRM(sirene_df, crm_db, output_path="crm_mis_a_jour.csv")

