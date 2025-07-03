import pandas as pd
from pandas.errors import EmptyDataError
import re
import unicodedata
from tqdm import tqdm
from fuzzywuzzy import fuzz

# --- Utilitaires ---

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

def create_sirene_by_dept(sirene_df: pd.DataFrame) -> dict:
    print("\nCréation de la structure par département...")
    df = sirene_df.copy()
    df['codePostalEtablissement'] = df['codePostalEtablissement'].astype(str)
    df['siren'] = df['siren'].astype(str)
    df['siret'] = df['siret'].astype(str)
    df['dept'] = df['codePostalEtablissement'].str[:2]

    sirene_by_dept = {}
    for dept, group in tqdm(
        df.groupby('dept'),
        desc="Traitement par département",
        unit="dept"
    ):
        sirene_by_dept[dept] = group

    print(f"\n✅ Structure créée avec {len(sirene_by_dept)} départements")
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
    # Remplissage de l'effectif en passant le sous-ensemble des sièges sociaux
    crm_updated[client_index]['effectif'] = fill_effectif(sirene_row, sirene_head_subset)


def match_single_company(
    client_name: str,
    client_city: str,
    sirene_subset: pd.DataFrame,
    threshold: float = 100.0
) -> tuple[pd.Series, float] | None:  # Type de retour modifié
    """
    Essaie de trouver une correspondance dans le DataFrame `sirene_subset` pour `client_name`,
    basé sur un score de similarité prenant en compte nom et ville.
    Retourne la ligne correspondante et le score si trouvé.
    """
    best_match = None
    best_score = -1.0
    total = sirene_subset.shape[0]

    # Normalisation client
    normalized_client_name = normalize_name(client_name)
    normalized_client_city = normalize_name(client_city)

    for _, row in tqdm(
        sirene_subset.iterrows(),
        total=total,
        desc=f"Matching '{client_name}'",
        unit="rec"
    ):
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
        return best_match, best_score  # Retourne maintenant un tuple
    return None


def update_CRM(
    sirene_df: pd.DataFrame,
    crm_db: list[dict],
    output_path: str | None = None
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

    for i, client in enumerate(crm_updated):
        dept = str(client.get('CP', ''))[:2]
        subset = sirene_by_dept.get(dept)
        if subset is None or subset.empty:
            continue

        client_name = client.get('societe', '')
        client_city = client.get('ville', '')

        match_result = match_single_company(
            client_name,
            client_city,
            subset
        )
        if match_result is not None:
            match_row, match_score_value = match_result
            # Passer sirene_head_subset à remplir_ligne_orion
            remplir_ligne_orion(
                crm_updated, 
                i, 
                match_row,
                sirene_head_subset,  # Nouvel argument
                match_row['nom_normalise'],
                match_score_value
            )

    if output_path:
        df_out = pd.DataFrame(crm_updated)
        df_out.to_csv(output_path, index=False)
        print(f"✅ Fichier sauvegardé : {output_path}")

    return crm_updated


# --- Chargement robuste et exécution sur base test ---

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

if __name__ == "__main__":
    crm_test_path = "mini_orion.csv"
    sirene_path = "SIRENE_normalise.csv"

    try:
        crm_df = load_csv(crm_test_path)
        sirene_df = load_csv(sirene_path)
    except Exception as e:
        print(f"Erreur de chargement : {e}")
        exit(1)

    crm_db = crm_df.to_dict(orient="records")

    update_CRM(sirene_df, crm_db, output_path="crm_mis_a_jour.csv")










