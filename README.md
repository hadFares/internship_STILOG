Mode emploi Mise à jour Orion par scypte python


But : 
mise à jour d'Orion à partir des infos de la base de données SIRENE disponible au lien suivant :
https://www.data.gouv.fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/

Cas d'usage :
    Mise à jour ou enrichissement d'Orion avec les données SIRENE parmis lesquelles :

    Données interessantes de Unité Légale :
    - siren
    - denominationUniteLegale : Nom officiel de l'entreprise
    - activitePrincipaleUniteLegale : Code APE/NAF 
    - trancheEffectifsUniteLegale
    - etatAdministratifUniteLegale : Statut ("A" = Actif, "C" = Cessé)

    Données interessantes de Établissement :
    - siret
    - etablissementSiege : true si c'est le siège social false sinon
    - adresseComplete : À reconstituer avec :
    numeroVoieEtablissement + typeVoieEtablissement + libelleVoieEtablissement
    codePostalEtablissement + libelleCommuneEtablissement
    - activitePrincipaleEtablissement : Activité locale si différente du siège.
    - etatAdministratifEtablissement : ("A" = Actif, "F" = Fermé).


prérequis :
- python
- biblioteques : pandas, tqdm, fuzzywuzzy
- fichiers SIRENE téléchargés


Mode d'emploi :

1 - nettoyage - Construire une 1ere version nettoyée de la SIRENE avec seulement les informations 
souhaitées (ex : NAF ou effectif), couplées aux infos nécéssaires pour identifier les entreprises (on peut utiliser le fichier nettoyage).

(2 si nécéssaire) - fusion -  Si les infos nécéssaires pour remplir Orion sont réparties dans plusieurs fichiers
SIRENE différents, on pourra fusionner les fichiers à l'aide du scrypte fusion.py


3 - normalisation - Normaliser tous les nom d'entreprise de la SIRENE à l'aide du scrypte normalisation

4 - update - Une fois la version finale nettoyée de SIRENE obtenue, on peut passer au scrypte final : update, qui rends en sortie Orion mis à jour avec les informations souhaitées.


A noter pendant la modification du code par le futur utilisateur :

- Tous les champs succeptibles d'etre modifiés sont précédés d'un commentaire comprenant 'MODIFIER' -> On peut donc retrouver ces champs avec crtl +f


Temps de chargement :
- nettoyage et normalisation s'exécutent rapidement,
- fusion s'éxécute en 3h environ
- update s'éxécute en environ une heure


Colonnes créées en plus dans le fichier de sortie :
    SIRET, SIREN, effectif, nom_match_sirene (le nom de l'entreprise rattaché à la ligne correspondante de ORION), score_match (score de similarité entre le match trouvé dans SIRENE, et l'entreprise de ORION)
    -> Les colonnes nom_match_sirene, et score_match sont présentes à titre indiquatif, ne sont pas utiles pour ORION


Précision colonne effectif :
    la colonne effectif est remplie avec le code d'effectif de l'INSEE. Il peut etre conerti dans excel en tranches plus larges avec un RECHERCHEV (codes présent dans le dossier).


Performances attendues :
    Environ 25% des entreprises francaises de ORION decraient trouver un match dans SIRENE dans le cas ou l'on utilise pas les SIRET








