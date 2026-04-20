
Erreurs Critiques

1. Erreur de parsing de date (Crash lors du chargement CSV)

Erreur :
ValueError: time data "2022-04-18 20:4108" doesn't match format "%Y-%m-%d %H:%M:%S"

Problème :
Format corrompu :
20:4108 (heure invalide)

Résultat :
Le pipeline s’arrête immédiatement (CRASH)

Solution :
df['date_transaction'] = pd.to_datetime(
    df['date_transaction'],
    errors='coerce'
)

Apres la modification :

Correction supplémentaire :
df['date_transaction'] = df['date_transaction'].str.replace(
    r'(\d{2}):(\d{4})$',
    r'\1:\2',
    regex=True
)


2. Colonnes manquantes dans le DataFrame

Erreur :
KeyError: columns ['client_nom', 'client_prenom', ...] are not in dataframe

Pourquoi :
- Structure du CSV différente du schéma attendu
- Le preprocessing a modifié les noms des colonnes

Résultat :
Échec du chargement de la dimension client

Solution :
print(df.columns)

Ensuite :
- Renommer les colonnes
- Ou mettre à jour le mapping ETL


3. Conflit de schéma dans une vue PostgreSQL

Erreur :
column c.client_code does not exist

Pourquoi :
- La vue dépend d’une colonne qui n’existe pas encore
- Problème de synchronisation du schéma

Erreur :
cannot change name of view column "client_nom_complet" to "client_code"

Pourquoi :
- PostgreSQL n’autorise pas la modification directe d’une VIEW
- CREATE OR REPLACE VIEW ne résout pas les conflits de colonnes

Résultat :
Le pipeline échoue lors de la création de la vue

Solution :
DROP VIEW IF EXISTS vue_transactions_detail CASCADE;

Ou :
- Mettre en place un versioning des vues


4. Mauvaise colonne de jointure (Erreur logique du schéma)

Erreur :
column t.produit does not exist
HINT: t.produit_id

Pourquoi :
- Incohérence entre les requêtes SQL et le schéma de la base

Résultat :
Échec de création des vues

Solution :
Uniformiser la convention de nommage :
t.produit_id = p.produit_id


5. Dépendance Python manquante

Erreur :
ModuleNotFoundError: No module named 'psycopg'

Pourquoi :
- Driver PostgreSQL manquant

Solution :
pip install psycopg[binary]

Ou :
pip install psycopg2-binary


6. Problèmes structurels (Design Issues)

1. Schema Drift
Changements fréquents :
- ajout de colonnes
- suppression de colonnes
- modification des vues

Résultat :
- vues cassées
- pipeline instable

2. Couplage fort
Entre :
- ETL
- vues SQL
- dashboard Streamlit

Résultat :
Le moindre changement casse le système

3. Absence de stratégie de migration
Manque de :
- Alembic
- migrations versionnées
- système de rollback

4. Problèmes de qualité des données
- timestamps corrompus
- colonnes manquantes
- nommage incohérent

