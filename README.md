# 📊 FinanceCore Dashboard
Dashboard interactif de visualisation des données financières avec Streamlit + PostgreSQL.

## 🚀 Lancer le projet

### 1. Installer les dépendances
pip install streamlit pandas sqlalchemy psycopg2-binary python-dotenv plotly

### 2. Créer le fichier .env
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=financecore_db

### 3. Lancer l'application
streamlit run app.py

### Fonctionnalités
    ## KPIs
Total transactions
Total montant (€)
Moyenne transaction
Nombre d’anomalies

### Graphiques
 Évolution mensuelle du montant
 Top comptes
 Types d’opérations
 Répartition des anomalies

### ⚠️ Important
Vérifier que PostgreSQL est lancé
Vérifier que la table transaction existe
Vérifier le fichier .env