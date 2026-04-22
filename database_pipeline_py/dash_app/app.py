import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
#_____________________

import streamlit as st
from sqlalchemy import text
from database_pipeline_py.dash_app.utils.filters import render_sidebar
from database_pipeline_py.dash_app.utils.db import get_engine, get_kpi_global

# ── Must be the FIRST Streamlit call ──────────────────────────────
st.set_page_config(
    page_title          = "FinanceCore Dashboard",
    page_icon           = "🏦",
    layout              = "wide",
    initial_sidebar_state = "expanded",
)

# ── Sidebar (renders on every page via session_state keys) ────────
filters = render_sidebar()

# ── Home page ─────────────────────────────────────────────────────
st.title("🏦 FinanceCore SA — Dashboard Analytique")
st.markdown(
    "Vue d'ensemble du portefeuille. "
    "Utilisez la **barre latérale** pour filtrer, puis naviguez vers une page d'analyse."
)
st.markdown("---")

col_status, col_nav = st.columns([1, 1])

with col_status:
    st.markdown("#### Connexion base de données")
    try:
        with get_engine().connect() as conn:
            row = conn.execute(text("SELECT current_database(), current_user")).fetchone()
            st.success(f"✅ Connecté à **{row[0]}** · utilisateur **{row[1]}**")
    except Exception as e:
        st.error(f"❌ Connexion échouée : {e}")
        st.info("Vérifiez les variables DB_USER / DB_PASSWORD / DB_HOST / DB_NAME dans votre fichier .env")
        st.stop()

with col_nav:
    st.markdown("#### Pages disponibles")
    st.markdown("""
| Page | Contenu |
|---|---|
| 📊 Vue Exécutive | KPIs · évolution mensuelle · CA agence/produit · segments |
| ⚠️ Analyse des Risques | Corrélations · scatter score/montant · top 10 clients à risque |
""")

st.markdown("---")

st.markdown("#### Snapshot global du portefeuille")

try:
    kpi = get_kpi_global()

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Clients actifs",     f"{int(kpi.get('nb_clients_actifs', 0)):,}")
    k2.metric("Transactions",       f"{int(kpi.get('nb_transactions_total', 0)):,}")
    k3.metric("Volume crédits €",   f"{float(kpi.get('volume_credits_eur', 0)):,.0f}")
    k4.metric("Volume débits €",    f"{float(kpi.get('volume_debits_eur', 0)):,.0f}")
    k5.metric("Anomalies",
              f"{int(kpi.get('nb_anomalies', 0))}",
              delta=f"{float(kpi.get('taux_anomalie_pct', 0))}%",
              delta_color="inverse")
    k6.metric("Rejets",             f"{int(kpi.get('nb_rejets', 0))}")

except Exception as e:
    st.warning(
        f"⚠️ Impossible de charger les KPIs globaux : {e}\n\n"
        "Assurez-vous que le pipeline a été exécuté "
        "(DATABASE-PIPELINE-PY/main.py) pour créer les vues analytiques."
    )

st.markdown("---")
st.caption(
    "Données : financecore_clean.csv · "
    "Base : financecore_db (PostgreSQL) · "
    "Pipeline : DATABASE-PIPELINE-PY · "
    "Analyses : ANALYTICS-SQL"
)