import streamlit as st
import pandas as pd
import plotly.express as px
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# =========================
# CONFIG DB
# =========================
load_dotenv(".env")

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

def run_query(query):
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)

# =========================
# STREAMLIT UI
# =========================
st.set_page_config(page_title="FinanceCore Dashboard", layout="wide")
st.title("📊 FinanceCore Dashboard")

# =========================
# LOAD DATA (ONLY YOUR TABLE)
# =========================
df_tx = run_query("SELECT * FROM transaction")

df_tx["date_transaction"] = pd.to_datetime(df_tx["date_transaction"])

# =========================
# KPIs
# =========================
total_transactions = len(df_tx)
total_amount = df_tx["montant_eur"].sum()
avg_amount = df_tx["montant_eur"].mean()
total_anomalies = df_tx["is_anomalie"].sum()

col1, col2, col3, col4 = st.columns(4)

col1.metric("Transactions", total_transactions)
col2.metric("Total Montant (€)", f"{total_amount:,.2f}")
col3.metric("Moyenne Transaction (€)", f"{avg_amount:,.2f}")
col4.metric("Anomalies", int(total_anomalies))

st.markdown("---")

# =========================
# GRAPH 1: MONTHLY EVOLUTION
# =========================
df_month = df_tx.copy()
df_month["month"] = df_month["date_transaction"].dt.to_period("M").astype(str)

tx_month = df_month.groupby("month")["montant_eur"].sum().reset_index()

fig1 = px.line(
    tx_month,
    x="month",
    y="montant_eur",
    title="📈 Évolution mensuelle du montant",
    markers=True
)

# =========================
# GRAPH 2: TOP COMPTES
# =========================
top_compte = df_tx.groupby("compte_id")["montant_eur"].sum().reset_index()
top_compte = top_compte.sort_values("montant_eur", ascending=False).head(10)

fig2 = px.bar(
    top_compte,
    x="compte_id",
    y="montant_eur",
    title="🏦 Top Comptes (Montant total)",
    color="montant_eur"
)

# =========================
# GRAPH 3: TYPE OPERATION
# =========================
fig3 = px.pie(
    df_tx,
    names="type_operation",
    title="📊 Répartition des opérations"
)

# =========================
# GRAPH 4: STATUT / ANOMALIES
# =========================
fig4 = px.histogram(
    df_tx,
    x="is_anomalie",
    title="⚠️ Distribution des anomalies"
)

# =========================
# SHOW GRAPHS
# =========================
col1, col2 = st.columns(2)

with col1:
    st.plotly_chart(fig1, use_container_width=True)
    st.plotly_chart(fig3, use_container_width=True)

with col2:
    st.plotly_chart(fig2, use_container_width=True)
    st.plotly_chart(fig4, use_container_width=True)

# =========================
# RAW DATA
# =========================
with st.expander("📄 Données brutes"):
    st.dataframe(df_tx.head(50))