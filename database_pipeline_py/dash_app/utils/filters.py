import streamlit as st
from database_pipeline_py.dash_app.utils.db import get_filter_options


def render_sidebar() -> dict:
  
    opts = get_filter_options()

    st.sidebar.title("🔍 Filtres")
    st.sidebar.markdown("---")

    # ── Agence ───────────────────────────────────────────────────
    agences = st.sidebar.multiselect(
        label   = "Agence",
        options = opts["agences"],
        default = opts["agences"],
        key     = "f_agences",      
    )

    # ── Segment client ────────────────────────────────────────────
    
    segments = st.sidebar.multiselect(
        label   = "Segment client",
        options = opts["segments"],
        default = opts["segments"],
        key     = "f_segments",
    )

    # ── Produit ───────────────────────────────────────────────────
    produits = st.sidebar.multiselect(
        label   = "Produit",
        options = opts["produits"],
        default = opts["produits"],
        key     = "f_produits",
    )

   
    year_min, year_max = st.sidebar.select_slider(
        label   = "Période (année)",
        options = sorted(opts["years"]),
        value   = (min(opts["years"]), max(opts["years"])),
        key     = "f_years",
    )

    st.sidebar.markdown("---")
    st.sidebar.caption("FinanceCore SA · Dashboard v1.0")

    # ── Empty-selection guard ─────────────────────────────────────
    return {
        "agences":  agences  or opts["agences"],
        "produits": produits or opts["produits"],
        "segments": segments or opts["segments"],
        "year_min": year_min,
        "year_max": year_max,
    }