import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st

from db.database import init_db
from app.data import load_projects, load_news, summary_stats
from app.tabs import overview, project_table, map_view, news

init_db()  # ensure schema migrations run before any data access

st.set_page_config(
    page_title="Boston CRE Tracker",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
  /* Tighten top padding */
  .block-container { padding-top: 1.5rem; padding-bottom: 1rem; }

  /* KPI metric styling (sidebar / inline) */
  [data-testid="stMetricValue"] { font-size: 1.8rem; font-weight: 700; }
  [data-testid="stMetricLabel"] { font-size: 0.8rem; color: #6b7280; text-transform: uppercase; letter-spacing: 0.05em; }

  /* ── Tab bar ──────────────────────────────────────────────────── */
  /* The tab list container */
  div[data-baseweb="tab-list"] {
    border-bottom: 1px solid #2a2d3e !important;
    gap: 0 !important;
  }

  /* Each tab button — padding lives here as an inline style override */
  button[data-baseweb="tab"],
  button[data-testid="stTab"] {
    padding: 16px 28px !important;
    background: transparent !important;
    border: none !important;
    outline: none !important;
  }

  /* The label text is in a div INSIDE the button (StreamlitMarkdown wrapper) */
  button[data-baseweb="tab"] div,
  button[data-testid="stTab"] div,
  button[data-baseweb="tab"] p,
  button[data-testid="stTab"] p {
    font-size: 16px !important;
    font-weight: 500 !important;
    color: #6b7280 !important;
    letter-spacing: 0.05em !important;
    transition: color 0.15s ease !important;
    margin: 0 !important;
  }

  /* Active tab text — bright white */
  button[data-baseweb="tab"][aria-selected="true"] div,
  button[data-testid="stTab"][aria-selected="true"] div,
  button[data-baseweb="tab"][aria-selected="true"] p,
  button[data-testid="stTab"][aria-selected="true"] p {
    color: #ffffff !important;
    font-weight: 600 !important;
  }

  /* Hover — brighten inactive */
  button[data-baseweb="tab"]:hover div,
  button[data-testid="stTab"]:hover div,
  button[data-baseweb="tab"]:hover p,
  button[data-testid="stTab"]:hover p {
    color: #cbd5e1 !important;
  }

  /* Blue underline on active tab */
  div[data-baseweb="tab-highlight"] {
    background-color: #3b82f6 !important;
    height: 3px !important;
  }

  /* Hide the redundant bottom border */
  div[data-baseweb="tab-border"] { display: none !important; }

  /* Expander */
  .streamlit-expanderHeader { font-weight: 600; }

  /* Table row hover */
  [data-testid="stDataFrame"] tr:hover { background: #f0f9ff; }

  /* Dividers */
  hr { border-color: #2a2d3e; }
</style>
""", unsafe_allow_html=True)


def main():
    # Header
    hc1, hc2 = st.columns([5, 1])
    hc1.markdown("# Boston CRE Development Tracker")
    hc1.caption("Article 80 Large & Small Project Review pipeline · Data sourced from BPDA / SIRE")

    df = load_projects()
    stats = summary_stats(df)

    # Extraction progress banner
    if stats["extracted"] < stats["total"]:
        pct = stats["extracted"] / stats["total"]
        hc2.markdown("**AI Extraction**")
        hc2.progress(pct, text=f"{stats['extracted']}/{stats['total']}")

    tab1, tab2, tab3, tab4 = st.tabs(["Overview", "Projects", "Map", "News"])

    with tab1:
        overview.render(df, stats)

    with tab2:
        project_table.render(df)

    with tab3:
        map_view.render(df)

    with tab4:
        news.render()

    with st.sidebar:
        st.markdown("### Boston CRE Tracker")
        st.divider()
        st.metric("Total Projects", stats["total"])
        st.metric("AI Extracted", f"{stats['extracted']} / {stats['total']}")
        st.progress(stats["extracted"] / max(stats["total"], 1))
        st.divider()
        st.metric("Units in Pipeline", f"{stats['total_units']:,}")
        st.metric("GSF in Pipeline", f"{stats['total_gsf']/1e6:.1f}M ft²")
        st.divider()
        news_df = load_news(500)
        st.metric("News Articles", len(news_df))
        linked = int(news_df["linked_project_id"].notna().sum()) if not news_df.empty else 0
        st.metric("News / Project Links", linked)
        st.divider()
        if st.button("Refresh Data", use_container_width=True):
            load_projects.clear()
            load_news.clear()
            st.rerun()
        st.caption("Cache TTL: 5 min · Run scrapers manually to fetch new data")


if __name__ == "__main__":
    main()
