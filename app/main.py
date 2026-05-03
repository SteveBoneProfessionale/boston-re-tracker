import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import streamlit.components.v1 as components

from db.database import init_db
from app.data import load_projects, load_news, summary_stats
from app.tabs import overview, project_table, map_view, news

init_db()

st.set_page_config(
    page_title="BOS CRE TERMINAL",
    page_icon="▪",
    layout="wide",
    initial_sidebar_state="collapsed",
)

_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700&family=Inter:wght@400;500;600;700&display=swap');

html, body,
[data-testid="stAppViewContainer"],
[data-testid="stMain"],
.main { background: #0d0f12 !important; }

[data-testid="stAppViewContainer"] > section:first-child > .block-container {
  padding: 0 2rem 2rem !important;
  max-width: 100% !important;
}

#MainMenu, footer,
[data-testid="stToolbar"],
[data-testid="stDecoration"],
[data-testid="stStatusWidget"] { display: none !important; }

/* ── Tab bar ─────────────────────────────────────────── */
div[data-baseweb="tab-list"] {
  background: transparent !important;
  border-bottom: 1px solid #1E2530 !important;
  gap: 0 !important;
  padding-left: 0 !important;
}
button[data-baseweb="tab"] {
  background: transparent !important;
  border: none !important;
  outline: none !important;
  padding: 14px 28px !important;
}
button[data-baseweb="tab"] p,
button[data-baseweb="tab"] div {
  font-family: 'JetBrains Mono', monospace !important;
  font-size: 11px !important;
  font-weight: 600 !important;
  letter-spacing: 0.14em !important;
  color: #8A9BB0 !important;
  margin: 0 !important;
}
button[data-baseweb="tab"]:hover p,
button[data-baseweb="tab"]:hover div { color: #d1d5db !important; }
button[data-baseweb="tab"][aria-selected="true"] p,
button[data-baseweb="tab"][aria-selected="true"] div { color: #ffffff !important; }
div[data-baseweb="tab-highlight"] {
  background: #F5821E !important;
  height: 2px !important;
}
div[data-baseweb="tab-border"] { display: none !important; }

/* ── Inputs & selects ────────────────────────────────── */
[data-testid="stSelectbox"] label,
[data-testid="stTextInput"] label {
  font-family: 'JetBrains Mono', monospace !important;
  font-size: 10px !important;
  font-weight: 700 !important;
  letter-spacing: 0.14em !important;
  text-transform: uppercase !important;
  color: #8A9BB0 !important;
}
[data-testid="stSelectbox"] > div > div,
[data-testid="stTextInput"] > div > div > input {
  background: #141720 !important;
  border: 1px solid #1E2530 !important;
  border-radius: 0 !important;
  color: #e2e8f0 !important;
  font-family: 'JetBrains Mono', monospace !important;
  font-size: 12px !important;
}

/* ── Buttons ─────────────────────────────────────────── */
[data-testid="stButton"] > button,
[data-testid="stDownloadButton"] > button {
  background: transparent !important;
  border: 1px solid #1E2530 !important;
  border-radius: 0 !important;
  color: #8A9BB0 !important;
  font-family: 'JetBrains Mono', monospace !important;
  font-size: 10px !important;
  letter-spacing: 0.12em !important;
  text-transform: uppercase !important;
  padding: 6px 16px !important;
  transition: border-color 0.1s, color 0.1s !important;
}
[data-testid="stButton"] > button:hover,
[data-testid="stDownloadButton"] > button:hover {
  border-color: #F5821E !important;
  color: #F5821E !important;
  background: rgba(245,130,30,0.06) !important;
}

/* ── Link buttons ────────────────────────────────────── */
[data-testid="stLinkButton"] a {
  background: transparent !important;
  border: 1px solid #1E2530 !important;
  border-radius: 0 !important;
  color: #8A9BB0 !important;
  font-family: 'JetBrains Mono', monospace !important;
  font-size: 10px !important;
  letter-spacing: 0.12em !important;
  text-transform: uppercase !important;
  padding: 6px 16px !important;
  text-decoration: none !important;
}
[data-testid="stLinkButton"] a:hover {
  border-color: #F5821E !important;
  color: #F5821E !important;
}

/* ── Dataframe ───────────────────────────────────────── */
[data-testid="stDataFrame"] {
  border: 1px solid #1E2530 !important;
  border-radius: 0 !important;
}

/* ── Expander ────────────────────────────────────────── */
details[data-testid="stExpander"] {
  border: 1px solid #1E2530 !important;
  border-radius: 0 !important;
  background: #0d0f12 !important;
}
details summary {
  font-family: 'JetBrains Mono', monospace !important;
  font-size: 10px !important;
  font-weight: 700 !important;
  letter-spacing: 0.12em !important;
  text-transform: uppercase !important;
  color: #8A9BB0 !important;
}

/* ── Caption / small ─────────────────────────────────── */
[data-testid="stCaptionContainer"] p {
  font-family: 'JetBrains Mono', monospace !important;
  font-size: 11px !important;
  color: #8A9BB0 !important;
}

/* ── Progress bar ────────────────────────────────────── */
[data-testid="stProgressBar"] > div {
  background: #1E2530 !important;
  border-radius: 0 !important;
}
[data-testid="stProgressBar"] > div > div {
  background: #F5821E !important;
  border-radius: 0 !important;
}

/* ── Divider ─────────────────────────────────────────── */
hr { border-color: #1E2530 !important; margin: 12px 0 !important; }

/* ── Info / alert ────────────────────────────────────── */
[data-testid="stInfo"] {
  background: rgba(245,130,30,0.05) !important;
  border: 1px solid rgba(245,130,30,0.2) !important;
  border-radius: 0 !important;
}

/* ── Sidebar ─────────────────────────────────────────── */
[data-testid="stSidebar"] {
  background: #0d0f12 !important;
  border-right: 1px solid #1E2530 !important;
}
[data-testid="stSidebar"] [data-testid="stMetricValue"] {
  font-family: 'JetBrains Mono', monospace !important;
  font-size: 1.4rem !important;
  color: #ffffff !important;
}
[data-testid="stSidebar"] [data-testid="stMetricLabel"] {
  font-family: 'JetBrains Mono', monospace !important;
  font-size: 9px !important;
  letter-spacing: 0.14em !important;
  text-transform: uppercase !important;
  color: #8A9BB0 !important;
}
</style>
"""


def _render_header(stats: dict):
    pct = stats["extracted"] / max(stats["total"], 1) * 100
    html = f"""<!DOCTYPE html><html><head>
<meta charset="utf-8">
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@500;600;700&display=swap');
*{{margin:0;padding:0;box-sizing:border-box}}
body{{
  background:#0d0f12;
  font-family:'JetBrains Mono',monospace;
  padding:15px 0 12px;
  border-bottom:1px solid #1E2530;
  overflow:hidden;
}}
.wrap{{display:flex;align-items:center;justify-content:space-between}}
.logo{{font-size:12px;font-weight:700;letter-spacing:0.22em;color:#fff;text-transform:uppercase}}
.logo em{{color:#F5821E;font-style:normal}}
.right{{display:flex;align-items:center;gap:28px}}
.prog{{display:flex;align-items:center;gap:8px}}
.lbl{{font-size:9px;color:#8A9BB0;letter-spacing:0.14em;text-transform:uppercase}}
.track{{width:90px;height:2px;background:#1E2530}}
.fill{{height:2px;background:#F5821E;width:{pct:.1f}%}}
.pct{{font-size:10px;color:#F5821E;font-weight:700;min-width:28px;letter-spacing:0.04em}}
.clock{{display:flex;align-items:center;gap:10px;font-size:11px}}
.cdate{{color:#8A9BB0}}
.ctime{{color:#e2e8f0;font-weight:600;letter-spacing:0.06em;min-width:80px}}
</style>
</head><body>
<div class="wrap">
  <div class="logo">BOS <em>▪</em> CRE TERMINAL</div>
  <div class="right">
    <div class="prog">
      <div class="lbl">AI EXTRACT</div>
      <div class="track"><div class="fill"></div></div>
      <div class="pct">{pct:.0f}%</div>
      <div class="lbl">{stats['extracted']}/{stats['total']}</div>
    </div>
    <div class="clock">
      <span class="cdate" id="d"></span>
      <span class="ctime" id="t">--:--:--</span>
    </div>
  </div>
</div>
<script>
function tick(){{
  const n=new Date();
  document.getElementById('d').textContent=
    n.toLocaleDateString('en-US',{{month:'short',day:'2-digit',year:'numeric'}}).toUpperCase();
  document.getElementById('t').textContent=
    n.toLocaleTimeString('en-US',{{hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false}});
}}
tick();setInterval(tick,1000);
</script>
</body></html>"""
    components.html(html, height=52)


def main():
    st.markdown(_CSS, unsafe_allow_html=True)

    df = load_projects()
    stats = summary_stats(df)

    _render_header(stats)

    tab1, tab2, tab3, tab4 = st.tabs(["OVERVIEW", "PROJECTS", "MAP", "NEWS"])

    with tab1:
        overview.render(df, stats)
    with tab2:
        project_table.render(df)
    with tab3:
        map_view.render(df)
    with tab4:
        news.render()

    with st.sidebar:
        st.markdown(
            '<p style="font-family:\'JetBrains Mono\',monospace;font-size:10px;'
            'letter-spacing:0.2em;color:#8A9BB0;text-transform:uppercase;margin-bottom:4px">'
            'BOS CRE TERMINAL</p>',
            unsafe_allow_html=True,
        )
        st.divider()
        st.metric("PROJECTS", stats["total"])
        st.metric("AI EXTRACTED", f"{stats['extracted']}/{stats['total']}")
        st.progress(stats["extracted"] / max(stats["total"], 1))
        st.divider()
        st.metric("UNITS", f"{stats['total_units']:,}")
        st.metric("PIPELINE SF", f"{stats['total_gsf']/1e6:.1f}M")
        st.divider()
        news_df = load_news(500)
        st.metric("NEWS ARTICLES", len(news_df))
        linked = int(news_df["linked_project_id"].notna().sum()) if not news_df.empty else 0
        st.metric("NEWS LINKED", linked)
        st.divider()
        if st.button("↺  REFRESH DATA", use_container_width=True):
            load_projects.clear()
            load_news.clear()
            st.rerun()
        st.caption("CACHE TTL: 5 MIN · RUN SCRAPERS LOCALLY")


if __name__ == "__main__":
    main()
