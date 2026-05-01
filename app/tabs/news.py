import json
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from app.data import load_news, backfill_topics

SOURCE_LABELS = {
    "boston_gov": "Boston.gov",
    "banker_tradesman": "Banker & Tradesman",
    "the_real_deal": "The Real Deal",
    "curbed": "Curbed",
    "boston_com": "Boston.com",
    "boston_re_times": "Boston Re Times",
    "bisnow_boston": "Bisnow Boston",
}

SOURCE_COLORS = {
    "boston_gov": "#3b82f6",
    "banker_tradesman": "#f59e0b",
    "the_real_deal": "#ef4444",
    "curbed": "#8b5cf6",
    "boston_com": "#10b981",
    "boston_re_times": "#06b6d4",
    "bisnow_boston": "#ea580c",
}

TOPICS = [
    "Events", "Architecture", "Construction", "Development",
    "Engineering", "Financing", "Investments", "Leasing", "Retail",
]


def render():
    if not st.session_state.get("_topics_backfilled"):
        backfill_topics()
        st.session_state["_topics_backfilled"] = True
        st.cache_data.clear()

    st.markdown("## Boston Real Estate News")
    st.caption(
        "Aggregated from Banker & Tradesman, The Real Deal, Curbed, Boston.gov, and Boston.com"
        " — articles auto-linked to tracked projects"
    )

    df = load_news(500)
    if df.empty:
        st.info("No news articles yet. Run `python scraper/news_fetcher.py` to populate.")
        return

    articles = []
    for _, row in df.iterrows():
        pub = ""
        if pd.notna(row.get("published_date")) and row["published_date"]:
            try:
                pub = pd.to_datetime(row["published_date"]).strftime("%Y-%m-%d")
            except Exception:
                pass
        articles.append({
            "title": str(row.get("title") or ""),
            "url": str(row.get("url") or "#"),
            "source": str(row.get("source") or ""),
            "date": pub,
            "summary": str(row.get("summary") or "")[:400],
            "project": str(row.get("linked_project_name") or ""),
            "linked": bool(row.get("linked_project_id")),
            "topics": str(row.get("topics") or ""),
        })

    articles_json = json.dumps(articles, ensure_ascii=False)
    source_labels_json = json.dumps(SOURCE_LABELS)
    source_colors_json = json.dumps(SOURCE_COLORS)
    topics_json = json.dumps(TOPICS)

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{
  background:#0e1117;
  color:#f1f5f9;
  font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
  height:100vh;
  overflow:hidden;
  display:flex;
  flex-direction:column;
  padding:2px 2px 4px 2px;
}}
#app{{display:flex;flex-direction:column;height:100%;overflow:hidden}}

/* ── Filter bar ── */
#filter-bar-wrap{{
  background:#161827;
  border:1px solid #2a2d3e;
  border-radius:10px;
  padding:14px 16px 16px;
  margin-bottom:10px;
  flex-shrink:0;
  position:relative;
  z-index:100;
}}
.filter-row{{display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end}}
.filter-group{{display:flex;flex-direction:column;gap:5px}}
.filter-label{{
  font-size:0.62rem;font-weight:700;color:#64748b;
  text-transform:uppercase;letter-spacing:0.09em;
}}
.filter-btn{{
  background:#1c1f2e;
  border:1px solid #2a2d3e;
  border-radius:6px;
  color:#cbd5e1;
  padding:8px 10px;
  cursor:pointer;
  display:flex;
  align-items:center;
  gap:6px;
  font-size:0.82rem;
  min-width:130px;
  justify-content:space-between;
  white-space:nowrap;
  height:36px;
  transition:border-color 0.12s ease,background 0.12s ease;
}}
.filter-btn:hover{{border-color:#3b82f6}}
.filter-btn.active{{border-color:#3b82f6;background:rgba(59,130,246,0.08);color:#93c5fd}}
.btn-left{{display:flex;align-items:center;gap:6px}}
.badge{{
  background:#3b82f6;color:#fff;font-size:0.62rem;
  font-weight:700;padding:1px 5px;border-radius:10px;display:none;
}}
.chevron{{font-size:0.65rem;color:#475569;transition:transform 0.15s ease}}
.chevron.open{{transform:rotate(180deg)}}

/* ── Dropdown panel ── */
.dropdown-wrap{{position:relative}}
.dropdown-panel{{
  position:absolute;
  top:calc(100% + 5px);
  left:0;
  background:#1c1f2e;
  border:1px solid #2a2d3e;
  border-radius:8px;
  min-width:190px;
  max-height:280px;
  overflow-y:auto;
  z-index:9999;
  box-shadow:0 8px 28px rgba(0,0,0,0.6);
  opacity:0;
  transform:translateY(-6px);
  transition:opacity 0.15s ease,transform 0.15s ease;
  pointer-events:none;
}}
.dropdown-panel.open{{opacity:1;transform:translateY(0);pointer-events:all}}
.dropdown-panel::-webkit-scrollbar{{width:3px}}
.dropdown-panel::-webkit-scrollbar-thumb{{background:#2a2d3e;border-radius:2px}}
.panel-header{{
  display:flex;justify-content:space-between;align-items:center;
  padding:8px 12px 7px;border-bottom:1px solid #2a2d3e;position:sticky;top:0;
  background:#1c1f2e;
}}
.panel-title{{font-size:0.65rem;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:0.08em}}
.clear-link{{font-size:0.72rem;color:#3b82f6;cursor:pointer;font-weight:600;border:none;background:none;padding:0}}
.clear-link:hover{{color:#60a5fa}}
.check-item{{
  display:flex;align-items:center;gap:9px;padding:8px 12px;
  cursor:pointer;font-size:0.82rem;color:#cbd5e1;user-select:none;
}}
.check-item:hover{{background:rgba(255,255,255,0.045)}}
.check-item input[type=checkbox]{{accent-color:#3b82f6;width:14px;height:14px;cursor:pointer;flex-shrink:0}}
.mini-dot{{width:8px;height:8px;border-radius:50%;flex-shrink:0}}
.link-option{{
  padding:9px 12px;cursor:pointer;font-size:0.84rem;color:#cbd5e1;
  transition:background 0.1s ease;
}}
.link-option:hover{{background:rgba(255,255,255,0.045)}}
.link-option.selected{{color:#60a5fa;font-weight:600;background:rgba(59,130,246,0.08)}}
.link-option.selected::before{{content:"✓ ";font-size:0.75rem}}

/* ── Date / Search inputs ── */
.date-input,.search-input{{
  background:#1c1f2e;
  border:1px solid #2a2d3e;
  border-radius:6px;
  color:#e2e8f0;
  padding:0 10px;
  font-size:0.82rem;
  outline:none;
  height:36px;
  width:100%;
  transition:border-color 0.12s ease;
}}
.date-input:focus,.search-input:focus{{border-color:#3b82f6}}
.date-input{{min-width:130px;max-width:150px}}
.date-input::-webkit-calendar-picker-indicator{{filter:invert(0.5);cursor:pointer}}
.search-group{{flex:1;min-width:180px}}

/* ── Stats row ── */
#stats-row{{
  display:flex;gap:18px;align-items:center;
  padding:2px 0 10px;
  border-bottom:1px solid #2a2d3e;
  margin-bottom:14px;
  flex-shrink:0;
  flex-wrap:wrap;
}}
.stat{{font-size:0.78rem;color:#94a3b8}}
.stat strong{{color:#e2e8f0;font-weight:600;margin-right:3px}}
#clear-all{{
  margin-left:auto;font-size:0.76rem;color:#3b82f6;
  cursor:pointer;font-weight:600;border:none;background:none;
  padding:0;display:none;
}}
#clear-all:hover{{color:#60a5fa}}

/* ── Articles ── */
#articles{{flex:1;overflow-y:auto;padding-right:2px}}
#articles::-webkit-scrollbar{{width:4px}}
#articles::-webkit-scrollbar-track{{background:transparent}}
#articles::-webkit-scrollbar-thumb{{background:#2a2d3e;border-radius:2px}}
.card{{
  background:#1c1f2e;
  border-radius:8px;
  padding:16px 20px 14px;
  margin-bottom:10px;
  border-left:3px solid #3b82f6;
  transition:transform 0.12s ease,box-shadow 0.12s ease;
}}
.card:hover{{transform:translateY(-1px);box-shadow:0 4px 18px rgba(0,0,0,0.45)}}
.card:hover .card-title{{color:#60a5fa}}
.card-meta{{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:8px}}
.card-title{{
  font-size:1.05rem;font-weight:600;color:#f1f5f9;
  text-decoration:none;display:block;margin-bottom:6px;
  line-height:1.4;transition:color 0.12s ease;
}}
.card-excerpt{{
  font-size:0.88rem;color:#94a3b8;line-height:1.55;
  overflow:hidden;display:-webkit-box;
  -webkit-line-clamp:2;-webkit-box-orient:vertical;
}}
.source-badge{{
  font-size:0.62rem;font-weight:700;padding:2px 6px;border-radius:3px;
  color:#fff;text-transform:uppercase;letter-spacing:0.06em;white-space:nowrap;
}}
.date-tag{{font-size:0.7rem;color:#64748b;white-space:nowrap}}
.proj-tag{{
  font-size:0.68rem;font-weight:600;padding:2px 7px;border-radius:3px;
  background:rgba(59,130,246,0.12);color:#60a5fa;
  border:1px solid rgba(59,130,246,0.25);white-space:nowrap;
}}
.empty-msg{{color:#94a3b8;padding:48px 0;text-align:center;font-size:0.92rem}}
</style>
</head>
<body>
<div id="app">

  <!-- Filter bar -->
  <div id="filter-bar-wrap">
    <div class="filter-row">

      <!-- Topic -->
      <div class="filter-group">
        <div class="filter-label">Topic</div>
        <div class="dropdown-wrap">
          <button class="filter-btn" id="topic-btn" onclick="toggleDD('topic-panel','topic-chevron')">
            <span class="btn-left">
              <span id="topic-label">All Topics</span>
              <span class="badge" id="topic-badge"></span>
            </span>
            <span class="chevron" id="topic-chevron">▾</span>
          </button>
          <div class="dropdown-panel" id="topic-panel">
            <div class="panel-header">
              <span class="panel-title">Topics</span>
              <button class="clear-link" onclick="clearTopics()">Clear</button>
            </div>
            <div id="topic-checks"></div>
          </div>
        </div>
      </div>

      <!-- Source -->
      <div class="filter-group">
        <div class="filter-label">Source</div>
        <div class="dropdown-wrap">
          <button class="filter-btn" id="src-btn" onclick="toggleDD('src-panel','src-chevron')">
            <span class="btn-left">
              <span id="src-label">All Sources</span>
              <span class="badge" id="src-badge"></span>
            </span>
            <span class="chevron" id="src-chevron">▾</span>
          </button>
          <div class="dropdown-panel" id="src-panel">
            <div class="panel-header">
              <span class="panel-title">Sources</span>
              <button class="clear-link" onclick="clearSources()">Clear</button>
            </div>
            <div id="src-checks"></div>
          </div>
        </div>
      </div>

      <!-- Project Link -->
      <div class="filter-group">
        <div class="filter-label">Project Link</div>
        <div class="dropdown-wrap">
          <button class="filter-btn" id="link-btn" onclick="toggleDD('link-panel','link-chevron')">
            <span class="btn-left"><span id="link-label">All Articles</span></span>
            <span class="chevron" id="link-chevron">▾</span>
          </button>
          <div class="dropdown-panel" id="link-panel" style="min-width:150px">
            <div class="link-option selected" data-val="all" onclick="setLink('all')">All Articles</div>
            <div class="link-option" data-val="linked" onclick="setLink('linked')">Linked to Project</div>
            <div class="link-option" data-val="unlinked" onclick="setLink('unlinked')">Not Linked</div>
          </div>
        </div>
      </div>

      <!-- From -->
      <div class="filter-group">
        <div class="filter-label">From</div>
        <input type="date" id="date-from" class="date-input"
               onchange="state.dateFrom=this.value;update()">
      </div>

      <!-- To -->
      <div class="filter-group">
        <div class="filter-label">To</div>
        <input type="date" id="date-to" class="date-input"
               onchange="state.dateTo=this.value;update()">
      </div>

      <!-- Search -->
      <div class="filter-group search-group">
        <div class="filter-label">Search</div>
        <input type="text" id="search-input" class="search-input"
               placeholder="Search headlines &amp; summaries…"
               oninput="state.search=this.value;update()">
      </div>

    </div>
  </div>

  <!-- Stats row -->
  <div id="stats-row">
    <div class="stat"><strong id="stats-total">0</strong>Total Articles</div>
    <div class="stat"><strong id="stats-showing">0</strong>Showing</div>
    <div class="stat"><strong id="stats-linked">0</strong>Linked to Projects</div>
    <div class="stat"><strong id="stats-sources">0</strong>Sources</div>
    <button id="clear-all" onclick="clearAll()">✕ Clear all filters</button>
  </div>

  <!-- Articles -->
  <div id="articles"></div>

</div>

<script>
const ARTICLES = {articles_json};
const SOURCE_LABELS = {source_labels_json};
const SOURCE_COLORS = {source_colors_json};
const TOPICS = {topics_json};

const state = {{
  topics: [],
  sources: [],
  linkFilter: 'all',
  dateFrom: '',
  dateTo: '',
  search: '',
}};

let openPanel = null;

// ── Dropdown toggle ────────────────────────────────────────────────────────
function toggleDD(panelId, chevronId) {{
  const panel = document.getElementById(panelId);
  const chevron = document.getElementById(chevronId);
  if (openPanel && openPanel !== panel) {{
    openPanel.classList.remove('open');
    openPanel._chevron && openPanel._chevron.classList.remove('open');
  }}
  const isOpen = panel.classList.toggle('open');
  chevron.classList.toggle('open', isOpen);
  panel._chevron = chevron;
  openPanel = isOpen ? panel : null;
}}

document.addEventListener('click', function(e) {{
  if (!openPanel) return;
  const wrap = openPanel.closest('.dropdown-wrap');
  if (wrap && wrap.contains(e.target)) return;
  openPanel.classList.remove('open');
  openPanel._chevron && openPanel._chevron.classList.remove('open');
  openPanel = null;
}}, true);

// ── Topic ──────────────────────────────────────────────────────────────────
function toggleTopic(t) {{
  const i = state.topics.indexOf(t);
  if (i === -1) state.topics.push(t); else state.topics.splice(i, 1);
  update();
}}
function clearTopics() {{
  state.topics = [];
  document.querySelectorAll('.topic-cb').forEach(cb => cb.checked = false);
  update();
}}

// ── Source ─────────────────────────────────────────────────────────────────
function toggleSource(s) {{
  const i = state.sources.indexOf(s);
  if (i === -1) state.sources.push(s); else state.sources.splice(i, 1);
  update();
}}
function clearSources() {{
  state.sources = [];
  document.querySelectorAll('.src-cb').forEach(cb => cb.checked = false);
  update();
}}

// ── Link filter ────────────────────────────────────────────────────────────
function setLink(val) {{
  state.linkFilter = val;
  document.querySelectorAll('.link-option').forEach(el => {{
    el.classList.toggle('selected', el.dataset.val === val);
  }});
  const labels = {{all:'All Articles', linked:'Linked to Project', unlinked:'Not Linked'}};
  document.getElementById('link-label').textContent = labels[val];
  document.getElementById('link-btn').classList.toggle('active', val !== 'all');
  const panel = document.getElementById('link-panel');
  panel.classList.remove('open');
  document.getElementById('link-chevron').classList.remove('open');
  openPanel = null;
  update();
}}

// ── Clear all ──────────────────────────────────────────────────────────────
function clearAll() {{
  state.topics = [];
  state.sources = [];
  state.linkFilter = 'all';
  state.dateFrom = '';
  state.dateTo = '';
  state.search = '';
  document.querySelectorAll('.topic-cb,.src-cb').forEach(cb => cb.checked = false);
  document.querySelectorAll('.link-option').forEach(el => {{
    el.classList.toggle('selected', el.dataset.val === 'all');
  }});
  document.getElementById('link-label').textContent = 'All Articles';
  document.getElementById('date-from').value = '';
  document.getElementById('date-to').value = '';
  document.getElementById('search-input').value = '';
  document.getElementById('link-btn').classList.remove('active');
  update();
}}

// ── Filter logic ───────────────────────────────────────────────────────────
function getFiltered() {{
  return ARTICLES.filter(a => {{
    if (state.topics.length > 0) {{
      const t = (a.topics || '').split(',').map(x => x.trim()).filter(Boolean);
      if (!state.topics.some(x => t.includes(x))) return false;
    }}
    if (state.sources.length > 0 && !state.sources.includes(a.source)) return false;
    if (state.linkFilter === 'linked' && !a.linked) return false;
    if (state.linkFilter === 'unlinked' && a.linked) return false;
    if (state.dateFrom && a.date && a.date < state.dateFrom) return false;
    if (state.dateTo && a.date && a.date > state.dateTo) return false;
    if (state.search) {{
      const q = state.search.toLowerCase();
      if (!a.title.toLowerCase().includes(q) && !(a.summary||'').toLowerCase().includes(q)) return false;
    }}
    return true;
  }});
}}

// ── Render ─────────────────────────────────────────────────────────────────
function esc(s) {{
  return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}}
function fmtDate(d) {{
  if (!d) return '';
  try {{
    const dt = new Date(d + 'T12:00:00');
    return dt.toLocaleDateString('en-US',{{month:'short',day:'numeric',year:'numeric'}});
  }} catch(e) {{ return d; }}
}}

function renderArticles(arr) {{
  const el = document.getElementById('articles');
  if (!arr.length) {{
    el.innerHTML = '<div class="empty-msg">No articles match your filters.</div>';
    return;
  }}
  el.innerHTML = arr.map(a => {{
    const color = SOURCE_COLORS[a.source] || '#6b7280';
    const label = SOURCE_LABELS[a.source] || a.source;
    const datePart = a.date ? `<span class="date-tag">${{fmtDate(a.date)}}</span>` : '';
    const projPart = a.project ? `<span class="proj-tag">${{esc(a.project)}}</span>` : '';
    const excerpt = a.summary.length > 150 ? a.summary.slice(0,150)+'…' : a.summary;
    return `<div class="card" style="border-left-color:${{color}}">
      <div class="card-meta">
        <span class="source-badge" style="background:${{color}}">${{esc(label)}}</span>
        ${{datePart}}${{projPart}}
      </div>
      <a href="${{esc(a.url)}}" target="_blank" rel="noopener" class="card-title">${{esc(a.title)}}</a>
      <p class="card-excerpt">${{esc(excerpt)}}</p>
    </div>`;
  }}).join('');
}}

function updateStats(filtered) {{
  const linked = filtered.filter(a => a.linked).length;
  const srcCount = new Set(filtered.map(a => a.source)).size;
  document.getElementById('stats-total').textContent = ARTICLES.length;
  document.getElementById('stats-showing').textContent = filtered.length;
  document.getElementById('stats-linked').textContent = linked;
  document.getElementById('stats-sources').textContent = srcCount;
  const hasFilters = state.topics.length || state.sources.length ||
    state.linkFilter !== 'all' || state.dateFrom || state.dateTo || state.search;
  document.getElementById('clear-all').style.display = hasFilters ? 'inline' : 'none';
}}

function updateBtns() {{
  const tb = document.getElementById('topic-badge');
  const tl = document.getElementById('topic-label');
  if (state.topics.length) {{
    tb.textContent = state.topics.length; tb.style.display='inline';
    tl.textContent = state.topics.length === 1 ? state.topics[0] : state.topics.length + ' topics';
    document.getElementById('topic-btn').classList.add('active');
  }} else {{
    tb.style.display='none'; tl.textContent='All Topics';
    document.getElementById('topic-btn').classList.remove('active');
  }}
  const sb = document.getElementById('src-badge');
  const sl = document.getElementById('src-label');
  if (state.sources.length) {{
    sb.textContent = state.sources.length; sb.style.display='inline';
    sl.textContent = state.sources.length === 1
      ? (SOURCE_LABELS[state.sources[0]] || state.sources[0])
      : state.sources.length + ' sources';
    document.getElementById('src-btn').classList.add('active');
  }} else {{
    sb.style.display='none'; sl.textContent='All Sources';
    document.getElementById('src-btn').classList.remove('active');
  }}
}}

function update() {{
  const filtered = getFiltered();
  renderArticles(filtered);
  updateStats(filtered);
  updateBtns();
}}

// ── Init dropdowns ─────────────────────────────────────────────────────────
function initDropdowns() {{
  const topicChecks = document.getElementById('topic-checks');
  TOPICS.forEach(t => {{
    const label = document.createElement('label');
    label.className = 'check-item';
    label.innerHTML = `<input type="checkbox" class="topic-cb" value="${{t}}" onchange="toggleTopic('${{t}}')">${{esc(t)}}`;
    topicChecks.appendChild(label);
  }});

  const availSources = [...new Set(ARTICLES.map(a => a.source))].filter(Boolean)
    .sort((a,b) => (SOURCE_LABELS[a]||a).localeCompare(SOURCE_LABELS[b]||b));
  const srcChecks = document.getElementById('src-checks');
  availSources.forEach(src => {{
    const lbl = SOURCE_LABELS[src] || src;
    const col = SOURCE_COLORS[src] || '#6b7280';
    const label = document.createElement('label');
    label.className = 'check-item';
    label.innerHTML = `<input type="checkbox" class="src-cb" value="${{src}}" onchange="toggleSource('${{src}}')">`
      + `<span class="mini-dot" style="background:${{col}}"></span>${{esc(lbl)}}`;
    srcChecks.appendChild(label);
  }});

  const dates = ARTICLES.map(a => a.date).filter(Boolean).sort();
  if (dates.length) {{
    document.getElementById('date-from').min = dates[0];
    document.getElementById('date-from').max = dates[dates.length-1];
    document.getElementById('date-to').min = dates[0];
    document.getElementById('date-to').max = dates[dates.length-1];
  }}
}}

initDropdowns();
update();
</script>
</body>
</html>"""

    components.html(html, height=960, scrolling=False)
