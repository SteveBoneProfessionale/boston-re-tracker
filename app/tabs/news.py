import json
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from app.data import load_news, backfill_topics

SOURCE_LABELS = {
    "boston_gov":        "BOSTON.GOV",
    "banker_tradesman":  "BANKER & T",
    "the_real_deal":     "REAL DEAL",
    "curbed":            "CURBED",
    "boston_com":        "BOSTON.COM",
    "boston_re_times":   "BRE TIMES",
    "bisnow_boston":     "BISNOW",
}
SOURCE_COLORS = {
    "boston_gov":        "#8A9BB0",
    "banker_tradesman":  "#F5821E",
    "the_real_deal":     "#ef4444",
    "curbed":            "#a78bfa",
    "boston_com":        "#22c55e",
    "boston_re_times":   "#06b6d4",
    "bisnow_boston":     "#F5821E",
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
            "title":   str(row.get("title") or ""),
            "url":     str(row.get("url") or "#"),
            "source":  str(row.get("source") or ""),
            "date":    pub,
            "summary": str(row.get("summary") or "")[:500],
            "project": str(row.get("linked_project_name") or ""),
            "linked":  bool(row.get("linked_project_id")),
            "topics":  str(row.get("topics") or ""),
        })

    articles_json      = json.dumps(articles, ensure_ascii=False)
    source_labels_json = json.dumps(SOURCE_LABELS)
    source_colors_json = json.dumps(SOURCE_COLORS)
    topics_json        = json.dumps(TOPICS)

    html = f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Inter:wght@400;500;600&display=swap');
*{{box-sizing:border-box;margin:0;padding:0}}
body{{
  background:#0d0f12;
  color:#e2e8f0;
  font-family:'Inter',-apple-system,sans-serif;
  height:100vh;overflow:hidden;display:flex;flex-direction:column;
  padding:2px 2px 4px;
}}
#app{{display:flex;flex-direction:column;height:100%;overflow:hidden}}

/* ── Toolbar ── */
#toolbar{{
  background:#0d0f12;
  border:1px solid #1E2530;
  padding:10px 14px;
  margin-bottom:8px;
  flex-shrink:0;
  display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end;
  position:relative;z-index:100;
}}
.fg{{display:flex;flex-direction:column;gap:4px}}
.fl{{font-family:'JetBrains Mono',monospace;font-size:8.5px;font-weight:700;
     letter-spacing:0.14em;color:#8A9BB0;text-transform:uppercase}}
.fb{{
  background:#141720;border:1px solid #1E2530;color:#e2e8f0;
  padding:0 10px;height:32px;font-family:'JetBrains Mono',monospace;
  font-size:11px;cursor:pointer;display:flex;align-items:center;gap:6px;
  min-width:120px;justify-content:space-between;white-space:nowrap;
  transition:border-color 0.1s;
}}
.fb:hover{{border-color:#F5821E}}
.fb.on{{border-color:#F5821E;color:#F5821E}}
.fbl{{display:flex;align-items:center;gap:5px}}
.fbadge{{
  background:#F5821E;color:#000;font-size:8px;font-weight:700;
  padding:1px 4px;display:none;
}}
.chev{{font-size:9px;color:#475569;transition:transform 0.12s}}
.chev.open{{transform:rotate(180deg)}}

.dp{{
  position:absolute;top:calc(100% + 4px);left:0;
  background:#141720;border:1px solid #1E2530;
  min-width:180px;max-height:260px;overflow-y:auto;
  z-index:9999;box-shadow:0 8px 24px rgba(0,0,0,0.7);
  opacity:0;transform:translateY(-4px);
  transition:opacity 0.12s,transform 0.12s;pointer-events:none;
}}
.dp.open{{opacity:1;transform:translateY(0);pointer-events:all}}
.dp::-webkit-scrollbar{{width:3px}}
.dp::-webkit-scrollbar-thumb{{background:#1E2530}}
.dp-head{{
  display:flex;justify-content:space-between;align-items:center;
  padding:7px 11px 6px;border-bottom:1px solid #1E2530;
  position:sticky;top:0;background:#141720;
}}
.dp-title{{font-family:'JetBrains Mono',monospace;font-size:8px;font-weight:700;
           letter-spacing:0.1em;color:#8A9BB0;text-transform:uppercase}}
.dp-clear{{font-family:'JetBrains Mono',monospace;font-size:10px;color:#F5821E;
           cursor:pointer;border:none;background:none;padding:0;font-weight:600}}
.dp-clear:hover{{color:#ffb06e}}
.ci{{
  display:flex;align-items:center;gap:8px;padding:7px 11px;
  cursor:pointer;font-size:11px;color:#cbd5e1;user-select:none;
  font-family:'JetBrains Mono',monospace;
}}
.ci:hover{{background:rgba(245,130,30,0.06)}}
.ci input[type=checkbox]{{accent-color:#F5821E;width:13px;height:13px;cursor:pointer;flex-shrink:0}}
.mdot{{width:7px;height:7px;border-radius:50%;flex-shrink:0}}
.lo{{
  padding:8px 11px;cursor:pointer;font-size:11px;color:#cbd5e1;
  font-family:'JetBrains Mono',monospace;
}}
.lo:hover{{background:rgba(245,130,30,0.06)}}
.lo.sel{{color:#F5821E;font-weight:700}}

.si{{
  background:#141720;border:1px solid #1E2530;color:#e2e8f0;
  padding:0 10px;height:32px;font-family:'JetBrains Mono',monospace;
  font-size:11px;outline:none;flex:1;min-width:160px;
  transition:border-color 0.1s;
}}
.si:focus{{border-color:#F5821E}}
.di{{
  background:#141720;border:1px solid #1E2530;color:#e2e8f0;
  padding:0 10px;height:32px;font-family:'JetBrains Mono',monospace;
  font-size:11px;outline:none;width:130px;
  transition:border-color 0.1s;
}}
.di:focus{{border-color:#F5821E}}
.di::-webkit-calendar-picker-indicator{{filter:invert(0.4);cursor:pointer}}
.sg{{flex:1;min-width:180px}}

/* ── Stats row ── */
#srow{{
  display:flex;gap:20px;align-items:center;
  padding:0 0 8px;
  border-bottom:1px solid #1E2530;
  margin-bottom:0;flex-shrink:0;flex-wrap:wrap;
}}
.st{{font-family:'JetBrains Mono',monospace;font-size:10px;color:#8A9BB0;letter-spacing:0.04em}}
.st strong{{color:#e2e8f0;font-weight:700;margin-right:4px}}
#ca{{
  margin-left:auto;font-family:'JetBrains Mono',monospace;
  font-size:10px;color:#F5821E;cursor:pointer;
  border:none;background:none;padding:0;display:none;letter-spacing:0.06em;
}}
#ca:hover{{color:#ffb06e}}

/* ── Wire ── */
#wire{{flex:1;overflow-y:auto;margin-top:0}}
#wire::-webkit-scrollbar{{width:4px}}
#wire::-webkit-scrollbar-track{{background:transparent}}
#wire::-webkit-scrollbar-thumb{{background:#1E2530}}

.row{{
  display:flex;flex-direction:column;
  border-bottom:1px solid #1E2530;
  cursor:pointer;
  transition:background 0.08s;
}}
.row:hover{{background:rgba(245,130,30,0.03)}}
.rhead{{
  display:flex;align-items:center;gap:0;
  padding:9px 12px;min-height:40px;
}}
.ts{{
  font-family:'JetBrains Mono',monospace;font-size:10px;
  color:#475569;white-space:nowrap;width:84px;flex-shrink:0;
  letter-spacing:0.02em;
}}
.sep{{color:#1E2530;margin:0 8px;font-size:12px;flex-shrink:0}}
.sbadge{{
  font-family:'JetBrains Mono',monospace;
  font-size:8.5px;font-weight:700;padding:2px 6px;
  color:#000;letter-spacing:0.08em;white-space:nowrap;
  flex-shrink:0;width:76px;text-align:center;overflow:hidden;
  text-overflow:ellipsis;
}}
.hl{{
  font-family:'Inter',sans-serif;font-size:13px;font-weight:500;
  color:#e2e8f0;flex:1;overflow:hidden;text-overflow:ellipsis;
  white-space:nowrap;padding:0 12px;line-height:1.3;
}}
.ptag{{
  font-family:'JetBrains Mono',monospace;font-size:8.5px;font-weight:700;
  padding:2px 7px;background:rgba(245,130,30,0.1);color:#F5821E;
  border:1px solid rgba(245,130,30,0.25);white-space:nowrap;
  flex-shrink:0;max-width:180px;overflow:hidden;text-overflow:ellipsis;
}}
.arrow{{
  font-family:'JetBrains Mono',monospace;font-size:9px;color:#1E2530;
  margin-left:8px;flex-shrink:0;transition:transform 0.12s,color 0.12s;
}}
.row.open .arrow{{transform:rotate(90deg);color:#F5821E}}

.rbody{{
  display:none;
  padding:0 12px 12px 168px;
  border-top:1px solid #1E2530;
  background:#141720;
}}
.row.open .rbody{{display:block}}
.excerpt{{
  font-family:'Inter',sans-serif;font-size:12px;color:#8A9BB0;
  line-height:1.6;margin:10px 0 10px;
}}
.topics-wrap{{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:10px}}
.topic-badge{{
  font-family:'JetBrains Mono',monospace;font-size:8px;font-weight:700;
  letter-spacing:0.1em;padding:2px 7px;
  background:rgba(255,255,255,0.04);color:#8A9BB0;
  border:1px solid #1E2530;text-transform:uppercase;
}}
.readlink{{
  font-family:'JetBrains Mono',monospace;font-size:10px;font-weight:700;
  letter-spacing:0.1em;color:#F5821E;text-decoration:none;
  border:1px solid rgba(245,130,30,0.4);padding:4px 12px;
  display:inline-block;transition:background 0.1s;
}}
.readlink:hover{{background:rgba(245,130,30,0.1)}}
.empty{{color:#8A9BB0;padding:48px 0;text-align:center;
        font-family:'JetBrains Mono',monospace;font-size:11px;letter-spacing:0.1em}}
</style>
</head>
<body>
<div id="app">

<!-- toolbar -->
<div id="toolbar">

  <!-- Topic -->
  <div class="fg">
    <div class="fl">TOPIC</div>
    <div style="position:relative">
      <button class="fb" id="tb" onclick="tgl('tp','tc')">
        <span class="fbl"><span id="tl">ALL TOPICS</span><span class="fbadge" id="tbadge"></span></span>
        <span class="chev" id="tc">▾</span>
      </button>
      <div class="dp" id="tp">
        <div class="dp-head"><span class="dp-title">TOPICS</span><button class="dp-clear" onclick="clrT()">CLEAR</button></div>
        <div id="tchecks"></div>
      </div>
    </div>
  </div>

  <!-- Source -->
  <div class="fg">
    <div class="fl">SOURCE</div>
    <div style="position:relative">
      <button class="fb" id="sb" onclick="tgl('sp','sc')">
        <span class="fbl"><span id="sl">ALL SOURCES</span><span class="fbadge" id="sbadge"></span></span>
        <span class="chev" id="sc">▾</span>
      </button>
      <div class="dp" id="sp">
        <div class="dp-head"><span class="dp-title">SOURCES</span><button class="dp-clear" onclick="clrS()">CLEAR</button></div>
        <div id="schecks"></div>
      </div>
    </div>
  </div>

  <!-- Link filter -->
  <div class="fg">
    <div class="fl">PROJECT LINK</div>
    <div style="position:relative">
      <button class="fb" id="lb" onclick="tgl('lp','lc')">
        <span class="fbl"><span id="ll">ALL ARTICLES</span></span>
        <span class="chev" id="lc">▾</span>
      </button>
      <div class="dp" id="lp" style="min-width:160px">
        <div class="lo sel" data-v="all"     onclick="setL('all')">ALL ARTICLES</div>
        <div class="lo"     data-v="linked"  onclick="setL('linked')">LINKED TO PROJECT</div>
        <div class="lo"     data-v="unlinked"onclick="setL('unlinked')">NOT LINKED</div>
      </div>
    </div>
  </div>

  <!-- From / To -->
  <div class="fg">
    <div class="fl">FROM</div>
    <input type="date" class="di" id="df" onchange="S.df=this.value;upd()">
  </div>
  <div class="fg">
    <div class="fl">TO</div>
    <input type="date" class="di" id="dt" onchange="S.dt=this.value;upd()">
  </div>

  <!-- Search -->
  <div class="fg sg">
    <div class="fl">SEARCH</div>
    <input type="text" class="si" id="q" placeholder="Search headlines &amp; summaries…"
           oninput="S.q=this.value;upd()">
  </div>

</div>

<!-- stats row -->
<div id="srow">
  <div class="st"><strong id="n0">0</strong>TOTAL</div>
  <div class="st"><strong id="n1">0</strong>SHOWING</div>
  <div class="st"><strong id="n2">0</strong>LINKED</div>
  <div class="st"><strong id="n3">0</strong>SOURCES</div>
  <button id="ca" onclick="clrAll()">✕ CLEAR ALL</button>
</div>

<!-- wire -->
<div id="wire"></div>

</div>

<script>
const A={articles_json};
const SL={source_labels_json};
const SC={source_colors_json};
const TP={topics_json};
const S={{topics:[],sources:[],link:'all',df:'',dt:'',q:''}};
let openP=null;

function tgl(pid,cid){{
  const p=document.getElementById(pid);
  const c=document.getElementById(cid);
  if(openP&&openP!==p){{
    openP.classList.remove('open');
    openP._c&&openP._c.classList.remove('open');
  }}
  const o=p.classList.toggle('open');
  c.classList.toggle('open',o);
  p._c=c;
  openP=o?p:null;
}}
document.addEventListener('click',function(e){{
  if(!openP)return;
  const w=openP.closest('[style*="position:relative"]');
  if(w&&w.contains(e.target))return;
  openP.classList.remove('open');
  openP._c&&openP._c.classList.remove('open');
  openP=null;
}},true);

function togT(t){{const i=S.topics.indexOf(t);if(i<0)S.topics.push(t);else S.topics.splice(i,1);upd();}}
function clrT(){{S.topics=[];document.querySelectorAll('.tcb').forEach(c=>c.checked=false);upd();}}
function togS(s){{const i=S.sources.indexOf(s);if(i<0)S.sources.push(s);else S.sources.splice(i,1);upd();}}
function clrS(){{S.sources=[];document.querySelectorAll('.scb').forEach(c=>c.checked=false);upd();}}
function setL(v){{
  S.link=v;
  document.querySelectorAll('.lo').forEach(e=>e.classList.toggle('sel',e.dataset.v===v));
  const m={{all:'ALL ARTICLES',linked:'LINKED TO PROJECT',unlinked:'NOT LINKED'}};
  document.getElementById('ll').textContent=m[v];
  document.getElementById('lb').classList.toggle('on',v!=='all');
  document.getElementById('lp').classList.remove('open');
  document.getElementById('lc').classList.remove('open');
  openP=null;upd();
}}
function clrAll(){{
  S.topics=[];S.sources=[];S.link='all';S.df='';S.dt='';S.q='';
  document.querySelectorAll('.tcb,.scb').forEach(c=>c.checked=false);
  document.querySelectorAll('.lo').forEach(e=>e.classList.toggle('sel',e.dataset.v==='all'));
  document.getElementById('ll').textContent='ALL ARTICLES';
  document.getElementById('lb').classList.remove('on');
  document.getElementById('df').value='';
  document.getElementById('dt').value='';
  document.getElementById('q').value='';
  upd();
}}

function filt(){{
  return A.filter(a=>{{
    if(S.topics.length){{
      const t=(a.topics||'').split(',').map(x=>x.trim()).filter(Boolean);
      if(!S.topics.some(x=>t.includes(x)))return false;
    }}
    if(S.sources.length&&!S.sources.includes(a.source))return false;
    if(S.link==='linked'&&!a.linked)return false;
    if(S.link==='unlinked'&&a.linked)return false;
    if(S.df&&a.date&&a.date<S.df)return false;
    if(S.dt&&a.date&&a.date>S.dt)return false;
    if(S.q){{const q=S.q.toLowerCase();if(!a.title.toLowerCase().includes(q)&&!(a.summary||'').toLowerCase().includes(q))return false;}}
    return true;
  }});
}}

function e(s){{return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');}}
function fd(d){{
  if(!d)return'';
  try{{const dt=new Date(d+'T12:00:00');return dt.toLocaleDateString('en-US',{{month:'short',day:'2-digit'}});}}catch(x){{return d.slice(5);}}
}}

function draw(arr){{
  const w=document.getElementById('wire');
  if(!arr.length){{w.innerHTML='<div class="empty">NO ARTICLES MATCH YOUR FILTERS</div>';return;}}
  w.innerHTML=arr.map((a,i)=>{{
    const col=SC[a.source]||'#8A9BB0';
    const lbl=SL[a.source]||a.source.toUpperCase().slice(0,9);
    const ts=fd(a.date);
    const proj=a.project?`<span class="ptag">${{e(a.project)}}</span>`:'';
    const topics=a.topics?a.topics.split(',').filter(Boolean).map(t=>`<span class="topic-badge">${{e(t.trim())}}</span>`).join(''):'';
    return `<div class="row" id="r${{i}}" onclick="tog(${{i}})">
      <div class="rhead">
        <span class="ts">${{e(ts)}}</span>
        <span class="sep">|</span>
        <span class="sbadge" style="background:${{col}}">${{e(lbl)}}</span>
        <span class="sep">|</span>
        <span class="hl">${{e(a.title)}}</span>
        ${{proj}}
        <span class="arrow">▶</span>
      </div>
      <div class="rbody">
        ${{a.summary?`<p class="excerpt">${{e(a.summary)}}</p>`:''}}
        ${{topics?`<div class="topics-wrap">${{topics}}</div>`:''}}
        <a href="${{e(a.url)}}" target="_blank" rel="noopener" class="readlink">READ ARTICLE ↗</a>
      </div>
    </div>`;
  }}).join('');
}}

function tog(i){{
  const r=document.getElementById('r'+i);
  r.classList.toggle('open');
}}

function updStats(arr){{
  const linked=arr.filter(a=>a.linked).length;
  const srcs=new Set(arr.map(a=>a.source)).size;
  document.getElementById('n0').textContent=A.length;
  document.getElementById('n1').textContent=arr.length;
  document.getElementById('n2').textContent=linked;
  document.getElementById('n3').textContent=srcs;
  const has=S.topics.length||S.sources.length||S.link!=='all'||S.df||S.dt||S.q;
  document.getElementById('ca').style.display=has?'inline':'none';
}}

function updBtns(){{
  const tb=document.getElementById('tbadge'),tl=document.getElementById('tl');
  if(S.topics.length){{tb.textContent=S.topics.length;tb.style.display='inline';
    tl.textContent=S.topics.length===1?S.topics[0].toUpperCase():S.topics.length+' TOPICS';
    document.getElementById('tb').classList.add('on');}}
  else{{tb.style.display='none';tl.textContent='ALL TOPICS';document.getElementById('tb').classList.remove('on');}}
  const sb=document.getElementById('sbadge'),sl=document.getElementById('sl');
  if(S.sources.length){{sb.textContent=S.sources.length;sb.style.display='inline';
    sl.textContent=S.sources.length===1?(SL[S.sources[0]]||S.sources[0]):S.sources.length+' SOURCES';
    document.getElementById('sb').classList.add('on');}}
  else{{sb.style.display='none';sl.textContent='ALL SOURCES';document.getElementById('sb').classList.remove('on');}}
}}

function upd(){{const f=filt();draw(f);updStats(f);updBtns();}}

// Init
(function init(){{
  const tc=document.getElementById('tchecks');
  TP.forEach(t=>{{
    const l=document.createElement('label');l.className='ci';
    l.innerHTML=`<input type="checkbox" class="tcb" value="${{t}}" onchange="togT('${{t}}')">${{e(t)}}`;
    tc.appendChild(l);
  }});
  const avs=[...new Set(A.map(a=>a.source))].filter(Boolean)
    .sort((a,b)=>(SL[a]||a).localeCompare(SL[b]||b));
  const sc=document.getElementById('schecks');
  avs.forEach(s=>{{
    const col=SC[s]||'#8A9BB0';
    const lbl=SL[s]||s;
    const l=document.createElement('label');l.className='ci';
    l.innerHTML=`<input type="checkbox" class="scb" value="${{s}}" onchange="togS('${{s}}')">`
      +`<span class="mdot" style="background:${{col}}"></span>${{e(lbl)}}`;
    sc.appendChild(l);
  }});
  const dates=A.map(a=>a.date).filter(Boolean).sort();
  if(dates.length){{
    ['df','dt'].forEach(id=>{{
      const el=document.getElementById(id);
      el.min=dates[0];el.max=dates[dates.length-1];
    }});
  }}
  upd();
}})();
</script>
</body></html>"""

    components.html(html, height=960, scrolling=False)
