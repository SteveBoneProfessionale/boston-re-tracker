import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components

_BG      = "#0d0f12"
_BG2     = "#141720"
_BORDER  = "#1E2530"
_ORANGE  = "#F5821E"
_MUTED   = "#8A9BB0"
_MONO    = "'JetBrains Mono', 'IBM Plex Mono', monospace"

STATUS_COLORS = {
    "Under Review":       _ORANGE,
    "Board Approved":     "#22c55e",
    "Letter of Intent":   "#475569",
    "Under Construction": "#ef4444",
}


def _section(label: str, mt: int = 20):
    st.markdown(
        f'<p style="font-family:{_MONO};font-size:9px;font-weight:700;'
        f'letter-spacing:0.18em;color:{_MUTED};text-transform:uppercase;'
        f'margin:{mt}px 0 6px 0">{label}</p>',
        unsafe_allow_html=True,
    )


def _chart_base(h: int = 320, r: int = 28) -> dict:
    return dict(
        height=h,
        margin=dict(l=0, r=r, t=4, b=4),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family=_MONO, size=10, color=_MUTED),
        showlegend=False,
    )


def render(df: pd.DataFrame, stats: dict):
    # ── Bloomberg stat tiles with count-up ──────────────────────────
    tiles = [
        ("TOTAL PROJECTS",    stats["total"],              "#ffffff", False),
        ("UNDER REVIEW",      stats["under_review"],       _ORANGE,   False),
        ("BOARD APPROVED",    stats["board_approved"],     "#22c55e", False),
        ("LOI",               stats["loi"],                _MUTED,    False),
        ("UNDER CONST.",      stats["under_construction"], "#ef4444", False),
        ("RESI UNITS",        stats["total_units"],        "#ffffff", False),
        ("PIPELINE SF",       stats["total_gsf"],          "#ffffff", True),
    ]
    tiles_json = json.dumps([
        {"label": t[0], "raw": t[1], "color": t[2], "big": t[3]}
        for t in tiles
    ])

    tiles_html = f"""<!DOCTYPE html><html><head>
<meta charset="utf-8">
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&display=swap');
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:{_BG};overflow:hidden}}
.grid{{
  display:grid;
  grid-template-columns:repeat(7,1fr);
  gap:1px;
  background:{_BORDER};
  border:1px solid {_BORDER};
}}
.tile{{
  background:{_BG};
  padding:15px 16px 13px;
  position:relative;
  overflow:hidden;
}}
.tile::before{{
  content:'';
  position:absolute;
  top:0;left:0;right:0;
  height:2px;
  background:var(--c);
}}
.lbl{{
  font-family:'JetBrains Mono',monospace;
  font-size:8px;font-weight:700;
  letter-spacing:0.14em;
  color:{_MUTED};
  text-transform:uppercase;
  margin-bottom:9px;
  white-space:nowrap;
}}
.val{{
  font-family:'JetBrains Mono',monospace;
  font-size:1.75rem;font-weight:700;
  color:#fff;line-height:1;white-space:nowrap;
}}
.sfx{{color:var(--c);font-size:0.95rem;margin-left:3px;font-weight:600}}
</style>
</head><body>
<div class="grid" id="g"></div>
<script>
const T={tiles_json};
function anim(el,raw,big,dur){{
  const s=performance.now();
  (function step(ts){{
    const p=Math.min((ts-s)/dur,1);
    const e=1-Math.pow(1-p,3);
    el.textContent=big?(raw/1e6*e).toFixed(1):Math.round(raw*e).toLocaleString('en-US');
    if(p<1)requestAnimationFrame(step);
  }})(s);
}}
const g=document.getElementById('g');
T.forEach(t=>{{
  const d=document.createElement('div');
  d.className='tile';
  d.style.setProperty('--c',t.color);
  d.innerHTML=`<div class="lbl">${{t.label}}</div>`
    +`<div class="val"><span class="num">0</span>`
    +`${{t.big?'<span class="sfx">M SF</span>':''}}</div>`;
  g.appendChild(d);
  setTimeout(()=>anim(d.querySelector('.num'),t.raw,t.big,850),60);
}});
</script>
</body></html>"""

    components.html(tiles_html, height=94)

    # ── Main charts ─────────────────────────────────────────────────
    col_a, col_b = st.columns([3, 2])

    with col_a:
        _section("PROJECTS BY NEIGHBORHOOD", mt=18)
        nbhd_status = (
            df[df["neighborhood"].astype(bool)]
            .groupby(["neighborhood", "status"])
            .size()
            .reset_index(name="count")
        )
        nbhd_order = (
            df[df["neighborhood"].astype(bool)]
            .groupby("neighborhood").size()
            .sort_values()
            .tail(15)
            .index.tolist()
        )
        nbhd_status = nbhd_status[nbhd_status["neighborhood"].isin(nbhd_order)]

        fig_nbhd = px.bar(
            nbhd_status,
            x="count", y="neighborhood",
            color="status",
            orientation="h",
            category_orders={
                "neighborhood": nbhd_order,
                "status": list(STATUS_COLORS.keys()),
            },
            color_discrete_map=STATUS_COLORS,
        )
        fig_nbhd.update_traces(marker_line_width=0)
        fig_nbhd.update_layout(
            **_chart_base(380),
            barmode="stack",
            xaxis=dict(visible=False, showgrid=False),
            yaxis=dict(
                showgrid=False,
                tickfont=dict(family=_MONO, size=10, color=_MUTED),
                linecolor=_BORDER, tickcolor=_BORDER,
            ),
            showlegend=True,
            legend=dict(
                font=dict(family=_MONO, size=9, color=_MUTED),
                bgcolor="rgba(0,0,0,0)",
                orientation="h",
                yanchor="bottom", y=1.01,
                xanchor="left", x=0,
                itemwidth=30,
            ),
            margin=dict(l=0, r=0, t=28, b=4),
        )
        st.plotly_chart(fig_nbhd, use_container_width=True, config={"displayModeBar": False})

        # GSF by asset class
        extracted = df[df["extraction_done"] & (df["asset_class"] != "")]
        if len(extracted) >= 5:
            _section("GROSS SF BY ASSET CLASS")
            ac = (
                extracted.groupby("asset_class")["total_gsf"]
                .sum()
                .reset_index()
                .sort_values("total_gsf", ascending=True)
            )
            ac["gsf_m"] = ac["total_gsf"] / 1e6
            bar_colors = [_MUTED] * len(ac)
            if len(ac):
                bar_colors[-1] = _ORANGE
            fig_ac = go.Figure(go.Bar(
                x=ac["gsf_m"], y=ac["asset_class"],
                orientation="h",
                marker_color=bar_colors,
                marker_line_width=0,
                text=ac["gsf_m"].apply(lambda v: f" {v:.1f}M"),
                textposition="outside",
                textfont=dict(family=_MONO, size=10, color=_MUTED),
                hovertemplate="%{y}: %{x:.1f}M SF<extra></extra>",
            ))
            fig_ac.update_layout(
                **_chart_base(200, r=50),
                xaxis=dict(visible=False, showgrid=False),
                yaxis=dict(
                    showgrid=False,
                    tickfont=dict(family=_MONO, size=10, color=_MUTED),
                    linecolor=_BORDER, tickcolor=_BORDER,
                ),
            )
            st.plotly_chart(fig_ac, use_container_width=True, config={"displayModeBar": False})

    with col_b:
        _section("STATUS BREAKDOWN", mt=18)
        status_df = df["status"].value_counts().reset_index()
        status_df.columns = ["status", "count"]
        total_s = status_df["count"].sum()
        fig_status = go.Figure()
        for _, row in status_df.iterrows():
            color = STATUS_COLORS.get(row["status"], _MUTED)
            fig_status.add_trace(go.Bar(
                x=[row["count"]], y=[""],
                orientation="h",
                marker_color=color,
                marker_line_width=0,
                name=row["status"],
                text=f'{row["count"]}',
                textposition="inside",
                constraintext="inside",
                insidetextanchor="middle",
                textfont=dict(family=_MONO, size=9, color="#000"),
                hovertemplate=f'{row["status"]}: {row["count"]} ({row["count"]/total_s*100:.0f}%)<extra></extra>',
            ))
        fig_status.update_layout(
            **_chart_base(44),
            barmode="stack",
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            margin=dict(l=0, r=0, t=0, b=0),
            showlegend=True,
            legend=dict(
                font=dict(family=_MONO, size=8, color=_MUTED),
                bgcolor="rgba(0,0,0,0)",
                orientation="h",
                yanchor="top", y=-0.15,
                xanchor="left", x=0,
            ),
        )
        st.plotly_chart(fig_status, use_container_width=True, config={"displayModeBar": False})

        _section("REVIEW SCALE")
        scale_df = df["project_scale"].value_counts().reset_index()
        scale_df.columns = ["scale", "count"]
        scale_colors_map = {"Large Project": _ORANGE, "Small Project": "#334155"}
        fig_scale = go.Figure()
        for _, row in scale_df.iterrows():
            fig_scale.add_trace(go.Bar(
                x=[row["count"]], y=[""],
                orientation="h",
                marker_color=scale_colors_map.get(row["scale"], _MUTED),
                marker_line_width=0,
                name=row["scale"],
                text=row["scale"].replace(" Project", "").upper() + f"  {row['count']}",
                textposition="inside",
                constraintext="inside",
                insidetextanchor="start",
                textfont=dict(family=_MONO, size=9, color="#fff"),
                hovertemplate=f'{row["scale"]}: {row["count"]}<extra></extra>',
            ))
        fig_scale.update_layout(
            **_chart_base(44),
            barmode="stack",
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            margin=dict(l=0, r=0, t=0, b=0),
        )
        st.plotly_chart(fig_scale, use_container_width=True, config={"displayModeBar": False})

        _section("MOST ACTIVE DEVELOPERS")
        from scraper.normalize_developer import is_real_company
        dev_df = df[df["developer_canonical"].apply(
            lambda x: bool(x) and is_real_company(str(x))
        )].copy()
        if len(dev_df) >= 3:
            dev_counts = (
                dev_df.groupby("developer_canonical").size()
                .reset_index(name="n")
                .sort_values("n", ascending=True)
                .tail(12)
            )
            bar_c = [_MUTED] * len(dev_counts)
            if len(bar_c) >= 1:
                bar_c[-1] = _ORANGE
            if len(bar_c) >= 2:
                bar_c[-2] = "#c46010"
            fig_dev = go.Figure(go.Bar(
                x=dev_counts["n"],
                y=dev_counts["developer_canonical"],
                orientation="h",
                marker_color=bar_c,
                marker_line_width=0,
                text=dev_counts["n"],
                textposition="outside",
                textfont=dict(family=_MONO, size=9, color=_MUTED),
                hovertemplate="%{y}: %{x} projects<extra></extra>",
            ))
            fig_dev.update_layout(
                **_chart_base(300, r=28),
                xaxis=dict(visible=False, showgrid=False),
                yaxis=dict(
                    showgrid=False,
                    tickfont=dict(family=_MONO, size=9, color=_MUTED),
                    linecolor=_BORDER, tickcolor=_BORDER,
                ),
            )
            st.plotly_chart(fig_dev, use_container_width=True, config={"displayModeBar": False})

    # ── Top projects table ─────────────────────────────────────────
    extracted = df[df["extraction_done"] & df["total_gsf"].notna()]
    if len(extracted) >= 5:
        _section("LARGEST PROJECTS BY SF")
        top = extracted.nlargest(10, "total_gsf").copy()
        top["dev"] = top["developer_canonical"].where(
            top["developer_canonical"].astype(bool), top["developer"]
        )
        disp = top[["name", "neighborhood", "dev", "total_gsf", "status", "asset_class"]].copy()
        disp["total_gsf"] = disp["total_gsf"].apply(lambda x: f"{int(x):,}")
        disp.columns = ["PROJECT", "NEIGHBORHOOD", "DEVELOPER", "GSF", "STATUS", "TYPE"]
        st.dataframe(disp, use_container_width=True, hide_index=True, height=322)
