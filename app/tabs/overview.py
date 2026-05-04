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
_TEAL    = "#0ea5e9"
_MONO    = "'JetBrains Mono', 'IBM Plex Mono', monospace"

STATUS_COLORS = {
    "Under Review":       _ORANGE,
    "Board Approved":     "#22c55e",
    "Letter of Intent":   "#64748b",
    "Under Construction": "#ef4444",
}


def _section(label: str, mt: int = 12):
    st.markdown(
        f'<p style="font-family:{_MONO};font-size:9px;font-weight:700;'
        f'letter-spacing:0.18em;color:{_MUTED};text-transform:uppercase;'
        f'margin:{mt}px 0 4px 0">{label}</p>',
        unsafe_allow_html=True,
    )


def _chart_base(h: int = 320) -> dict:
    return dict(
        height=h,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family=_MONO, size=10, color=_MUTED),
    )


def render(df: pd.DataFrame, stats: dict):
    # ── Bloomberg stat tiles with count-up ──────────────────────────
    tiles = [
        ("TOTAL PROJECTS",    stats["total"],              "#ffffff", False),
        ("UNDER REVIEW",      stats["under_review"],       _ORANGE,   False),
        ("BOARD APPROVED",    stats["board_approved"],     "#22c55e", False),
        ("LOI",               stats["loi"],                "#64748b", False),
        ("UNDER CONST.",      stats["under_construction"], "#ef4444", False),
        ("RESI UNITS",        stats["total_units"],        "#ffffff", False),
        ("PIPELINE SF",       stats["total_gsf"],          "#ffffff", True),
    ]
    tiles_json = json.dumps([
        {"label": t[0], "raw": int(t[1]), "color": t[2], "big": t[3]}
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

    # ── Status color legend ──────────────────────────────────────────
    legend_items = "".join(
        f'<div style="display:flex;align-items:center;gap:5px">'
        f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:{color};flex-shrink:0"></span>'
        f'<span style="color:{_MUTED};text-transform:uppercase;letter-spacing:0.08em">{label}</span>'
        f'</div>'
        for label, color in STATUS_COLORS.items()
    )
    st.markdown(
        f'<div style="display:flex;align-items:center;gap:18px;padding:6px 0 2px;'
        f'font-family:{_MONO};font-size:9px">{legend_items}</div>',
        unsafe_allow_html=True,
    )

    # ── Main charts ─────────────────────────────────────────────────
    col_a, col_b = st.columns([3, 2])

    with col_a:
        _section("PROJECTS BY NEIGHBORHOOD", mt=10)
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
            **_chart_base(370),
            barmode="stack",
            margin=dict(l=0, r=0, t=28, b=44),
            showlegend=False,
            xaxis=dict(
                visible=True,
                showgrid=True,
                gridcolor=_BORDER,
                tickfont=dict(family=_MONO, size=9, color=_MUTED),
                title=dict(
                    text="NUMBER OF PROJECTS",
                    font=dict(family=_MONO, size=9, color=_MUTED),
                    standoff=8,
                ),
                tickcolor=_BORDER,
                linecolor=_BORDER,
            ),
            yaxis=dict(
                showgrid=False,
                tickfont=dict(family=_MONO, size=10, color=_MUTED),
                linecolor=_BORDER, tickcolor=_BORDER,
            ),
        )
        st.plotly_chart(fig_nbhd, use_container_width=True, config={"displayModeBar": False})

        # GSF by asset class
        extracted = df[df["extraction_done"] & (df["asset_class"] != "")]
        if len(extracted) >= 5:
            _section("GROSS SF BY ASSET CLASS", mt=8)
            ac = (
                extracted.groupby("asset_class")["total_gsf"]
                .sum()
                .reset_index()
                .sort_values("total_gsf", ascending=True)
            )
            ac["gsf_m"] = ac["total_gsf"] / 1e6
            # Extend x-range 30% past max so outside text labels never clip
            x_max = ac["gsf_m"].max() * 1.30
            bar_h = max(180, 38 * len(ac))
            fig_ac = go.Figure(go.Bar(
                x=ac["gsf_m"], y=ac["asset_class"],
                orientation="h",
                marker_color=_ORANGE,
                marker_line_width=0,
                cliponaxis=False,
                text=ac["gsf_m"].apply(lambda v: f"{v:.1f}M"),
                textposition="outside",
                textfont=dict(family=_MONO, size=10, color="#e2e8f0"),
                hovertemplate="<b>%{y}</b><br>%{x:.2f}M SF<extra></extra>",
            ))
            fig_ac.update_layout(
                **_chart_base(bar_h),
                margin=dict(l=0, r=4, t=4, b=36),
                showlegend=False,
                xaxis=dict(
                    visible=True,
                    range=[0, x_max],
                    showgrid=True,
                    gridcolor=_BORDER,
                    tickfont=dict(family=_MONO, size=9, color=_MUTED),
                    title=dict(
                        text="SQUARE FOOTAGE (MILLIONS)",
                        font=dict(family=_MONO, size=9, color=_MUTED),
                        standoff=8,
                    ),
                    tickcolor=_BORDER,
                    linecolor=_BORDER,
                    zeroline=False,
                ),
                yaxis=dict(
                    showgrid=False,
                    automargin=True,
                    tickfont=dict(family=_MONO, size=10, color=_MUTED),
                    linecolor=_BORDER, tickcolor=_BORDER,
                ),
            )
            st.plotly_chart(fig_ac, use_container_width=True, config={"displayModeBar": False})

    with col_b:
        _section("STATUS BREAKDOWN", mt=10)
        status_df = df["status"].value_counts().reset_index()
        status_df.columns = ["status", "count"]
        total_s = int(status_df["count"].sum())
        fig_status = go.Figure()
        for _, row in status_df.iterrows():
            color = STATUS_COLORS.get(row["status"], _MUTED)
            fig_status.add_trace(go.Bar(
                x=[row["count"]], y=[""],
                orientation="h",
                marker_color=color,
                marker_line_width=0,
                name=row["status"],
                hovertemplate=f'{row["status"]}: {int(row["count"])} ({int(row["count"])/total_s*100:.0f}%)<extra></extra>',
            ))
        fig_status.update_layout(
            **_chart_base(50),
            barmode="stack",
            margin=dict(l=0, r=0, t=2, b=0),
            showlegend=False,
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
        )
        st.plotly_chart(fig_status, use_container_width=True, config={"displayModeBar": False})

        # Custom legend — 2-column grid, uniform spacing
        status_order = ["Under Review", "Board Approved", "Under Construction", "Letter of Intent"]
        legend_items = ""
        for s in status_order:
            cnt_arr = status_df[status_df["status"] == s]["count"].values
            cnt = int(cnt_arr[0]) if len(cnt_arr) else 0
            color = STATUS_COLORS.get(s, _MUTED)
            legend_items += (
                f'<div style="display:flex;align-items:center;gap:7px">'
                f'<span style="width:8px;height:8px;border-radius:50%;background:{color};flex-shrink:0"></span>'
                f'<span style="color:#e2e8f0;font-weight:700;min-width:22px">{cnt}</span>'
                f'<span style="color:{_MUTED}">{s.upper()}</span>'
                f'</div>'
            )
        st.markdown(
            f'<div style="display:grid;grid-template-columns:1fr 1fr;gap:4px 12px;'
            f'margin-top:4px;font-family:{_MONO};font-size:9px;letter-spacing:0.06em">'
            f'{legend_items}</div>',
            unsafe_allow_html=True,
        )

        _section("REVIEW SCALE", mt=10)
        scale_df = df["project_scale"].value_counts().reset_index()
        scale_df.columns = ["scale", "count"]
        scale_colors_map = {"Large Project": _ORANGE, "Small Project": _TEAL}
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
            margin=dict(l=0, r=0, t=0, b=0),
            showlegend=False,
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
        )
        st.plotly_chart(fig_scale, use_container_width=True, config={"displayModeBar": False})

        # Inline legend for Review Scale
        st.markdown(
            f'<div style="display:flex;gap:14px;margin:-4px 0 0;'
            f'font-family:{_MONO};font-size:9px;letter-spacing:0.06em">'
            f'<div style="display:flex;align-items:center;gap:5px">'
            f'<span style="display:inline-block;width:8px;height:8px;background:{_ORANGE}"></span>'
            f'<span style="color:{_MUTED}">LARGE</span>'
            f'</div>'
            f'<div style="display:flex;align-items:center;gap:5px">'
            f'<span style="display:inline-block;width:8px;height:8px;background:{_TEAL}"></span>'
            f'<span style="color:{_MUTED}">SMALL</span>'
            f'</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

        _section("MOST ACTIVE DEVELOPERS", mt=10)
        from scraper.normalize_developer import is_real_company
        dev_df = df[df["developer_canonical"].apply(
            lambda x: bool(x) and is_real_company(str(x))
        )].copy()
        if len(dev_df) >= 3:
            dev_counts = (
                dev_df.groupby("developer_canonical").size()
                .reset_index(name="n")
                .sort_values("n", ascending=True)
                .tail(10)
            )
            # Truncate long names with ellipsis
            dev_counts = dev_counts.copy()
            dev_counts["developer_canonical"] = dev_counts["developer_canonical"].apply(
                lambda x: (x[:21] + "…") if len(x) > 23 else x
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
                cliponaxis=False,
                textfont=dict(family=_MONO, size=9, color=_MUTED),
                hovertemplate="%{y}: %{x} projects<extra></extra>",
            ))
            fig_dev.update_layout(
                **_chart_base(280),
                margin=dict(l=8, r=36, t=4, b=36),
                showlegend=False,
                xaxis=dict(
                    visible=True,
                    showgrid=True,
                    gridcolor=_BORDER,
                    tickfont=dict(family=_MONO, size=8, color=_MUTED),
                    title=dict(
                        text="NUMBER OF PROJECTS",
                        font=dict(family=_MONO, size=8, color=_MUTED),
                        standoff=4,
                    ),
                    tickcolor=_BORDER,
                    linecolor=_BORDER,
                    dtick=1,
                    fixedrange=True,
                ),
                yaxis=dict(
                    showgrid=False,
                    automargin=True,
                    tickfont=dict(family=_MONO, size=9, color=_MUTED),
                    linecolor=_BORDER, tickcolor=_BORDER,
                    fixedrange=True,
                ),
            )
            st.plotly_chart(fig_dev, use_container_width=True, config={"displayModeBar": False})

        # Developer market share by SF
        dev_sf_df = df[
            df["extraction_done"] &
            df["total_gsf"].notna() &
            df["developer_canonical"].apply(lambda x: bool(x) and is_real_company(str(x)))
        ].copy()
        if len(dev_sf_df) >= 3:
            _section("DEVELOPER MARKET SHARE BY SF", mt=10)
            dev_sf = (
                dev_sf_df.groupby("developer_canonical")
                .agg(total_sf=("total_gsf", "sum"), n_projects=("id", "count"))
                .reset_index()
                .sort_values("total_sf", ascending=False)
                .head(10)
                .sort_values("total_sf", ascending=True)  # ascending so largest is at top
            )
            dev_sf["sf_m"] = dev_sf["total_sf"] / 1e6
            x_max_sf = dev_sf["sf_m"].max() * 1.32
            dev_h = max(200, 38 * len(dev_sf))
            fig_dev_sf = go.Figure(go.Bar(
                x=dev_sf["sf_m"],
                y=dev_sf["developer_canonical"],
                orientation="h",
                marker_color=_ORANGE,
                marker_line_width=0,
                cliponaxis=False,
                text=dev_sf["sf_m"].apply(lambda v: f"{v:.1f}M"),
                textposition="outside",
                textfont=dict(family=_MONO, size=9, color="#e2e8f0"),
                customdata=dev_sf["n_projects"],
                hovertemplate=(
                    "<b>%{y}</b><br>"
                    "%{x:.2f}M SF<br>"
                    "%{customdata} project(s)<extra></extra>"
                ),
            ))
            fig_dev_sf.update_layout(
                **_chart_base(dev_h),
                margin=dict(l=8, r=4, t=4, b=36),
                showlegend=False,
                xaxis=dict(
                    visible=True,
                    range=[0, x_max_sf],
                    showgrid=True,
                    gridcolor=_BORDER,
                    tickfont=dict(family=_MONO, size=8, color=_MUTED),
                    title=dict(
                        text="SQUARE FOOTAGE (MILLIONS)",
                        font=dict(family=_MONO, size=8, color=_MUTED),
                        standoff=6,
                    ),
                    tickcolor=_BORDER,
                    linecolor=_BORDER,
                    zeroline=False,
                    fixedrange=True,
                ),
                yaxis=dict(
                    showgrid=False,
                    automargin=True,
                    tickfont=dict(family=_MONO, size=9, color=_MUTED),
                    linecolor=_BORDER, tickcolor=_BORDER,
                    fixedrange=True,
                ),
            )
            st.plotly_chart(fig_dev_sf, use_container_width=True, config={"displayModeBar": False})

    # ── Top projects table ─────────────────────────────────────────
    extracted = df[df["extraction_done"] & df["total_gsf"].notna()]
    if len(extracted) >= 5:
        _section("LARGEST PROJECTS BY SF", mt=8)
        top = extracted.nlargest(10, "total_gsf").copy()
        top["dev"] = top["developer_canonical"].where(
            top["developer_canonical"].astype(bool), top["developer"]
        )
        disp = top[["name", "neighborhood", "dev", "total_gsf", "status", "asset_class"]].copy()
        disp["total_gsf"] = disp["total_gsf"].apply(lambda x: f"{int(x):,}")
        disp.columns = ["PROJECT", "NEIGHBORHOOD", "DEVELOPER", "GSF", "STATUS", "TYPE"]
        st.dataframe(disp, use_container_width=True, hide_index=True, height=322)
