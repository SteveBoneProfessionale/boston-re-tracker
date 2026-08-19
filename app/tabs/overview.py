import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components

from app.data import (
    summary_stats, STAGE_COLORS, STAGE_ORDER,
    REVIEW_SCALE_COLORS, review_scale_vocab,
    RESOLUTION_METHODS, resolution_method, METHOD_ORDER,
)

_BG      = "#0d0f12"
_BG2     = "#141720"
_BORDER  = "#1E2530"
_ORANGE  = "#F5821E"
_MUTED   = "#8A9BB0"
_TEAL    = "#0ea5e9"
_MONO    = "'JetBrains Mono', 'IBM Plex Mono', monospace"

# Consistent chart margins
_M_AXIS  = dict(l=0, r=4, t=6, b=40)   # charts with x-axis label
_M_THIN  = dict(l=0, r=0, t=2, b=0)    # thin bar charts (no axis)

# Kept for backward compat with any external reference; charts below color by
# the normalized cross-city `stage` field (STAGE_COLORS) instead, since
# Boston's 4-value status vocab and Cambridge's 7-value one share no values.
STATUS_COLORS = {
    "Under Review":       _ORANGE,
    "Board Approved":     "#22c55e",
    "Letter of Intent":   "#64748b",
    "Under Construction": "#ef4444",
}


def _section(label: str, mt: int = 14):
    st.markdown(
        f'<p style="font-family:{_MONO};font-size:9px;font-weight:700;'
        f'letter-spacing:0.18em;color:{_MUTED};text-transform:uppercase;'
        f'margin:{mt}px 0 4px 0">{label}</p>',
        unsafe_allow_html=True,
    )


def _chart_base(h: int = 300) -> dict:
    return dict(
        height=h,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family=_MONO, size=10, color=_MUTED),
    )


def _xaxis(title: str, grid: bool = True, dtick=None, x_range=None) -> dict:
    d = dict(
        visible=True,
        showgrid=grid,
        gridcolor=_BORDER,
        tickfont=dict(family=_MONO, size=9, color=_MUTED),
        title=dict(text=title, font=dict(family=_MONO, size=9, color=_MUTED), standoff=8),
        tickcolor=_BORDER,
        linecolor=_BORDER,
        zeroline=False,
        fixedrange=True,
    )
    if dtick is not None:
        d["dtick"] = dtick
    if x_range is not None:
        d["range"] = x_range
    return d


def _yaxis(automargin: bool = True) -> dict:
    return dict(
        showgrid=False,
        automargin=automargin,
        tickfont=dict(family=_MONO, size=9, color=_MUTED),
        linecolor=_BORDER,
        tickcolor=_BORDER,
        fixedrange=True,
    )


def render(df: pd.DataFrame, stats: dict):
    # ── City / conditional-alternative filters ───────────────────────
    fc1, fc2, _ = st.columns([1.2, 2.5, 4])
    cities = ["All"] + sorted([c for c in df["city"].unique() if c])
    city_f = fc1.selectbox("CITY", cities, key="ov_city")
    include_conditional = fc2.checkbox(
        "Include competing/conditional plans in totals",
        value=False, key="ov_include_conditional",
        help="Some Cambridge projects (e.g. the MXD Infill PB315 case) have more than one "
             "unbuilt alternative plan on file for the same site. Off by default to avoid "
             "double-counting GFA/units for a site that will only be built once.",
    )
    if city_f != "All":
        df = df[df["city"] == city_f]
    stats = summary_stats(df, include_conditional=include_conditional)

    if stats["conditional_alternative_count"] and not include_conditional:
        st.caption(
            f'{stats["conditional_alternative_count"]} project(s) with competing alternative '
            f'plans on the same site are excluded from the totals and charts below. '
            f'Check "Include competing/conditional plans" above to include them.'
        )
    st.caption(f'BOSTON {stats["total_boston"]:,}  ·  CAMBRIDGE {stats["total_cambridge"]:,}')

    # Apply the same exclusion to every chart below, not just the KPI tiles --
    # summary_stats() computed its own totals from a local copy before this
    # point, so this is safe to do now for the rest of the function.
    if not include_conditional and "conditional_alternative" in df.columns:
        df = df[~df["conditional_alternative"]]
    if not stats["stage_reconciles"]:
        st.warning(
            "Stage-card counts don't reconcile to Total Projects — some project(s) have no "
            "recognized stage. Check for a status value outside the known Boston/Cambridge "
            "vocabularies.",
            icon="⚠",
        )

    # ── Bloomberg stat tiles with count-up ──────────────────────────
    # Cards are driven by the normalized `stage` field (shared across cities),
    # not a market-native status string, and labeled with the stage names
    # directly -- Boston-specific labels ("LOI", "Board Approved") over
    # Cambridge data would be wrong even if the counts were right.
    tiles = [("TOTAL PROJECTS", stats["total"], "#ffffff", False)]
    for stage in STAGE_ORDER:
        tiles.append((stage.upper(), stats["stage_counts"][stage], STAGE_COLORS[stage], False))
    tiles.append(("RESI UNITS", stats["total_units"], "#ffffff", False))
    tiles.append(("PIPELINE SF", stats["total_gsf"], "#ffffff", True))

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
  grid-template-columns:repeat(8,1fr);
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

    # ── Stage color legend — full width, above both columns ──────────
    # Normalized stage (not raw status) since it's shared across both cities.
    legend_items = "".join(
        f'<div style="display:flex;align-items:center;gap:5px">'
        f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;'
        f'background:{color};flex-shrink:0"></span>'
        f'<span style="color:{_MUTED};text-transform:uppercase;letter-spacing:0.08em">{label}</span>'
        f'</div>'
        for label, color in STAGE_COLORS.items()
    )
    st.markdown(
        f'<div style="display:flex;align-items:center;gap:20px;padding:8px 0 4px;'
        f'font-family:{_MONO};font-size:9px;border-bottom:1px solid {_BORDER};'
        f'margin-bottom:2px">{legend_items}</div>',
        unsafe_allow_html=True,
    )

    from scraper.normalize_developer import is_real_company

    # ── Row 1: Neighborhood | Most Active Developers ──────────────────
    col_a, col_b = st.columns(2)

    with col_a:
        _section("PROJECTS BY NEIGHBORHOOD", mt=14)
        nbhd_status = (
            df[df["neighborhood"].astype(bool)]
            .groupby(["neighborhood", "stage"])
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
            color="stage",
            orientation="h",
            category_orders={
                "neighborhood": nbhd_order,
                "stage": STAGE_ORDER,
            },
            color_discrete_map=STAGE_COLORS,
        )
        fig_nbhd.update_traces(marker_line_width=0)
        fig_nbhd.update_layout(
            **_chart_base(450),
            barmode="stack",
            margin=dict(l=0, r=4, t=6, b=40),
            showlegend=False,
            xaxis=_xaxis("NUMBER OF PROJECTS"),
            yaxis=dict(
                showgrid=False,
                automargin=True,
                tickfont=dict(family=_MONO, size=10, color=_MUTED),
                linecolor=_BORDER, tickcolor=_BORDER,
                fixedrange=True,
            ),
        )
        st.plotly_chart(fig_nbhd, use_container_width=True, config={"displayModeBar": False})

    with col_b:
        _section("MOST ACTIVE DEVELOPERS", mt=14)
        has_real_dev = df["developer_canonical"].apply(lambda x: bool(x) and is_real_company(str(x)))
        dev_df = df[has_real_dev].copy()
        n_no_dev = int((~has_real_dev).sum())
        if len(dev_df) >= 3:
            dev_counts = (
                dev_df.groupby("developer_canonical").size()
                .reset_index(name="n")
                .sort_values("n", ascending=True)
                .tail(10)
            )
            dev_counts = dev_counts.copy()
            dev_counts["developer_canonical"] = dev_counts["developer_canonical"].apply(
                lambda x: (x[:21] + "…") if len(x) > 23 else x
            )
            x_max_n = dev_counts["n"].max() * 1.45
            order = dev_counts["developer_canonical"].tolist()

            # Split each developer's bar by how the name was resolved, so an
            # inferred name never looks like a verified one. Hatched segments
            # are web-corroborated; solid are registry-confirmed.
            dev_df = dev_df.copy()
            dev_df["_method"] = dev_df["developer_resolution_method"].apply(resolution_method)
            dev_df["_label"] = dev_df["developer_canonical"].apply(
                lambda x: (x[:21] + "…") if len(x) > 23 else x
            )
            by_method = (
                dev_df[dev_df["_label"].isin(order)]
                .groupby(["_label", "_method"]).size().reset_index(name="n")
            )

            fig_dev = go.Figure()
            for method in METHOD_ORDER:
                seg = by_method[by_method["_method"] == method]
                if seg.empty:
                    continue
                meta = RESOLUTION_METHODS[method]
                fig_dev.add_trace(go.Bar(
                    x=seg["n"], y=seg["_label"],
                    orientation="h",
                    name=meta["label"],
                    marker=dict(
                        color=_ORANGE if meta["verified"] else "#8A6A3F",
                        line_width=0,
                        pattern=dict(shape=meta["pattern"], fgcolor="#1b1e26", size=5),
                    ),
                    cliponaxis=False,
                    hovertemplate="%{y}: %{x} project(s) — " + meta["label"] + "<extra></extra>",
                ))
            n_inferred = int((dev_df["_method"] == "web_corroborated").sum())
            fig_dev.update_layout(
                **_chart_base(450),
                margin=_M_AXIS,
                barmode="stack",
                showlegend=bool(n_inferred),
                legend=dict(orientation="h", yanchor="bottom", y=1.0, x=0,
                            font=dict(family=_MONO, size=9, color=_MUTED)),
                xaxis=_xaxis("NUMBER OF PROJECTS", dtick=1, x_range=[0, x_max_n]),
                yaxis=dict(**_yaxis(), categoryorder="array", categoryarray=order),
            )
            st.plotly_chart(fig_dev, use_container_width=True, config={"displayModeBar": False})
            if n_inferred:
                st.caption(
                    f"{n_inferred} project(s) have a developer inferred from press coverage "
                    f"(hatched) rather than confirmed against the corporate registry (solid). "
                    f"Source links are on each project's detail view."
                )
        else:
            st.caption("No developer data for this selection.")
        if n_no_dev:
            st.caption(f"{n_no_dev} project(s) in this selection have no developer on record — excluded above.")

    # ── SF charts row — Gross SF and Developer Market Share side by side
    col_c, col_d = st.columns(2)

    # See load_projects() -- true once a row's financial fields are ready to
    # chart, regardless of which market/pipeline it came from.
    has_financials = df["has_financials"]

    # total_gsf is None (not 0) for any row whose source GFA was the literal
    # string "TBD" -- guarded at ingestion time (cambridge_devlog.py's
    # to_int()), never coerced to 0. `.notna()` here excludes those rows from
    # the SF ranking rather than understating anyone's total.
    n_tbd_gfa = int(df["total_gfa_tbd"].sum()) if "total_gfa_tbd" in df.columns else 0

    extracted_ac = df[has_financials & (df["asset_class"] != "")]
    dev_sf_df = df[
        has_financials &
        df["total_gsf"].notna() &
        df["developer_canonical"].apply(lambda x: bool(x) and is_real_company(str(x)))
    ].copy()

    with col_c:
        if len(extracted_ac) >= 5:
            _section("GROSS SF BY ASSET CLASS", mt=14)
            ac = (
                extracted_ac.groupby("asset_class")["total_gsf"]
                .sum()
                .reset_index()
                .sort_values("total_gsf", ascending=True)
            )
            ac["gsf_m"] = ac["total_gsf"] / 1e6
            x_max_ac = ac["gsf_m"].max() * 1.30
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
                **_chart_base(320),
                margin=_M_AXIS,
                showlegend=False,
                xaxis=_xaxis("SQUARE FOOTAGE (MILLIONS)", x_range=[0, x_max_ac]),
                yaxis=_yaxis(),
            )
            st.plotly_chart(fig_ac, use_container_width=True, config={"displayModeBar": False})
        else:
            st.caption("No asset-class SF data for this selection.")

    with col_d:
        if len(dev_sf_df) >= 3:
            _section("DEVELOPER MARKET SHARE BY SF", mt=14)
            dev_sf = (
                dev_sf_df.groupby("developer_canonical")
                .agg(total_sf=("total_gsf", "sum"), n_projects=("id", "count"))
                .reset_index()
                .sort_values("total_sf", ascending=False)
                .head(10)
                .sort_values("total_sf", ascending=True)
            )
            dev_sf["sf_m"] = dev_sf["total_sf"] / 1e6
            x_max_sf = dev_sf["sf_m"].max() * 1.32
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
                **_chart_base(320),
                margin=_M_AXIS,
                showlegend=False,
                xaxis=_xaxis("SQUARE FOOTAGE (MILLIONS)", x_range=[0, x_max_sf]),
                yaxis=_yaxis(),
            )
            st.plotly_chart(fig_dev_sf, use_container_width=True, config={"displayModeBar": False})
        else:
            st.caption("No developer SF data for this selection.")
        if n_tbd_gfa:
            st.caption(f"{n_tbd_gfa} project(s) with GFA marked TBD by the source are excluded from SF rankings.")

    # ── Row 3: Status Breakdown | Review Scale ────────────────────────
    col_e, col_f = st.columns(2)

    with col_e:
        # Grouped by normalized stage, not raw status -- Boston's and
        # Cambridge's status vocabularies share no values, so this is the
        # only breakdown that means the same thing across both cities.
        # Native status per project is still shown on its detail view.
        _section("STAGE BREAKDOWN", mt=14)
        status_counts = (
            df[df["stage"] != ""].groupby("stage").size()
            .reset_index(name="count")
            .sort_values("count", ascending=True)
        )
        status_counts["color"] = status_counts["stage"].map(STAGE_COLORS).fillna(_MUTED)
        x_max_st = status_counts["count"].max() * 1.45 if len(status_counts) else 1
        fig_status = go.Figure(go.Bar(
            x=status_counts["count"],
            y=status_counts["stage"],
            orientation="h",
            marker_color=status_counts["color"].tolist(),
            marker_line_width=0,
            cliponaxis=False,
            text=status_counts["count"],
            textposition="outside",
            textfont=dict(family=_MONO, size=9, color=_MUTED),
            hovertemplate="%{y}: %{x}<extra></extra>",
        ))
        fig_status.update_layout(
            **_chart_base(200),
            margin=dict(l=0, r=4, t=6, b=4),
            showlegend=False,
            yaxis=dict(**_yaxis(), ticks="", showline=False),
        )
        fig_status.update_xaxes(
            visible=False,
            showticklabels=False,
            showgrid=False,
            zeroline=False,
            rangeslider=dict(visible=False),
        )
        st.plotly_chart(fig_status, use_container_width=True, config={"displayModeBar": False})

    with col_f:
        _section("REVIEW SCALE", mt=14)
        # Reads only the normalized `review_scale` column -- no market branches
        # here. Which values are possible, and whether the concept applies at
        # all, come from the market registry (app/data.py::MARKETS), so a new
        # market is a registry entry rather than a change to this chart.
        vocab = review_scale_vocab(df["city"].unique())
        if not vocab:
            # Distinct from "no data": these markets have no statutory scale
            # classification, so an empty chart would misread as missing data.
            cities_here = sorted({c for c in df["city"].unique() if c})
            who = cities_here[0] if len(cities_here) == 1 else "The selected market(s)"
            st.markdown(
                f'<div style="border:1px solid {_BORDER};background:{_BG2};padding:22px 16px;'
                f'font-family:{_MONO};font-size:11px;color:{_MUTED};line-height:1.6;height:200px;'
                f'display:flex;align-items:center;justify-content:center;text-align:center">'
                f'NOT APPLICABLE — {who} {"does" if len(cities_here) == 1 else "do"} not '
                f'classify projects by review scale.</div>',
                unsafe_allow_html=True,
            )
            st.markdown('<div style="height:26px"></div>', unsafe_allow_html=True)
        else:
            # ONLY the market's declared vocabulary may render as a category.
            # The Rhode Island ingest wrote whatever an agenda's "classification"
            # field happened to hold straight into review_scale without checking
            # it, so the column carries section headings ("PUBLIC HEARING",
            # "NEW BUSINESS") and whole sentences, including a land use
            # attorney's name. Those are not scales and must never become axis
            # labels. Anything outside the vocabulary folds into Unclassified,
            # which is a state the tracker already has a meaning for.
            _scales = df["review_scale"].replace("", pd.NA)
            _scales = _scales.where(_scales.isin(vocab), other=pd.NA)
            scale_counts = _scales.dropna().value_counts().reset_index()
            scale_counts.columns = ["scale", "count"]
            _unclassified = int(len(df) - scale_counts["count"].sum())
            if _unclassified > 0:
                scale_counts = pd.concat([
                    scale_counts,
                    pd.DataFrame([{"scale": "Unclassified", "count": _unclassified}]),
                ], ignore_index=True)
            scale_counts = scale_counts.sort_values("count", ascending=True)
            scale_counts["color"] = scale_counts["scale"].map(REVIEW_SCALE_COLORS).fillna(_MUTED)
            x_max_scale = scale_counts["count"].max() * 1.45 if len(scale_counts) else 1
            fig_scale = go.Figure(go.Bar(
                x=scale_counts["count"],
                y=scale_counts["scale"],
                orientation="h",
                marker_color=scale_counts["color"].tolist(),
                marker_line_width=0,
                cliponaxis=False,
                text=scale_counts["count"],
                textposition="outside",
                textfont=dict(family=_MONO, size=9, color=_MUTED),
                hovertemplate="%{y}: %{x}<extra></extra>",
            ))
            fig_scale.update_layout(
                **_chart_base(200),
                margin=dict(l=0, r=4, t=6, b=4),
                showlegend=False,
                yaxis=dict(**_yaxis(), ticks="", showline=False),
            )
            fig_scale.update_xaxes(
                visible=False,
                showticklabels=False,
                showgrid=False,
                zeroline=False,
                rangeslider=dict(visible=False),
            )
            st.plotly_chart(fig_scale, use_container_width=True, config={"displayModeBar": False})
            # Legend covers the full vocabulary of the markets in scope, not
            # just the values that happen to be present, so a tier with zero
            # projects still reads as a tier rather than as a missing category.
            scale_legend = "".join(
                f'<div style="display:flex;align-items:center;gap:5px">'
                f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;'
                f'background:{REVIEW_SCALE_COLORS.get(label, _MUTED)};flex-shrink:0"></span>'
                f'<span style="color:{_MUTED};text-transform:uppercase;letter-spacing:0.08em">{label}</span>'
                f'</div>'
                for label in vocab
            )
            st.markdown(
                f'<div style="display:flex;align-items:center;gap:20px;padding:4px 0 8px;'
                f'font-family:{_MONO};font-size:9px;flex-wrap:wrap">{scale_legend}</div>',
                unsafe_allow_html=True,
            )

    # ── Largest projects table — full width below both columns ───────
    extracted = df[has_financials & df["total_gsf"].notna()]
    if len(extracted) >= 5:
        _section("LARGEST PROJECTS BY SF", mt=14)
        top = extracted.nlargest(10, "total_gsf").copy()
        top["dev"] = top["developer_canonical"].where(
            top["developer_canonical"].astype(bool), top["developer"]
        )
        disp = top[["name", "neighborhood", "dev", "total_gsf", "status", "asset_class"]].copy()
        disp["total_gsf"] = disp["total_gsf"].apply(lambda x: f"{int(x):,}")
        disp.columns = ["PROJECT", "NEIGHBORHOOD", "DEVELOPER", "GSF", "STATUS", "TYPE"]
        st.dataframe(disp, use_container_width=True, hide_index=True, height=322)
    elif len(df):
        st.caption("Not enough SF data in this selection to rank largest projects.")
