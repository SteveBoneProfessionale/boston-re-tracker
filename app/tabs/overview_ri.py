"""
Rhode Island landing page.

WHY THIS IS A SEPARATE VIEW. The shared overview is built for Boston, where
square footage is stated on nearly every filing. In Rhode Island it is stated
on about 18%, so every SF-based chart there measures DISCLOSURE rather than
development: "Gross SF by Asset Class" put Institutional on top because school
projects are the ones that publish a floor area, not because Providence is
building schools. "Developer Market Share by SF" was worse -- it ranked EQT
Exeter first on 0.3M SF, which is Emblem 125, a finished and leasing building.

So this page is built only on fields that are actually populated: address,
stage, neighbourhood, asset class, units, and hearing dates. Hearing dates are
the densest signal in the dataset -- 1,101 recorded appearances across 216
Providence projects -- and they answer two questions nothing else can: how much
is being filed lately, and how long approval takes.

TWO RULES APPLIED THROUGHOUT:

  * A row whose value is zero is not plotted. An empty bar is not information.
  * Where a chart is drawn from a subset, the denominator goes ON the chart,
    not in a caption underneath. A developer chart covering a quarter of the
    city has to say so where it is read.
"""

import re
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from app.data import STAGE_COLORS, STAGE_ORDER, load_stage_history, RI_SF_NOTE, RI_SF_NOTE_TITLE

_BG2 = "#141720"
_BORDER = "#1E2530"
_ORANGE = "#F5821E"
_MUTED = "#8A9BB0"
_TEXT = "#e2e8f0"
_MONO = "'JetBrains Mono', 'IBM Plex Mono', monospace"

RI_CITIES = ("Providence", "Warwick", "Cranston", "Pawtucket", "Newport")

# Asset-class palette. Distinct hues, since these stack and sit side by side.
AC_COLORS = {
    "Residential": "#38bdf8", "Mixed-Use": "#a78bfa", "Office": "#f59e0b",
    "Lab/Research": "#2dd4bf", "Retail": "#fb7185", "Hotel": "#f472b6",
    "Industrial": "#94a3b8", "Institutional": "#4ade80", "Parking": "#64748b",
    "Other": "#475569",
}


def _section(label, mt=14, sub=None):
    st.markdown(
        f'<div style="margin-top:{mt}px;margin-bottom:6px">'
        f'<span style="font-family:{_MONO};font-size:11px;letter-spacing:.14em;'
        f'color:{_TEXT};font-weight:700">{label}</span>'
        + (f'<span style="font-family:{_MONO};font-size:10px;color:{_MUTED};'
           f'margin-left:10px">{sub}</span>' if sub else "")
        + "</div>", unsafe_allow_html=True)


def _base(h=300, legend=False):
    return dict(
        height=h, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=8, r=14, t=8, b=8),
        font=dict(family=_MONO, size=10, color=_MUTED),
        showlegend=legend,
        legend=dict(orientation="h", y=-0.16, font=dict(size=9), bgcolor="rgba(0,0,0,0)"),
        hoverlabel=dict(font=dict(family=_MONO, size=11)),
    )


def _ax(grid=True):
    return dict(showgrid=grid, gridcolor=_BORDER, zeroline=False,
                linecolor=_BORDER, tickfont=dict(size=9))


def _note(fig, text):
    """The denominator, drawn INSIDE the plot rather than captioned below it."""
    fig.add_annotation(
        text=text, xref="paper", yref="paper", x=1, y=1.02,
        xanchor="right", yanchor="bottom", showarrow=False,
        font=dict(family=_MONO, size=9, color=_MUTED))
    return fig


def _empty(msg, h=240):
    st.markdown(
        f'<div style="border:1px solid {_BORDER};background:{_BG2};padding:20px;'
        f'font-family:{_MONO};font-size:11px;color:{_MUTED};height:{h}px;display:flex;'
        f'align-items:center;justify-content:center;text-align:center">{msg}</div>',
        unsafe_allow_html=True)


def _tile(label, value, sub, color="#ffffff"):
    return (
        f'<div style="border:1px solid {_BORDER};background:{_BG2};padding:12px 14px;flex:1">'
        f'<div style="font-family:{_MONO};font-size:10px;letter-spacing:.12em;color:{_MUTED}">{label}</div>'
        f'<div style="font-family:{_MONO};font-size:26px;font-weight:700;color:{color};'
        f'line-height:1.25">{value}</div>'
        f'<div style="font-family:{_MONO};font-size:9.5px;color:{_MUTED};line-height:1.45">{sub}</div>'
        f'</div>')


# ── developer name folding ───────────────────────────────────────────────
_SUFFIX = re.compile(
    r"\b(?:llc|l\.l\.c\.?|inc|incorporated|corp|corporation|co|company|companies|"
    r"lp|llp|ltd|trust|group|properties|property|development|developments|"
    r"realty|holdings|partners|partnership|associates|enterprises)\b\.?", re.I)


def fold_developer(name):
    """Group spelling variants of one firm.

    "The Procaccianti Group" and "Procaccianti Companies" are the same
    developer with two Providence projects between them. Counted as written
    they are two firms with one project each, and both fall below any
    activity threshold -- so the most active developer in the city disappears
    from a chart of the most active developers.
    """
    n = (name or "").strip()
    if not n:
        return None
    n = re.sub(r"^the\s+", "", n, flags=re.I)
    n = _SUFFIX.sub(" ", n)
    n = re.sub(r"[^A-Za-z0-9& ]", " ", n)
    n = re.sub(r"\s+", " ", n).strip().lower()
    return n or None


def render(df, city_label="Rhode Island"):
    d = df[df["city"].isin(RI_CITIES)].copy() if "city" in df.columns else df.copy()
    if city_label in RI_CITIES:
        d = d[d["city"] == city_label]
    # Delivered buildings and dead applications stay in the table and the
    # filters, but they are not pipeline and must not reach these counts.
    delivered = d[d["stage"] == "Complete"] if "stage" in d.columns else d.iloc[0:0]
    dead = d[d.get("project_status_filing", "").isin(["Withdrawn", "Denied"])]         if "project_status_filing" in d.columns else d.iloc[0:0]
    d = d[~d.index.isin(delivered.index) & ~d.index.isin(dead.index)]
    if not len(d):
        _empty("No projects in this selection.")
        return

    hist = load_stage_history()
    hist = hist[hist["project_id"].isin(set(d["id"]))] if len(hist) else hist

    # ── header strip ────────────────────────────────────────────────
    n_total = len(d)
    unit_rows = d[d["residential_units"].fillna(0) > 0]
    n_units = int(unit_rows["residential_units"].sum())

    if len(hist):
        cutoff = hist["date"].max() - pd.Timedelta(days=365)
        recent = hist[hist["date"] >= cutoff]["project_id"].nunique()
        recent_sub = f"first or further hearing since {cutoff.date():%b %Y}"
    else:
        recent, recent_sub = 0, "no hearing dates"

    # Median months from first hearing to approval.
    #
    # Measured ONLY where the approval came after an earlier hearing. Most
    # approved projects here were approved at their FIRST recorded hearing --
    # 35 of 49 in Providence -- because the corpus frequently captures the
    # approving meeting and not the ones before it. Including those gives a
    # median of zero, which reads as a broken card rather than as the
    # sampling artefact it is. Restricting to the projects whose review is
    # actually visible measures review duration; the ones excluded are
    # counted on the card so the restriction is not hidden.
    med, med_sub = None, "no approval events recorded"
    if len(hist):
        firsts = hist.groupby("project_id")["date"].min()
        appr = hist[(hist["stage"] == "Approved") |
                    (hist["outcome"].str.contains("approv", case=False, na=False))]
        if len(appr):
            a = appr.groupby("project_id")["date"].min()
            both = pd.concat([firsts.rename("f"), a.rename("a")], axis=1).dropna()
            visible = both[both["a"] > both["f"]]
            same_day = int((both["a"] == both["f"]).sum())
            if len(visible):
                med = ((visible["a"] - visible["f"]).dt.days / 30.44).median()
                med_sub = (f"across {len(visible)} projects with hearings before approval · "
                           f"{same_day} more approved at their first recorded hearing")
            elif same_day:
                med_sub = f"all {same_day} approvals landed at the first recorded hearing"

    st.markdown(
        '<div style="display:flex;gap:10px;margin-bottom:6px">'
        + _tile("TOTAL PROJECTS", f"{n_total:,}",
                f"{city_label} · pipeline only · {len(delivered)} delivered and "
                f"{len(dead)} dead excluded")
        + _tile("RESIDENTIAL UNITS", f"{n_units:,}",
                f"across {len(unit_rows)} of {n_total} projects stating a unit count", "#38bdf8")
        + _tile("FILED, LAST 12 MONTHS", f"{recent:,}", recent_sub, _ORANGE)
        + _tile("MEDIAN MONTHS TO APPROVAL",
                f"{med:.0f}" if med is not None else "—", med_sub, "#22c55e")
        + "</div>", unsafe_allow_html=True)

    with st.expander(RI_SF_NOTE_TITLE, expanded=False):
        st.markdown(RI_SF_NOTE)

    # ── 1. projects by neighbourhood, stacked by stage ───────────────
    _section("PROJECTS BY NEIGHBOURHOOD, BY STAGE")
    nb = d[d["neighborhood"].astype(bool)]
    if not len(nb):
        _empty("No neighbourhood recorded.")
    else:
        piv = nb.pivot_table(index="neighborhood", columns="stage", values="id",
                             aggfunc="count", fill_value=0)
        piv = piv.loc[piv.sum(axis=1) > 0]
        piv = piv.loc[piv.sum(axis=1).sort_values().index]
        fig = go.Figure()
        for s in STAGE_ORDER:
            if s in piv.columns and piv[s].sum() > 0:
                fig.add_bar(y=piv.index, x=piv[s], name=s, orientation="h",
                            marker_color=STAGE_COLORS[s])
        fig.update_layout(barmode="stack", xaxis=_ax(), yaxis=_ax(False),
                          **_base(h=max(260, 20 * len(piv)), legend=True))
        _note(fig, f"{len(nb)} of {n_total} projects have a neighbourhood")
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    # ── 2. units by neighbourhood | project count by asset class ─────
    c1, c2 = st.columns(2)
    with c1:
        _section("RESIDENTIAL UNITS BY NEIGHBOURHOOD")
        u = unit_rows[unit_rows["neighborhood"].astype(bool)]
        if not len(u):
            _empty("No unit counts with a neighbourhood.")
        else:
            g = u.groupby("neighborhood")["residential_units"].sum()
            g = g[g > 0].sort_values()
            fig = go.Figure(go.Bar(y=g.index, x=g.values, orientation="h",
                                   marker_color="#38bdf8",
                                   text=[f"{int(v):,}" for v in g.values],
                                   textposition="outside", textfont=dict(size=9)))
            fig.update_layout(xaxis=_ax(), yaxis=_ax(False),
                              **_base(h=max(260, 22 * len(g))))
            _note(fig, f"{len(u)} of {n_total} projects state units")
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    with c2:
        _section("PROJECT COUNT BY ASSET CLASS")
        a = d[d["asset_class"].astype(bool)]
        if not len(a):
            _empty("No asset class recorded.")
        else:
            g = a.groupby("asset_class").size()
            g = g[g > 0].sort_values()
            fig = go.Figure(go.Bar(y=g.index, x=g.values, orientation="h",
                                   marker_color=[AC_COLORS.get(i, _MUTED) for i in g.index],
                                   text=[f"{v}" for v in g.values],
                                   textposition="outside", textfont=dict(size=9)))
            fig.update_layout(xaxis=_ax(), yaxis=_ax(False),
                              **_base(h=max(260, 22 * len(g))))
            _note(fig, f"{len(a)} of {n_total} classified")
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    # ── 3. asset class stacked by stage ──────────────────────────────
    _section("ASSET CLASS, BY STAGE")
    a = d[d["asset_class"].astype(bool)]
    if not len(a):
        _empty("No asset class recorded.")
    else:
        piv = a.pivot_table(index="asset_class", columns="stage", values="id",
                            aggfunc="count", fill_value=0)
        piv = piv.loc[piv.sum(axis=1) > 0]
        piv = piv.loc[piv.sum(axis=1).sort_values().index]
        fig = go.Figure()
        for s in STAGE_ORDER:
            if s in piv.columns and piv[s].sum() > 0:
                fig.add_bar(y=piv.index, x=piv[s], name=s, orientation="h",
                            marker_color=STAGE_COLORS[s])
        fig.update_layout(barmode="stack", xaxis=_ax(), yaxis=_ax(False),
                          **_base(h=max(260, 26 * len(piv)), legend=True))
        _note(fig, f"{len(a)} of {n_total} classified")
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    # ── 4. asset class by neighbourhood ──────────────────────────────
    _section("ASSET CLASS BY NEIGHBOURHOOD")
    an = d[d["asset_class"].astype(bool) & d["neighborhood"].astype(bool)]
    if not len(an):
        _empty("Not enough asset class and neighbourhood data.")
    else:
        piv = an.pivot_table(index="neighborhood", columns="asset_class", values="id",
                             aggfunc="count", fill_value=0)
        piv = piv.loc[piv.sum(axis=1) > 0]
        piv = piv.loc[piv.sum(axis=1).sort_values().index]
        fig = go.Figure()
        for ac in piv.columns:
            if piv[ac].sum() > 0:
                fig.add_bar(y=piv.index, x=piv[ac], name=ac, orientation="h",
                            marker_color=AC_COLORS.get(ac, _MUTED))
        fig.update_layout(barmode="stack", xaxis=_ax(), yaxis=_ax(False),
                          **_base(h=max(280, 20 * len(piv)), legend=True))
        _note(fig, f"{len(an)} of {n_total} have both")
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    # ── 5. filings per quarter, split by asset class ─────────────────
    _section("FILINGS PER QUARTER, BY ASSET CLASS")
    if not len(hist):
        _empty("No hearing dates recorded.")
    else:
        firsts = hist.groupby("project_id")["date"].min().rename("first")
        j = d.set_index("id").join(firsts, how="inner")
        j = j[j["first"].notna()].copy()
        if not len(j):
            _empty("No hearing dates recorded.")
        else:
            j["q"] = j["first"].dt.to_period("Q").astype(str)
            j["ac"] = j["asset_class"].replace("", "Unclassified")
            piv = j.pivot_table(index="q", columns="ac", values="name",
                                aggfunc="count", fill_value=0).sort_index()
            fig = go.Figure()
            for ac in piv.columns:
                if piv[ac].sum() > 0:
                    fig.add_bar(x=piv.index, y=piv[ac], name=ac,
                                marker_color=AC_COLORS.get(ac, "#334155"))
            fig.update_layout(barmode="stack", xaxis=_ax(False), yaxis=_ax(),
                              **_base(h=300, legend=True))
            _note(fig, f"{len(j)} of {n_total} projects, dated by first hearing")
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    # ── 6. stage breakdown | 7. most active developers ───────────────
    c3, c4 = st.columns(2)
    with c3:
        _section("STAGE BREAKDOWN")
        staged = d[d["stage"].astype(bool)]
        nostage = n_total - len(staged)
        if not len(staged):
            _empty("No stages recorded.")
        else:
            g = staged.groupby("stage").size()
            g = g[g > 0]
            order = [s for s in STAGE_ORDER if s in g.index]
            g = g.reindex(order)
            fig = go.Figure(go.Bar(y=g.index, x=g.values, orientation="h",
                                   marker_color=[STAGE_COLORS[s] for s in g.index],
                                   text=[f"{v}" for v in g.values],
                                   textposition="outside", textfont=dict(size=9)))
            fig.update_layout(xaxis=_ax(), yaxis=_ax(False), **_base(h=260))
            _note(fig, f"{nostage} project(s) with no stage are not shown")
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    with c4:
        _section("MOST ACTIVE DEVELOPERS, BY PROJECT COUNT")
        dv = d.copy()
        dv["_raw"] = dv["developer_canonical"].where(
            dv["developer_canonical"].astype(bool), dv["developer"])
        dv["_fold"] = dv["_raw"].apply(fold_developer)
        named = dv[dv["_fold"].notna()]
        if not len(named):
            _empty("No developer names.")
        else:
            counts = named.groupby("_fold").size()
            # Two or more projects. An owner-occupant or a one-off filer never
            # clears it, which removes almost all the noise without anyone
            # having to classify individuals by hand.
            counts = counts[counts >= 2].sort_values()
            if not len(counts):
                _empty(f"No developer has two or more projects.<br>"
                       f"{len(named)} of {n_total} projects name a developer.")
            else:
                label = {}
                for f, grp in named.groupby("_fold"):
                    label[f] = grp["_raw"].mode().iat[0]
                fig = go.Figure(go.Bar(
                    y=[label[i] for i in counts.index], x=counts.values, orientation="h",
                    marker_color=_ORANGE, text=[f"{v}" for v in counts.values],
                    textposition="outside", textfont=dict(size=9)))
                fig.update_layout(xaxis=_ax(), yaxis=_ax(False),
                                  **_base(h=max(260, 26 * len(counts))))
                _note(fig, f"{len(named)} of {n_total} projects name a developer · "
                           f"{len(counts)} firms with 2+")
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    # ── 8. mixed-use split, per project ─────────────────────────────
    _section("MIXED-USE SPLIT, WHERE BOTH ARE STATED",
             sub="the one place square footage is worth showing, because it is per project")
    mu = d[(d["residential_units"].fillna(0) > 0) & (d.get("commercial_gsf").fillna(0) > 0)] \
        if "commercial_gsf" in d.columns else d.iloc[0:0]
    if not len(mu):
        _empty("No project states both a unit count and a commercial floor area.<br>"
               "That combination is the rarest in the Rhode Island data.")
    else:
        mu = mu.sort_values("residential_units", ascending=True)
        lbl = mu["name"].where(mu["name"].astype(bool), mu["address"]).str.slice(0, 34)
        fig = go.Figure()
        fig.add_bar(y=lbl, x=mu["residential_units"], name="Residential units",
                    orientation="h", marker_color="#38bdf8")
        fig.add_bar(y=lbl, x=mu["commercial_gsf"] / 1000, name="Commercial (000s sq ft)",
                    orientation="h", marker_color="#f59e0b")
        fig.update_layout(barmode="group", xaxis=_ax(), yaxis=_ax(False),
                          **_base(h=max(260, 30 * len(mu)), legend=True))
        _note(fig, f"{len(mu)} of {n_total} projects state both")
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
