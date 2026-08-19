import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import json
import pandas as pd
import streamlit as st

from app.data import (
    load_filings, load_cambridge_permits, STAGE_COLORS, review_scale_vocab,
    RESOLUTION_METHODS, resolution_method, shows_provenance_badge, METHOD_ORDER,
    DEVELOPER_CONFIDENCE, developer_confidence, SF_SOURCES, ARCHITECT_SOURCES,
    load_field_citations, RI_SF_NOTE, RI_SF_NOTE_TITLE, UNITS_CONFIDENCE,
    city_options, keep_city_selectable,
)
from scraper.normalize_developer import is_real_company

_BG     = "#0d0f12"
_BG2    = "#141720"
_BORDER = "#1E2530"
_ORANGE = "#F5821E"
_MUTED  = "#8A9BB0"
_MONO   = "'JetBrains Mono', 'IBM Plex Mono', monospace"

STATUS_COLORS = {
    "Under Review":       _ORANGE,
    "Board Approved":     "#22c55e",
    "Letter of Intent":   "#475569",
    "Under Construction": "#ef4444",
}
STATUS_SHORT = {
    "Under Review":       "REVIEW",
    "Board Approved":     "APPROVED",
    "Letter of Intent":   "LOI",
    "Under Construction": "CONST.",
}
STATUS_DOT = {
    "Under Review":       "◈",
    "Board Approved":     "●",
    "Letter of Intent":   "○",
    "Under Construction": "◆",
}
LIFECYCLE_STAGES = ["LOI", "UNDER REVIEW", "BOARD APPROVED", "UNDER CONST.", "COMPLETE"]
LIFECYCLE_IDX = {
    "Letter of Intent":   0,
    "Under Review":       1,
    "Board Approved":     2,
    "Under Construction": 3,
}

_BAD_DEVS = {"Unknown - review needed", "Unknown", "UNKNOWN",
             "Zoning Petitions for Text Amendments", ""}


def _section(label: str):
    st.markdown(
        f'<p style="font-family:{_MONO};font-size:9px;font-weight:700;'
        f'letter-spacing:0.18em;color:{_MUTED};text-transform:uppercase;'
        f'margin:16px 0 8px 0">{label}</p>',
        unsafe_allow_html=True,
    )


# ── grouped city dropdown ────────────────────────────────────────────────
# Streamlit's selectbox has no option groups and no disabled options, so the
# grouping is built into the option list itself: a header entry per market,
# and the cities under it indented. A header is not a valid choice, and an
# on_change callback puts the previous city back if one is picked. The
# callback is the reason this works -- session_state cannot be written after
# a widget is instantiated, but it can be written from that widget's own
# callback, which runs before the rerender.
#
# The groups come from the `market` field on every row, never a hardcoded
# list, so a city added to a new market appears under its own header without
# anyone editing this file.
_CITY_HEADER = "── %s ──"
_CITY_INDENT = "  "          # figure space: aligns under the header


def _is_city_header(v) -> bool:
    return isinstance(v, str) and v.startswith("──")


def _city_options(scope):
    """(options, display -> city). Markets alphabetical, cities within them."""
    by_market = {}
    for city, market in zip(scope["city"], scope.get("market", "")):
        if not city:
            continue
        by_market.setdefault(market or "Other", set()).add(city)
    opts, lookup = ["All"], {"All": "All"}
    for market in sorted(by_market):
        head = _CITY_HEADER % market.upper()
        opts.append(head)
        lookup[head] = None                      # not selectable
        for c in sorted(by_market[market]):
            disp = _CITY_INDENT + c
            opts.append(disp)
            lookup[disp] = c
    return opts, lookup


def _keep_city_selectable():
    """Reject a header pick and restore the last real selection."""
    v = st.session_state.get("tbl_city")
    if _is_city_header(v):
        st.session_state["tbl_city"] = st.session_state.get("_tbl_city_prev", "All")
    else:
        st.session_state["_tbl_city_prev"] = v


def _dev_display(row) -> str:
    canonical = str(row["developer_canonical"] or "").strip()
    if canonical and canonical not in _BAD_DEVS and is_real_company(canonical):
        return canonical
    raw = str(row["developer"] or "").strip()
    return raw if raw and raw not in _BAD_DEVS else "—"


def render(df: pd.DataFrame):
    # ── Filter toolbar ─────────────────────────────────────────────
    _section("FILTER")

    # SHOW comes first because it decides what the table is FOR. A delivered
    # building and a withdrawn application are worth keeping and worth being
    # able to look at, but they are not pipeline, and listing them by default
    # put 70 finished buildings and 29 dead applications in front of the
    # reader as if they were live. Pipeline is the default; the others are one
    # click away rather than mixed in.
    # THREE ROWS OF FOUR, not one row of nine.
    #
    # Nine controls across one row left each about a tenth of the width, which
    # is narrower than the values they hold: SHOW rendered "Withdra", MARKET
    # rendered "Massachus" and "Rhode Isl". Equal columns across every row also
    # make the rows line up, which one row of nine and two ragged half-width
    # rows underneath never did.
    #
    # Grouped by what the reader is asking: where is it, what is it, who is
    # building it. Free-text search sits on its own line under the grid.
    _css = """
    <style>
      /* An open menu sizes to its longest option instead of to the field. */
      div[data-baseweb="popover"] ul[role="listbox"] { min-width: max-content !important; }
      div[data-baseweb="popover"] li { white-space: nowrap !important; }
      /* The closed field shows its whole value. Most fit on one line at this
         width; the one outlier is Cambridge's "Approved PUD/Master Plan
         Development Remaining" at 442px against 354px of field, and widening
         every column by a quarter for one status would be the wrong trade, so
         a value that does not fit wraps and the field grows instead. */
      div[data-baseweb="select"] div[title] {
        white-space: normal;
        overflow: visible;
        text-overflow: clip;
        line-height: 1.25;
      }
      div[data-testid="stSelectbox"] div[data-baseweb="select"] > div {
        height: auto;
        min-height: 38px;
      }
    </style>"""
    st.markdown(_css, unsafe_allow_html=True)

    r1a, r1b, r1c, r1d = st.columns(4)          # where
    r2a, r2b, r2c, r2d = st.columns(4)          # what
    fshow, fcm, fc0, fc1 = r1a, r1b, r1c, r1d
    fc2, fcft, fc3, fc4 = r2a, r2b, r2c, r2d
    SHOW_OPTS = ["Pipeline", "Delivered", "Withdrawn / denied", "All"]
    show = fshow.selectbox(
        "SHOW", SHOW_OPTS, key="tbl_show",
        help="Pipeline excludes buildings established as complete and "
             "applications that were withdrawn or denied. They stay in the "
             "tracker and are one selection away.")
    _delivered = df["stage"] == "Complete" if "stage" in df.columns else pd.Series(False, index=df.index)
    _dead = (df["project_status_filing"].isin(["Withdrawn", "Denied"])
             if "project_status_filing" in df.columns else pd.Series(False, index=df.index))
    if show == "Pipeline":
        df = df[~_delivered & ~_dead]
    elif show == "Delivered":
        df = df[_delivered]
    elif show == "Withdrawn / denied":
        df = df[_dead]

    # Market groups the cities so the two states can be compared without
    # selecting five municipalities one at a time. The city list is scoped to
    # the chosen market rather than listing all eleven regardless.
    markets = ["All"] + sorted([m for m in df["market"].unique() if m])
    market = fcm.selectbox("MARKET", markets, key="tbl_market")
    city_scope = df if market == "All" else df[df["market"] == market]

    # Cities grouped under their state. Built from city_scope, so choosing a
    # MARKET narrows the list to that state's cities and drops the other
    # header entirely -- the two filters agree instead of contradicting.
    city_opts, city_lookup = city_options(city_scope)
    if st.session_state.get("tbl_city") not in city_opts:
        st.session_state["tbl_city"] = "All"      # market change orphaned it
    city_disp = fc0.selectbox("CITY", city_opts, key="tbl_city",
                              on_change=keep_city_selectable("tbl_city"))
    city = city_lookup.get(city_disp) or "All"

    # The square-footage column is 15% filled for Rhode Island, and without the
    # reason on the page that reads as a broken tracker rather than as a fact
    # about how the market files. Shown whenever the view is scoped to Rhode
    # Island, collapsed so it explains without interrupting.
    _ri_cities = {"Providence", "Warwick", "Cranston", "Pawtucket", "Newport"}
    _showing_ri = (market == "Rhode Island") or (city in _ri_cities)

    # Status vocab is city-specific (Boston's 4 values vs Cambridge's 7 don't
    # overlap), so scope the status options to whatever city is selected --
    # otherwise "All" would show all 11 values mixed together.
    status_scope = city_scope if city == "All" else city_scope[city_scope["city"] == city]
    neighborhoods = ["All"] + sorted([n for n in status_scope["neighborhood"].unique() if n])
    nbhd = fc1.selectbox("NEIGHBORHOOD", neighborhoods, key="tbl_nbhd")

    statuses = ["All"] + sorted([s for s in status_scope["status"].unique() if s])
    status = fc2.selectbox("STATUS", statuses, key="tbl_status")

    # FILING TYPE is its own axis. It was previously poured into STATUS, so the
    # status dropdown offered "Lot Merger" and "Design Waiver" as if they were
    # pipeline stages. They are the action a board was asked to take, which is
    # a genuinely useful filter -- just not the same question.
    ft_opts = ["All"] + sorted([f for f in status_scope.get(
        "filing_type", pd.Series(dtype=str)).unique() if f])
    filing_type = (fcft.selectbox("FILING TYPE", ft_opts, key="tbl_filing_type")
                   if len(ft_opts) > 1 else "All")

    # Scale vocabulary is market-specific (Article 80's two tiers vs. RIGL's
    # three), so scope the options to the selected city via the registry rather
    # than hardcoding one market's values.
    scale_opts = review_scale_vocab([city] if city != "All" else df["city"].unique())
    scale = fc3.selectbox("SCALE", ["All"] + scale_opts, key="tbl_scale")

    classes = ["All"] + sorted([a for a in df["asset_class"].unique() if a])
    asset = fc4.selectbox("ASSET CLASS", classes, key="tbl_asset")

    # Row three: who is building it. Same four equal columns as the rows
    # above, so the grid lines up instead of stepping between a short field
    # and a full-width one.
    fd1, fd2, fd3, fd4 = st.columns(4)
    all_devs = sorted(
        {d for d in df["developer_canonical"].unique() if is_real_company(d)},
        key=lambda x: x.lstrip("Tt").lower() if x.lower().startswith("the ") else x.lower()
    )
    dev_search = fd1.text_input("DEVELOPER SEARCH", "", key="tbl_dev_search",
                                placeholder="e.g. Marcus Partners")
    matching_devs = (
        [d for d in all_devs if dev_search.lower() in d.lower()]
        if dev_search else all_devs
    )
    developer = fd2.selectbox("DEVELOPER", ["All"] + matching_devs, key="tbl_developer")

    # The four confidence states, as their own filter. Most Rhode Island
    # developers are document_only, so being able to select exactly those for
    # review is the point rather than a convenience.
    conf_opts = ["All"] + [DEVELOPER_CONFIDENCE[k]["label"] for k in
                           ("confirmed", "document_only", "conflicted",
                            "human_set", "unattributed")]
    conf_f = fd4.selectbox(
        "DEVELOPER CONFIDENCE", conf_opts, key="tbl_dev_conf",
        help="Confirmed = outside coverage names the same party. Document only = "
             "the planning filing alone. Conflicted = two names on record. "
             "Set by hand = a person entered it and the rederive pass will not "
             "touch it.",
    )
    method_f = fd3.selectbox(
        "DEVELOPER SOURCE",
        ["All"] + [RESOLUTION_METHODS[m]["label"] for m in METHOD_ORDER]
                + ["Any inferred (web)"],
        key="tbl_dev_method",
        help="How the developer name was established. 'Any inferred' selects every "
             "name derived from press coverage rather than the corporate registry.",
    )

    # Free-text search last and full width. It is the one control whose useful
    # length is unbounded, so it gets the row rather than a ninth of one.
    search = st.text_input("SEARCH", "", key="tbl_search",
                           placeholder="project name or address…")

    # The note sits under the whole filter block rather than between two rows
    # of it, so the grid stays unbroken.
    if _showing_ri:
        with st.expander(RI_SF_NOTE_TITLE, expanded=False):
            st.markdown(RI_SF_NOTE)

    # Apply filters
    filtered = df.copy()
    if market != "All":
        filtered = filtered[filtered["market"] == market]
    if city != "All":
        filtered = filtered[filtered["city"] == city]
    if nbhd != "All":
        filtered = filtered[filtered["neighborhood"] == nbhd]
    if status != "All":
        filtered = filtered[filtered["status"] == status]
    if filing_type != "All":
        filtered = filtered[filtered["filing_type"] == filing_type]
    if scale != "All":
        filtered = filtered[filtered["review_scale"] == scale]
    if asset != "All":
        filtered = filtered[filtered["asset_class"] == asset]
    if developer != "All":
        filtered = filtered[filtered["developer_canonical"] == developer]
    if conf_f != "All":
        _want = next(k for k, v in DEVELOPER_CONFIDENCE.items() if v["label"] == conf_f)
        filtered = filtered[filtered["developer_confidence"] == _want]
    if method_f != "All":
        _m = filtered["developer_resolution_method"].apply(resolution_method)
        if method_f == "Any inferred (web)":
            filtered = filtered[_m.isin(["web_corroborated", "web_low_confidence"])]
        else:
            wanted = next(k for k, v in RESOLUTION_METHODS.items()
                          if v["label"] == method_f)
            filtered = filtered[_m == wanted]
    if search:
        q = search.lower()
        mask = (
            filtered["name"].str.lower().str.contains(q, na=False) |
            filtered["address"].str.lower().str.contains(q, na=False)
        )
        # A case filed on several parcels names them all, and the ingest kept
        # only the first. Searching 1077 Westminster Street returned nothing
        # for a project that is in the tracker as 311 Knight Street.
        if "alt_addresses" in filtered.columns:
            mask = mask | filtered["alt_addresses"].str.lower().str.contains(q, na=False)
        filtered = filtered[mask]

    # Count row
    cnt_col, exp_col = st.columns([5, 1])
    cnt_col.markdown(
        f'<p style="font-family:{_MONO};font-size:10px;color:{_MUTED};margin:4px 0 8px">'
        f'<span style="color:#e2e8f0;font-weight:700">{len(filtered)}</span> PROJECTS'
        f'&nbsp;&nbsp;·&nbsp;&nbsp;{len(df)} TOTAL'
        + "".join(
            f'&nbsp;&nbsp;·&nbsp;&nbsp;<span style="color:{v["color"]}">{v["mark"]}</span>'
            f'&nbsp;{v["label"].upper()}'
            for k, v in DEVELOPER_CONFIDENCE.items()
        )
        + '</p>',
        unsafe_allow_html=True,
    )
    csv = filtered.to_csv(index=False).encode()
    exp_col.download_button("↓ EXPORT CSV", csv, "boston_cre_pipeline.csv", "text/csv")

    # ── Table ─────────────────────────────────────────────────────
    _section("SCREENER")

    display = filtered[[
        "name", "developer_canonical", "developer", "architect", "neighborhood", "city",
        "asset_class", "status", "stage", "total_gsf", "residential_units",
        "building_height_ft", "expected_delivery",
    ]].copy()

    display["developer_canonical"] = display.apply(_dev_display, axis=1)

    # Every developer name carries its confidence wherever it appears. This
    # used to mark only the INFERRED names, which left confirmed and
    # document-only looking identical -- and document_only is the majority of
    # the Rhode Island data, so an unmarked name read as verified.
    display["_conf"] = filtered["developer_confidence"]
    display["developer_canonical"] = [
        f'{DEVELOPER_CONFIDENCE[c]["mark"]} {name}'
        if c in DEVELOPER_CONFIDENCE and name != "—" else name
        for name, c in zip(display["developer_canonical"], display["_conf"])
    ]
    display.drop(columns=["developer", "_conf"], inplace=True)

    # The architect sits beside the developer and is marked the same way, for
    # the same reason: most of these names come from a drawing title block or
    # a permit record rather than from the planning filing, and an unmarked
    # name would read as though the filing itself stated it. Blank stays
    # blank -- an em dash here, not a guess.
    _asrc = (filtered["architect_source"] if "architect_source" in filtered.columns
             else [""] * len(filtered))
    display["architect"] = [
        (ARCHITECT_SOURCES.get(src, {}).get("mark", "") + " " + a).strip() if a else "—"
        for a, src in zip(display["architect"].fillna(""), _asrc)
    ]

    def _status_fmt(row):
        if not row["status"]:
            return "—"
        if row["status"] in STATUS_DOT:
            return f"{STATUS_DOT[row['status']]} {STATUS_SHORT[row['status']]}"
        # Cambridge (or any city outside the Boston vocab): dot colored by
        # normalized stage, label is the native status text.
        return f"● {row['status']}"

    display["status_fmt"] = display.apply(_status_fmt, axis=1)

    # SF carries its provenance, for the same reason developer names do: a
    # web-sourced figure and a filing-stated one must not read alike.
    display["_sf_src"] = filtered["total_gsf_source"]
    display["total_gsf"] = pd.to_numeric(display["total_gsf"], errors="coerce")
    display["residential_units"] = pd.to_numeric(display["residential_units"], errors="coerce")
    display["building_height_ft"] = pd.to_numeric(display["building_height_ft"], errors="coerce")

    # SF stays a NUMBER so the column sorts numerically. Formatting a figure
    # into a string and handing that to a TextColumn made 99,925 sort above
    # 1,234,567, because as text "9" beats "1" -- every Boston project over
    # 100,000 sq ft was ranking below far smaller ones. Formatting is display
    # only, done by the column config. Provenance moves to its own narrow
    # column rather than being welded into the value, which is what forced the
    # value to be text in the first place.
    # A unit count carries how far it can be trusted, for the same reason a
    # developer name does: 34 units at 1077 Westminster looked exactly like a
    # verified figure until the final plan said 41.
    _uc = filtered["units_confidence"] if "units_confidence" in filtered.columns else None
    display["units_mark"] = ([UNITS_CONFIDENCE.get(c, {}).get("mark", "") for c in _uc]
                             if _uc is not None else [""] * len(display))

    display["sf_src_mark"] = [
        SF_SOURCES[src]["mark"] if src in SF_SOURCES else ""
        for src in display["_sf_src"]
    ]
    display = display[[
        "name", "developer_canonical", "architect", "neighborhood", "city",
        "asset_class", "status_fmt", "total_gsf", "sf_src_mark",
        "residential_units", "units_mark", "building_height_ft", "expected_delivery",
    ]]
    display.columns = [
        "PROJECT", "DEVELOPER", "ARCHITECT", "NEIGHBORHOOD", "CITY",
        "TYPE", "STATUS", "SF", "SRC",
        "UNITS", "U?", "HEIGHT", "DELIVERY",
    ]

    selection = st.dataframe(
        display,
        use_container_width=True,
        hide_index=True,
        height=400,
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            # Numeric so it sorts by magnitude; the comma is display only.
            # Blanks sort to the end rather than being coerced to zero, which
            # would rank an unknown size alongside a genuine nothing.
            "SF":     st.column_config.NumberColumn(
                format="%,d",
                help="Gross square feet, as stated by the source. Sorts by "
                     "magnitude; blank means no source states one."),
            "SRC":    st.column_config.TextColumn(
                width="small",
                help="Square-footage provenance. ◈ = web-sourced, ✲ = corrected "
                     "from a lot area, ▣ = plan set or staff report. "
                     "Unmarked = stated in the planning filing."),
            "ARCHITECT": st.column_config.TextColumn(
                help="Architecture practice, not the individual. ▣ = named on a "
                     "plan set or staff report, ◉ = from a permit record, "
                     "◈ = web research. Unmarked = stated in the planning "
                     "filing. — = no source names one."),
            "UNITS":  st.column_config.NumberColumn(format="%d"),
            "U?":     st.column_config.TextColumn(
                width="small",
                help="Unit-count confidence. Blank = corroborated by two or more "
                     "documents. · = a single document. ≠ = a later document "
                     "states a different figure. ? = no document in the corpus "
                     "states it at all."),
            "HEIGHT": st.column_config.NumberColumn(format="%d ft"),
        },
    )

    # ── Detail panel ──────────────────────────────────────────────
    if selection and selection.selection.rows:
        idx = selection.selection.rows[0]
        _detail_panel(filtered.iloc[idx], df)


def _lifecycle_bar(status: str) -> str:
    cur = LIFECYCLE_IDX.get(status, -1)
    items = []
    for i, stage in enumerate(LIFECYCLE_STAGES):
        if i < cur:
            dot = f"background:#22c55e;border:1.5px solid #22c55e"
            lbl_c = "#22c55e"
        elif i == cur:
            dot = f"background:{_ORANGE};border:1.5px solid {_ORANGE}"
            lbl_c = _ORANGE
        else:
            dot = f"background:{_BG};border:1.5px solid {_BORDER}"
            lbl_c = _BORDER

        connector = ""
        if i > 0:
            line_c = "#22c55e" if i <= cur else _BORDER
            connector = (
                f'<div style="flex:1;height:1px;background:{line_c};'
                f'margin-top:5px;min-width:8px"></div>'
            )
        items.append(
            connector +
            f'<div style="display:flex;flex-direction:column;align-items:center;gap:4px">'
            f'<div style="width:10px;height:10px;border-radius:50%;{dot}"></div>'
            f'<div style="font-family:{_MONO};font-size:7.5px;font-weight:700;'
            f'letter-spacing:0.1em;color:{lbl_c};text-align:center;white-space:nowrap">{stage}</div>'
            f'</div>'
        )
    return (
        f'<div style="display:flex;align-items:flex-start;gap:0;'
        f'margin:14px 0 18px;padding:12px 16px;'
        f'background:{_BG2};border:1px solid {_BORDER}">'
        + "".join(items) +
        f'</div>'
    )


def _kv(label: str, value) -> str:
    if not value or (isinstance(value, float) and value != value):
        return ""
    return (
        f'<div style="margin-bottom:10px">'
        f'<div style="font-family:{_MONO};font-size:8.5px;font-weight:700;'
        f'letter-spacing:0.12em;color:{_MUTED};text-transform:uppercase;margin-bottom:3px">{label}</div>'
        f'<div style="font-family:{_MONO};font-size:12px;color:#e2e8f0;font-weight:500">{value}</div>'
        f'</div>'
    )


def _detail_panel(p: pd.Series, df: pd.DataFrame):
    st.markdown('<div style="height:6px"></div>', unsafe_allow_html=True)

    is_cambridge = p.get("city") == "Cambridge"
    status_color = STATUS_COLORS.get(p["status"]) or STAGE_COLORS.get(p.get("stage"), _MUTED)
    status_short = STATUS_SHORT.get(p["status"], p["status"])

    # Header
    st.markdown(
        f'<div style="border-left:3px solid {_ORANGE};padding:12px 16px 10px;'
        f'background:{_BG2};border-top:1px solid {_BORDER};border-right:1px solid {_BORDER};'
        f'border-bottom:1px solid {_BORDER};margin-bottom:0">'
        f'<div style="font-family:{_MONO};font-size:9px;font-weight:700;'
        f'letter-spacing:0.14em;color:{_MUTED};text-transform:uppercase;margin-bottom:6px">'
        f'PROJECT DETAIL</div>'
        f'<div style="font-family:Inter,sans-serif;font-size:1.1rem;font-weight:700;'
        f'color:#ffffff;margin-bottom:8px;line-height:1.3">{p["name"]}</div>'
        f'<div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap">'
        f'<span style="font-family:{_MONO};font-size:9px;font-weight:700;'
        f'letter-spacing:0.1em;color:{status_color};border:1px solid {status_color};'
        f'padding:3px 8px">{status_short}</span>'
        f'<span style="font-family:{_MONO};font-size:9px;color:{_MUTED}">'
        f'{p["neighborhood"]}</span>'
        f'{"&nbsp;·&nbsp;<span style=\"font-family:" + _MONO + ";font-size:9px;color:" + _MUTED + "\">" + p["city"] + "</span>" if p.get("city") and p["city"] != "Boston" else ""}'
        f'</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # A stage derived from an agenda alone means the project was SCHEDULED at
    # that stage, not that the outcome was recorded. Surfacing this is the whole
    # point of the two-field split -- without it, "heard at Final Plan" and
    # "Final Plan approved" would look identical on the detail view.
    if p.get("stage_provisional"):
        heard = p.get("stage_heard") or p.get("stage") or "—"
        st.markdown(
            f'<div style="border:1px solid #f59e0b;background:rgba(245,158,11,0.08);'
            f'padding:10px 16px;margin:10px 0;font-family:{_MONO};font-size:11px;'
            f'color:#f59e0b;line-height:1.5">'
            f'⚠ STAGE NOT CONFIRMED — this project was heard at <b>{heard}</b> per the '
            f'meeting agenda, but no minutes recording the outcome are available, so the '
            f'vote result is unknown. The stage shown reflects the furthest stage reached '
            f'on an agenda, not an approval.</div>',
            unsafe_allow_html=True,
        )

    if p.get("conditional_alternative"):
        st.markdown(
            f'<div style="border:1px solid #f59e0b;background:rgba(245,158,11,0.08);'
            f'padding:10px 16px;margin:10px 0;font-family:{_MONO};font-size:11px;'
            f'color:#f59e0b;line-height:1.5">'
            f'⚠ COMPETING PLAN — this project shares a special permit base number with '
            f'other current-edition entries under different amendments. It represents one '
            f'of two or more alternative build-outs for the same site; the developer has not '
            f'finalized which will proceed. Excluded from aggregate totals by default.</div>',
            unsafe_allow_html=True,
        )
    if p.get("spans_municipalities"):
        st.markdown(
            f'<div style="border:1px solid {_MUTED};background:rgba(138,155,176,0.06);'
            f'padding:8px 16px;margin:6px 0;font-family:{_MONO};font-size:10px;'
            f'color:{_MUTED}">↔ Spans more than one municipality — see description/notes.</div>',
            unsafe_allow_html=True,
        )

    # Lifecycle bar (Boston's Article 80 phase sequence -- doesn't apply to
    # Cambridge's different permitting tracks, so show the normalized stage instead)
    if is_cambridge:
        stage = p.get("stage") or "—"
        stage_color = STAGE_COLORS.get(stage, _MUTED)
        st.markdown(
            f'<div style="margin:12px 0 16px;padding:10px 16px;background:{_BG2};'
            f'border:1px solid {_BORDER};font-family:{_MONO};font-size:10px;'
            f'display:flex;align-items:center;gap:10px">'
            f'<span style="color:{_MUTED};letter-spacing:0.1em">STAGE</span>'
            f'<span style="color:{stage_color};font-weight:700;letter-spacing:0.08em">{stage.upper()}</span>'
            f'<span style="color:{_MUTED}">(native status: {p["status"]})</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(_lifecycle_bar(p["status"]), unsafe_allow_html=True)

    # Description
    if p.get("description"):
        st.markdown(
            f'<div style="font-family:Inter,sans-serif;font-size:13px;color:{_MUTED};'
            f'line-height:1.6;padding:12px 16px;background:{_BG2};border:1px solid {_BORDER};'
            f'margin-bottom:14px">{p["description"]}</div>',
            unsafe_allow_html=True,
        )

    # Cambridge Notes / Parking Notes (distinct from Description -- these are
    # the CDD's own free-text caveats: amendment history, stale-figure flags,
    # shared-parking arrangements, etc.)
    if is_cambridge and (p.get("notes") or p.get("parking_notes")):
        notes_html = ""
        if p.get("notes"):
            notes_html += (
                f'<div style="margin-bottom:8px"><span style="color:{_MUTED};'
                f'font-weight:700;letter-spacing:0.1em">NOTES: </span>{p["notes"]}</div>'
            )
        if p.get("parking_notes"):
            notes_html += (
                f'<div><span style="color:{_MUTED};font-weight:700;letter-spacing:0.1em">'
                f'PARKING NOTES: </span>{p["parking_notes"]}</div>'
            )
        st.markdown(
            f'<div style="font-family:Inter,sans-serif;font-size:12px;color:#e2e8f0;'
            f'line-height:1.6;padding:12px 16px;background:{_BG2};border-left:3px solid {_MUTED};'
            f'border-top:1px solid {_BORDER};border-right:1px solid {_BORDER};'
            f'border-bottom:1px solid {_BORDER};margin-bottom:14px">{notes_html}</div>',
            unsafe_allow_html=True,
        )

    # Two-column key-value
    col1, col2, col3 = st.columns(3)

    gsf = p.get("total_gsf")
    gsf_str = f"{int(gsf):,} SF" if pd.notna(gsf) and gsf else None
    # Provenance on the figure itself. A square footage a reporter published
    # and one the filing stated are different kinds of fact.
    _sf_src = p.get("total_gsf_source") or ""
    if gsf_str and _sf_src in SF_SOURCES and _sf_src != "filing":
        _sm = SF_SOURCES[_sf_src]
        gsf_str = (f'{_sm["mark"]} {gsf_str}<br><span style="color:{_sm["color"]};'
                   f'font-size:9px;letter-spacing:0.08em">{_sm["label"].upper()}</span>')
    units = p.get("residential_units")
    units_str = f"{int(units):,}" if pd.notna(units) and units else None
    cgsf = p.get("commercial_gsf")
    cgsf_str = f"{int(cgsf):,} SF" if pd.notna(cgsf) and cgsf else None
    ht = p.get("building_height_ft")
    ht_str = f"{ht:.0f} FT" if pd.notna(ht) and ht else None
    stories = p.get("num_stories")
    stories_str = f"{int(stories)}" if pd.notna(stories) and stories else None
    parking = p.get("parking_spaces")
    parking_str = f"{int(parking):,}" if pd.notna(parking) and parking else None

    # Developer with its provenance, and every corroborating source, so an
    # inferred name can be clicked through and checked from the detail view.
    _dev_name = p.get("developer_canonical") or p.get("developer")
    _method = resolution_method(p.get("developer_resolution_method", ""))
    # Confidence is shown for ALL four states, not only the inferred ones. A
    # document-only name with no badge reads as verified, and document-only is
    # most of the Rhode Island data.
    _conf = p.get("developer_confidence") or developer_confidence(
        p.get("developer_resolution_method", ""), _dev_name or "")
    if _dev_name and _conf in DEVELOPER_CONFIDENCE:
        _cm = DEVELOPER_CONFIDENCE[_conf]
        _raw = p.get("developer_resolution_method") or ""
        _detail = f" · {_raw}" if _raw and _raw != _conf else ""
        _dev_name = (f'{_cm["mark"]} {_dev_name}<br>'
                     f'<span style="color:{_cm["color"]};font-size:9px;'
                     f'letter-spacing:0.08em">{_cm["label"].upper()}{_detail.upper()}</span>')

    with col1:
        st.markdown(
            _kv("ADDRESS",          p.get("address")) +
            _kv("DEVELOPER",        _dev_name) +
            # Shown only when the applicant is not the party executing the
            # work -- a public agency or passive landowner.
            _kv("OWNER / AGENCY",   p.get("owner_or_agency")) +
            _kv("EQUITY PARTNER",   p.get("equity_partner")) +
            _kv("ARCHITECT",        p.get("architect")) +
            _kv("CIVIL ENGINEER",   p.get("civil_engineer")),
            unsafe_allow_html=True,
        )
    # Canonical class, with the source's own classification alongside it when
    # it was folded (e.g. Cambridge's "Fire Department" -> "Institutional").
    ac_str = p.get("asset_class") or None
    raw_ac = p.get("asset_class_raw")
    if ac_str and raw_ac and raw_ac != ac_str:
        ac_str = f'{ac_str} <span style="color:{_MUTED}">({raw_ac})</span>'

    with col2:
        st.markdown(
            _kv("ASSET CLASS",      ac_str) +
            _kv("TOTAL SF",         gsf_str) +
            _kv("RESIDENTIAL UNITS", units_str) +
            _kv("COMMERCIAL SF",    cgsf_str) +
            _kv("PARKING SPACES",   parking_str),
            unsafe_allow_html=True,
        )
    # Normalized scale, with the source's verbatim wording alongside it when it
    # differs -- same split as stage vs. native status.
    scale_str = p.get("review_scale") or None
    raw_scale = p.get("review_scale_raw")
    if scale_str and raw_scale and raw_scale != scale_str:
        scale_str = f'{scale_str} <span style="color:{_MUTED}">({raw_scale})</span>'

    with col3:
        st.markdown(
            _kv("HEIGHT",           ht_str) +
            _kv("STORIES",          stories_str) +
            _kv("REVIEW SCALE",     scale_str) +
            _kv("EXPECTED DELIVERY", p.get("expected_delivery")) +
            _kv("FILING TYPE",      (p.get("processed_filing_type") or "").upper() or None),
            unsafe_allow_html=True,
        )

    # Cambridge Development Log fields
    if is_cambridge:
        far_str = None
        if pd.notna(p.get("far")) and p.get("far"):
            scope_note = " (whole PUD)" if p.get("far_scope") == "pud" else ""
            far_str = f'{p["far"]:.2f}{scope_note}'
        lot_area_str = f'{int(p["lot_area"]):,} SF' if pd.notna(p.get("lot_area")) and p.get("lot_area") else None
        affordable_str = "TBD" if p.get("affordable_units_tbd") else (
            f'{int(p["affordable_units"]):,}' if pd.notna(p.get("affordable_units")) and p.get("affordable_units") else None
        )
        hotel_str = f'{int(p["hotel_rooms"]):,}' if pd.notna(p.get("hotel_rooms")) and p.get("hotel_rooms") else None

        st.markdown('<div style="height:4px"></div>', unsafe_allow_html=True)
        cc1, cc2, cc3 = st.columns(3)
        with cc1:
            st.markdown(
                _kv("PERMIT TYPE",  p.get("permit_type")) +
                _kv("PROJECT TYPE", p.get("project_type")) +
                _kv("ZONING",       p.get("zoning_raw")),
                unsafe_allow_html=True,
            )
        with cc2:
            st.markdown(
                _kv("LOT AREA", lot_area_str) +
                _kv("FAR",      far_str) +
                _kv("AFFORDABLE UNITS", affordable_str),
                unsafe_allow_html=True,
            )
        with cc3:
            permits = load_cambridge_permits(int(p["id"]))
            special_str = ", ".join(
                f'{s["base"]} {s["amendment"]}' if s["amendment"] else s["base"]
                for s in permits["special_permits"]
            ) or None
            building_str = ", ".join(
                f'{b["number"]} ({b["label"]})' if b["label"] else b["number"]
                for b in permits["building_permits"]
            ) or None
            st.markdown(
                _kv("HOTEL ROOMS",     hotel_str) +
                _kv("SPECIAL PERMIT",  special_str) +
                _kv("BUILDING PERMIT", building_str),
                unsafe_allow_html=True,
            )

        if permits["aliases"]:
            st.markdown(
                _kv("FORMERLY", " → ".join(permits["aliases"] + [p["name"]])),
                unsafe_allow_html=True,
            )

        if p.get("parent_project_id"):
            parent_rows = df[df["id"] == p["parent_project_id"]]
            if not parent_rows.empty:
                st.markdown(
                    _kv("PART OF", f'{p.get("phase_group") or ""} — {parent_rows.iloc[0]["name"]}'),
                    unsafe_allow_html=True,
                )
        elif p.get("phase_group"):
            st.markdown(_kv("PHASE GROUP", p["phase_group"]), unsafe_allow_html=True)

    # Corroborating sources for an inferred developer name. Stored in full,
    # not just the first, so the attribution can actually be audited.
    if _dev_name and _method in ("web_corroborated", "web_low_confidence"):
        try:
            _srcs = json.loads(p.get("developer_sources") or "[]")
        except (ValueError, TypeError):
            _srcs = []
        if _srcs:
            with st.expander(f"DEVELOPER SOURCES  ({len(_srcs)})"):
                st.caption(RESOLUTION_METHODS[_method]["note"])
                for s in _srcs:
                    pub = s.get("publisher") or s.get("domain", "")
                    url = s.get("url", "")
                    st.markdown(f"**{pub}** — [{url}]({url})")
                    st.markdown(f"*Address:* {s.get('address_sentence', '—')}")
                    st.markdown(f"*Developer:* {s.get('developer_sentence', '—')}")
                    st.divider()

    # Per-field citations. Every extracted value says which filing it came
    # from, so a figure can be traced without leaving the row.
    _cites = load_field_citations(int(p["id"]))
    if _cites:
        with st.expander(f"FIELD SOURCES  ({len(_cites)})"):
            st.caption("Where each extracted value came from. A field with no row here "
                       "was not stated in the filing.")
            st.dataframe(
                pd.DataFrame([{
                    "FIELD": c["field"],
                    "VALUE": c["value"],
                    "SOURCE": c["filing"],
                    "DATE": c["date"],
                    "URL": c["url"],
                } for c in _cites]),
                use_container_width=True, hide_index=True,
                column_config={"URL": st.column_config.LinkColumn("URL", display_text="open ↗")},
            )

    # Links
    lc1, lc2, _ = st.columns([1, 1, 4])
    if p.get("bpda_url") and not str(p["bpda_url"]).startswith("manual:"):
        lc1.link_button("BPDA PAGE ↗", p["bpda_url"])
    elif is_cambridge:
        lc1.link_button("SEARCH SPECIAL PERMITS ↗", "https://www.cambridgema.gov/specialpermits")
    if p.get("processed_filing_url"):
        lc2.link_button(f"SOURCE {(p.get('processed_filing_type') or 'PDF').upper()} ↗",
                        p["processed_filing_url"])

    # Filings
    filings_df = load_filings(int(p["id"]))
    if not filings_df.empty:
        with st.expander(f"ALL FILINGS  ({len(filings_df)})"):
            st.dataframe(filings_df, use_container_width=True, hide_index=True)

    st.markdown(f'<div style="height:1px;background:{_BORDER};margin:16px 0 8px"></div>',
                unsafe_allow_html=True)
