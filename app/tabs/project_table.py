import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pandas as pd
import streamlit as st

from app.data import load_filings
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


def _dev_display(row) -> str:
    canonical = str(row["developer_canonical"] or "").strip()
    if canonical and canonical not in _BAD_DEVS and is_real_company(canonical):
        return canonical
    raw = str(row["developer"] or "").strip()
    return raw if raw and raw not in _BAD_DEVS else "—"


def render(df: pd.DataFrame):
    # ── Filter toolbar ─────────────────────────────────────────────
    _section("FILTER")

    fc1, fc2, fc3, fc4, fc5 = st.columns(5)

    neighborhoods = ["All"] + sorted([n for n in df["neighborhood"].unique() if n])
    nbhd = fc1.selectbox("NEIGHBORHOOD", neighborhoods, key="tbl_nbhd")

    statuses = ["All"] + sorted([s for s in df["status"].unique() if s])
    status = fc2.selectbox("STATUS", statuses, key="tbl_status")

    scale = fc3.selectbox("SCALE", ["All", "Large Project", "Small Project"], key="tbl_scale")

    classes = ["All"] + sorted([a for a in df["asset_class"].unique() if a])
    asset = fc4.selectbox("ASSET CLASS", classes, key="tbl_asset")

    search = fc5.text_input("SEARCH", "", key="tbl_search", placeholder="name or address…")

    # Developer filter in its own row
    fd1, fd2 = st.columns([2, 5])
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

    # Apply filters
    filtered = df.copy()
    if nbhd != "All":
        filtered = filtered[filtered["neighborhood"] == nbhd]
    if status != "All":
        filtered = filtered[filtered["status"] == status]
    if scale != "All":
        filtered = filtered[filtered["project_scale"] == scale]
    if asset != "All":
        filtered = filtered[filtered["asset_class"] == asset]
    if developer != "All":
        filtered = filtered[filtered["developer_canonical"] == developer]
    if search:
        q = search.lower()
        mask = (
            filtered["name"].str.lower().str.contains(q, na=False) |
            filtered["address"].str.lower().str.contains(q, na=False)
        )
        filtered = filtered[mask]

    # Count row
    cnt_col, exp_col = st.columns([5, 1])
    cnt_col.markdown(
        f'<p style="font-family:{_MONO};font-size:10px;color:{_MUTED};margin:4px 0 8px">'
        f'<span style="color:#e2e8f0;font-weight:700">{len(filtered)}</span> PROJECTS'
        f'&nbsp;&nbsp;·&nbsp;&nbsp;{len(df)} TOTAL</p>',
        unsafe_allow_html=True,
    )
    csv = filtered.to_csv(index=False).encode()
    exp_col.download_button("↓ EXPORT CSV", csv, "boston_cre_pipeline.csv", "text/csv")

    # ── Table ─────────────────────────────────────────────────────
    _section("SCREENER")

    display = filtered[[
        "name", "developer_canonical", "developer", "neighborhood",
        "asset_class", "status", "total_gsf", "residential_units",
        "building_height_ft", "expected_delivery",
    ]].copy()

    display["developer_canonical"] = display.apply(_dev_display, axis=1)
    display.drop(columns=["developer"], inplace=True)

    display["status_fmt"] = display["status"].apply(
        lambda s: f"{STATUS_DOT.get(s, '○')} {STATUS_SHORT.get(s, s)}" if s else "—"
    )

    display["total_gsf"] = pd.to_numeric(display["total_gsf"], errors="coerce")
    display["residential_units"] = pd.to_numeric(display["residential_units"], errors="coerce")
    display["building_height_ft"] = pd.to_numeric(display["building_height_ft"], errors="coerce")

    display = display[[
        "name", "developer_canonical", "neighborhood",
        "asset_class", "status_fmt", "total_gsf",
        "residential_units", "building_height_ft", "expected_delivery",
    ]]
    display.columns = [
        "PROJECT", "DEVELOPER", "NEIGHBORHOOD",
        "TYPE", "STATUS", "SF",
        "UNITS", "HEIGHT", "DELIVERY",
    ]

    selection = st.dataframe(
        display,
        use_container_width=True,
        hide_index=True,
        height=400,
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            "SF":     st.column_config.NumberColumn(format="%d", help="Gross square feet"),
            "UNITS":  st.column_config.NumberColumn(format="%d"),
            "HEIGHT": st.column_config.NumberColumn(format="%d ft"),
        },
    )

    # ── Detail panel ──────────────────────────────────────────────
    if selection and selection.selection.rows:
        idx = selection.selection.rows[0]
        _detail_panel(filtered.iloc[idx])


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


def _detail_panel(p: pd.Series):
    st.markdown('<div style="height:6px"></div>', unsafe_allow_html=True)

    status_color = STATUS_COLORS.get(p["status"], _MUTED)
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

    # Lifecycle bar
    st.markdown(_lifecycle_bar(p["status"]), unsafe_allow_html=True)

    # Description
    if p.get("description"):
        st.markdown(
            f'<div style="font-family:Inter,sans-serif;font-size:13px;color:{_MUTED};'
            f'line-height:1.6;padding:12px 16px;background:{_BG2};border:1px solid {_BORDER};'
            f'margin-bottom:14px">{p["description"]}</div>',
            unsafe_allow_html=True,
        )

    # Two-column key-value
    col1, col2, col3 = st.columns(3)

    gsf = p.get("total_gsf")
    gsf_str = f"{int(gsf):,} SF" if pd.notna(gsf) and gsf else None
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

    with col1:
        st.markdown(
            _kv("ADDRESS",          p.get("address")) +
            _kv("DEVELOPER",        p.get("developer_canonical") or p.get("developer")) +
            _kv("EQUITY PARTNER",   p.get("equity_partner")) +
            _kv("ARCHITECT",        p.get("architect")) +
            _kv("CIVIL ENGINEER",   p.get("civil_engineer")),
            unsafe_allow_html=True,
        )
    with col2:
        st.markdown(
            _kv("ASSET CLASS",      p.get("asset_class")) +
            _kv("TOTAL SF",         gsf_str) +
            _kv("RESIDENTIAL UNITS", units_str) +
            _kv("COMMERCIAL SF",    cgsf_str) +
            _kv("PARKING SPACES",   parking_str),
            unsafe_allow_html=True,
        )
    with col3:
        st.markdown(
            _kv("HEIGHT",           ht_str) +
            _kv("STORIES",          stories_str) +
            _kv("EXPECTED DELIVERY", p.get("expected_delivery")) +
            _kv("FILING TYPE",      (p.get("processed_filing_type") or "").upper() or None),
            unsafe_allow_html=True,
        )

    # Links
    lc1, lc2, _ = st.columns([1, 1, 4])
    if p.get("bpda_url"):
        lc1.link_button("BPDA PAGE ↗", p["bpda_url"])
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
