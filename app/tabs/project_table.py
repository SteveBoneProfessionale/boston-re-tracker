import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pandas as pd
import streamlit as st

from app.data import load_filings
from scraper.normalize_developer import is_real_company

STATUS_COLORS = {
    "Under Review": "#f59e0b",
    "Board Approved": "#10b981",
    "Letter of Intent": "#3b82f6",
    "Under Construction": "#ef4444",
}
SCALE_COLORS = {"Large Project": "#6366f1", "Small Project": "#a78bfa"}


def render(df: pd.DataFrame):
    st.markdown("## Project Directory")

    # ── Filters ────────────────────────────────────────────────────────────
    with st.expander("Filter Projects", expanded=True):
        fc1, fc2, fc3, fc4 = st.columns(4)

        neighborhoods = ["All"] + sorted([n for n in df["neighborhood"].unique() if n])
        nbhd = fc1.selectbox("Neighborhood", neighborhoods, key="tbl_nbhd")

        statuses = ["All"] + sorted([s for s in df["status"].unique() if s])
        status = fc2.selectbox("Status", statuses, key="tbl_status")

        scale = fc3.selectbox("Scale", ["All", "Large Project", "Small Project"], key="tbl_scale")

        classes = ["All"] + sorted([a for a in df["asset_class"].unique() if a])
        asset = fc4.selectbox("Asset Class", classes, key="tbl_asset")

        fd1, fd2 = st.columns([2, 3])
        all_devs = sorted(
            {d for d in df["developer_canonical"].unique() if is_real_company(d)},
            key=lambda x: x.lstrip("Tt").lower() if x.lower().startswith("the ") else x.lower()
        )
        dev_search = fd1.text_input("Search developer", "", key="tbl_dev_search",
                                    placeholder="e.g. Marcus Partners")
        matching_devs = (
            [d for d in all_devs if dev_search.lower() in d.lower()]
            if dev_search else all_devs
        )
        developer = fd1.selectbox("Developer", ["All"] + matching_devs, key="tbl_developer")

        search = fd2.text_input("Search by name or address", "", key="tbl_search")

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

    col_count, col_export = st.columns([4, 1])
    col_count.caption(f"**{len(filtered)}** projects shown · {len(df)} total")
    csv = filtered.to_csv(index=False).encode()
    col_export.download_button("Export CSV", csv, "boston_cre_pipeline.csv", "text/csv")

    # ── Table ──────────────────────────────────────────────────────────────
    display = filtered[[
        "name", "neighborhood", "status", "project_scale",
        "developer_canonical", "developer", "asset_class", "total_gsf", "residential_units",
        "building_height_ft", "expected_delivery",
    ]].copy()

    # Keep numeric columns as numbers so Streamlit sorts them numerically.
    # NaN stays NaN — Streamlit always puts nulls at the bottom.
    display["total_gsf"] = pd.to_numeric(display["total_gsf"], errors="coerce")
    display["residential_units"] = pd.to_numeric(display["residential_units"], errors="coerce")
    display["building_height_ft"] = pd.to_numeric(display["building_height_ft"], errors="coerce")

    _BAD_DEVS = {"Unknown - review needed", "Unknown", "UNKNOWN",
                 "Zoning Petitions for Text Amendments", ""}

    def _dev_display(row) -> str:
        canonical = str(row["developer_canonical"] or "").strip()
        if canonical and canonical not in _BAD_DEVS and is_real_company(canonical):
            return canonical
        raw = str(row["developer"] or "").strip()
        if raw and raw not in _BAD_DEVS:
            return raw
        return "Unknown"

    display["developer_canonical"] = display.apply(_dev_display, axis=1)
    display.drop(columns=["developer"], inplace=True)

    display.columns = [
        "Project", "Neighborhood", "Status", "Scale",
        "Developer", "Asset Class", "GSF", "Units", "Height", "Delivery",
    ]

    selection = st.dataframe(
        display,
        use_container_width=True,
        hide_index=True,
        height=420,
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            "Status": st.column_config.TextColumn(width="medium"),
            "Scale": st.column_config.TextColumn(width="medium"),
            "GSF": st.column_config.NumberColumn(
                format="%d", help="Gross square feet"
            ),
            "Units": st.column_config.NumberColumn(
                format="%d", help="Residential units"
            ),
            "Height": st.column_config.NumberColumn(
                format="%d ft", help="Building height in feet"
            ),
        },
    )

    # ── Detail card ────────────────────────────────────────────────────────
    if selection and selection.selection.rows:
        idx = selection.selection.rows[0]
        _detail_card(filtered.iloc[idx])


def _status_pill(label: str, color: str) -> str:
    return (
        f"<span style='background:{color};color:white;padding:3px 10px;"
        f"border-radius:4px;font-size:0.78rem;font-weight:600'>{label}</span>"
    )


def _detail_card(p: pd.Series):
    st.divider()
    status_color = STATUS_COLORS.get(p["status"], "#9ca3af")
    scale_color = SCALE_COLORS.get(p["project_scale"], "#9ca3af")

    st.markdown(f"### {p['name']}")
    st.markdown(
        _status_pill(p["status"], status_color) + "&nbsp;&nbsp;" +
        _status_pill(p["project_scale"], scale_color) + "&nbsp;&nbsp;" +
        f"<span style='color:#6b7280;font-size:0.9rem'>{p['neighborhood']}</span>",
        unsafe_allow_html=True,
    )

    if p.get("description"):
        st.info(p["description"])

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("**Project Details**")
        _row("Address", p.get("address"))
        canonical = p.get("developer_canonical") or p.get("developer")
        raw = p.get("developer")
        if canonical:
            st.markdown(
                f"<small style='color:#6b7280'>Developer</small><br>"
                f"<span style='font-weight:600'>{canonical}</span>",
                unsafe_allow_html=True,
            )
            if raw and raw != canonical:
                st.markdown(
                    f"<small style='color:#9ca3af'>Legal entity: {raw}</small>",
                    unsafe_allow_html=True,
                )
            st.write("")
        _row("Architect", p.get("architect"))
        _row("Civil Engineer", p.get("civil_engineer"))
        _row("Expected Delivery", p.get("expected_delivery"))

    with col2:
        st.markdown("**Program**")
        _row("Asset Class", p.get("asset_class"))
        gsf = p.get("total_gsf")
        _row("Total GSF", f"{int(gsf):,} ft²" if pd.notna(gsf) and gsf else None)
        units = p.get("residential_units")
        _row("Residential Units", f"{int(units):,}" if pd.notna(units) and units else None)
        cgsf = p.get("commercial_gsf")
        _row("Commercial GSF", f"{int(cgsf):,} ft²" if pd.notna(cgsf) and cgsf else None)
        parking = p.get("parking_spaces")
        _row("Parking Spaces", f"{int(parking):,}" if pd.notna(parking) and parking else None)

    with col3:
        st.markdown("**Building**")
        ht = p.get("building_height_ft")
        _row("Height", f"{ht:.0f} ft" if pd.notna(ht) and ht else None)
        _row("Stories", p.get("num_stories") if pd.notna(p.get("num_stories") or float("nan")) else None)
        _row("Filing Type", (p.get("processed_filing_type") or "").upper() or None)

    link1, link2, _ = st.columns([1, 1, 3])
    if p.get("bpda_url"):
        link1.link_button("BPDA Project Page", p["bpda_url"])
    if p.get("processed_filing_url"):
        label = f"Source {(p.get('processed_filing_type') or 'PDF').upper()}"
        link2.link_button(label, p["processed_filing_url"])

    filings_df = load_filings(int(p["id"]))
    if not filings_df.empty:
        with st.expander(f"All Filings ({len(filings_df)})"):
            st.dataframe(filings_df, use_container_width=True, hide_index=True)


def _row(label: str, value):
    if value and str(value).strip():
        st.markdown(f"<small style='color:#6b7280'>{label}</small><br>"
                    f"<span style='font-weight:500'>{value}</span>",
                    unsafe_allow_html=True)
        st.write("")
