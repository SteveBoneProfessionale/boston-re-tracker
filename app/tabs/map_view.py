import random
import re

import pandas as pd
import streamlit as st
import folium
from streamlit_folium import st_folium

# Neighborhood centroids for Boston
NEIGHBORHOOD_COORDS = {
    "Allston": (42.3534, -71.1326),
    "Back Bay": (42.3503, -71.0810),
    "Brighton": (42.3490, -71.1567),
    "Charlestown": (42.3780, -71.0602),
    "Chinatown": (42.3517, -71.0622),
    "Dorchester": (42.3010, -71.0674),
    "Downtown": (42.3554, -71.0603),
    "East Boston": (42.3744, -71.0398),
    "Fenway": (42.3442, -71.0990),
    "Hyde Park": (42.2552, -71.1245),
    "Jamaica Plain": (42.3101, -71.1132),
    "Longwood Medical Area": (42.3368, -71.1058),
    "Mattapan": (42.2738, -71.0927),
    "Mission Hill": (42.3281, -71.1053),
    "North End": (42.3647, -71.0542),
    "Roslindale": (42.2847, -71.1263),
    "Roxbury": (42.3133, -71.0892),
    "South Boston": (42.3355, -71.0481),
    "South Boston Waterfront": (42.3470, -71.0432),
    "South End": (42.3396, -71.0786),
    "West End": (42.3638, -71.0670),
    "West Roxbury": (42.2806, -71.1601),
}

STATUS_COLORS = {
    "Under Review": "orange",
    "Board Approved": "green",
    "Letter of Intent": "blue",
    "Under Construction": "red",
}

# Folium named colors → hex, for rendering in HTML legend
_FOLIUM_HEX = {
    "orange":   "#f97316",
    "green":    "#22c55e",
    "blue":     "#3b82f6",
    "red":      "#ef4444",
    "gray":     "#9ca3af",
    "purple":   "#a855f7",
    "darkred":  "#991b1b",
    "darkblue": "#1d4ed8",
    "darkgreen":"#15803d",
    "pink":     "#ec4899",
    "white":    "#f1f5f9",
    "black":    "#1e293b",
}

_FILTER_KEYS = [
    "map_status", "map_scale", "map_developer", "map_asset_class",
    "map_neighborhood", "map_city", "map_delivery_year", "map_equity_partner",
]


def _delivery_year(val: str) -> str:
    """Extract 4-digit year from expected_delivery, return 'Unknown' if absent."""
    if not val:
        return "Unknown"
    m = re.search(r"\b(20\d{2})\b", str(val))
    return m.group(1) if m else "Unknown"


def render(df: pd.DataFrame):
    st.header("Project Map")

    # Augment with delivery_year column for filtering
    df = df.copy()
    df["_delivery_year"] = df["expected_delivery"].apply(_delivery_year)

    # ── Filter option lists (dynamic) ──────────────────────────────────────
    statuses = ["All"] + sorted(df["status"].replace("", pd.NA).dropna().unique().tolist())
    scales = ["All", "Large Project", "Small Project"]
    developers = ["All"] + sorted(
        df["developer_canonical"].replace("", pd.NA).dropna().unique().tolist()
    )
    asset_classes = ["All"] + sorted(
        df["asset_class"].replace("", pd.NA).dropna().unique().tolist()
    )
    neighborhoods = ["All"] + sorted(
        df["neighborhood"].replace("", pd.NA).dropna().unique().tolist()
    )
    cities = ["All"] + sorted(
        df["city"].replace("", pd.NA).dropna().unique().tolist()
    )
    equity_partners = ["All"] + sorted(
        df["equity_partner"].replace("", pd.NA).dropna().unique().tolist()
    )

    # Fixed delivery year options — buckets
    all_years = sorted(
        {y for y in df["_delivery_year"] if y != "Unknown" and y >= "2025"}
    )
    year_buckets = ["All", "2025", "2026", "2027", "2028", "2029+", "Unknown"]

    # ── Clear All button ───────────────────────────────────────────────────
    if st.button("Clear All Filters", key="map_clear"):
        for k in _FILTER_KEYS:
            if k in st.session_state:
                del st.session_state[k]
        st.rerun()

    # ── Filter row 1: Status, Scale, City ─────────────────────────────────
    c1, c2, c3 = st.columns(3)
    status_f = c1.selectbox("Status", statuses, key="map_status")
    scale_f = c2.selectbox("Scale", scales, key="map_scale")
    city_f = c3.selectbox("City", cities, key="map_city")

    # ── Filter row 2: Neighborhood, Asset Class, Delivery Year ────────────
    c4, c5, c6 = st.columns(3)
    neighborhood_f = c4.selectbox("Neighborhood", neighborhoods, key="map_neighborhood")
    asset_class_f = c5.selectbox("Asset Class", asset_classes, key="map_asset_class")
    delivery_f = c6.selectbox("Delivery Year", year_buckets, key="map_delivery_year")

    # ── Filter row 3: Developer, Equity Partner ───────────────────────────
    c7, c8 = st.columns(2)
    developer_f = c7.selectbox("Developer", developers, key="map_developer")
    equity_f = c8.selectbox("Equity Partner", equity_partners, key="map_equity_partner")

    # ── Apply filters ──────────────────────────────────────────────────────
    filtered = df.copy()

    if status_f != "All":
        filtered = filtered[filtered["status"] == status_f]
    if scale_f != "All":
        filtered = filtered[filtered["project_scale"] == scale_f]
    if city_f != "All":
        filtered = filtered[filtered["city"] == city_f]
    if neighborhood_f != "All":
        filtered = filtered[filtered["neighborhood"] == neighborhood_f]
    if asset_class_f != "All":
        filtered = filtered[filtered["asset_class"] == asset_class_f]
    if developer_f != "All":
        filtered = filtered[filtered["developer_canonical"] == developer_f]
    if equity_f != "All":
        filtered = filtered[filtered["equity_partner"] == equity_f]

    if delivery_f != "All":
        if delivery_f == "Unknown":
            filtered = filtered[filtered["_delivery_year"] == "Unknown"]
        elif delivery_f == "2029+":
            filtered = filtered[
                (filtered["_delivery_year"] != "Unknown") &
                (filtered["_delivery_year"] >= "2029")
            ]
        else:
            filtered = filtered[filtered["_delivery_year"] == delivery_f]

    st.caption(f"Showing {len(filtered):,} of {len(df):,} projects")

    # ── Build map ──────────────────────────────────────────────────────────
    m = folium.Map(location=[42.3277, -71.0700], zoom_start=12, tiles="CartoDB positron")

    added = 0
    for _, row in filtered.iterrows():
        if pd.notna(row.get("latitude")) and pd.notna(row.get("longitude")):
            lat, lon = float(row["latitude"]), float(row["longitude"])
        else:
            coords = NEIGHBORHOOD_COORDS.get(row["neighborhood"])
            if not coords:
                continue
            random.seed(row["id"])
            lat = coords[0] + random.uniform(-0.003, 0.003)
            lon = coords[1] + random.uniform(-0.003, 0.003)

        color = STATUS_COLORS.get(row["status"], "gray")
        icon_symbol = "building" if row["project_scale"] == "Large Project" else "home"

        gsf_str = f"{int(row['total_gsf']):,} GSF" if pd.notna(row.get("total_gsf")) and row.get("total_gsf") else ""
        units_str = f"{int(row['residential_units']):,} units" if pd.notna(row.get("residential_units")) and row.get("residential_units") else ""
        dev = row.get("developer_canonical") or row.get("developer") or ""
        dev_str = f"<b>Developer:</b> {dev}<br>" if dev else ""
        eq = row.get("equity_partner") or ""
        eq_str = f"<b>Equity Partner:</b> {eq}<br>" if eq else ""
        city_str = f" &middot; {row['city']}" if row.get("city") and row["city"] != "Boston" else ""

        bpda_link = ""
        url = row.get("bpda_url", "")
        if url and not url.startswith("manual:"):
            bpda_link = f"<br><a href='{url}' target='_blank'>BPDA Page ↗</a>"

        popup_html = f"""
        <div style='min-width:200px'>
          <b>{row['name']}</b><br>
          {row['address']}{city_str}<br>
          {dev_str}{eq_str}
          <b>Status:</b> {row['status']}<br>
          <b>Asset Class:</b> {row['asset_class'] or '—'}<br>
          {gsf_str}{' · ' if gsf_str and units_str else ''}{units_str}
          {'<br><b>Delivery:</b> ' + row['expected_delivery'] if row.get('expected_delivery') else ''}
          {bpda_link}
        </div>"""

        folium.Marker(
            location=[lat, lon],
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=row["name"],
            icon=folium.Icon(color=color, icon=icon_symbol, prefix="fa"),
        ).add_to(m)
        added += 1

    # Build legend rows directly from STATUS_COLORS so they always match pin logic.
    # Also add a gray "Other" row for any unmapped statuses.
    legend_entries = list(STATUS_COLORS.items()) + [("Other / Unknown", "gray")]
    rows = "".join(
        f"<div style='display:flex;align-items:center;gap:7px;margin:3px 0'>"
        f"<span style='color:{_FOLIUM_HEX.get(color,'#9ca3af')};font-size:16px;line-height:1'>&#9679;</span>"
        f"<span style='color:#e2e8f0'>{label}</span>"
        f"</div>"
        for label, color in legend_entries
    )
    legend_html = (
        "<div style='"
        "position:fixed;bottom:20px;left:20px;"
        "background:rgba(15,23,42,0.88);"
        "padding:10px 14px;"
        "border-radius:8px;"
        "border:1px solid rgba(255,255,255,0.15);"
        "font-size:12px;"
        "z-index:9999;"
        "box-shadow:0 2px 8px rgba(0,0,0,0.5);"
        "min-width:160px;"
        "'>"
        f"<div style='font-weight:700;margin-bottom:6px;color:#ffffff;font-size:12px'>Status</div>"
        f"{rows}"
        "</div>"
    )
    m.get_root().html.add_child(folium.Element(legend_html))

    st_folium(m, width="100%", height=600, returned_objects=[])

    if added < len(filtered):
        st.caption(f"Note: {len(filtered) - added} projects could not be mapped (no coordinates).")
