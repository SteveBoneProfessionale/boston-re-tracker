import math
import random
import re

import pandas as pd
import streamlit as st
import folium
from streamlit_folium import st_folium

_ORANGE = "#F5821E"
_MUTED  = "#8A9BB0"
_BORDER = "#1E2530"
_MONO   = "'JetBrains Mono', monospace"

NEIGHBORHOOD_COORDS = {
    "Allston":                  (42.3534, -71.1326),
    "Back Bay":                 (42.3503, -71.0810),
    "Brighton":                 (42.3490, -71.1567),
    "Charlestown":              (42.3780, -71.0602),
    "Chinatown":                (42.3517, -71.0622),
    "Dorchester":               (42.3010, -71.0674),
    "Downtown":                 (42.3554, -71.0603),
    "East Boston":              (42.3744, -71.0398),
    "Fenway":                   (42.3442, -71.0990),
    "Hyde Park":                (42.2552, -71.1245),
    "Jamaica Plain":            (42.3101, -71.1132),
    "Longwood Medical Area":    (42.3368, -71.1058),
    "Mattapan":                 (42.2738, -71.0927),
    "Mission Hill":             (42.3281, -71.1053),
    "North End":                (42.3647, -71.0542),
    "Roslindale":               (42.2847, -71.1263),
    "Roxbury":                  (42.3133, -71.0892),
    "South Boston":             (42.3355, -71.0481),
    "South Boston Waterfront":  (42.3470, -71.0432),
    "South End":                (42.3396, -71.0786),
    "West End":                 (42.3638, -71.0670),
    "West Roxbury":             (42.2806, -71.1601),
}

STATUS_COLORS = {
    "Under Review":       _ORANGE,
    "Board Approved":     "#22c55e",
    "Letter of Intent":   "#64748b",
    "Under Construction": "#ef4444",
}

_FILTER_KEYS = [
    "map_status", "map_scale", "map_developer", "map_asset_class",
    "map_neighborhood", "map_city", "map_delivery_year", "map_equity_partner",
    "map_min_sf", "map_include_unknown_sf",
]


def _delivery_year(val: str) -> str:
    if not val:
        return "Unknown"
    m = re.search(r"\b(20\d{2})\b", str(val))
    return m.group(1) if m else "Unknown"


def _section(label: str):
    st.markdown(
        f'<p style="font-family:{_MONO};font-size:9px;font-weight:700;'
        f'letter-spacing:0.18em;color:{_MUTED};text-transform:uppercase;'
        f'margin:16px 0 8px 0">{label}</p>',
        unsafe_allow_html=True,
    )


def render(df: pd.DataFrame):
    df = df.copy()
    df["_delivery_year"] = df["expected_delivery"].apply(_delivery_year)

    statuses     = ["All"] + sorted(df["status"].replace("", pd.NA).dropna().unique().tolist())
    scales       = ["All", "Large Project", "Small Project"]
    developers   = ["All"] + sorted(df["developer_canonical"].replace("", pd.NA).dropna().unique().tolist())
    asset_classes= ["All"] + sorted(df["asset_class"].replace("", pd.NA).dropna().unique().tolist())
    neighborhoods= ["All"] + sorted(df["neighborhood"].replace("", pd.NA).dropna().unique().tolist())
    cities       = ["All"] + sorted(df["city"].replace("", pd.NA).dropna().unique().tolist())
    equity_partners = ["All"] + sorted(df["equity_partner"].replace("", pd.NA).dropna().unique().tolist())
    year_buckets = ["All", "2025", "2026", "2027", "2028", "2029+", "Unknown"]

    _section("FILTERS")

    c1, c2, c3, c4 = st.columns(4)
    status_f      = c1.selectbox("STATUS",        statuses,      key="map_status")
    scale_f       = c2.selectbox("SCALE",         scales,        key="map_scale")
    city_f        = c3.selectbox("CITY",          cities,        key="map_city")
    neighborhood_f= c4.selectbox("NEIGHBORHOOD",  neighborhoods, key="map_neighborhood")

    c5, c6, c7, c8 = st.columns(4)
    asset_class_f = c5.selectbox("ASSET CLASS",   asset_classes, key="map_asset_class")
    delivery_f    = c6.selectbox("DELIVERY YEAR", year_buckets,  key="map_delivery_year")
    developer_f   = c7.selectbox("DEVELOPER",     developers,    key="map_developer")
    equity_f      = c8.selectbox("EQUITY PARTNER",equity_partners, key="map_equity_partner")

    if st.button("✕  CLEAR ALL FILTERS", key="map_clear"):
        for k in _FILTER_KEYS:
            st.session_state.pop(k, None)
        st.rerun()

    # ── Minimum SF threshold slider ───────────────────────────────────
    st.markdown("""
    <style>
    div[data-testid="stSlider"] div[role="slider"] {
        background: #F5821E !important;
        border-color: #F5821E !important;
        box-shadow: 0 0 0 3px rgba(245,130,30,0.18) !important;
    }
    div[data-testid="stSlider"] div[data-baseweb="slider"] > div > div:nth-child(2) {
        background: #1E2530 !important;
    }
    div[data-testid="stSlider"] div[data-baseweb="slider"] > div > div:nth-child(3) {
        background: #F5821E !important;
    }
    </style>
    """, unsafe_allow_html=True)

    sf_vals = df["total_gsf"].dropna()
    sf_max = int(math.ceil(sf_vals.max() / 100_000) * 100_000) if len(sf_vals) > 0 else 1_000_000

    cur_min_sf = st.session_state.get("map_min_sf", 0)
    sf_readout = "ALL SIZES" if cur_min_sf == 0 else f"MIN SIZE: {cur_min_sf // 1000}K SF"

    _section("MINIMUM PROJECT SIZE")
    st.markdown(
        f'<p style="font-family:{_MONO};font-size:11px;font-weight:700;'
        f'letter-spacing:0.08em;color:#e2e8f0;margin:0 0 2px 0">{sf_readout}</p>',
        unsafe_allow_html=True,
    )
    min_sf = st.slider(
        "min_sf_slider",
        min_value=0,
        max_value=sf_max,
        step=5_000,
        key="map_min_sf",
        label_visibility="collapsed",
    )
    include_unknown_sf = st.checkbox(
        "Include projects with unknown size",
        value=True,
        key="map_include_unknown_sf",
    )

    # Apply filters
    filtered = df.copy()
    if status_f       != "All": filtered = filtered[filtered["status"]             == status_f]
    if scale_f        != "All": filtered = filtered[filtered["project_scale"]      == scale_f]
    if city_f         != "All": filtered = filtered[filtered["city"]               == city_f]
    if neighborhood_f != "All": filtered = filtered[filtered["neighborhood"]       == neighborhood_f]
    if asset_class_f  != "All": filtered = filtered[filtered["asset_class"]        == asset_class_f]
    if developer_f    != "All": filtered = filtered[filtered["developer_canonical"]== developer_f]
    if equity_f       != "All": filtered = filtered[filtered["equity_partner"]     == equity_f]

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

    # SF minimum threshold
    has_sf = filtered["total_gsf"].notna()
    if min_sf > 0:
        meets_threshold = has_sf & (filtered["total_gsf"] >= min_sf)
        if include_unknown_sf:
            filtered = filtered[meets_threshold | ~has_sf]
        else:
            filtered = filtered[meets_threshold]
    elif not include_unknown_sf:
        filtered = filtered[has_sf]

    st.markdown(
        f'<p style="font-family:{_MONO};font-size:10px;color:{_MUTED};margin:4px 0 10px">'
        f'<span style="color:#e2e8f0;font-weight:700">{len(filtered):,}</span> OF '
        f'{len(df):,} PROJECTS MAPPED</p>',
        unsafe_allow_html=True,
    )

    # ── Map ────────────────────────────────────────────────────────
    m = folium.Map(
        location=[42.3277, -71.0700],
        zoom_start=12,
        tiles="CartoDB dark_matter",
    )

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

        color = STATUS_COLORS.get(row["status"], _MUTED)

        gsf_str   = f"{int(row['total_gsf']):,} SF" if pd.notna(row.get("total_gsf")) and row.get("total_gsf") else ""
        units_str = f"{int(row['residential_units']):,} UNITS" if pd.notna(row.get("residential_units")) and row.get("residential_units") else ""
        dev       = row.get("developer_canonical") or row.get("developer") or ""
        eq        = row.get("equity_partner") or ""
        city_s    = f" · {row['city']}" if row.get("city") and row["city"] != "Boston" else ""

        bpda_link = ""
        url = row.get("bpda_url", "")
        if url and not url.startswith("manual:"):
            bpda_link = f"<br><a href='{url}' target='_blank' style='color:{_ORANGE}'>BPDA PAGE ↗</a>"

        popup_html = f"""
        <div style='min-width:220px;background:#141720;padding:12px 14px;
                    font-family:monospace;border-left:3px solid {color}'>
          <div style='font-size:12px;font-weight:700;color:#fff;margin-bottom:6px;
                      line-height:1.3'>{row['name']}</div>
          <div style='font-size:10px;color:{_MUTED};margin-bottom:4px'>{row['address']}{city_s}</div>
          {"<div style='font-size:10px;color:#e2e8f0;margin-bottom:2px'>" + dev + "</div>" if dev else ""}
          {"<div style='font-size:10px;color:" + _MUTED + ";margin-bottom:2px'>EQ: " + eq + "</div>" if eq else ""}
          <div style='margin-top:8px;display:flex;gap:8px;flex-wrap:wrap'>
            <span style='font-size:9px;font-weight:700;letter-spacing:0.1em;
                         color:{color};border:1px solid {color};padding:2px 6px'>
              {row['status'].upper()}</span>
            {"<span style='font-size:9px;color:" + _MUTED + "'>" + row['asset_class'] + "</span>" if row.get('asset_class') else ""}
          </div>
          {"<div style='font-size:10px;color:" + _MUTED + ";margin-top:6px'>" + gsf_str + (" · " if gsf_str and units_str else "") + units_str + "</div>" if gsf_str or units_str else ""}
          {"<div style='font-size:10px;color:" + _MUTED + ";margin-top:2px'>DELIVERY: " + row['expected_delivery'] + "</div>" if row.get('expected_delivery') else ""}
          {bpda_link}
        </div>"""

        folium.CircleMarker(
            location=[lat, lon],
            radius=7,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.85,
            weight=1.5,
            popup=folium.Popup(popup_html, max_width=280),
            tooltip=f'<span style="font-family:monospace;font-size:11px">{row["name"]}</span>',
        ).add_to(m)
        added += 1

    # Terminal legend
    legend_rows = "".join(
        f'<div style="display:flex;align-items:center;gap:8px;margin:4px 0">'
        f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;'
        f'background:{color}"></span>'
        f'<span style="font-family:monospace;font-size:10px;letter-spacing:0.08em;'
        f'color:#e2e8f0;text-transform:uppercase">{label}</span>'
        f'</div>'
        for label, color in list(STATUS_COLORS.items()) + [("Other / Unknown", _MUTED)]
    )
    legend_html = (
        "<div style='"
        "position:fixed;bottom:24px;left:20px;"
        f"background:rgba(13,15,18,0.92);"
        f"border:1px solid {_BORDER};"
        "padding:10px 14px;"
        "z-index:9999;"
        "box-shadow:0 4px 20px rgba(0,0,0,0.7);"
        "'>"
        f"<div style='font-family:monospace;font-size:9px;font-weight:700;letter-spacing:0.18em;"
        f"color:{_MUTED};text-transform:uppercase;margin-bottom:8px'>STATUS</div>"
        f"{legend_rows}"
        "</div>"
    )
    m.get_root().html.add_child(folium.Element(legend_html))

    st_folium(m, width="100%", height=580, returned_objects=[])

    if added < len(filtered):
        st.caption(f"{len(filtered) - added} PROJECTS COULD NOT BE MAPPED (NO COORDINATES)")
