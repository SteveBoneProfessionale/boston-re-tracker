"""Cached data access layer for the Streamlit app."""

import sys
from pathlib import Path
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import get_session, init_db
from db.models import (
    Project, ProjectFiling, NewsItem,
    CambridgeSpecialPermit, CambridgeBuildingPermit, CambridgeProjectAlias,
)
from scraper.classifier import classify_topics

# Normalized cross-city "stage" -- each city keeps its own native status
# vocabulary (shown as-is on the detail view / in city-scoped filters), but
# charts and the map that mix both cities together color/group by this
# instead, since Boston's 4-value status vocabulary and Cambridge's 7-value
# one don't share any values.
STAGE_MAP_BOSTON = {
    "Letter of Intent":   "Planning",
    "Under Review":       "Permitting",
    "Board Approved":     "Approved",
    "Under Construction": "Under Construction",
}
STAGE_MAP_CAMBRIDGE = {
    "Pre-Permitting":                                    "Planning",
    "Permitting":                                        "Permitting",
    "Design Review":                                      "Permitting",
    "Approved PUD/Master Plan Development Remaining":     "Approved",
    "Zoning Permit Granted or As of Right":               "Approved",
    "Building Permit Granted":                            "Under Construction",
    "Complete":                                           "Complete",
}
STAGE_COLORS = {
    "Planning":            "#64748b",
    "Permitting":          "#F5821E",
    "Approved":            "#22c55e",
    "Under Construction":  "#ef4444",
    "Complete":            "#0ea5e9",
}
STAGE_ORDER = ["Planning", "Permitting", "Approved", "Under Construction", "Complete"]


def normalize_stage(status: str, city: str) -> str:
    m = STAGE_MAP_CAMBRIDGE if city == "Cambridge" else STAGE_MAP_BOSTON
    return m.get(status, "")


@st.cache_data(ttl=300)
def load_projects() -> pd.DataFrame:
    session = get_session()
    try:
        projects = session.query(Project).all()
        rows = []
        for p in projects:
            rows.append({
                "id": p.id,
                "name": p.name or "",
                "address": p.address or "",
                "neighborhood": p.neighborhood or "",
                "status": p.status or "",
                "project_scale": p.project_scale or "",
                "bpda_gsf": p.bpda_gsf,
                "bpda_url": p.bpda_url or "",
                # Extracted
                "developer": p.developer or "",
                "developer_canonical": p.developer_canonical or "",
                "asset_class": p.asset_class or "",
                "total_gsf": p.total_gsf or p.bpda_gsf,
                "residential_units": p.residential_units,
                "commercial_gsf": p.commercial_gsf,
                "building_height_ft": p.building_height_ft,
                "num_stories": p.num_stories,
                "parking_spaces": p.parking_spaces,
                "architect": p.architect or "",
                "civil_engineer": p.civil_engineer or "",
                "expected_delivery": p.expected_delivery or "",
                "description": p.description or "",
                "processed_filing_type": p.processed_filing_type or "",
                "processed_filing_url": p.processed_filing_url or "",
                "extraction_done": p.extraction_timestamp is not None,
                "requires_extraction": p.requires_extraction if p.requires_extraction is not None else True,
                "filing_count": len(p.filings),
                "latitude": p.latitude,
                "longitude": p.longitude,
                "city": p.city or "Boston",
                "equity_partner": p.equity_partner or "",
                "stage": normalize_stage(p.status or "", p.city or "Boston"),
                # Cambridge Development Log fields (blank for Boston rows)
                "cambridge_project_id": p.cambridge_project_id or "",
                "permit_type": p.permit_type or "",
                "project_type": p.project_type or "",
                "lot_area": p.lot_area,
                "far": p.far,
                "far_scope": p.far_scope or "",
                "affordable_units": p.affordable_units,
                "affordable_units_tbd": bool(p.affordable_units_tbd),
                "total_gfa_tbd": bool(p.total_gfa_tbd),
                "hotel_rooms": p.hotel_rooms,
                "neighborhood_id": p.neighborhood_id,
                "zoning_raw": p.zoning_raw or "",
                "notes": p.notes or "",
                "parking_notes": p.parking_notes or "",
                "parent_project_id": p.parent_project_id,
                "phase_group": p.phase_group or "",
                "conditional_alternative": bool(p.conditional_alternative),
                "spans_municipalities": bool(p.spans_municipalities),
                "coords_approximate": bool(p.coords_approximate),
                "special_permit_raw": p.special_permit_raw or "",
                "building_permit_raw": p.building_permit_raw or "",
            })
        df = pd.DataFrame(rows)
        if not df.empty:
            # A row's financial fields (total_gsf, residential_units, etc.) are ready to
            # chart once extraction has run, OR immediately if the row came from a
            # structured-data pipeline that never needed extraction in the first place.
            df["has_financials"] = df["extraction_done"] | ~df["requires_extraction"]
        return df
    finally:
        session.close()


@st.cache_data(ttl=300)
def load_cambridge_permits(project_id: int) -> dict:
    """Special-permit and building-permit child rows for one Cambridge project."""
    session = get_session()
    try:
        special = session.query(CambridgeSpecialPermit).filter_by(project_id=project_id).all()
        building = session.query(CambridgeBuildingPermit).filter_by(project_id=project_id).all()
        aliases = session.query(CambridgeProjectAlias).filter_by(project_id=project_id).all()
        return {
            "special_permits": [
                {"base": s.base_permit, "amendment": s.amendment_raw, "raw": s.full_raw}
                for s in special
            ],
            "building_permits": [
                {"number": b.permit_number, "label": b.label} for b in building
            ],
            "aliases": [a.former_name for a in aliases],
        }
    finally:
        session.close()


@st.cache_data(ttl=300)
def load_filings(project_id: int) -> pd.DataFrame:
    session = get_session()
    try:
        filings = session.query(ProjectFiling).filter_by(project_id=project_id).all()
        return pd.DataFrame([{
            "name": f.name,
            "date": f.date,
            "category": f.filing_category,
            "url": f.url,
            "processed": f.is_processed,
        } for f in filings])
    finally:
        session.close()


@st.cache_data(ttl=300)
def load_news(limit: int = 200) -> pd.DataFrame:
    session = get_session()
    try:
        items = (
            session.query(NewsItem)
            .order_by(NewsItem.published_date.desc().nullslast())
            .limit(limit)
            .all()
        )
        rows = []
        for n in items:
            proj_name = ""
            if n.linked_project_id:
                proj = session.query(Project).get(n.linked_project_id)
                proj_name = proj.name if proj else ""
            rows.append({
                "id": n.id,
                "title": n.title or "",
                "url": n.url or "",
                "published_date": n.published_date,
                "source": n.source or "",
                "summary": n.summary or "",
                "linked_project_id": n.linked_project_id,
                "linked_project_name": proj_name,
                "match_score": n.match_score,
                "topics": getattr(n, "topics", "") or "",
            })
        return pd.DataFrame(rows)
    finally:
        session.close()


def backfill_topics() -> int:
    """Classify any news articles that have no topics yet. Returns count updated."""
    init_db()
    session = get_session()
    try:
        untagged = session.query(NewsItem).filter(
            (NewsItem.topics == None) | (NewsItem.topics == "")  # noqa: E711
        ).all()
        if not untagged:
            return 0
        for item in untagged:
            item.topics = classify_topics(item.title or "", item.summary or "")
        session.commit()
        return len(untagged)
    except Exception:
        session.rollback()
        return 0
    finally:
        session.close()


def summary_stats(df: pd.DataFrame, include_conditional: bool = False) -> dict:
    """Aggregate stats for the sidebar/overview tiles.

    `has_financials` (see load_projects) is true once a row's financial
    fields are ready to chart, regardless of which market/pipeline it came
    from. By default, rows flagged conditional_alternative (competing
    unbuilt plans for the same site, e.g. the MXD Infill PB315 MA2/MA3 case)
    are excluded from SF/unit totals to avoid double-counting; pass
    include_conditional=True to include them anyway.
    """
    has_conditional_col = "conditional_alternative" in df.columns
    n_conditional = int(df["conditional_alternative"].sum()) if has_conditional_col else 0

    extracted = df[df["has_financials"]] if "has_financials" in df.columns else df[df["extraction_done"]]
    if not include_conditional and has_conditional_col:
        extracted = extracted[~extracted["conditional_alternative"]]

    # Stage counts drive the KPI cards -- computed from the SAME conditional-
    # filtered set as the SF/unit totals below, so every number on the
    # Overview tab toggles together. Every row has a stage (native status
    # always maps to one of the 5 canonical stages), so stage_counts sums to
    # len(stage_scope) exactly; add back n_conditional when they're excluded
    # to reconcile to the full project total.
    stage_scope = df if include_conditional or not has_conditional_col else df[~df["conditional_alternative"]]
    stage_counts = {s: int((stage_scope["stage"] == s).sum()) for s in STAGE_ORDER}
    excluded_from_stages = n_conditional if (not include_conditional and has_conditional_col) else 0

    return {
        "total": len(df),
        "total_boston": int((df["city"] == "Boston").sum()),
        "total_cambridge": int((df["city"] == "Cambridge").sum()),
        "large": (df["project_scale"] == "Large Project").sum(),
        "small": (df["project_scale"] == "Small Project").sum(),
        "stage_counts": stage_counts,
        "stage_reconciles": sum(stage_counts.values()) + excluded_from_stages == len(df),
        "extracted": len(extracted),
        "total_units": int(extracted["residential_units"].dropna().sum()),
        "total_gsf": int(extracted["total_gsf"].dropna().sum()),
        "conditional_alternative_count": n_conditional,
    }
