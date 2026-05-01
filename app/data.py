"""Cached data access layer for the Streamlit app."""

import sys
from pathlib import Path
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import get_session, init_db
from db.models import Project, ProjectFiling, NewsItem
from scraper.classifier import classify_topics


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
                "filing_count": len(p.filings),
                "latitude": p.latitude,
                "longitude": p.longitude,
                "city": p.city or "Boston",
                "equity_partner": p.equity_partner or "",
            })
        return pd.DataFrame(rows)
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


def summary_stats(df: pd.DataFrame) -> dict:
    extracted = df[df["extraction_done"]]
    return {
        "total": len(df),
        "large": (df["project_scale"] == "Large Project").sum(),
        "small": (df["project_scale"] == "Small Project").sum(),
        "under_review": (df["status"] == "Under Review").sum(),
        "board_approved": (df["status"] == "Board Approved").sum(),
        "loi": (df["status"] == "Letter of Intent").sum(),
        "under_construction": (df["status"] == "Under Construction").sum(),
        "extracted": len(extracted),
        "total_units": int(extracted["residential_units"].dropna().sum()),
        "total_gsf": int(extracted["total_gsf"].dropna().sum()),
    }
