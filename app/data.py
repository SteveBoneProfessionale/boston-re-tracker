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
# How a developer name was arrived at, and how it renders. Verified and
# inferred names must never look identical in a chart.
RESOLUTION_METHODS = {
    "registry_confirmed": {
        "label": "Registry confirmed",
        "verified": True,
        "pattern": "",           # solid
        "note": "sole operating company in the applicant's RI Corporate Database address cluster",
    },
    "web_corroborated": {
        "label": "Web corroborated",
        "verified": False,
        "pattern": "/",          # hatched — visibly inferred
        "note": "named by 2+ independent sources reporting on this specific address, "
                "at least one of them local trade press or a government record",
    },
    "web_low_confidence": {
        "label": "Web — low confidence",
        "verified": False,
        "pattern": "x",          # cross-hatched — weakest tier
        "note": "cleared 2 independent sources, but the sources are weaker or the "
                "developer link is indirect (established via a named principal). "
                "Populated, but flagged for review.",
    },
    "human_set": {
        "label": "Set by hand",
        "verified": True,
        "pattern": ".",
        "note": "entered or corrected manually",
    },
}
# Boston and Cambridge developers predate this field. Their names came from
# filings and a curated audit, so they are treated as verified rather than
# being lumped in with inferred names purely for having a blank column.
LEGACY_METHOD = "registry_confirmed"


def resolution_method(raw: str) -> str:
    return raw if raw in RESOLUTION_METHODS else LEGACY_METHOD


def is_inferred(raw: str) -> bool:
    """True when the developer name was inferred rather than verified."""
    return not RESOLUTION_METHODS[resolution_method(raw)]["verified"]


STAGE_COLORS = {
    "Planning":            "#64748b",
    "Permitting":          "#F5821E",
    "Approved":            "#22c55e",
    "Under Construction":  "#ef4444",
    "Complete":            "#0ea5e9",
}
STAGE_ORDER = ["Planning", "Permitting", "Approved", "Under Construction", "Complete"]

# Rhode Island review-stage vocabulary -> canonical stage. Derived empirically
# from 240 development items across all five municipalities' Tier 1 boards
# (see scraper/ri_vocab_analysis.py), not from the statute alone.
#
# Two entries are additions to the original proposal:
#   "Development Plan Review" -- absent from the starting mapping but the
#     dominant vocabulary outside Providence (18 of 20 Newport items, 18 of 31
#     Warwick, 26 of 58 Cranston). Under RIGL 45-23 it runs administratively
#     (one stage) or formally (preliminary + final), so bare DPR is Permitting
#     and "DPR - Final Plan" is Approved.
#   "Rezoning" -- City Council referrals appearing on CPC agendas. Earliest
#     pipeline signal, so Planning. Per the Tier 3 decision on Providence
#     Zoning Commission, these link to a parcel and never create a record.
RI_STAGE_MAP = {
    "Pre-application Conference":        "Planning",
    "Informational / No Vote":           "Planning",
    "Master Plan":                       "Planning",
    "Conceptual":                        "Planning",
    "Rezoning":                          "Planning",
    "Preliminary Plan":                  "Permitting",
    "Development Plan Review":           "Permitting",
    "Unified Development Review":        "Permitting",
    "Special Use Permit":                "Permitting",
    "Combined Master and Preliminary":   "Permitting",
    "Final Plan":                        "Approved",
    "Development Plan Review - Final":   "Approved",
    "Administrative Review":             "Approved",
    "Plan Recorded":                     "Approved",
}

# Events that are recorded in stage history but must NEVER move the current
# stage: an extension does not advance a project, a continuance is a non-event,
# and a waiver or modification is an attribute of a filing.
RI_NON_ADVANCING = {
    "Extension", "Modification", "Continued", "Waiver", "Dimensional Variance",
    "Use Variance",
}

# ── Market registry ─────────────────────────────────────────────────────
# One entry per municipality. Everything that varies by market lives here,
# so chart and filter components stay city-agnostic: they look values up in
# this table instead of branching on a city name. Adding a municipality is
# a registry entry, not an `if city == ...` in a component.
#
#   market              -- grouping label for the market-level City filter
#   stage_map           -- native status vocabulary -> canonical STAGE_ORDER
#   review_scale_vocab  -- ordered permitted values for `review_scale`, most
#                          intensive review first. None means this market's
#                          statute has no scale classification at all, which
#                          the Review Scale chart renders as "not applicable"
#                          rather than as an empty chart or a zero.
MARKETS: dict[str, dict] = {
    "Boston": {
        "market": "Massachusetts",
        "stage_map": STAGE_MAP_BOSTON,
        "review_scale_vocab": ["Large Project", "Small Project"],   # Article 80
    },
    "Cambridge": {
        "market": "Massachusetts",
        "stage_map": STAGE_MAP_CAMBRIDGE,
        "review_scale_vocab": None,          # no Article 80 equivalent
    },
    # Manually-entered MA municipalities outside BPDA/CDD jurisdiction. They
    # carried Boston-vocabulary statuses before this registry existed, so they
    # keep the Boston stage map; none of them carry a scale classification.
    "Hudson": {"market": "Massachusetts", "stage_map": STAGE_MAP_BOSTON, "review_scale_vocab": None},
    "Revere": {"market": "Massachusetts", "stage_map": STAGE_MAP_BOSTON, "review_scale_vocab": None},
    "Woburn": {"market": "Massachusetts", "stage_map": STAGE_MAP_BOSTON, "review_scale_vocab": None},
}

# Rhode Island municipalities share one vocabulary and one constraint.
#
# reachable_stages makes Under Construction and Complete STRUCTURALLY
# unreachable rather than merely unlikely: across 240 development items those
# two stages had exactly zero occurrences, because agendas and minutes never
# report groundbreaking or occupancy. Enforcing it here means no future parser
# change or extraction bug can quietly start populating them -- a project that
# is approved stays Approved until a source that actually knows says otherwise.
for _ri_city in ("Providence", "Cranston", "Pawtucket", "Newport", "Warwick"):
    MARKETS[_ri_city] = {
        "market": "Rhode Island",
        "stage_map": RI_STAGE_MAP,
        "review_scale_vocab": ["Major", "Minor", "Administrative"],   # RIGL 45-23
        "reachable_stages": ["Planning", "Permitting", "Approved"],
    }

# Unregistered cities behave exactly as they did before the registry existed:
# Boston's stage vocabulary, no scale classification.
_DEFAULT_MARKET = {
    "market": "Massachusetts",
    "stage_map": STAGE_MAP_BOSTON,
    "review_scale_vocab": None,
}

# Review-scale colors are assigned by tier position within each market's
# vocabulary, not by matching a literal string -- so a market's most-intensive
# review tier is always orange, its second teal, and so on, and a new market
# gets consistent colors without touching the chart. First registration of a
# label wins, keeping a shared label stable across markets.
_SCALE_RAMP = ["#F5821E", "#0ea5e9", "#64748b", "#8A9BB0"]
REVIEW_SCALE_COLORS: dict[str, str] = {}
for _entry in MARKETS.values():
    for _i, _label in enumerate(_entry["review_scale_vocab"] or []):
        REVIEW_SCALE_COLORS.setdefault(_label, _SCALE_RAMP[min(_i, len(_SCALE_RAMP) - 1)])


# ── Canonical asset classes ─────────────────────────────────────────────
# The only values `asset_class` may hold. Every market writes to this set and
# nothing else, so ingesting a new market can't widen the vocabulary. The
# source's own wording is kept verbatim in `asset_class_raw`, so a fold stays
# recoverable and the detail view can still show what the filing actually said.
ASSET_CLASSES = [
    "Residential", "Mixed-Use", "Office", "Lab/Research", "Retail",
    "Hotel", "Industrial", "Institutional", "Parking", "Other",
]

# Non-canonical values seen in existing data -> canonical. Applied at ingestion
# for every market, and by scraper/fold_asset_class.py to existing records.
#
# Office/R&D and Lab/R&D are deliberately NOT listed: folding them moves 20
# rows and ~10.7M SF between bars on the Gross SF by Asset Class chart, which
# is a separate task requiring its own before/after diff.
ASSET_CLASS_FOLDS = {
    "Fire Department": "Institutional",
    "Educational":     "Institutional",
}


def canonical_asset_class(raw: str | None) -> str | None:
    """Map a source classification onto the canonical set.

    Returns None for an unrecognized value rather than guessing -- a blank
    asset class is correct, an invented one is not.

    For classifying NEW records at ingestion only. Do not run it over existing
    rows: the deferred values (Office/R&D, Lab/R&D, Parking Garage) are not in
    ASSET_CLASS_FOLDS yet, so this would null 21 Cambridge rows rather than
    fold them. scraper/fold_asset_class.py is the safe path for existing data
    -- it leaves anything it doesn't recognize untouched and reports it.
    """
    if not raw or not str(raw).strip():
        return None
    v = str(raw).strip()
    v = ASSET_CLASS_FOLDS.get(v, v)
    return v if v in ASSET_CLASSES else None


def market_of(city: str) -> str:
    """Market (state-level grouping) a municipality belongs to."""
    return MARKETS.get(city, _DEFAULT_MARKET)["market"]


def normalize_stage(status: str, city: str) -> str:
    entry = MARKETS.get(city, _DEFAULT_MARKET)
    stage = entry["stage_map"].get(status, "")
    return _enforce_reachable(stage, entry)


def _enforce_reachable(stage: str, entry: dict) -> str:
    """Drop a stage a market cannot legitimately reach.

    Returns "" rather than silently downgrading, so the reconciliation warning
    on the Overview tab surfaces it instead of it passing as a real value.
    """
    allowed = entry.get("reachable_stages")
    if stage and allowed and stage not in allowed:
        return ""
    return stage


def resolve_stage(city: str, status: str, heard: str | None,
                  confirmed: str | None) -> tuple[str, bool]:
    """The stage to chart, and whether it is provisional.

    Markets whose source records an outcome (Boston, Cambridge) keep driving
    stage off their native status. Rhode Island carries two fields instead:
    `confirmed` comes from minutes and is authoritative; `heard` comes from the
    agenda and means only "this project was scheduled at this stage".

    Provisional (True) means the stage came from an agenda with no minutes to
    confirm it. The UI must surface that -- a project must never read as
    Approved on the strength of having been scheduled for a vote.
    """
    entry = MARKETS.get(city, _DEFAULT_MARKET)
    if confirmed:
        return _enforce_reachable(_as_stage(confirmed, entry), entry), False
    if heard:
        return _enforce_reachable(_as_stage(heard, entry), entry), True
    return normalize_stage(status or "", city), False


def _as_stage(value: str, entry: dict) -> str:
    """Accept either a canonical stage or a market's native review-stage label.

    Ingestion reads a review-stage label off an agenda ("Preliminary Plan");
    storing the canonical stage is preferred, but accepting both means a stored
    label is mapped rather than silently discarded as unrecognized.
    """
    if value in STAGE_ORDER:
        return value
    return entry["stage_map"].get(value, "")


def review_scale_vocab(cities) -> list[str]:
    """Ordered union of review-scale vocabularies across the given cities.

    Empty means no market in the selection classifies projects by review
    scale, which is a meaningfully different statement from "no data yet"
    and is rendered as such.
    """
    out: list[str] = []
    for city in cities:
        for label in MARKETS.get(city, _DEFAULT_MARKET)["review_scale_vocab"] or []:
            if label not in out:
                out.append(label)
    return out


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
                "review_scale": p.review_scale or "",
                "review_scale_raw": p.review_scale_raw or "",
                "bpda_gsf": p.bpda_gsf,
                "bpda_url": p.bpda_url or "",
                # Extracted
                "developer": p.developer or "",
                "developer_canonical": p.developer_canonical or "",
                # Provenance of the developer name. Charts distinguish an
                # inferred name from a verified one rather than presenting
                # both as equally solid.
                "developer_resolution_method": p.developer_resolution_method or "",
                "developer_sources": p.developer_sources or "",
                "asset_class": p.asset_class or "",
                "asset_class_raw": p.asset_class_raw or "",
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
                "market": market_of(p.city or "Boston"),
                "equity_partner": p.equity_partner or "",
                "stage_heard": p.stage_heard or "",
                "stage_confirmed": p.stage_confirmed or "",
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
            # Charted stage plus whether it is agenda-only. Computed here so
            # every consumer sees the same resolution and no component has to
            # know which markets carry two-field status.
            resolved = [
                resolve_stage(r["city"], r["status"], r["stage_heard"], r["stage_confirmed"])
                for r in rows
            ]
            df["stage"] = [s for s, _ in resolved]
            df["stage_provisional"] = [p for _, p in resolved]
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
