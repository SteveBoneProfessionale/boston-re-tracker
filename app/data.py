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
    "registry_self": {
        "label": "Registry — applicant is the company",
        "verified": True,
        "pattern": "-",          # dashed — verified, but weaker evidence
        "note": "the applicant is itself a registered operating company and is not "
                "shell-shaped, so it is reported as its own sponsor. The registry "
                "named no separate parent — this is NOT an address-cluster finding.",
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

# Strongest evidence first. Charts and legends iterate this rather than a
# literal tuple, so adding a tier stays a one-line change here instead of a
# hunt through the view code.
METHOD_ORDER = ["registry_confirmed", "human_set", "registry_self",
                "web_corroborated", "web_low_confidence"]


def shows_provenance_badge(method: str) -> bool:
    """Whether a resolved name needs its provenance stated alongside it.

    The solid tier is the baseline and needs no badge; every tier carrying a
    fill pattern is weaker or inferred and must say so wherever it appears.
    """
    return bool(RESOLUTION_METHODS[resolution_method(method)]["pattern"])


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
    # Added after reading the verbatim language on projects that had no stage.
    # These are phrasings the five municipalities actually use; each maps to
    # the stage the filing names, not to one inferred from context.
    "Preliminary Application":           "Permitting",   # Warwick's wording
    "Site Plan Review":                  "Permitting",
    "Design Waiver":                     "Permitting",
    "Comprehensive Plan Amendment":      "Planning",
    "Advisory Opinion":                  "Planning",     # RIGL 45-24-51/52
    "City Council Referral":             "Planning",
    "Zoning Board Recommendation":       "Planning",
    "Demolition":                        "Permitting",
    "Lot Merger":                        "Permitting",
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

# Where a square footage came from. A figure a reporter published and a figure
# the filing stated are not the same kind of fact, and the audit that preceded
# this found the column's worst problem was figures that LOOKED reported and
# were actually lot areas. So provenance renders on the number itself.
# How an architect firm was established. Same shape and the same purpose as
# SF_SOURCES: a name pulled off a drawing title block and a name pulled off a
# web article must not read alike. Unmarked is the strongest -- the planning
# filing itself names the firm.
ARCHITECT_SOURCES = {
    "filing":   {"label": "Filing-stated", "mark": ""},
    "plan_set": {"label": "Plan set",      "mark": "▣"},
    "permit":   {"label": "Permit record", "mark": "◉"},
    "web":      {"label": "Web-sourced",   "mark": "◈"},
}

# How strongly a project-team value is evidenced, from field_provenance.
# This supersedes ARCHITECT_SOURCES for the three team fields: the backfill
# writes it for architect, civil engineer and contractor alike, and it records
# the STRENGTH of the evidence rather than merely which channel found it. A
# value carried forward from before that run and never checked against a
# document is the weakest thing in the table, so it is marked rather than
# left to read as verified.
FIELD_TIERS = {
    "document_confirmed": {"label": "Document-confirmed", "mark": "▣"},
    "registry_confirmed": {"label": "Registry-confirmed", "mark": "◉"},
    "web_corroborated":   {"label": "Web, two sources",   "mark": "◈"},
    "web_low_confidence": {"label": "Web, one source",    "mark": "◇"},
    "unverified_prior":   {"label": "Unverified",         "mark": "·"},
}

# Delivery dates render at the precision their source had -- see
# scraper/delivery_dates.py. Re-exported here so every tab formats a date the
# same way rather than each growing its own.
from scraper.delivery_dates import format_date as format_delivery_date  # noqa: E402

SF_SOURCES = {
    "filing":    {"label": "Filing-stated", "color": "#e2e8f0", "mark": ""},
    "web":       {"label": "Web-sourced",   "color": "#38bdf8", "mark": "◈"},
    "corrected": {"label": "Corrected",     "color": "#F5821E", "mark": "✲"},
    # A figure the commission's own staff report or plan set states. Stronger
    # than an agenda line -- the agenda summarises, the staff report describes
    # the programme -- so it renders as its own thing rather than as "filing".
    "plan_set":  {"label": "Plan set",      "color": "#4ade80", "mark": "▣"},
}


# Why the Rhode Island square-footage column is thin, in plain language.
#
# This exists because a 15%-filled column reads as a broken tracker unless the
# reason is on the page. It is not a gap in the data collection -- it is how
# the market files. Saying so turns an apparent weakness into a statement
# about the market, which is the more useful thing to be able to make.
#
# Every claim here was established against the sources and can be defended:
#   * 2,214 agendas and minutes were read in full. Building floor area appears
#     in 6% of documents; land area outnumbers building area 269 to 13.
#   * Providence is the only one of the five cities that publishes per-case
#     staff reports and plan sets.
#   * Warwick's permits are issued per building -- a 14-unit scheme at 1515
#     Centerville Road carries six, one stating "the foundation sf is 1,040".
#   * Pawtucket's permit form has no square-footage field at all.
# How a delivery was established, and how it renders. A completion resting on
# a certificate of occupancy is not the same claim as one resting on a rental
# listing, and the two must never look alike -- the same rule the developer
# confidence states follow.
COMPLETION_BASIS = {
    "assessor_confirmed":        {"label": "Assessor confirmed", "mark": "◆", "rank": 1},
    "co_issued":                 {"label": "Certificate of occupancy", "mark": "◆", "rank": 1},
    "permit_final":              {"label": "Permit closed", "mark": "◇", "rank": 2},
    "permit_active":             {"label": "Permit active", "mark": "△", "rank": 2},
    "subsidy_placed_in_service": {"label": "Placed in service", "mark": "◇", "rank": 2},
    "leasing_active":            {"label": "Leasing", "mark": "○", "rank": 3},
    "news_confirmed":            {"label": "Reported", "mark": "○", "rank": 3},
    "human_set":                 {"label": "Set by hand", "mark": "■", "rank": 0},
}


# Unit-count confidence, graded against the documents behind each figure.
# 1077 Westminster carried 34 units when its final plan said 41, so the
# obvious next question is which of the others are wrong. This is the answer
# to that, per record, rather than a blanket assurance.
UNITS_CONFIDENCE = {
    "corroborated":  {"label": "Corroborated", "mark": "",  "rank": 0,
                      "note": "two or more documents agree, and it is the latest figure"},
    "single_source": {"label": "One source",   "mark": "·", "rank": 1,
                      "note": "stated in a single document; probably right, unverifiable here"},
    "contradicted":  {"label": "Contradicted", "mark": "≠", "rank": 2,
                      "note": "a later document states a different figure; both are on the record"},
    "unsourced":     {"label": "Unsourced",    "mark": "?", "rank": 3,
                      "note": "no document in the corpus states this figure -- distrust first"},
}


# ── grouped city dropdown, shared by every tab ──────────────────────────
# There are THREE city filters -- Overview, Projects and Map -- and grouping
# only one of them is why the change looked like it had not deployed. The
# helpers live here so all three read the same list and a fourth tab cannot
# quietly diverge again.
#
# Streamlit has no option groups and no disabled options, so the grouping is
# in the option list itself: a header per market, cities indented under it. A
# header is not a valid choice; each widget's on_change callback restores the
# previous city if one is picked. session_state cannot be written after a
# widget is created, but it CAN be written from that widget's own callback.
CITY_HEADER_FMT = "── %s ──"
CITY_INDENT = "  "


def is_city_header(v) -> bool:
    return isinstance(v, str) and v.startswith("──")


def city_options(scope):
    """(options, display -> city). Markets alphabetical, cities within them.

    Groups come from the `market` column, never a hardcoded list, so a city in
    a new market appears under its own header with no code change.
    """
    by_market: dict = {}
    for city, market in zip(scope["city"], scope.get("market", "")):
        if not city:
            continue
        by_market.setdefault(market or "Other", set()).add(city)
    opts, lookup = ["All"], {"All": "All"}
    for market in sorted(by_market):
        head = CITY_HEADER_FMT % market.upper()
        opts.append(head)
        lookup[head] = None
        for c in sorted(by_market[market]):
            disp = CITY_INDENT + c
            opts.append(disp)
            lookup[disp] = c
    return opts, lookup


def keep_city_selectable(key: str):
    """on_change callback: reject a header pick, restore the last real city."""
    def _cb():
        import streamlit as _st
        prev_key = "_%s_prev" % key
        v = _st.session_state.get(key)
        if is_city_header(v):
            _st.session_state[key] = _st.session_state.get(prev_key, "All")
        else:
            _st.session_state[prev_key] = v
    return _cb


RI_SF_NOTE_TITLE = "About square footage in Rhode Island"

RI_SF_NOTE = """
**Rhode Island planning boards describe projects in units and storeys, not floor
area.** That is a fact about how this market files, not a gap in this tracker.

Across 2,214 agendas and minutes read in full, a building floor-area figure
appears in about 6% of documents. Land area outnumbers building area by more
than twenty to one — most numbers on a Rhode Island agenda describe the lot,
not the building.

**Four of the five cities publish no document that states it.** Providence alone
posts per-case staff reports and plan sets. Warwick, Cranston, Pawtucket and
Newport publish agendas and minutes only, so there is no filing to read a floor
area from.

**Permits do not close the gap either.** Warwick issues permits per building, so
a fourteen-unit scheme carries six separate permits and no single one states a
project total. Pawtucket's permit form has no square-footage field at all.
Providence's does, and where a permit exists its figure is shown — marked
distinctly, because a permitted building and a proposed programme are different
things.

**So square footage appears per project where a source states it, and there is
no market total.** Summing a column that is 15% complete would produce a number
that looks authoritative and means nothing.

**Unit count is the headline programme metric for this market.** It is the
figure the boards actually state, the one the press reports, and the one these
projects are argued and approved on.
"""



# Developer-name confidence, as ONE definition every consumer reads.
#
# 204 of 370 Rhode Island developers rest on the planning document alone, so
# this is the majority of the data and not an edge case -- a name has to render
# with its confidence attached wherever it appears, or the table quietly
# presents a filing-only name as if outside reporting had confirmed it.
#
# The legacy resolution methods fold into the same four states rather than
# forming a parallel vocabulary. developer_resolution_method keeps the raw
# value for the detail view.
DEVELOPER_CONFIDENCE = {
    "human_set":     {"label": "Set by hand",   "color": "#F5821E", "mark": "✎"},
    "confirmed":     {"label": "Confirmed",     "color": "#22c55e", "mark": "✓"},
    "document_only": {"label": "Document only", "color": "#eab308", "mark": "◐"},
    "conflicted":    {"label": "Conflicted",    "color": "#ef4444", "mark": "!"},
    # A name carried over from a pipeline that recorded no method at all --
    # every Boston and Cambridge row. Shown as its own state rather than being
    # silently folded into one of the four.
    "unattributed":  {"label": "Unattributed",  "color": "#64748b", "mark": "–"},
}

_CONFIDENCE_OF_METHOD = {
    "human_set": "human_set",
    "confirmed": "confirmed",
    "web_corroborated": "confirmed",
    "document_only": "document_only",
    "registry_self": "document_only",
    "web_low_confidence": "document_only",
    "conflicted": "conflicted",
}


def developer_confidence(method: str, developer: str) -> str:
    """The canonical confidence state for one developer name."""
    if not developer:
        return ""
    return _CONFIDENCE_OF_METHOD.get(method or "", "unattributed")


# Review-scale colors are assigned by tier position within each market's
# vocabulary, not by matching a literal string -- so a market's most-intensive
# review tier is always orange, its second teal, and so on, and a new market
# gets consistent colors without touching the chart. First registration of a
# label wins, keeping a shared label stable across markets.
_SCALE_RAMP = ["#F5821E", "#0ea5e9", "#64748b", "#8A9BB0"]
REVIEW_SCALE_COLORS: dict[str, str] = {"Unclassified": "#3f4451"}
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
def load_projects(include_excluded: bool = False) -> pd.DataFrame:
    session = get_session()
    try:
        # QUARANTINE. Excluded rows are non-commercial items -- deed
        # corrections, lot line adjustments, fences, murals -- kept in the
        # table but dropped here so they cannot reach any count or chart.
        # include_excluded exists for the review tab, which must still see
        # them to un-exclude one.
        q = session.query(Project)
        if not include_excluded:
            q = q.filter((Project.excluded.is_(None)) | (Project.excluded == False))  # noqa: E712
        projects = q.all()
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
                # DELIVERY. A completed building is not pipeline, so the stage
                # a completion source established overrides the filing status
                # in every count. The basis travels with it, because a
                # certificate of occupancy and a rental listing are not the
                # same strength of claim.
                # The ACTION asked of the board, kept apart from the stage.
                "filing_type": p.filing_type or "",
                # Other addresses the same case is filed under. Search covers
                # these, because a project spanning three parcels is findable
                # by any of its doors or by none of them.
                "alt_addresses": p.alt_addresses or "",
                # How far the unit count can be trusted. Rendered, because a
                # figure the reader cannot weigh is worse than one with a
                # caveat attached.
                "units_confidence": p.units_confidence or "",
                # Withdrawn or Denied. Dead is not pipeline either.
                "project_status_filing": p.project_status_filing or "",
                "completion_stage": p.completion_stage or "",
                "completion_basis": p.completion_basis or "",
                "completion_evidence": p.completion_evidence or "",
                "completion_source_url": p.completion_source_url or "",
                "completion_date": p.completion_date or "",
                # Stale says nothing was recorded for a long time. It is NOT a
                # claim about whether the project was built.
                "is_stale": bool(p.is_stale),
                "stale_months": p.stale_months,
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
                # BPDA'S OWN PUBLISHED FIGURE FIRST. This precedence was the
                # other way round, and it was wrong on 48 of the 78 Boston rows
                # above 250,000 GSF -- overstating the pipeline by 5,998,678 SF.
                #
                # `bpda_gsf` is the "Gross Floor Area" field scraped off the
                # BPDA project page, which is Tier 1 and per-building.
                # `total_gsf` comes from an LLM reading a filing, under a prompt
                # that asked for "gross square feet of entire project" -- so on a
                # component parcel of a phased development it returns the PHASE
                # total. All three On the Dot rows carried 1,386,500, which is
                # the phase; their pages publish 487,400, 510,900 and 388,200,
                # which sum to 1,386,500 exactly.
                #
                # NEITHER COLUMN IS MODIFIED. total_gsf is still stored verbatim
                # and is still the fallback wherever BPDA publishes no figure --
                # every Cambridge row, and the manual entries. The extraction
                # prompt has NOT been fixed yet, so a future re-extraction will
                # write phase totals into total_gsf again; this precedence is
                # what keeps them off the screen until it is.
                # ...EXCEPT where the page's field describes one parcel of a
                # larger site. Austin Street Lots publishes 126,000 in the field
                # and 790,000 in its own description; preferring the field there
                # understated the project by 664,000 SF.
                "total_gsf": ((None if p.bpda_gsf_is_partial else p.bpda_gsf)
                              or p.total_gsf),
                # Which column the figure above actually came from, so the
                # provenance is legible rather than implicit.
                "gsf_column": ("bpda_gsf" if (p.bpda_gsf and not p.bpda_gsf_is_partial)
                               else ("total_gsf" if p.total_gsf else "")),
                "residential_units": p.residential_units,
                "commercial_gsf": p.commercial_gsf,
                "building_height_ft": p.building_height_ft,
                "num_stories": p.num_stories,
                "parking_spaces": p.parking_spaces,
                "architect": p.architect or "",
                "architect_source": p.architect_source or "",
                "architect_person": p.architect_person or "",
                "civil_engineer": p.civil_engineer or "",
                "surveyor": p.surveyor or "",
                "landscape_architect": p.landscape_architect or "",
                "contractor": p.general_contractor or "",
                "attorney": p.attorney or "",
                # DELIVERY, as two separate claims. Real dates so the
                # screener can sort them chronologically, the precision of
                # the period each source actually named, and -- for a
                # forecast -- when it was made and by whom. A target and an
                # actual never occupy the same column: see the note on
                # Project.delivered_date.
                "delivered_date": p.delivered_date,
                "delivered_precision": p.delivered_precision or "",
                "target_date": p.target_date,
                "target_precision": p.target_precision or "",
                "target_stated_on": p.target_stated_on,
                "target_stated_by": p.target_stated_by or "",
                # The verbatim phrase behind the parsed target, kept because
                # the parse is lossy: a range stores its start.
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
                # The party that OWNS or sponsors, when it is not the party
                # executing the work -- a public agency or passive landowner.
                "owner_or_agency": p.owner_or_agency or "",
                "excluded": bool(p.excluded),
                "excluded_reason": p.excluded_reason or "",
                "total_gsf_source": p.total_gsf_source or "",
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
            # Real datetimes, so a sort on either delivery column is
            # chronological and a missing date is NaT rather than a string
            # that would sort among the real ones.
            for _c in ("delivered_date", "target_date", "target_stated_on"):
                df[_c] = pd.to_datetime(df[_c], errors="coerce")
            # Charted stage plus whether it is agenda-only. Computed here so
            # every consumer sees the same resolution and no component has to
            # know which markets carry two-field status.
            resolved = [
                resolve_stage(r["city"], r["status"], r["stage_heard"], r["stage_confirmed"])
                for r in rows
            ]
            df["stage"] = [s for s, _ in resolved]
            df["stage_provisional"] = [p for _, p in resolved]

            # A DELIVERY OVERRIDES THE FILING STATUS.
            #
            # resolve_stage() reads the status a board recorded, and a board
            # stops recording once it has approved. So a building that opened
            # in 2022 still resolves to "Approved" forever. Where a completion
            # was established from a source OUTSIDE the planning documents --
            # a certificate of occupancy, an assessor step change, an active
            # leasing listing -- that is the later and better fact and it wins.
            #
            # Without this the completion work had no effect on any number:
            # all 69 projects found to be built were still charted as
            # Planning, Permitting or Approved, holding 5,350 units and 13.1M
            # sq ft in the pipeline totals.
            done = df["completion_stage"].isin(["Complete", "Under Construction"])
            df.loc[done, "stage"] = df.loc[done, "completion_stage"]
            df.loc[done, "stage_provisional"] = False

            # AND A DELIVERED DATE IS ITSELF THE PROOF, whatever else the row
            # says. completion_stage above is one route to establishing that a
            # building is finished; delivered_date is the other, and the newer
            # one -- a certificate of occupancy joined on its permit number, a
            # school that opened for the term, minutes recording a tenant
            # moving in. Those never wrote completion_stage, so five delivered
            # buildings were still being charted and filtered as pipeline: the
            # New Tobin School and 2 Garden Street as Under Construction, 150
            # Richmond as Approved, 228 Broad Street as Permitting.
            #
            # The rule is the plain one: if the tracker holds a date on which a
            # building was finished, the building is finished. Anything showing
            # under SHOW = Pipeline must be a project that has not been built.
            delivered = df["delivered_date"].notna()
            df.loc[delivered, "stage"] = "Complete"
            df.loc[delivered, "stage_provisional"] = False
            # A row's financial fields (total_gsf, residential_units, etc.) are ready to
            # chart once extraction has run, OR immediately if the row came from a
            # structured-data pipeline that never needed extraction in the first place.
            df["has_financials"] = df["extraction_done"] | ~df["requires_extraction"]
            # One confidence state per row, folded from the raw method, so no
            # component has to know the legacy vocabulary.
            df["developer_confidence"] = [
                developer_confidence(r["developer_resolution_method"], r["developer"])
                for r in rows
            ]
        return df
    finally:
        session.close()


@st.cache_data(ttl=300)
@st.cache_data(ttl=300)
def load_stage_history() -> pd.DataFrame:
    """Every recorded hearing, one row per appearance.

    The Rhode Island landing page is built on what is actually populated, and
    hearing dates are the densest signal in the whole dataset -- far denser
    than square footage. They answer two questions no other field can: how
    much was filed recently, and how long a project takes to get approved.
    """
    from db.models import ProjectStageEvent
    session = get_session()
    try:
        rows = []
        for e in session.query(ProjectStageEvent).all():
            d = str(e.meeting_date or "")[:10]
            if len(d) != 10:
                continue
            rows.append({
                "project_id": e.project_id,
                "date": pd.to_datetime(d, errors="coerce"),
                "stage": e.stage or "",
                "stage_raw": e.review_stage_raw or "",
                "body": e.reviewing_body or "",
                "outcome": (e.outcome or ""),
            })
        df = pd.DataFrame(rows)
        if len(df):
            df = df.dropna(subset=["date"])
        return df
    finally:
        session.close()


@st.cache_data(ttl=300)
def load_field_tiers() -> dict:
    """Live evidence tier for every project-team value, keyed (project_id, field).

    field_provenance holds one live row per project and field. The value is
    already mirrored onto projects, so what is needed here is the strength
    behind it and, for the contractor, whether a blank means "nobody has been
    appointed yet" or "we looked and found no one" -- those are different
    facts and must not render alike.
    """
    from sqlalchemy import text
    from db.database import engine
    out = {}
    with engine.connect() as conn:
        rows = conn.execute(text(
            "select project_id, field, tier, outcome from field_provenance "
            "where superseded = 0 and coalesce(retracted, 0) = 0"
        ))
        for pid, field, tier, outcome in rows:
            out[(int(pid), field)] = {"tier": tier or "", "outcome": outcome or ""}
    return out


def mark_team_value(value: str, meta: dict | None) -> str:
    """Render one team value with its evidence mark, or say why it is blank."""
    outcome = (meta or {}).get("outcome", "")
    if outcome == "not_yet_selected":
        return "⋯ not yet appointed"
    v = (value or "").strip()
    if not v or v == "not_yet_selected":
        return "—"
    # A value with no provenance row at all is one the backfill never reached
    # -- the thirteen rows with no city were held out of it by instruction.
    # It gets the unverified mark, because leaving it bare would make the
    # weakest thing in the table look like the strongest.
    tier = (meta or {}).get("tier", "")
    mark = FIELD_TIERS.get(tier, {}).get("mark", "·" if meta is None else "")
    return f"{mark} {v}".strip()


def load_field_citations(project_id: int) -> list[dict]:
    """Per-field source citations for one project, newest field order first.

    extraction_sources has been populated since the Rhode Island ingest but was
    never read by the UI, so every citation written was invisible. A citation
    that nothing displays is not provenance.
    """
    from db.models import ExtractionSource
    session = get_session()
    try:
        rows = (session.query(ExtractionSource)
                .filter_by(project_id=project_id)
                .order_by(ExtractionSource.field_name).all())
        return [{
            "field": r.field_name,
            "value": r.field_value,
            "filing": r.filing_name or "",
            "date": r.filing_date or "",
            "url": r.pdf_url or "",
            "page": r.page_number,
        } for r in rows]
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

    # PIPELINE EXCLUDES WHAT IS FINISHED OR DEAD.
    #
    # A delivered building and a withdrawn application are both real records
    # worth keeping and filtering to, but neither is pipeline, and counting
    # them inflates every headline. They stay in `extracted` -- and so in the
    # table, the map and every chart -- and drop only out of the totals.
    pipe = extracted
    if "stage" in pipe.columns:
        pipe = pipe[pipe["stage"] != "Complete"]
    if "project_status_filing" in pipe.columns:
        pipe = pipe[~pipe["project_status_filing"].isin(["Withdrawn", "Denied"])]
    delivered = extracted[extracted["stage"] == "Complete"] if "stage" in extracted.columns         else extracted.iloc[0:0]

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
        "total_units": int(pipe["residential_units"].dropna().sum()),
        "total_gsf": int(pipe["total_gsf"].dropna().sum()),
        "pipeline_projects": len(pipe),
        # Delivered, reported alongside rather than folded in, so the pipeline
        # number is clean and the delivered number is still available.
        "delivered_projects": len(delivered),
        "delivered_units": int(delivered["residential_units"].dropna().sum()),
        "delivered_gsf": int(delivered["total_gsf"].dropna().sum()),
        "conditional_alternative_count": n_conditional,
    }

@st.cache_data(ttl=300)
def load_transactions() -> pd.DataFrame:
    """Commercial transactions for the Acquisitions tab.

    Numeric columns come back numeric and the date comes back a date, so the
    grid's own header sort orders by magnitude and chronology rather than by
    text -- the bug that bit the screener on square footage.
    """
    from sqlalchemy import text
    from db.database import engine
    with engine.connect() as conn:
        # Quarantined rows are affiliated-party transfers -- a company conveying
        # to itself. They are not acquisitions and must not enter any count,
        # volume or ranking, so they are excluded here rather than filtered in
        # each consumer, where one missed filter would silently reinstate them.
        # They remain in the table with their reason for review.
        rows = conn.execute(text(
            "select * from transactions where coalesce(quarantined,0) = 0"
        )).mappings().all()
    df = pd.DataFrame([dict(r) for r in rows])
    if df.empty:
        return df
    for c in ("price", "implied_valuation", "building_sf", "unit_count",
              "land_sf", "price_per_sf", "price_per_unit", "pct_acquired",
              "excise_stamp", "excise_implied_price"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "sale_date" in df.columns:
        df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce")
    return df
