"""
Cambridge Development Log ingestion.

Data source: City of Cambridge CDD Socrata Open Data API (structured JSON,
not scraped HTML -- Cambridge has no Article 80 equivalent and no BPDA-style
project pages; the Development Log is the authoritative source).

  Current edition (primary): https://data.cambridgema.gov/resource/wjwg-93qh.json
  Dataset ID is stable across quarters -- only the display title's quarter
  changes (verify via /api/views/wjwg-93qh.json -> "name" before each run).

Source verification notes (2026-07-23, live-checked against the "2026 Q1" edition):
  - wjwg-93qh: 67 rows, confirmed. Status counts, per-status GFA subtotals,
    residential units (7,212), parking spaces (11,465), hotel rooms (235), and
    4 named spot-check records all matched exactly. See cambridge_validate.py.
  - Total GFA across all 67 rows is 16,941,843 (sum of `total_gfa`), which also
    matches summing the per-status subtotals. This is used as the validation
    target -- NOT the "12,641,651" figure floated during planning, which
    doesn't reconcile with the source data.
  - Use-data child table (5nqm-2ns2, per-use GFA breakdown) is titled "2025 Q4"
    -- one quarter stale vs. the current edition, and does not reconcile to
    the 67 current projects. There is currently no API source for an accurate
    current-quarter per-use split. We fall back to a single
    cambridge_project_uses row per project (primary_use + total_gfa,
    is_fallback=True) until Socrata republishes a current-quarter use table.
  - Use-data-map dataset (n2h4-yabd) returns 404 (removed). No replacement
    built -- the main table already carries lat/long per project, which is
    sufficient for the map view.
"""

import argparse
import json
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import httpx
from rapidfuzz import fuzz

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import init_db, get_session
from db.models import (
    Project, CambridgeProjectUse, CambridgeBuildingPermit,
    CambridgeSpecialPermit, CambridgeProjectAlias, CambridgeQuarterlySnapshot,
)
from scraper.normalize_developer import suffix_stripped, _SUFFIX_RE

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

CURRENT_EDITION_DATASET_ID = "wjwg-93qh"
SOCRATA_BASE = "https://data.cambridgema.gov/resource"
SOCRATA_METADATA = "https://data.cambridgema.gov/api/views"
APP_TOKEN = os.environ.get("CAMBRIDGE_SOCRATA_APP_TOKEN")
PAGE_SIZE = 1000

EXPECTED_PROJECT_COUNT = 67
EXPECTED_TOTAL_GFA = 16_941_843
EXPECTED_RESIDENTIAL_UNITS = 7_212
EXPECTED_PARKING_SPACES = 11_465
EXPECTED_HOTEL_ROOMS = 235

# Canonical neighborhood names keyed by the leading number in the source string.
# The source data is inconsistent in the trailing text ("Neighborhood Nine" vs
# "Neighborhood 9", "The Port/Area IV" vs "The Port/Area Four", etc.) so we
# normalize off the number, per the known hazard.
NEIGHBORHOOD_BY_ID = {
    1: "East Cambridge",
    2: "Area 2/MIT",
    3: "Wellington Harrington",
    4: "The Port/Area Four",
    5: "Cambridgeport",
    6: "Mid-Cambridge",
    7: "Riverside",
    8: "Baldwin",
    9: "Neighborhood Nine",
    10: "West Cambridge",
    11: "North Cambridge",
    12: "Cambridge Highlands",
    13: "Strawberry Hill",
}

_SPECIAL_PERMIT_RE = re.compile(r"^\s*(PB\d+[A-Z]?)\s*(.*?)\s*$")
_BUILDING_PERMIT_SEG_RE = re.compile(r"^\s*([\w-]+)\s*(?:\(([^)]+)\))?\s*$")
_NEIGHBORHOOD_NUM_RE = re.compile(r"^\s*(\d+)")
_RENAME_NOTE_RE = re.compile(
    r'name (?:updated|changed) to\s*[“"]([^”".]+)[”"]\s*from\s*[“"]([^”".]+)[”"]',
    re.IGNORECASE,
)
_FORMERLY_NOTE_RE = re.compile(
    r'formerly (?:referred to as|referred to|known as|named)\s*[“"]?([^”".,]+)',
    re.IGNORECASE,
)
_PUD_SCOPE_FAR_RE = re.compile(r"\b(overall|entire|whole)\b", re.IGNORECASE)
_APPROX_COORDS_RE = re.compile(r"\bapproximate\b", re.IGNORECASE)
_OTHER_MUNICIPALITY_RE = re.compile(r"\b(Somerville|Boston)\b")


# ---------------------------------------------------------------------------
# Socrata HTTP client
# ---------------------------------------------------------------------------

def _headers():
    h = {"Accept": "application/json"}
    if APP_TOKEN:
        h["X-App-Token"] = APP_TOKEN
    return h


def socrata_get(client: httpx.Client, url: str, params: dict) -> list[dict]:
    """GET with retry/backoff on 429 and 5xx."""
    for attempt in range(5):
        try:
            resp = client.get(url, params=params, headers=_headers(), timeout=30)
        except httpx.RequestError as exc:
            log.warning("Request error (attempt %d): %s", attempt + 1, exc)
            time.sleep(2 ** attempt)
            continue
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 429 or resp.status_code >= 500:
            wait = 2 ** attempt
            log.warning("HTTP %d for %s, retrying in %ds", resp.status_code, url, wait)
            time.sleep(wait)
            continue
        resp.raise_for_status()
    raise RuntimeError(f"Failed to fetch {url} after 5 attempts")


def fetch_all_rows(dataset_id: str, order: str = "project_id") -> list[dict]:
    """Paginate through a Socrata dataset and return all rows."""
    url = f"{SOCRATA_BASE}/{dataset_id}.json"
    rows: list[dict] = []
    offset = 0
    with httpx.Client() as client:
        while True:
            params = {"$limit": PAGE_SIZE, "$offset": offset, "$order": order}
            page = socrata_get(client, url, params)
            rows.extend(page)
            if len(page) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
    return rows


def fetch_dataset_title(dataset_id: str) -> str | None:
    with httpx.Client() as client:
        try:
            resp = client.get(f"{SOCRATA_METADATA}/{dataset_id}.json", headers=_headers(), timeout=30)
            if resp.status_code == 200:
                return resp.json().get("name")
        except httpx.RequestError:
            pass
    return None


# ---------------------------------------------------------------------------
# Field guards / parsing
# ---------------------------------------------------------------------------

def to_int(v) -> tuple[int | None, bool]:
    """Returns (value, is_tbd_or_blank)."""
    if v is None or v == "":
        return None, False
    if isinstance(v, str) and v.strip().upper() == "TBD":
        return None, True
    try:
        return int(float(v)), False
    except (TypeError, ValueError):
        return None, False


def to_float(v) -> float | None:
    if v is None or v == "":
        return None
    if isinstance(v, str) and v.strip().upper() == "TBD":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def normalize_neighborhood(raw: str | None) -> tuple[int | None, str | None]:
    if not raw:
        return None, None
    m = _NEIGHBORHOOD_NUM_RE.match(raw)
    if not m:
        return None, None
    num = int(m.group(1))
    return num, NEIGHBORHOOD_BY_ID.get(num)


def parse_special_permit(raw: str | None) -> tuple[str | None, str | None]:
    """'PB315 MA2' -> ('PB315', 'MA2'). 'PB410' -> ('PB410', '')."""
    if not raw or not raw.strip():
        return None, None
    m = _SPECIAL_PERMIT_RE.match(raw.strip())
    if not m:
        return raw.strip(), None
    base, amendment = m.group(1), m.group(2)
    return base, (amendment or None)


def parse_building_permits(raw: str | None) -> list[tuple[str, str | None]]:
    """'195497, 195498' or '1186734 (Bldg A), 1187446 (Bldg B)' -> [(number, label), ...]."""
    if not raw or not raw.strip():
        return []
    out = []
    for seg in raw.split(","):
        seg = seg.strip()
        if not seg:
            continue
        m = _BUILDING_PERMIT_SEG_RE.match(seg)
        if m:
            out.append((m.group(1), m.group(2)))
        else:
            out.append((seg, None))
    return out


def parse_phase_group(name: str) -> tuple[str | None, str]:
    """'Kendall Common - Building C1' -> ('Kendall Common', 'Building C1')."""
    if " - " in name:
        parent, component = name.split(" - ", 1)
        return parent.strip(), component.strip()
    return None, name


def detect_rename(note: str | None) -> tuple[str, str] | None:
    """Returns (new_name, former_name) if the note documents a rename, else None."""
    if not note:
        return None
    m = _RENAME_NOTE_RE.search(note)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    m = _FORMERLY_NOTE_RE.search(note)
    if m:
        return None, m.group(1).strip()  # new name is the current project_name, filled in by caller
    return None


_LEADING_STREET_NUM_RE = re.compile(r"^\s*(\d+[\d-]*)\s+(.*)$")

# Confirmed spelling/punctuation variants for the SAME entity in the live
# Cambridge Development Log data (checked 2026-07-23). Keyed on the
# suffix-stripped, lowercased, punctuation-stripped name.
# MIT and MITIMCO are related but distinct -- MITIMCO is MIT's investment
# management arm -- and are deliberately NOT merged here.
_DEVELOPER_ALIASES = {
    "biomed": "BioMed Realty",
    "just a start": "Just-A-Start",
    "homeowners rehab": "Homeowner's Rehab, Inc (HRI)",
}


def _display_suffix_stripped(raw: str) -> str:
    """Like scraper.normalize_developer.suffix_stripped, but preserves the
    original casing -- that helper returns a lowercased comparison KEY
    (its own docstring says so), which is right for matching but wrong to
    show in a chart."""
    n = re.sub(r"\s+and\s+(its\s+)?affiliates?\.?$", "", raw.strip(), flags=re.I).strip()
    n = _SUFFIX_RE.sub("", n).strip()
    n = _SUFFIX_RE.sub("", n).strip()  # twice, to catch ", LLC" after "Company" etc.
    return n


def normalize_cambridge_developer(raw: str | None) -> str | None:
    """Canonicalize known name variants; generic legal-suffix stripping
    handles cases like 'DND Homes' vs 'DND Homes, LLC' without needing an
    explicit alias. Raw value is preserved separately for display."""
    if not raw or not raw.strip():
        return raw
    key = re.sub(r"[^\w\s]", "", suffix_stripped(raw)).strip()
    key = re.sub(r"\s+", " ", key)
    return _DEVELOPER_ALIASES.get(key, _display_suffix_stripped(raw))


def normalize_name(name: str) -> str:
    """Loose key for fuzzy dedupe against Boston project names."""
    name = re.sub(r"\s*\([^)]*\)", "", name)          # drop parenthetical
    name = re.split(r"\s+-\s+", name)[0]                # drop "- Component" suffix (requires real
                                                          # whitespace around the dash, so street-number
                                                          # ranges like "87-101" survive intact)
    name = re.sub(r"[^a-z0-9 -]", "", name.lower())
    return re.sub(r"\s+", " ", name).strip()


def split_street_number(text: str) -> tuple[str | None, str]:
    """'2161 Massachusetts Avenue' -> ('2161', 'massachusetts avenue')."""
    m = _LEADING_STREET_NUM_RE.match(normalize_name(text))
    if m:
        return m.group(1), m.group(2)
    return None, normalize_name(text)


# ---------------------------------------------------------------------------
# Normalization of a single row
# ---------------------------------------------------------------------------

def normalize_row(row: dict) -> dict:
    total_gfa, total_gfa_tbd = to_int(row.get("total_gfa"))
    affordable_units, affordable_units_tbd = to_int(row.get("affordable_units"))
    residential_units, _ = to_int(row.get("residential_units"))
    parking_spaces, _ = to_int(row.get("parking_spaces"))
    hotel_rooms, _ = to_int(row.get("hotel_rooms"))
    lot_area, _ = to_int(row.get("lot_area"))
    far = to_float(row.get("far"))
    latitude = to_float(row.get("latitude"))
    longitude = to_float(row.get("longitude"))

    neighborhood_id, neighborhood_name = normalize_neighborhood(row.get("neighborhood"))
    special_base, special_amend = parse_special_permit(row.get("planning_board_special_permit"))
    building_permits = parse_building_permits(row.get("building_permit"))
    phase_group, _component = parse_phase_group(row.get("project_name") or "")

    note = row.get("note") or ""
    description = row.get("project_description") or ""
    coords_approximate = bool(_APPROX_COORDS_RE.search(note))
    spans = bool(_OTHER_MUNICIPALITY_RE.search(note) or _OTHER_MUNICIPALITY_RE.search(description))
    rename = detect_rename(note)

    return {
        "cambridge_project_id": str(row.get("project_id")),
        "name": row.get("project_name") or "",
        "address": row.get("address") or "",
        "neighborhood_raw": row.get("neighborhood"),
        "neighborhood_id": neighborhood_id,
        "neighborhood": neighborhood_name or row.get("neighborhood"),
        "developer": row.get("developer") or "",
        "status": row.get("status"),
        "permit_type": row.get("permit_type"),
        "project_type": row.get("project_type"),
        "zoning_raw": row.get("zoning"),
        "zoning_components": ",".join(p.strip() for p in re.split(r"/", row.get("zoning") or "") if p.strip()),
        "lot_area": lot_area,
        "far": far,
        "far_scope": "pud" if note and _PUD_SCOPE_FAR_RE.search(note) else ("building" if far is not None else None),
        "total_gsf": total_gfa,
        "total_gfa_tbd": total_gfa_tbd,
        "residential_units": residential_units,
        "affordable_units": affordable_units,
        "affordable_units_tbd": affordable_units_tbd,
        "parking_spaces": parking_spaces,
        "hotel_rooms": hotel_rooms,
        "primary_use": row.get("primary_use"),
        "special_permit_raw": row.get("planning_board_special_permit") or None,
        "special_permit_base": special_base,
        "special_permit_amendment": special_amend,
        "building_permit_raw": row.get("building_permit") or None,
        "building_permits": building_permits,
        "description": description,
        "notes": note,
        "phase_group": phase_group,
        "coords_approximate": coords_approximate,
        "spans_municipalities": spans,
        "rename": rename,
        "latitude": latitude,
        "longitude": longitude,
    }


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def snapshot_raw_rows(session, raw_rows: list[dict], edition: str):
    for row in raw_rows:
        session.add(CambridgeQuarterlySnapshot(
            cambridge_project_id=str(row.get("project_id")),
            edition=edition,
            full_row_json=json.dumps(row),
            ingested_at=datetime.utcnow(),
        ))
    session.commit()
    log.info("Snapshotted %d raw rows for edition %s", len(raw_rows), edition)


def upsert_project(session, n: dict) -> Project:
    project = session.query(Project).filter_by(
        cambridge_project_id=n["cambridge_project_id"]
    ).first()
    is_new = project is None
    if is_new:
        project = Project(
            cambridge_project_id=n["cambridge_project_id"],
            # "manual:" prefix (not literally a manual entry here) reuses the existing UI
            # convention -- app/tabs/{project_table,map_view}.py already suppress the
            # "BPDA PAGE" link button/popup-link for any bpda_url starting with "manual:",
            # which is exactly the right behavior since Cambridge has no BPDA-style page.
            bpda_url=f"manual:cambridge-devlog-{n['cambridge_project_id']}",
            city="Cambridge",
            first_seen_date=datetime.utcnow(),
        )
        session.add(project)

    if n["rename"]:
        _new_name, former_name = n["rename"]  # new_name may be None -> current project name
        if former_name and former_name != n["name"]:
            existing_alias = session.query(CambridgeProjectAlias).filter_by(
                project_id=project.id, former_name=former_name
            ).first() if project.id else None
            if not existing_alias:
                session.flush()
                session.add(CambridgeProjectAlias(
                    project_id=project.id, former_name=former_name,
                    date_observed=datetime.utcnow(),
                ))

    project.name = n["name"]
    project.address = n["address"]
    project.neighborhood = n["neighborhood"]
    project.neighborhood_raw = n["neighborhood_raw"]
    project.neighborhood_id = n["neighborhood_id"]
    project.developer = n["developer"]
    project.developer_canonical = normalize_cambridge_developer(n["developer"])
    project.requires_extraction = False  # structured API data, no AI-extraction step needed
    project.status = n["status"]
    project.permit_type = n["permit_type"]
    project.project_type = n["project_type"]
    project.zoning_raw = n["zoning_raw"]
    project.zoning_components = n["zoning_components"]
    project.lot_area = n["lot_area"]
    project.far = n["far"]
    project.far_scope = n["far_scope"]
    project.total_gsf = n["total_gsf"]
    project.total_gfa_tbd = n["total_gfa_tbd"]
    project.residential_units = n["residential_units"]
    project.affordable_units = n["affordable_units"]
    project.affordable_units_tbd = n["affordable_units_tbd"]
    project.parking_spaces = n["parking_spaces"]
    project.hotel_rooms = n["hotel_rooms"]
    project.asset_class = n["primary_use"]
    project.special_permit_raw = n["special_permit_raw"]
    project.building_permit_raw = n["building_permit_raw"]
    project.description = n["description"]
    project.notes = n["notes"]
    project.phase_group = n["phase_group"]
    project.coords_approximate = n["coords_approximate"]
    project.spans_municipalities = n["spans_municipalities"]
    project.latitude = n["latitude"]
    project.longitude = n["longitude"]
    project.last_checked_date = datetime.utcnow()

    session.flush()

    # Special permit child row
    session.query(CambridgeSpecialPermit).filter_by(project_id=project.id).delete()
    if n["special_permit_base"]:
        session.add(CambridgeSpecialPermit(
            project_id=project.id,
            base_permit=n["special_permit_base"],
            amendment_raw=n["special_permit_amendment"],
            full_raw=n["special_permit_raw"],
        ))

    # Building permit child rows
    session.query(CambridgeBuildingPermit).filter_by(project_id=project.id).delete()
    for number, label in n["building_permits"]:
        session.add(CambridgeBuildingPermit(
            project_id=project.id, permit_number=number, label=label,
        ))

    # Use breakdown: single fallback row (see module docstring -- use-data
    # table is a stale quarter, so we can't do a real multi-use split yet).
    session.query(CambridgeProjectUse).filter_by(project_id=project.id).delete()
    if n["primary_use"] and n["total_gsf"] is not None:
        session.add(CambridgeProjectUse(
            project_id=project.id,
            use_category=n["primary_use"],
            use_category_raw=n["primary_use"],
            gfa=n["total_gsf"],
            gfa_tbd=n["total_gfa_tbd"],
            is_fallback=True,
        ))

    return project


def link_phase_parents(session, projects_by_cid: dict[str, Project]):
    """Link buildings to their 'Remaining'/'Master Plan' sibling within the same phase_group."""
    by_group: dict[str, list[Project]] = defaultdict(list)
    for p in projects_by_cid.values():
        if p.phase_group:
            by_group[p.phase_group].append(p)

    linked = 0
    for group, members in by_group.items():
        parent_candidates = [
            p for p in members
            if re.search(r"remaining|master plan", p.name, re.IGNORECASE)
        ]
        if not parent_candidates:
            continue
        parent = parent_candidates[0]
        for p in members:
            if p.id != parent.id and p.parent_project_id != parent.id:
                p.parent_project_id = parent.id
                linked += 1
    session.commit()
    log.info("Linked %d projects to a phase parent across %d phase groups", linked, len(by_group))


def flag_conditional_alternatives(session, projects_by_cid: dict[str, Project]) -> int:
    """Flag genuinely competing/conditional plans (see MXD Infill / PB315 case).

    Sharing a special-permit base number with >1 distinct amendment is NOT
    sufficient on its own -- PB315 covers all four MXD Infill buildings, but
    only 250 Binney St and 105 Broadway are actually alternate plans for the
    same GFA (the MA3 decision transfers square footage between them, per
    their own notes). 119-123 Broadway and 290 Binney St just happen to share
    the umbrella permit and are ordinary, uncontested buildings. A project is
    only flagged if its own note text explicitly cross-references a
    *different* amendment of the same base permit -- i.e. the two sides of
    the actual transfer describe each other by amendment number.
    """
    by_base: dict[str, list[tuple[Project, str]]] = defaultdict(list)
    for p in projects_by_cid.values():
        sp = session.query(CambridgeSpecialPermit).filter_by(project_id=p.id).first()
        if sp and sp.base_permit:
            by_base[sp.base_permit].append((p, sp.amendment_raw or ""))

    flagged = 0
    for base, members in by_base.items():
        amendments = {a for _, a in members if a}
        if len(amendments) <= 1:
            continue
        cross_referenced = []
        for p, amendment in members:
            note = p.notes or ""
            others = amendments - {amendment}
            if any(
                re.search(rf"\b{re.escape(base)}\s*{re.escape(other)}\b", note, re.IGNORECASE)
                for other in others
            ):
                cross_referenced.append(p)

        cross_ids = {p.id for p in cross_referenced}
        if len(cross_referenced) > 1:
            log.warning(
                "Conditional alternative detected: %s -- %s cross-reference each other's "
                "amendments in their notes, excluded from default aggregates",
                base, [p.name for p in cross_referenced],
            )
        for p, _ in members:
            should_flag = p.id in cross_ids and len(cross_referenced) > 1
            if should_flag and not p.conditional_alternative:
                p.conditional_alternative = True
                flagged += 1
            elif not should_flag and p.conditional_alternative:
                p.conditional_alternative = False  # re-run after a heuristic fix: un-flag stale positives

    session.commit()
    return flagged


def dedupe_report(session, projects_by_cid: dict[str, Project]) -> list[dict]:
    """Compare newly-ingested Cambridge project names/addresses against every
    pre-existing project row (Boston-tracker rows AND any prior manual entry,
    regardless of its own city tag -- e.g. a manually-added "Cambridge
    Crossing" row predates this pipeline and is exactly this kind of
    collision). Report only -- never auto-merge.

    Two match strategies, both deliberately strict to avoid noise: generic
    street names ("Massachusetts Avenue") produce high token_sort_ratio
    scores against completely unrelated addresses purely from shared common
    words, so a numbered-address match requires the street *number* to match
    exactly, not just the street name. Named developments (no street number,
    e.g. "Cambridge Crossing (North Point)") fall back to a strict
    order-sensitive ratio on the full name.
    """
    existing = session.query(Project).filter(Project.cambridge_project_id.is_(None)).all()
    existing_entries = []
    for e in existing:
        for text in (e.name, e.address):
            if text:
                num, rest = split_street_number(text)
                existing_entries.append((e, num, rest))

    matches = []
    seen = set()
    for p in projects_by_cid.values():
        for text in (p.name, p.address):
            if not text:
                continue
            c_num, c_rest = split_street_number(text)
            for b, b_num, b_rest in existing_entries:
                if c_num and b_num:
                    if c_num != b_num:
                        continue
                    score = fuzz.ratio(c_rest, b_rest)
                    threshold = 80
                elif not c_num and not b_num:
                    score = fuzz.ratio(c_rest, b_rest)
                    threshold = 90
                else:
                    continue  # one has a street number, the other doesn't -- not comparable
                if score >= threshold:
                    key = (p.id, b.id)
                    if key in seen:
                        continue
                    seen.add(key)
                    matches.append({
                        "cambridge_id": p.id, "cambridge_name": p.name,
                        "existing_id": b.id, "existing_name": b.name,
                        "existing_city": b.city, "score": score,
                    })
    return matches


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_ingestion(limit: int | None = None) -> dict:
    title = fetch_dataset_title(CURRENT_EDITION_DATASET_ID)
    log.info("Current edition dataset title: %s", title)
    if title and "2026 Q1" not in title:
        log.warning(
            "Dataset title is %r, not the '2026 Q1' edition this pipeline was verified "
            "against. Validation targets in cambridge_validate.py may be stale -- "
            "re-verify before trusting results.", title,
        )

    if not APP_TOKEN:
        log.warning(
            "CAMBRIDGE_SOCRATA_APP_TOKEN not set -- unauthenticated requests are "
            "throttled more aggressively by Socrata. Fine for a one-off run, but "
            "set this for scheduled runs."
        )

    log.info("Fetching current edition (%s)...", CURRENT_EDITION_DATASET_ID)
    raw_rows = fetch_all_rows(CURRENT_EDITION_DATASET_ID)
    log.info("Fetched %d rows", len(raw_rows))
    if limit:
        raw_rows = raw_rows[:limit]

    if len(raw_rows) != EXPECTED_PROJECT_COUNT and not limit:
        log.warning(
            "Fetched %d projects, expected %d. The published edition may have "
            "changed since this pipeline was verified -- do not trust downstream "
            "validation numbers without re-checking.", len(raw_rows), EXPECTED_PROJECT_COUNT,
        )

    init_db()
    session = get_session()
    try:
        snapshot_raw_rows(session, raw_rows, edition=title or "unknown")

        projects_by_cid: dict[str, Project] = {}
        for row in raw_rows:
            n = normalize_row(row)
            project = upsert_project(session, n)
            projects_by_cid[n["cambridge_project_id"]] = project
        session.commit()
        log.info("Upserted %d Cambridge projects", len(projects_by_cid))

        link_phase_parents(session, projects_by_cid)
        n_flagged = flag_conditional_alternatives(session, projects_by_cid)

        matches = dedupe_report(session, projects_by_cid)
        if matches:
            log.warning("=== Dedupe matches against pre-existing rows (review manually, not auto-merged) ===")
            for m in matches:
                log.warning(
                    "  [%d%%] Cambridge #%d %r  <->  #%d %r (city=%s)",
                    m["score"], m["cambridge_id"], m["cambridge_name"],
                    m["existing_id"], m["existing_name"], m["existing_city"],
                )
        else:
            log.info("No collisions found against pre-existing rows")

        return {
            "count": len(projects_by_cid),
            "conditional_alternatives_flagged": n_flagged,
            "dedupe_matches": matches,
        }
    finally:
        session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cambridge Development Log ingestion")
    parser.add_argument("--limit", type=int, default=None, help="Only ingest first N rows (test mode)")
    args = parser.parse_args()
    result = run_ingestion(limit=args.limit)
    print(f"\nIngested {result['count']} Cambridge projects "
          f"({result['conditional_alternatives_flagged']} flagged as conditional alternatives, "
          f"{len(result['dedupe_matches'])} dedupe matches to review).")
