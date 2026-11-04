"""Shared helpers for the South Florida ingest.

Five municipalities, one set of rules. Everything that is the same for all of
them lives here so each city module is only the part that differs -- which is
almost entirely its source, since City of Miami is the only one with a bulk
feed and the other four are read off boards, portals and dated trade press.

The three design decisions worth knowing before reading a city module:

1. FILTERING HAPPENS AT INTAKE, NOT AFTER. `passes_three_tests` is called
   before a record is built, and a record that fails is written to the run log
   and dropped. There is no quarantine column and no review queue for this
   market -- the Rhode Island ingest grew both, and clearing them afterwards
   cost more than the ingest did.

2. EVERY FILLED VALUE CARRIES ONE SOURCE URL, recorded in extraction_sources,
   which the project detail panel already renders as "FIELD SOURCES". A value
   with no source row is a value nothing can check, so `record_sources`
   refuses to write one.

3. A PLANNING DOCUMENT CANNOT ESTABLISH CONSTRUCTION OR COMPLETION. `STAGE_*`
   below names the five stages and `construction_proof` is the only route to
   the last two. Rhode Island left 69 finished buildings in its pipeline by
   skipping that check.
"""

import json
import math
import re
import time

import requests

# ── canonical stages, as the brief names them ───────────────────────────
STAGE_PLANNING = "Planning"
STAGE_PERMITTING = "Permitting"
STAGE_APPROVED = "Approved"
STAGE_UNDER_CONSTRUCTION = "Under Construction"
STAGE_COMPLETE = "Complete"

# Stages a planning document alone may establish. Anything further needs a
# permit, a recorded Notice of Commencement, a certificate, or dated press.
PLANNING_REACHABLE = {STAGE_PLANNING, STAGE_PERMITTING, STAGE_APPROVED}

# "Dead" is not a sixth stage. It is the existing withdrawn/denied field, so a
# denied project keeps the stage it died at and drops out of pipeline totals
# through the same filter Boston's withdrawn projects already use.
DEAD_DENIED = "Denied"
DEAD_WITHDRAWN = "Withdrawn"

NOT_FOUND = ""   # stored as NULL/blank; rendered as "—" by the detail panel

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/122.0 Safari/537.36")


def session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    return s


# ── ArcGIS REST ─────────────────────────────────────────────────────────
def arcgis_query(layer_url: str, *, where="1=1", out_fields="*",
                 geometry=False, extra=None, page=2000, sess=None,
                 max_pages=400, order_by=None) -> list[dict]:
    """Every feature matching `where`, paged past the service's record cap.

    Returns attribute dicts (with "_geometry" added when geometry=True).
    Paging is explicit rather than trusting exceededTransferLimit, because a
    silently truncated first page is indistinguishable from a small result and
    would quietly shorten the pipeline.

    `order_by` must be passed whenever the result is paged, and must name the
    layer's own OID field -- which is spelled ObjectId on the City of Miami
    services and OBJECTID on the county's. An unordered paged query is not
    guaranteed to return each row once, so the default is no ordering and the
    caller states it.
    """
    s = sess or session()
    out, offset = [], 0
    for _ in range(max_pages):
        params = {
            "where": where, "outFields": out_fields, "f": "json",
            "returnGeometry": "true" if geometry else "false",
            "resultOffset": offset, "resultRecordCount": page,
            "orderByFields": f"{order_by} ASC" if order_by else None,
        }
        if geometry:
            params["outSR"] = 4326
        if extra:
            params.update(extra)
        params = {k: v for k, v in params.items() if v is not None}
        data = _get_json(s, layer_url + "/query", params)
        if "error" in data:
            # An orderBy on a layer without OBJECTID is the one recoverable
            # error here; retry once without it rather than losing the layer.
            params.pop("orderByFields", None)
            data = _get_json(s, layer_url + "/query", params)
            if "error" in data:
                raise RuntimeError(f"{layer_url}: {data['error']}")
        feats = data.get("features", [])
        for f in feats:
            a = dict(f.get("attributes") or {})
            if geometry:
                a["_geometry"] = f.get("geometry")
            out.append(a)
        if len(feats) < page:
            break
        offset += page
    return out


def _get_json(s, url, params, tries=4):
    """POSTed, not GET.

    An `IN (...)` clause over even a few dozen parcel identifiers overruns the
    URL length the county's gateway accepts, and what comes back is an HTML
    error page -- which surfaces as "Expecting value: line 1 column 1",
    i.e. as a parse failure rather than as the request being too long. POST
    carries the same query with no length to overrun.
    """
    last = None
    for attempt in range(tries):
        try:
            return s.post(url, data=params, timeout=180).json()
        except Exception as exc:        # transient network / JSON truncation
            last = exc
            time.sleep(2 + 3 * attempt)
    raise RuntimeError(f"{url}: {last}")


# ── point in polygon, for the neighborhood joins ────────────────────────
# Each city publishes a neighborhood polygon layer (Miami and West Palm Beach
# on their ArcGIS hubs, Miami Beach on its own MapServer). Coral Gables and
# Doral publish none, and for those the rule is the source document's own area
# name or nothing -- never a guess from coordinates.
def _ring_contains(ring, x, y) -> bool:
    inside = False
    n = len(ring)
    for i in range(n):
        x1, y1 = ring[i][0], ring[i][1]
        x2, y2 = ring[(i + 1) % n][0], ring[(i + 1) % n][1]
        if (y1 > y) != (y2 > y):
            t = (y - y1) / (y2 - y1) if y2 != y1 else 0.0
            if x < x1 + t * (x2 - x1):
                inside = not inside
    return inside


class PolygonIndex:
    """Name lookup for a point, over an ArcGIS polygon layer.

    Holds the rings in memory and tests locally rather than issuing one
    spatial query per project: a few hundred points against a few hundred
    polygons is nothing, and it keeps the ingest runnable offline from the
    cached layer.
    """

    def __init__(self, features: list[dict], name_field: str):
        self.shapes = []
        for f in features:
            geom = f.get("_geometry") or {}
            rings = geom.get("rings") or []
            if not rings:
                continue
            name = f.get(name_field)
            if not name:
                continue
            xs = [p[0] for r in rings for p in r]
            ys = [p[1] for r in rings for p in r]
            self.shapes.append((str(name).strip(), rings,
                                (min(xs), min(ys), max(xs), max(ys))))

    def __len__(self):
        return len(self.shapes)

    def name_at(self, lon, lat) -> str:
        """The polygon containing the point, or "" -- never a nearest guess."""
        if lon is None or lat is None:
            return ""
        if not (math.isfinite(lon) and math.isfinite(lat)):
            return ""
        for name, rings, (x0, y0, x1, y1) in self.shapes:
            if not (x0 <= lon <= x1 and y0 <= lat <= y1):
                continue
            # Odd-even over every ring handles the holes as well as the
            # islands: a point in a hole flips twice and lands outside.
            if sum(1 for r in rings if _ring_contains(r, lon, lat)) % 2 == 1:
                return name
        return ""


# ── the three tests ─────────────────────────────────────────────────────
# A record must pass all three to enter the database. Each test returns the
# reason it failed, so the run log can say exactly what was dropped and why
# instead of reporting a count.

# Test 1 -- scopes that do NOT create or materially change a building.
EXCLUDED_SCOPE_WORDS = (
    "interior fit", "fit-out", "fitout", "tenant improvement", "signage",
    "sign only", "fence", "parking lot", "landscap", "lot split",
    "change of use only", "re-roof", "reroof", "recertification",
)

# Test 2 -- uses that are not commercial. 1-4 family housing is the whole of
# the exclusion; everything from a 5-unit walk-up upwards is in scope.
NONCOMMERCIAL_USE_WORDS = (
    "single family", "single-family", "two-family", "duplex", "triplex",
    "townhouse (3", "accessory dwelling", "ancillary dwelling",
)


def passes_three_tests(*, changes_building: bool, commercial: bool,
                       has_program: bool) -> str:
    """"" if the record is in scope, else the test it failed.

    Deliberately takes three booleans a caller has already decided rather
    than guessing from free text: each city's source states these differently
    (a work-item vocabulary in Miami, an agenda description elsewhere), and a
    shared text heuristic over all five would be the "new parser" this ingest
    is not allowed to grow.
    """
    if not changes_building:
        return "test1_no_building_change"
    if not commercial:
        return "test2_not_commercial"
    if not has_program:
        return "test3_no_stated_program"
    return ""


# ── construction and completion proof ───────────────────────────────────
CONSTRUCTION_PROOFS = {
    "building_permit_issued", "notice_of_commencement", "press_construction",
}
COMPLETION_PROOFS = {
    "certificate_of_occupancy", "temporary_certificate", "permit_final",
    "leasing_active", "sales_closing", "press_complete",
}


def construction_proof(kind: str) -> bool:
    return kind in CONSTRUCTION_PROOFS


def completion_proof(kind: str) -> bool:
    return kind in COMPLETION_PROOFS


def stage_from_evidence(kind: str, fallback: str) -> str:
    """The stage a piece of evidence supports, with the brief's hard rule.

    A planning document never returns Under Construction or Complete, however
    confident the wording: the check Rhode Island skipped, which is why 69
    finished buildings sat in its pipeline.
    """
    if completion_proof(kind):
        return STAGE_COMPLETE
    if construction_proof(kind):
        return STAGE_UNDER_CONSTRUCTION
    if fallback in PLANNING_REACHABLE:
        return fallback
    raise ValueError(f"{fallback!r} cannot be established by {kind!r}")


# ── identity ────────────────────────────────────────────────────────────
def norm_folio(value) -> str:
    """Miami-Dade folio as the 13 digits it is.

    The permit feed types the folio as a float, so 0101140301010 arrives as
    101140301010.0 with the leading zero gone and a decimal point added. A
    folio compared in that shape never matches the parcel layer's string, and
    the dedupe silently stops working.
    """
    if value in (None, ""):
        return ""
    s = str(value).strip()
    if s.endswith(".0"):
        s = s[:-2]
    s = re.sub(r"\D", "", s)
    if not s or set(s) == {"0"}:
        return ""
    return s.zfill(13) if len(s) <= 13 else s


def norm_pcn(value) -> str:
    """Palm Beach County parcel control number as its 17 digits."""
    if value in (None, ""):
        return ""
    s = re.sub(r"\D", "", str(value))
    if not s:
        return ""
    return s.zfill(17) if len(s) <= 17 else s


_ADDR_SUFFIX = {
    "AVENUE": "AVE", "AV": "AVE", "STREET": "ST", "ROAD": "RD",
    "BOULEVARD": "BLVD", "BLWY": "BLVD", "TERRACE": "TER", "COURT": "CT",
    "DRIVE": "DR", "PLACE": "PL", "LANE": "LN", "CIRCLE": "CIR",
    "PARKWAY": "PKWY", "HIGHWAY": "HWY", "WAY": "WAY",
}


def norm_address(addr: str) -> str:
    """A comparable form of a street address, for cross-source matching only.

    Never stored. The stored address is whatever the source wrote, because a
    normalized address is a worse thing to show a reader than a messy real one.
    """
    if not addr:
        return ""
    a = str(addr).upper()
    a = re.sub(r"[#].*$", "", a)              # unit suffixes
    a = re.sub(r"[^A-Z0-9 ]", " ", a)
    parts = [_ADDR_SUFFIX.get(p, p) for p in a.split()]
    parts = [p for p in parts if p not in ("N", "S", "E", "W", "NE", "NW", "SE", "SW")
             or True]                          # direction kept: 1 NE 1 ST != 1 NW 1 ST
    return " ".join(parts).strip()


# ── asset class ─────────────────────────────────────────────────────────
# Rental and condo are separate values -- see the note in app/data.py beside
# ASSET_CLASSES. Parking and "other" are ancillary: a garage under an
# apartment building does not make the project mixed-use, so they only become
# the asset class when nothing else is present.
PRIMARY_USES = ("Residential", "Condo", "Office", "Lab/Research", "Retail",
                "Hotel", "Industrial", "Institutional")
ANCILLARY_USES = ("Parking", "Other")


def asset_class_from_uses(uses) -> str:
    """One canonical asset class from the set of uses a source states."""
    primary = [u for u in PRIMARY_USES if u in set(uses)]
    if len(primary) > 1:
        return "Mixed-Use"
    if primary:
        return primary[0]
    ancillary = [u for u in ANCILLARY_USES if u in set(uses)]
    return ancillary[0] if ancillary else ""


# ── persistence ─────────────────────────────────────────────────────────
def record_sources(session_db, project_id: int, field_values: dict,
                   source_name: str, source_url: str, source_date: str = ""):
    """One citation row per filled field. Refuses a value with no URL.

    This is the whole provenance model for South Florida: no confidence
    tiers, one URL per value, which is what every other field in the tracker
    already does and what the detail panel already renders.
    """
    from db.models import ExtractionSource
    if not source_url:
        raise ValueError(f"project {project_id}: {sorted(field_values)} has no source URL")
    for field, value in field_values.items():
        if value in (None, "", []):
            continue
        session_db.add(ExtractionSource(
            project_id=project_id,
            field_name=field,
            field_value=str(value),
            filing_name=source_name,
            filing_date=source_date or "",
            pdf_url=source_url,
        ))


def dump_json(path, obj):
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, indent=1, default=str)


def load_json(path, default=None):
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return default
