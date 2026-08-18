r"""
Geocode Rhode Island projects.

The existing geocoder (scraper/geocode_projects.py) cannot be reused: it
hardcodes city="Boston", state="MA" and a greater-Boston bounding box, so every
Rhode Island address either fails or is rejected as out of bounds.

Attempt order, per the market's data reality:

  1. RIGIS E-911 Sites -- the statewide address-point layer, 417,719 points,
     matched on street number + street name + municipality. Authoritative and
     exact. Current edition is FACILITY_Sites_E911_24r1; older names such as
     e911Sites22r1 are deprecated and must not be used.
  2. US Census geocoder with state=RI, as a fallback for addresses the E-911
     layer does not carry.
  3. Nothing. A project that cannot be verified is left without coordinates and
     flagged, never placed on the map at a guessed position.

Results are bounds-checked against Rhode Island. A coordinate outside the state
is discarded rather than written, because a marker in the wrong state is worse
than no marker.
"""

import re
import sys
import time
import logging
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import init_db, get_session
from db.models import Project

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

E911 = ("https://services2.arcgis.com/S8zZg9pg23JUEexQ/arcgis/rest/services/"
        "FACILITY_Sites_E911_24r1/FeatureServer/0/query")
CENSUS = "https://geocoding.geo.census.gov/geocoder/locations/address"

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                         "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"}

# Rhode Island bounding box. A result outside this is discarded -- street
# numbers and names repeat across state lines constantly.
LAT_MIN, LAT_MAX = 41.09, 42.02
LON_MIN, LON_MAX = -71.91, -71.08

_SUFFIX = {
    "STREET": "ST", "AVENUE": "AVE", "ROAD": "RD", "DRIVE": "DR", "LANE": "LN",
    "BOULEVARD": "BLVD", "PLACE": "PL", "COURT": "CT", "TERRACE": "TER",
    "HIGHWAY": "HWY", "SQUARE": "SQ", "PARKWAY": "PKWY", "CIRCLE": "CIR",
    "EXTENSION": "EXT",
}
_SUFFIX_WORDS = set(_SUFFIX) | set(_SUFFIX.values())


def split_address(address: str) -> tuple[str | None, str | None]:
    """(street number, street name) with the suffix removed.

    The E-911 layer stores St_Name WITHOUT its type ("WEEDEN", not "WEEDEN
    ST"), so the suffix has to come off before matching.
    """
    if not address:
        return None, None
    a = re.sub(r"\(.*?\)", " ", address.upper())
    a = re.sub(r"[.,]", " ", a)
    a = re.sub(r"^\s*(\d+)\s*(?:-|–|TO|AND|&)\s*\d+", r"\1", a)   # ranges
    m = re.match(r"\s*(\d+)\s+(.+)", a)
    if not m:
        return None, None
    num, rest = m.group(1), m.group(2).split()
    while rest and rest[-1] in _SUFFIX_WORDS:
        rest.pop()
    # Drop a trailing unit designator the agenda may carry.
    rest = [w for w in rest if w not in {"UNIT", "APT", "STE", "SUITE", "REAR"}]
    return num, " ".join(rest).strip() or None


def in_rhode_island(lat: float, lon: float) -> bool:
    return LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX


def _esc(s: str) -> str:
    return s.replace("'", "''")


def geocode_e911(client: httpx.Client, address: str, municipality: str) -> tuple | None:
    num, street = split_address(address)
    if not (num and street):
        return None
    where = (f"Add_Number={int(num)} AND UPPER(St_Name)='{_esc(street)}' "
             f"AND UPPER(MSAGComm)='{_esc(municipality.upper())}'")
    try:
        r = client.get(E911, params={
            "where": where, "outFields": "Add_Full,MSAGComm,St_Name,Add_Number",
            "returnGeometry": "true", "outSR": 4326, "resultRecordCount": 5,
            "f": "json"}, timeout=45)
        feats = r.json().get("features", [])
    except Exception as exc:
        log.debug("E911 error for %s: %s", address, exc)
        return None
    if not feats:
        return None
    g = feats[0].get("geometry") or {}
    lat, lon = g.get("y"), g.get("x")
    if lat is None or not in_rhode_island(lat, lon):
        return None
    return lat, lon, "rigis_e911", feats[0]["attributes"].get("Add_Full", "")


def geocode_census(client: httpx.Client, address: str, municipality: str) -> tuple | None:
    try:
        r = client.get(CENSUS, params={
            "street": address, "city": municipality, "state": "RI",
            "benchmark": "Public_AR_Current", "format": "json"}, timeout=30)
        matches = r.json().get("result", {}).get("addressMatches", [])
    except Exception:
        return None
    if not matches:
        return None
    c = matches[0].get("coordinates", {})
    lat, lon = float(c.get("y", 0)), float(c.get("x", 0))
    if not in_rhode_island(lat, lon):
        return None
    return lat, lon, "census_ri", matches[0].get("matchedAddress", "")


def run(limit: int | None = None) -> dict:
    init_db()
    session = get_session()
    stats = {"e911": 0, "census": 0, "failed": 0, "no_address": 0}
    try:
        targets = [p for p in session.query(Project)
                   .filter(Project.bpda_url.like("manual:ri-%")).all()
                   if p.latitude is None]
        if limit:
            targets = targets[:limit]
        log.info("Rhode Island projects needing coordinates: %d", len(targets))

        with httpx.Client(headers=HEADERS, follow_redirects=True) as client:
            for i, p in enumerate(targets, 1):
                if not p.address:
                    stats["no_address"] += 1
                    continue
                hit = geocode_e911(client, p.address, p.city or "")
                if hit is None:
                    time.sleep(0.2)
                    hit = geocode_census(client, p.address, p.city or "")
                if hit is None:
                    stats["failed"] += 1
                    log.debug("no match: %s, %s", p.address, p.city)
                    time.sleep(0.2)
                    continue
                lat, lon, method, matched = hit
                p.latitude, p.longitude = lat, lon
                # E-911 is an exact address point; Census is interpolated, so
                # it is marked approximate and the map renders it faded.
                p.coords_approximate = (method != "rigis_e911")
                stats["e911" if method == "rigis_e911" else "census"] += 1
                if i % 25 == 0:
                    session.commit()
                    log.info("  %d/%d  e911=%d census=%d failed=%d",
                             i, len(targets), stats["e911"], stats["census"], stats["failed"])
                time.sleep(0.15)
        session.commit()
    finally:
        session.close()

    total = sum(stats.values())
    log.info("\n=== Geocoding complete ===")
    for k, v in stats.items():
        log.info("  %-12s %4d  (%.0f%%)", k, v, 100 * v / total if total else 0)
    return stats


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    run(ap.parse_args().limit)
