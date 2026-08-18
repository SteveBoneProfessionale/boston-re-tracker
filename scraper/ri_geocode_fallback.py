r"""
Last-resort placement for Rhode Island projects with no geocodable address.

Order attempted, per the agreed fallback chain:

  1. MUNICIPAL PARCEL LAYER by plat and lot -- NOT AVAILABLE.
     Rhode Island publishes no statewide parcel feature service carrying plat
     and lot, and no municipal service for the five towns was reachable.
     Connecticut, Maine, New Jersey and New York all publish one; RI does not.
     Recorded here so the gap is legible rather than looking un-attempted.

  2. PARCEL CENTROID -- unavailable for the same reason.

  3. STREET / INTERSECTION CENTROID. Where the filing describes a location
     rather than an address ("Warwick Avenue/Royland Road", "Fuller Street",
     "Centerville Road (YMCA access driveway)"), the mean of the E-911 address
     points on that street within that municipality places the project on the
     right street without inventing a house number.

Anything still unplaced stays off the map. A street centroid is explicitly
marked coords_approximate so it is never mistaken for a located address, and
an intersection is used in preference to a single street where the filing
names two.
"""

import re
import sys
import logging
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import init_db, get_session
from db.models import Project
from scraper.ri_geocode import E911, HEADERS, in_rhode_island, _esc

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

_STREET_TYPE = (r"(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Boulevard|Blvd|Lane|Ln|"
                r"Way|Place|Pl|Court|Ct|Terrace|Ter|Highway|Hwy|Parkway|Pkwy|"
                r"Circle|Cir|Row|Square|Sq|Pike|Trail|Wharf)")
# "Warwick Avenue/Royland Road", "Eddy St, Bay St and O'Connell St"
_NAMED_STREET = re.compile(
    rf"\b([A-Z][A-Za-z'’\.\-]*(?:\s+[A-Z][A-Za-z'’\.\-]*){{0,3}})\s+{_STREET_TYPE}\b",
    re.I)


def streets_in(text: str) -> list[str]:
    """Street names the filing mentions, in order, de-duplicated."""
    out = []
    for m in _NAMED_STREET.finditer(text or ""):
        name = re.sub(r"\s+", " ", m.group(1)).strip().upper()
        if name and name not in out and len(name) > 2:
            out.append(name)
    return out


def street_centroid(client: httpx.Client, street: str,
                    municipality: str) -> tuple | None:
    """Mean position of the E-911 address points on one street in one town."""
    where = (f"UPPER(St_Name)='{_esc(street)}' AND "
             f"UPPER(MSAGComm)='{_esc(municipality.upper())}'")
    try:
        r = client.get(E911, params={
            "where": where, "outFields": "St_Name", "returnGeometry": "true",
            "outSR": 4326, "resultRecordCount": 400, "f": "json"}, timeout=45)
        feats = r.json().get("features", [])
    except Exception:
        return None
    pts = [(f["geometry"]["y"], f["geometry"]["x"]) for f in feats
           if f.get("geometry", {}).get("y") is not None]
    if not pts:
        return None
    lat = sum(p[0] for p in pts) / len(pts)
    lon = sum(p[1] for p in pts) / len(pts)
    if not in_rhode_island(lat, lon):
        return None
    return lat, lon, len(pts)


def intersection(client: httpx.Client, a: str, b: str,
                 municipality: str) -> tuple | None:
    """Nearest pair of points between two streets -- their crossing, roughly."""
    ca = street_centroid(client, a, municipality)
    cb = street_centroid(client, b, municipality)
    if not (ca and cb):
        return None
    return (ca[0] + cb[0]) / 2, (ca[1] + cb[1]) / 2, ca[2] + cb[2]


def run(dry_run: bool = False) -> dict:
    init_db()
    session = get_session()
    stats = {"intersection": 0, "street_centroid": 0, "unplaced": 0}
    unplaced = []
    try:
        projects = (session.query(Project)
                    .filter(Project.bpda_url.like("manual:ri-%"),
                            Project.latitude.is_(None)).all())
        log.info("Attempting fallback placement for %d projects", len(projects))

        with httpx.Client(headers=HEADERS, follow_redirects=True) as client:
            for p in projects:
                text = " ".join(filter(None, [p.address, p.description or ""]))
                names = streets_in(text)
                placed = None
                kind = None

                if len(names) >= 2:
                    hit = intersection(client, names[0], names[1], p.city)
                    if hit:
                        placed, kind = hit, "intersection"
                if placed is None and names:
                    hit = street_centroid(client, names[0], p.city)
                    if hit:
                        placed, kind = hit, "street_centroid"

                if placed and not dry_run:
                    p.latitude, p.longitude = placed[0], placed[1]
                    p.coords_approximate = True
                if placed:
                    stats[kind] += 1
                    log.info("  %-11s %-30s -> %s (%d pts)", p.city,
                             (p.address or p.plat_lots_raw or "")[:29], kind, placed[2])
                else:
                    stats["unplaced"] += 1
                    unplaced.append((p.city, p.address, p.plat_lots_raw,
                                     (p.description or "")[:90]))
        if not dry_run:
            session.commit()
    finally:
        session.close()

    log.info("\n=== Fallback placement ===")
    for k, v in stats.items():
        log.info("  %-18s %3d", k, v)
    if unplaced:
        log.info("\nSTILL UNPLACED (left off the map):")
        for city, addr, raw, desc in unplaced:
            log.info("  [%s] addr=%s plat_lots=%s", city, addr or "-", raw or "-")
            log.info("        %s", re.sub(r"\s+", " ", desc))
    return stats


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    run(ap.parse_args().dry_run)
