r"""
Classify Rhode Island projects: asset class and submarket.

ASSET CLASS
-----------
Writes only the ten canonical values (app/data.py::ASSET_CLASSES). Adaptive
reuse of a mill is Residential or Mixed-Use with the adaptive_reuse flag set --
never a Rhode Island specific category, per the shared taxonomy.

Classification is evidence-based: it reads the project description and the
program fields actually extracted. A project whose description says nothing
about use is left unclassified rather than defaulted to Residential, because a
wrong bar on Gross SF by Asset Class is worse than a short one.

SUBMARKET
---------
The dimension differs by municipality (scraper/ri_submarkets.py):

  Providence   neighborhood, stated inline on the agenda ("AP 68 Lot 846,
               Smith Hill") and validated against the city's 25-polygon layer
  Pawtucket    neighborhood, from the city DPW layer, by point-in-polygon
  Warwick      village, from RIGIS villages clipped to the municipal boundary
  Cranston     neighborhood, provisional layer, by point-in-polygon
  Newport      zoning district -- no boundary layer exists

Point-in-polygon needs coordinates, so this runs after geocoding.
"""

import re
import sys
import json
import logging
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import init_db, get_session
from db.models import Project
from scraper.ri_submarkets import (
    SUBMARKET_SOURCES, normalize_submarket_name, validate_submarket_name,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                         "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"}

# Ordered: the first rule that fires wins, so specific uses beat generic ones.
_ASSET_RULES = [
    ("Hotel",        r"\bhotel\b|transient guest|guest\s*rooms?|lodging|inn\b"),
    ("Lab/Research", r"\blab(oratory|s)?\b|life scien|research and development|\bR&D\b"),
    # Parking is checked BEFORE Industrial: "Blackstone Distribution Center
    # Parking Facility" is a parking structure, and matching "distribution
    # center" first would file it as Industrial.
    ("Parking",      r"parking (facility|garage|structure)\b"),
    ("Industrial",   r"\bwarehouse\b|distribution center|manufactur|\bindustrial\b|self[- ]storage"),
    ("Institutional", r"\bschool\b|\bchurch\b|\bhospital\b|\bmuseum\b|library|"
                      r"place of worship|community center|fire station|municipal"),
    ("Retail",       r"\bretail\b|\brestaurant\b|storefront|shopping"),
    ("Office",       r"\boffice\b"),
    ("Mixed-Use",    r"mixed[- ]use|commercial (?:use )?on the (?:first|ground) floor|"
                     r"ground[- ]floor (?:retail|commercial)"),
    ("Residential",  r"\bresidential\b|\bdwellings?\b|\bapartments?\b|"
                     r"\bmulti-?family\b|\btownhouses?\b|\bcondominium|\bhousing\b"),
]


def classify_asset(description: str, units: int | None, mixed_hint: bool = False) -> str | None:
    """Canonical asset class, or None when the filing does not say."""
    text = description or ""
    if not text.strip():
        return None

    hits = [label for label, pat in _ASSET_RULES if re.search(pat, text, re.I)]
    if not hits:
        return None

    # Residential plus a commercial use in the same description is Mixed-Use.
    commercial = {"Retail", "Office", "Hotel", "Industrial", "Lab/Research"}
    if "Residential" in hits and (commercial & set(hits) or "Mixed-Use" in hits):
        return "Mixed-Use"
    if "Mixed-Use" in hits:
        return "Mixed-Use"
    # Otherwise the highest-priority specific hit wins.
    for label, _ in _ASSET_RULES:
        if label in hits:
            return label
    return None


def _point_in_polygon_lookup(client: httpx.Client, service: str, name_field: str,
                             lat: float, lon: float) -> str | None:
    """Which polygon contains this point."""
    geom = json.dumps({"x": lon, "y": lat, "spatialReference": {"wkid": 4326}})
    try:
        r = client.get(service + "/query", params={
            "geometry": geom, "geometryType": "esriGeometryPoint", "inSR": 4326,
            "spatialRel": "esriSpatialRelIntersects", "outFields": name_field,
            "returnGeometry": "false", "f": "json"}, timeout=45)
        feats = r.json().get("features", [])
    except Exception:
        return None
    if not feats:
        return None
    return feats[0]["attributes"].get(name_field)


def run(dry_run: bool = False) -> dict:
    init_db()
    session = get_session()
    stats = {"asset_set": 0, "asset_none": 0,
             "submarket_inline": 0, "submarket_gis": 0,
             "submarket_zoning": 0, "submarket_none": 0}
    flagged_names = []
    try:
        projects = (session.query(Project)
                    .filter(Project.bpda_url.like("manual:ri-%")).all())
        log.info("Classifying %d Rhode Island projects", len(projects))

        with httpx.Client(headers=HEADERS, follow_redirects=True) as client:
            for i, p in enumerate(projects, 1):
                # ── asset class ──
                ac = classify_asset(p.description, p.residential_units)
                if ac:
                    if not dry_run:
                        p.asset_class = ac
                        p.asset_class_raw = ac
                    stats["asset_set"] += 1
                else:
                    stats["asset_none"] += 1

                # ── submarket ──
                cfg = SUBMARKET_SOURCES.get(p.city or "", {})
                value = None
                source = None

                if p.neighborhood:
                    # Providence states it on the agenda itself.
                    value, source = normalize_submarket_name(p.neighborhood), "inline"
                elif cfg.get("service") and p.latitude is not None:
                    raw = _point_in_polygon_lookup(
                        client, cfg["service"], cfg["name_field"],
                        p.latitude, p.longitude)
                    if raw:
                        problems = validate_submarket_name(raw)
                        if any("digit" in x for x in problems):
                            flagged_names.append((p.city, raw))
                        value, source = normalize_submarket_name(raw), "gis"
                elif cfg.get("dimension") == "zoning_district":
                    # Newport: no boundary layer exists, so the zoning district
                    # from the filing is the submarket dimension.
                    value, source = (p.zoning_district_raw or None), "zoning"

                if value:
                    if not dry_run:
                        p.neighborhood = value
                    stats[f"submarket_{source}"] += 1
                else:
                    stats["submarket_none"] += 1

                if i % 50 == 0 and not dry_run:
                    session.commit()
                    log.info("  %d/%d", i, len(projects))
        if not dry_run:
            session.commit()
    finally:
        session.close()

    log.info("\n=== Classification complete ===")
    for k, v in stats.items():
        log.info("  %-20s %4d", k, v)
    if flagged_names:
        log.warning("  Names containing a digit (likely source typos): %s",
                    flagged_names[:8])
    return stats


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    run(ap.parse_args().dry_run)
