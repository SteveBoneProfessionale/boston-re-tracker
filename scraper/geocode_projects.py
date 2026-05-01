"""
Geocode project addresses using the US Census Bureau Geocoder API.

Free, no API key, no rate-limit restrictions.
Skips projects that already have lat/lon set.
"""

import sys
import time
import logging
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import init_db, get_session
from db.models import Project

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

CENSUS_URL = "https://geocoding.geo.census.gov/geocoder/locations/address"

# Boston bounding box — reject geocodes outside greater Boston area
LAT_MIN, LAT_MAX = 42.20, 42.42
LON_MIN, LON_MAX = -71.22, -70.92


def _parse_street(address: str) -> str:
    """Strip unit/suite info that confuses the geocoder."""
    import re
    # Remove trailing ", Unit X" or ", Apt X" etc.
    address = re.sub(r',?\s*(Unit|Apt|Suite|Ste|#)\s*\S+$', '', address, flags=re.I)
    return address.strip()


def geocode(client: httpx.Client, address: str) -> tuple[float, float] | None:
    street = _parse_street(address)
    try:
        r = client.get(
            CENSUS_URL,
            params={
                "street": street,
                "city": "Boston",
                "state": "MA",
                "benchmark": "Public_AR_Current",
                "format": "json",
            },
            timeout=15,
        )
        if r.status_code != 200:
            log.debug("HTTP %d for '%s'", r.status_code, street[:50])
            return None
        data = r.json()
        matches = data.get("result", {}).get("addressMatches", [])
        if not matches:
            return None
        coords = matches[0].get("coordinates", {})
        lon = float(coords.get("x", 0))
        lat = float(coords.get("y", 0))
        if not (LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX):
            log.debug("Out of bounds: %.5f, %.5f for '%s'", lat, lon, street[:50])
            return None
        return lat, lon
    except Exception as exc:
        log.debug("Geocode error for '%s': %s", address[:50], exc)
        return None


def run():
    init_db()
    session = get_session()
    try:
        targets = [
            p for p in session.query(Project).all()
            if p.address and not p.latitude
        ]
        log.info("Projects needing geocoding: %d", len(targets))

        found = 0
        failed = 0

        with httpx.Client(follow_redirects=True) as client:
            for i, proj in enumerate(targets, 1):
                coords = geocode(client, proj.address)
                if coords:
                    proj.latitude, proj.longitude = coords
                    found += 1
                    log.info("[%d/%d] %-50s  %.5f, %.5f",
                             i, len(targets), proj.address[:50], *coords)
                else:
                    failed += 1
                    log.debug("[%d/%d] NOT FOUND: %s", i, len(targets), proj.address[:50])

                if i % 25 == 0:
                    session.commit()
                    log.info("  Progress: %d geocoded, %d failed so far", found, failed)

                time.sleep(0.5)

        session.commit()
        log.info("\n=== Geocoding complete ===\n  Found: %d\n  Failed: %d\n", found, failed)

        with_coords = session.query(Project).filter(Project.latitude.isnot(None)).count()
        total = session.query(Project).count()
        log.info("Coverage: %d / %d (%.0f%%)", with_coords, total,
                 100 * with_coords / total)

    finally:
        session.close()


if __name__ == "__main__":
    run()
