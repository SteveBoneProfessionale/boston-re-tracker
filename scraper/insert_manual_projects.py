"""
Insert/update the 14 manually-specified addresses into the database.
Projects outside BPDA jurisdiction get a synthetic bpda_url like 'manual:<slug>'.
Geocodes new entries via Census Bureau, then normalizes developer names.
"""

import sys
import re
import time
import logging
import httpx
from pathlib import Path
from datetime import datetime, timezone
sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import init_db, get_session
from db.models import Project
from scraper.normalize_developer import normalize as normalize_developer, is_real_company

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

CENSUS_URL = "https://geocoding.geo.census.gov/geocoder/locations/address"

# Expanded bounding box to cover Woburn, Revere, Cambridge, Hudson area
LAT_MIN, LAT_MAX = 41.90, 42.55
LON_MIN, LON_MAX = -71.60, -70.80


def make_slug(address: str) -> str:
    return re.sub(r'[^a-z0-9]+', '-', address.lower()).strip('-')


def geocode_address(client: httpx.Client, address: str, city: str = "Boston") -> tuple[float, float] | None:
    # Strip unit/suite
    street = re.sub(r',?\s*(Unit|Apt|Suite|Ste|#)\s*\S+$', '', address, flags=re.I).strip()
    try:
        r = client.get(
            CENSUS_URL,
            params={
                "street": street,
                "city": city,
                "state": "MA",
                "benchmark": "Public_AR_Current",
                "format": "json",
            },
            timeout=15,
        )
        if r.status_code != 200:
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


# ── Project data from research ─────────────────────────────────────────────

PROJECTS = [
    # 1 — Woburn, outside BPDA
    {
        "name": "Station 316 Apartments",
        "address": "316 New Boston St",
        "city": "Woburn",
        "neighborhood": None,
        "status": "Under Construction",
        "developer": "Fairfield Residential",
        "developer_canonical": "Fairfield Residential",
        "asset_class": "Residential",
        "total_gsf": None,
        "residential_units": 445,
        "commercial_gsf": None,
        "building_height_ft": None,
        "num_stories": None,
        "parking_spaces": None,
        "architect": None,
        "civil_engineer": None,
        "expected_delivery": "2026",
        "description": "445-unit luxury apartment complex at 316 New Boston Street in Woburn, MA. Mixed-income community near the Anderson Regional Transportation Center. Part of transit-oriented development near commuter rail.",
    },
    # 2 — Revere (adjacent to Suffolk Downs BOS side), outside BPDA
    {
        "name": "Beachmont Square at Suffolk Downs",
        "address": "525 William F McClellan Hwy",
        "city": "Revere",
        "neighborhood": None,
        "status": "Under Construction",
        "developer": "HYM Investment Group",
        "developer_canonical": "HYM Investment Group",
        "asset_class": "Mixed-Use",
        "total_gsf": 1_700_000,
        "residential_units": 1400,
        "commercial_gsf": 200_000,
        "building_height_ft": None,
        "num_stories": None,
        "parking_spaces": None,
        "architect": None,
        "civil_engineer": None,
        "expected_delivery": "2030",
        "description": "1.7M GSF mixed-use Revere portion of the Suffolk Downs redevelopment by HYM Investment Group. Includes approximately 1,400 residential units and 200,000 SF of commercial space adjacent to the Beachmont Blue Line station.",
    },
    # 3 — East Boston, BPDA
    {
        "name": "355 Bennington Street",
        "address": "355 Bennington St",
        "city": "Boston",
        "neighborhood": "East Boston",
        "status": "Under Construction",
        "developer": "Redgate Capital Partners",
        "developer_canonical": "Redgate Capital Partners",
        "asset_class": "Residential",
        "total_gsf": 168_000,
        "residential_units": 170,
        "commercial_gsf": None,
        "building_height_ft": None,
        "num_stories": 6,
        "parking_spaces": 85,
        "architect": None,
        "civil_engineer": None,
        "expected_delivery": "2026",
        "description": "170-unit residential development at 355 Bennington Street in East Boston. 6-story building with 85 parking spaces near the Maverick Square Blue Line station.",
    },
    # 4 — Cambridge (NorthPoint area), outside BPDA
    {
        "name": "Cambridge Crossing",
        "address": "2 O'Brien Hwy",
        "city": "Cambridge",
        "neighborhood": None,
        "status": "Under Construction",
        "developer": "DivcoWest",
        "developer_canonical": "DivcoWest",
        "asset_class": "Mixed-Use",
        "total_gsf": 4_500_000,
        "residential_units": 2700,
        "commercial_gsf": 2_100_000,
        "building_height_ft": None,
        "num_stories": None,
        "parking_spaces": None,
        "architect": None,
        "civil_engineer": None,
        "expected_delivery": "2030",
        "description": "Master-planned 4.5M SF mixed-use development at the Cambridge/Boston border by DivcoWest. Includes approximately 2,700 residential units and 2.1M SF of lab/office space. Formerly known as NorthPoint.",
    },
    # 5 — Back Bay, BPDA
    {
        "name": "380 Stuart Street",
        "address": "380 Stuart St",
        "city": "Boston",
        "neighborhood": "Back Bay",
        "status": "Board Approved",
        "developer": "Skanska USA Commercial Development",
        "developer_canonical": "Skanska",
        "asset_class": "Office",
        "total_gsf": 625_000,
        "residential_units": None,
        "commercial_gsf": 625_000,
        "building_height_ft": 525,
        "num_stories": 31,
        "parking_spaces": None,
        "architect": "Gensler",
        "civil_engineer": None,
        "expected_delivery": "2028",
        "description": "625,000 SF Class A office tower at 380 Stuart Street in Back Bay, developed by Skanska. 31-story, 525-foot building designed by Gensler. Board approved but on hold pending market conditions.",
    },
    # 6 — Downtown/Chinatown, BPDA
    {
        "name": "41 LaGrange Street",
        "address": "41 LaGrange St",
        "city": "Boston",
        "neighborhood": "Chinatown",
        "status": "Under Construction",
        "developer": "Planning Office for Urban Affairs",
        "developer_canonical": "Planning Office for Urban Affairs",
        "asset_class": "Residential",
        "total_gsf": 105_000,
        "residential_units": 126,
        "commercial_gsf": None,
        "building_height_ft": None,
        "num_stories": 13,
        "parking_spaces": None,
        "architect": "Utile Architecture",
        "civil_engineer": None,
        "expected_delivery": "2026",
        "description": "126-unit 100% affordable housing development at 41 LaGrange Street in Chinatown by the Planning Office for Urban Affairs. 13-story mixed-income building serving low- and moderate-income households.",
    },
    # 7 — Leather District, BPDA
    {
        "name": "125 Lincoln Street",
        "address": "125 Lincoln St",
        "city": "Boston",
        "neighborhood": "Leather District",
        "status": "Board Approved",
        "developer": "Oxford Properties Group",
        "developer_canonical": "Oxford Properties Group",
        "asset_class": "Lab/Research",
        "total_gsf": 335_000,
        "residential_units": None,
        "commercial_gsf": 335_000,
        "building_height_ft": None,
        "num_stories": 12,
        "parking_spaces": None,
        "architect": None,
        "civil_engineer": None,
        "expected_delivery": "2028",
        "description": "335,000 SF lab and office conversion/development at 125 Lincoln Street in the Leather District by Oxford Properties. 12-story building targeting life sciences tenants in the Innovation District corridor.",
    },
    # 8 — Roxbury/Mission Hill, BPDA
    {
        "name": "840 Columbus Avenue",
        "address": "840 Columbus Ave",
        "city": "Boston",
        "neighborhood": "Roxbury",
        "status": "Under Construction",
        "developer": "Northeastern University",
        "developer_canonical": "Northeastern University",
        "asset_class": "Mixed-Use",
        "total_gsf": 445_000,
        "residential_units": 345,
        "commercial_gsf": 100_000,
        "building_height_ft": None,
        "num_stories": 18,
        "parking_spaces": None,
        "architect": None,
        "civil_engineer": None,
        "expected_delivery": "2026",
        "description": "445,000 SF mixed-use development at 840 Columbus Avenue by Northeastern University. Includes 345 units of student/graduate housing and 100,000 SF of academic and retail space along the Columbus Avenue corridor.",
    },
    # 9 — Chinatown, BPDA
    {
        "name": "49-63 Hudson Street",
        "address": "49 Hudson St",
        "city": "Boston",
        "neighborhood": "Chinatown",
        "status": "Board Approved",
        "developer": "Asian Community Development Corporation",
        "developer_canonical": "Asian Community Development Corporation",
        "asset_class": "Residential",
        "total_gsf": 85_000,
        "residential_units": 110,
        "commercial_gsf": 5_000,
        "building_height_ft": None,
        "num_stories": 9,
        "parking_spaces": None,
        "architect": None,
        "civil_engineer": None,
        "expected_delivery": "2027",
        "description": "110-unit affordable housing development at 49-63 Hudson Street in Chinatown by the Asian Community Development Corporation. 9-story building preserving and expanding affordable housing in the Chinatown neighborhood.",
    },
    # 10 — Financial District, BPDA
    {
        "name": "55 India Street",
        "address": "55 India St",
        "city": "Boston",
        "neighborhood": "Financial District",
        "status": "Under Construction",
        "developer": "Boston Residential Group",
        "developer_canonical": "Boston Residential Group",
        "asset_class": "Residential",
        "total_gsf": 35_000,
        "residential_units": 29,
        "commercial_gsf": None,
        "building_height_ft": None,
        "num_stories": 7,
        "parking_spaces": None,
        "architect": None,
        "civil_engineer": None,
        "expected_delivery": "2026",
        "description": "29-unit luxury residential conversion at 55 India Street in Boston's Financial District by Boston Residential Group. Adaptive reuse of historic commercial building into high-end condominiums.",
    },
    # 11 — Charlestown, BPDA
    {
        "name": "Bunker Hill Housing Redevelopment",
        "address": "55 Bunker Hill St",
        "city": "Boston",
        "neighborhood": "Charlestown",
        "status": "Under Construction",
        "developer": "Leggat McCall Properties / Boston Housing Authority",
        "developer_canonical": "Leggat McCall Properties",
        "asset_class": "Residential",
        "total_gsf": 2_800_000,
        "residential_units": 2699,
        "commercial_gsf": 100_000,
        "building_height_ft": None,
        "num_stories": None,
        "parking_spaces": None,
        "architect": "Utile Architecture",
        "civil_engineer": None,
        "expected_delivery": "2034",
        "description": "$1.46B phased redevelopment of the Bunker Hill public housing development in Charlestown by Leggat McCall Properties and the Boston Housing Authority. 2,699 total units across multiple phases replacing 1,100 existing public housing units with mixed-income housing.",
    },
    # 12 — Hudson, outside BPDA
    {
        "name": "75 Reed Road Industrial",
        "address": "75 Reed Rd",
        "city": "Hudson",
        "neighborhood": None,
        "status": "Board Approved",
        "developer": "National Development",
        "developer_canonical": "National Development",
        "asset_class": "Industrial",
        "total_gsf": 950_000,
        "residential_units": None,
        "commercial_gsf": 950_000,
        "building_height_ft": None,
        "num_stories": None,
        "parking_spaces": None,
        "architect": None,
        "civil_engineer": None,
        "expected_delivery": "2027",
        "description": "950,000 SF industrial/warehouse development at 75 Reed Road in Hudson, MA by National Development. Modern logistics and distribution facility in MetroWest submarket targeting e-commerce and life sciences supply chain tenants.",
    },
    # 13 — Seaport, BPDA
    {
        "name": "1 Harbor Shore Drive",
        "address": "1 Harbor Shore Dr",
        "city": "Boston",
        "neighborhood": "South Boston Waterfront",
        "status": "Under Construction",
        "developer": "The Fallon Company",
        "developer_canonical": "The Fallon Company",
        "asset_class": "Residential",
        "total_gsf": 155_000,
        "residential_units": 122,
        "commercial_gsf": 5_000,
        "building_height_ft": None,
        "num_stories": 12,
        "parking_spaces": 90,
        "architect": None,
        "civil_engineer": None,
        "expected_delivery": "2026",
        "description": "122-unit luxury residential development at 1 Harbor Shore Drive in Boston's Seaport District by The Fallon Company. 12-story waterfront building with harbor views and ground-floor retail.",
    },
    # 14 — Back Bay, BPDA
    {
        "name": "171 Dartmouth Street",
        "address": "171 Dartmouth St",
        "city": "Boston",
        "neighborhood": "Back Bay",
        "status": "Board Approved",
        "developer": "Boston Properties (BXP)",
        "developer_canonical": "Boston Properties",
        "asset_class": "Office",
        "total_gsf": 660_000,
        "residential_units": None,
        "commercial_gsf": 660_000,
        "building_height_ft": 600,
        "num_stories": 33,
        "parking_spaces": None,
        "architect": "Skidmore, Owings & Merrill",
        "civil_engineer": None,
        "expected_delivery": None,
        "description": "660,000 SF Class A office tower at 171 Dartmouth Street in Back Bay by Boston Properties (BXP). 33-story, 600-foot tower designed by Skidmore, Owings & Merrill adjacent to Back Bay Station. Project on hold pending office market recovery.",
    },
]


def run():
    init_db()
    session = get_session()

    added = 0
    updated = 0
    skipped = 0

    try:
        with httpx.Client(follow_redirects=True) as http:
            for data in PROJECTS:
                city = data.pop("city", "Boston")
                # Build synthetic URL for non-BPDA or for address matching
                slug = make_slug(f"{data['address']}-{city}")
                manual_url = f"manual:{slug}"

                # Check by address similarity first (covers already-in-DB BPDA projects)
                addr_lower = data["address"].lower().split(",")[0].strip()
                existing = None
                for p in session.query(Project).all():
                    if p.address and p.address.lower().split(",")[0].strip() == addr_lower:
                        existing = p
                        break

                if not existing:
                    # Double-check by manual URL
                    existing = session.query(Project).filter_by(bpda_url=manual_url).first()

                if existing:
                    log.info("UPDATE  %s  (%s)", data["name"], existing.address)
                    # Only patch fields that are null or clearly worse
                    for field in (
                        "name", "neighborhood", "status",
                        "developer", "developer_canonical",
                        "asset_class", "total_gsf", "residential_units", "commercial_gsf",
                        "building_height_ft", "num_stories", "parking_spaces",
                        "architect", "civil_engineer", "expected_delivery", "description",
                    ):
                        val = data.get(field)
                        if val is not None and getattr(existing, field) is None:
                            setattr(existing, field, val)
                    # Always update status if we have a newer value
                    if data.get("status"):
                        existing.status = data["status"]
                    # Update developer_canonical if we have a better one
                    if data.get("developer_canonical") and not is_real_company(existing.developer_canonical or ""):
                        existing.developer_canonical = data["developer_canonical"]
                    # Geocode if missing
                    if not existing.latitude:
                        coords = geocode_address(http, data["address"], city)
                        if coords:
                            existing.latitude, existing.longitude = coords
                            log.info("  Geocoded: %.5f, %.5f", *coords)
                        time.sleep(0.4)
                    updated += 1
                else:
                    log.info("INSERT  %s  (%s, %s)", data["name"], data["address"], city)
                    proj = Project(
                        bpda_url=manual_url,
                        name=data["name"],
                        address=data["address"],
                        city=city,
                        neighborhood=data.get("neighborhood"),
                        status=data.get("status"),
                        developer=data.get("developer"),
                        developer_canonical=data.get("developer_canonical"),
                        asset_class=data.get("asset_class"),
                        total_gsf=data.get("total_gsf"),
                        residential_units=data.get("residential_units"),
                        commercial_gsf=data.get("commercial_gsf"),
                        building_height_ft=data.get("building_height_ft"),
                        num_stories=data.get("num_stories"),
                        parking_spaces=data.get("parking_spaces"),
                        architect=data.get("architect"),
                        civil_engineer=data.get("civil_engineer"),
                        expected_delivery=data.get("expected_delivery"),
                        description=data.get("description"),
                        extraction_model="manual",
                        extraction_timestamp=datetime.now(timezone.utc),
                        first_seen_date=datetime.now(timezone.utc),
                    )
                    # Geocode
                    coords = geocode_address(http, data["address"], city)
                    if coords:
                        proj.latitude, proj.longitude = coords
                        log.info("  Geocoded: %.5f, %.5f", *coords)
                    time.sleep(0.4)
                    session.add(proj)
                    added += 1

                session.commit()

        log.info("\n=== Done ===  Added: %d  Updated: %d  Skipped: %d", added, updated, skipped)

        total = session.query(Project).count()
        log.info("Total projects in DB: %d", total)

    except Exception as exc:
        session.rollback()
        log.error("Fatal error: %s", exc)
        raise
    finally:
        session.close()


if __name__ == "__main__":
    run()
