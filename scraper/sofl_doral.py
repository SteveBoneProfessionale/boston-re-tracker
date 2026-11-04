"""Doral ingest, from the City's own Development Projects layer.

Doral publishes its development projects as a GIS layer behind the City's
"Development Projects" web experience -- name, folio, address, status,
development type and a description that states the programme. That is the
City's own record of what it has approved, so it is used as the master list in
place of reading Council agendas item by item, and Florida YIMBY's Doral
coverage is overlaid on it (data/sofl/doral.json) for the fields a City record
does not hold: developer, architect, contractor, lender, height, and whether
construction has actually started.

Doral's open-permit search is a paid per-address request and is not used.

WHY THE DECISION IS A TABLE AND NOT A RULE
The first version of this module decided the three tests from the description
text with keyword rules, and the review of its output found eleven wrong calls
in 75 records -- a 5,812 SF addition dropped because the rule wanted "sq ft"
and the City wrote "SF", a 552-unit project dropped because the rule wanted
"residential units" and the City wrote "units", a mixed-use tower filed as
Office because the word "office" appeared before the word "residential".
Fixing those one at a time is the twenty-parser spiral this ingest is not
allowed to have.

The list is 75 records long and every description was read, so the decision is
pinned below instead: DECISIONS names each project that is in scope, with the
asset class and the programme figures the City's own description states, and
EXCLUSIONS names each one that is not, with the test it fails. A name that is
in neither list is reported as unreviewed rather than guessed at, which is
what happens when the City adds a project.
"""

import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.database import get_session, init_db       # noqa: E402
from db.models import Project, ExtractionSource    # noqa: E402
from scraper import sofl_common as sc              # noqa: E402

CITY = "Doral"

PROJECTS_LAYER = ("https://utility.arcgis.com/usrsvcs/servers/"
                  "de2b0ed6d0cd49039f788289e5a10efb/rest/services/Planning/"
                  "DevelopmentProjects/MapServer/0")
EXPERIENCE_PAGE = ("https://experience.arcgis.com/experience/"
                   "f53641e40f6b40d88fa7721bf5ca38cb/")

# The City's status vocabulary mapped onto the canonical stages. This layer
# records what the City has ACTED on, which can establish Planning,
# Permitting or Approved and nothing beyond -- a site-plan approval is not a
# construction start. Under Construction and Complete for a Doral project
# come only from the press overlay.
STATUS_MAP = {
    "Approved": sc.STAGE_APPROVED,
    "Under Review": sc.STAGE_PERMITTING,
    "Denied": sc.STAGE_PLANNING,
    "Withdrawn": sc.STAGE_PLANNING,
}
DEAD_STATUS = {"Denied": sc.DEAD_DENIED, "Withdrawn": sc.DEAD_WITHDRAWN}

# name -> (asset class, residential units or None, hotel rooms or None)
#
# Units are the figure the City's own description states. Where it states none,
# the field stays blank rather than being inferred from a floor area.
DECISIONS = {
    "5250 Condo Tower Phase 2":                     ("Condo", 228, None),
    "Airways Auto Tag":                             ("Retail", None, None),
    "Atrium":                                       ("Retail", None, None),
    "Baptist Health South Florida":                 ("Institutional", None, None),
    "Bridge BP Doral 826 FL, LLC":                  ("Industrial", None, None),
    "Bridge Point Doral Distribution Center":       ("Industrial", None, None),
    "Chick-Fil-A East":                             ("Retail", None, None),
    "Chick-fil-A":                                  ("Retail", None, None),
    "Collection Suites":                            ("Other", None, None),
    "District 79":                                  ("Other", None, None),
    "Divine Savior":                                ("Institutional", None, None),
    "Doral Central Park":                           ("Institutional", None, None),
    "Doral City Place 7":                           ("Residential", 391, None),
    "Doral Cultural Arts Center":                   ("Institutional", None, None),
    "Doral Gateway Garage":                         ("Parking", None, None),
    "Doral Inn/ IMF Doral Residential Conversion Live L": ("Residential", None, None),
    "Doral International Towers (Trump)":           ("Mixed-Use", 1498, None),
    "Doral Marketplace (Bridge Point Retail)":      ("Retail", None, None),
    "Doral Outparcel":                              ("Retail", None, None),
    "Doral Square Expansion":                       ("Retail", None, None),
    "Downtown Doral Parking Garages & Retail Expansion": ("Mixed-Use", None, None),
    "El Car Wash":                                  ("Retail", None, None),
    "Farmasi #2":                                   ("Industrial", None, None),
    "Farmasi USA Storage & Distribution Center":    ("Industrial", None, None),
    "Flightway Ten":                                ("Industrial", None, None),
    "Foundry Transal":                              ("Other", None, None),
    "Healthpeak/ Medical Office":                   ("Office", None, None),
    "Hyatt Hotel":                                  ("Hotel", None, 124),
    "Hyundai Remodel":                              ("Retail", None, None),
    "ICP @ 97 Ave BUILDING H":                      ("Industrial", None, None),
    "Kelly Tractor":                                ("Industrial", None, None),
    "Legacy at Doral (EWE Warehouse)":              ("Residential", 185, None),
    "Lehman Doral (Kia & Subaru Dealership":        ("Retail", None, None),
    "MDPLS Doral Branch Library":                   ("Institutional", None, None),
    "MG3 Doral School":                             ("Institutional", None, None),
    "Mi Tierra":                                    ("Office", None, None),
    "Midtown IV V VI":                              ("Mixed-Use", 552, None),
    "Midtown PUD Phase II Modification":            ("Mixed-Use", 734, None),
    "Mission BBQ":                                  ("Retail", None, None),
    "Nazari Mobil":                                 ("Retail", None, None),
    "Oasis at Doral MixedUse Residential Live Local Act": ("Mixed-Use", 623, None),
    "Ocean Bank":                                   ("Retail", None, None),
    "Orion Sixto Gas Station":                      ("Retail", None, None),
    "PDC Doral Panattoni Development":              ("Industrial", None, None),
    "Raising Cane's / ABC Liquors Inc.":            ("Retail", None, None),
    "Sevilla at Downtown Doral":                    ("Residential", 412, None),
    "Shelton Academy":                              ("Institutional", None, None),
    "Shoppes @ 97":                                 ("Retail", None, None),
    "SmartStop Expansion":                          ("Industrial", None, None),
    "Subaru":                                       ("Retail", None, None),
    "Twin Peaks Doral":                             ("Retail", None, None),
    "University of Miami UHealth":                  ("Institutional", None, None),
    "Walmart Modification":                         ("Retail", None, None),
    "ZOM / Westside Live Local Act":                ("Mixed-Use", 380, None),
}

# name -> the reason it is not a project for this tracker.
EXCLUSIONS = {
    "ALS San Marco FHSLA Charter School":
        "test1_no_building_change: an existing 11,921 sq ft building; no construction stated",
    "BP Plaza": "test1_no_building_change: urban plazas",
    "Downtown Doral Entrance": "test1_no_building_change: NW 87th Avenue improvements",
    "Downtown Doral Paseo and Pocket Park": "test3_no_stated_program: no description",
    "Hub Lobby (Doral Yard)":
        "test1_no_building_change: outdoor portion of a restaurant, dining and food stations",
    "LMC Doral / Downtown Doral South & White Course":
        "test1_no_building_change: a pattern-book modification adding a roadway section",
    "Lithia Modifications":
        "test1_no_building_change: reduces showroom floor area and adds two parking spaces; "
        "a later modification of the dealership project at 10155 NW 12 St",
    "Kia Subaru":
        "duplicate: the same dealership project at 10155 NW 12 St recorded under "
        "Lehman Doral (Kia & Subaru Dealership)",
    "Marmol Export USA S&S International Corporation Tr":
        "test1_no_building_change: storage area and truck trailer parking",
    "Neovita (Doral Palms North)": "test1_no_building_change: playground in a green area",
    "Pickleball Yard @ Downtown Doral":
        "test1_no_building_change: four pickleball and four padel courts with a check-in booth",
    "Pollo Campero": "test3_no_stated_program: no description",
    "Smash Padel Courts": "test1_no_building_change: padel courts",
    "TA Realty Parking Reconfiguration @ 1910 NW 97 AVE":
        "test1_no_building_change: parking capacity increased from 74 to 121 spaces",
    "Tesla Motors":
        "test1_no_building_change: service and sales use in an existing single-storey building",
    "The Future Begins Here Daycare":
        "test1_no_building_change: a new daycare use inside an existing 20,000 SF building",
    "White Course Park":
        "test1_no_building_change: 130,728 sq ft of park; no building programme stated",
    "Site Plan for 10405 NW 19 ST":
        "duplicate: the same 40,141 sq ft building at 10405 NW 19 St recorded under Farmasi #2",
    "Century Towne Center / Midtown Phase II":
        "superseded: an earlier programme for Midtown PUD Phase II (675 units), recorded "
        "under Midtown PUD Phase II Modification (734 units)",
    "Midtown Doral PUD / Century Towne Center":
        "superseded: an earlier programme for Midtown PUD Phase II (675 units), recorded "
        "under Midtown PUD Phase II Modification (734 units)",
}


def classify(rec) -> tuple[str, dict]:
    """("" and details) if in scope, else (the reason it is not, details)."""
    name = re.sub(r"\s+", " ", (rec.get("development_name") or "")).strip()
    status = (rec.get("status") or "").strip()
    info = {"name": name, "status": status,
            "address": (rec.get("address") or "").strip(),
            "description": re.sub(r"\s+", " ", rec.get("description") or "").strip()}
    if not name:
        return "incomplete_record_no_name", info
    if name in EXCLUSIONS:
        return EXCLUSIONS[name], info
    if name not in DECISIONS:
        # Not a guess and not a silent drop: the City has added a project and
        # nobody has read it yet.
        return "unreviewed_new_city_record", info
    if status not in STATUS_MAP:
        return f"unmapped_status:{status or 'blank'}", info
    info["asset_class"], info["units"], info["hotel_rooms"] = DECISIONS[name]
    return "", info


def _folios(rec) -> tuple[str, list[str]]:
    """The folio the project is keyed on, plus the rest of an assembled site.

    Doral writes an assembled site as one string in its own shorthand, and the
    notation has to be read or the aliases come out as nonsense. The field
    holds, across the 75 records: "/" ",", ";", "&" and a newline as
    separators; "Portion of" before a partial take; and -- the part that
    matters -- an abbreviated continuation that gives only the digits that
    change, so "35-3008-000-0041, -0048, -0051" is three parcels and
    "35-3027-053-0010 ; -058-0010" is two.

    Read naively, those concatenate into a single 39-digit string that matches
    no parcel and silently breaks the dedupe. So a part shorter than a full
    folio is expanded against the preceding full one by replacing that many
    trailing digits, and anything that still will not reach 13 digits is
    dropped rather than stored wrong. A portion is never the key.
    """
    raw = rec.get("folio") or ""
    parts = [p.strip() for p in re.split(r"[/,;&\r\n]+", raw) if p.strip()]
    whole, partial = [], []
    base = ""
    for p in parts:
        is_portion = p.lower().startswith("portion")
        digits = re.sub(r"\D", "", p)
        if not digits:
            continue
        if len(digits) >= 13:
            norm = sc.norm_folio(digits)
            base = norm or base
        elif base:
            # An abbreviated continuation: the digits given replace the same
            # number of trailing digits on the parcel before it.
            norm = base[: 13 - len(digits)] + digits
        else:
            continue
        if not norm or len(norm) != 13:
            continue
        (partial if is_portion else whole).append(norm)
    whole += partial
    seen, ordered = set(), []
    for f in whole:
        if f not in seen:
            seen.add(f)
            ordered.append(f)
    return (ordered[0] if ordered else ""), ordered[1:]


def _centroid(rec):
    rings = ((rec.get("_geometry") or {}).get("rings")) or []
    pts = [p for r in rings for p in r]
    if not pts:
        return None, None
    return (sum(p[0] for p in pts) / len(pts), sum(p[1] for p in pts) / len(pts))


def fetch(sess=None):
    return sc.arcgis_query(PROJECTS_LAYER, out_fields="*", geometry=True,
                           sess=sess or sc.session())


def report(records=None):
    """Print the in/out decision for every record, which is the review."""
    for rec in sorted(records if records is not None else fetch(),
                      key=lambda r: (r.get("development_name") or "")):
        reason, info = classify(rec)
        print("%-4s %-50s %-13s %s" % (
            "OUT" if reason else "IN", info["name"][:50],
            info.get("asset_class", "")[:13], reason or info["description"][:70]))


def ingest(verbose=True):
    init_db()
    sess = sc.session()
    records = fetch(sess)
    if verbose:
        print(f"{CITY}: {len(records)} records in the City's layer")

    accepted, rejected = [], []
    for rec in records:
        reason, info = classify(rec)
        if reason:
            rejected.append({"reason": reason, **info})
        else:
            rec["_decided"] = info
            accepted.append(rec)
    if verbose:
        print(f"  {len(accepted)} in scope, {len(rejected)} dropped at intake")
        unreviewed = [r for r in rejected if r["reason"] == "unreviewed_new_city_record"]
        if unreviewed:
            print(f"  ⚠ {len(unreviewed)} record(s) the City has added since the review: "
                  + ", ".join(r["name"] for r in unreviewed))

    db = get_session()
    written = updated = 0
    try:
        for rec in accepted:
            if _upsert(db, rec):
                written += 1
            else:
                updated += 1
        db.commit()
    finally:
        db.close()
    if verbose:
        print(f"  wrote {written} new, updated {updated}")
    return {"accepted": accepted, "rejected": rejected,
            "written": written, "updated": updated}


def _upsert(db, rec) -> bool:
    decided = rec["_decided"]
    folio, aliases = _folios(rec)
    name = decided["name"]
    key = f"sofl://doral/{folio or name.lower().replace(' ', '-')}"

    row = None
    if folio:
        row = db.query(Project).filter(Project.parcel_id == folio,
                                       Project.city == CITY).one_or_none()
    if row is None:
        row = db.query(Project).filter(Project.bpda_url == key).one_or_none()
    is_new = row is None
    if is_new:
        row = Project(bpda_url=key)
        db.add(row)

    status = decided["status"]
    lon, lat = _centroid(rec)

    row.city = CITY
    row.name = name
    row.address = decided["address"] or "address not stated"
    row.parcel_id = folio or None
    row.parcel_id_aliases = ",".join(aliases) or None
    row.status = STATUS_MAP[status]
    row.project_status_filing = DEAD_STATUS.get(status)
    row.asset_class = decided["asset_class"]
    row.asset_class_raw = (rec.get("development_type") or "").strip() or None
    row.residential_units = decided["units"]
    row.hotel_rooms = decided["hotel_rooms"]
    row.description = decided["description"] or None
    row.filing_type = "City of Doral site plan"
    row.requires_extraction = False
    row.excluded = False
    row.out_of_scope = False
    row.entry_type = "development"
    row.review_scale = None
    row.review_scale_basis = "not_applicable"
    row.latitude = lat
    row.longitude = lon
    row.coords_approximate = False
    # Doral publishes no neighborhood layer, so the rule is the source
    # document's own area name or nothing -- and the City's layer states none.
    row.neighborhood = ""
    row.completion_stage = None
    row.completion_basis = None

    db.flush()
    db.query(ExtractionSource).filter(
        ExtractionSource.project_id == row.id,
        ExtractionSource.filing_name.like("City of Doral Development Projects%"),
    ).delete(synchronize_session=False)
    sc.record_sources(
        db, row.id,
        {"status": row.status, "asset_class": row.asset_class,
         "residential_units": row.residential_units,
         "hotel_rooms": row.hotel_rooms,
         "parcel_id": row.parcel_id, "address": row.address},
        "City of Doral Development Projects layer",
        EXPERIENCE_PAGE,
    )
    return is_new


if __name__ == "__main__":
    if "--report" in sys.argv:
        report()
    else:
        ingest()
