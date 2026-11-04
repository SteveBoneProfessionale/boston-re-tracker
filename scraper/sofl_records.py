"""Load the research-sourced South Florida projects into the tracker.

City of Miami has a bulk permit feed and is ingested by scraper/sofl_miami.py.
The other four municipalities do not: Miami Beach publishes its Land
Development Dashboard only as a Power BI embed, West Palm Beach has no bulk
dataset at all, and Coral Gables and Doral publish board agendas. For those,
a project's facts come from the city's own published lists where they are
machine-readable and otherwise from named trade press, and each project is
recorded here as a reviewed record with a source URL per value.

WHY THE RECORDS ARE DATA FILES AND NOT A SCRAPER
Because what varies between these four cities is not a format, it is a source.
Writing a parser per city -- an agenda-PDF reader for West Palm Beach, a
Power BI query client for Miami Beach, an HTML scraper for Coral Gables -- is
four parsers to build and then maintain, and the brief rules that out
explicitly. The records live in data/sofl/<city>.json, every value carries the
URL it came from, and re-running this module re-applies them.

WHAT THIS MODULE ENFORCES, RATHER THAN TRUSTS
  * the three tests, at intake -- a record that fails is reported and dropped
  * that Under Construction and Complete rest on construction/completion
    evidence, never on a planning document (sofl_common.stage_from_evidence)
  * that every filled value has a source URL (sofl_common.record_sources)
  * that a project is keyed on its parcel identifier and deduplicated on it
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.database import get_session, init_db       # noqa: E402
from db.models import Project, ExtractionSource    # noqa: E402
from scraper import sofl_common as sc              # noqa: E402

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "data", "sofl")

MD_PARCELS = ("https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/"
              "services/PaParcelView_gdb/FeatureServer/0")
PBC_PARCELS = ("https://services1.arcgis.com/ZWOoUZbtaYePLlPw/arcgis/rest/"
               "services/Parcels_and_Property_Details_WebMercator/FeatureServer/0")

# Neighborhood polygon layers, per city. Coral Gables and Doral publish none,
# and the rule for those is the source document's own area name or nothing --
# so they are absent here deliberately rather than by omission.
NEIGHBORHOOD_LAYERS = {
    "Miami Beach": (("https://gis.miamibeachfl.gov/public/rest/services/gc/"
                     "gc_Cadastral/MapServer/12"), "NAME"),
    "West Palm Beach": (("https://wpbgisportal.wpb.org/server/rest/services/"
                         "MayorOffice/WPB_Neighborhoods/FeatureServer/0"), "Name"),
    "Miami": (("https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/"
               "services/Miami_Neighborhoods_Shapefile/FeatureServer/0"), "LABEL"),
}

# Fields a record may fill, and the column each writes to. Anything not listed
# cannot be set from a record file -- so a typo in a record is a reported error
# rather than a silently ignored value.
FIELD_COLUMNS = {
    "developer": "developer",
    "architect": "architect",
    "contractor": "general_contractor",
    "civil_engineer": "civil_engineer",
    "lender": "lender",
    "residential_units": "residential_units",
    "total_gsf": "total_gsf",
    "commercial_gsf": "commercial_gsf",
    "height_ft": "building_height_ft",
    "stories": "num_stories",
    "asset_class": "asset_class",
    "neighborhood": "neighborhood",
    "status": "status",
    "owner_or_agency": "owner_or_agency",
    "hotel_rooms": "hotel_rooms",
    "parking_spaces": "parking_spaces",
}


def _norm_parcel(city: str, value) -> str:
    return sc.norm_pcn(value) if city == "West Palm Beach" else sc.norm_folio(value)


def load_records(city: str) -> list[dict]:
    path = os.path.join(DATA_DIR, city.lower().replace(" ", "_") + ".json")
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def check_record(rec: dict) -> str:
    """"" if the record is in scope, else the reason it is not.

    The three tests are asserted per record rather than inferred from text:
    each record states what its source says it is, and a record that cannot
    make all three claims is not a project for this tracker.
    """
    tests = rec.get("tests") or {}
    failed = sc.passes_three_tests(
        changes_building=bool(tests.get("changes_building")),
        commercial=bool(tests.get("commercial")),
        has_program=bool(tests.get("has_program")),
    )
    if failed:
        return failed
    if not rec.get("name") or not rec.get("address"):
        return "incomplete_record_no_name_or_address"
    stage = rec.get("status")
    if stage not in sc.PLANNING_REACHABLE | {sc.STAGE_UNDER_CONSTRUCTION,
                                             sc.STAGE_COMPLETE}:
        return f"bad_stage:{stage}"
    # The rule Rhode Island skipped. A record claiming construction or
    # completion must name the evidence, and the evidence must be of a kind
    # that can establish it.
    if stage in (sc.STAGE_UNDER_CONSTRUCTION, sc.STAGE_COMPLETE):
        proof = (rec.get("stage_evidence") or {}).get("kind", "")
        if stage == sc.STAGE_COMPLETE and not sc.completion_proof(proof):
            return f"completion_not_proven:{proof or 'none'}"
        if stage == sc.STAGE_UNDER_CONSTRUCTION and not (
                sc.construction_proof(proof) or sc.completion_proof(proof)):
            return f"construction_not_proven:{proof or 'none'}"
    for field in (rec.get("values") or {}):
        if field not in FIELD_COLUMNS:
            return f"unknown_field:{field}"
    return ""


def _resolve_parcels(city, records, sess, verbose=True):
    """Parcel identifier and owner of record per project, from the county.

    Only called for records that did not already state one. Miami-Dade is
    queried by its own address string; Palm Beach by street number and name,
    because its address field is not indexed and a LIKE over it does not
    return inside a request timeout.
    """
    out = {}
    todo = [r for r in records if not r.get("parcel_id") and r.get("address")]
    if verbose:
        print(f"  resolving parcels for {len(todo)} record(s) ...")
    for rec in todo:
        addr = rec["address"]
        try:
            if city == "West Palm Beach":
                num, street = _split_address(addr)
                if not num or not street:
                    continue
                rows = sc.arcgis_query(
                    PBC_PARCELS,
                    where=(f"STREET_NUMBER={num} AND STREET_NAME='{street}' "
                           f"AND MUNICIPALITY='WEST PALM BEACH'"),
                    out_fields="PARID,PARCEL_NUMBER,SITE_ADDR_STR,OWNER_NAME1",
                    sess=sess, page=50, geometry=True)
                if rows:
                    lon, lat = _centroid(rows[0])
                    out[addr] = {"parcel_id": sc.norm_pcn(rows[0].get("PARCEL_NUMBER")
                                                          or rows[0].get("PARID")),
                                 "owner": (rows[0].get("OWNER_NAME1") or "").strip(),
                                 "lon": lon, "lat": lat,
                                 "source": PBC_PARCELS}
            else:
                rows = sc.arcgis_query(
                    MD_PARCELS,
                    where=f"TRUE_SITE_ADDR LIKE '{_md_addr(addr)}%'",
                    out_fields="FOLIO,TRUE_SITE_ADDR,TRUE_OWNER1",
                    sess=sess, page=50, geometry=True)
                if rows:
                    lon, lat = _centroid(rows[0])
                    out[addr] = {"parcel_id": sc.norm_folio(rows[0].get("FOLIO")),
                                 "owner": (rows[0].get("TRUE_OWNER1") or "").strip(),
                                 "lon": lon, "lat": lat,
                                 "source": MD_PARCELS}
        except RuntimeError as exc:
            if verbose:
                print(f"    {addr}: parcel lookup failed ({exc})")
    if verbose:
        print(f"  {len(out)} parcel(s) resolved")
    return out


def _centroid(row):
    """Mean vertex of a parcel polygon, in lon/lat.

    Good enough to place a pin and to pick a neighborhood polygon, and it is
    the parcel the project sits on rather than a geocoder's guess at the
    street address -- which is what matters when the marketed address and the
    address of record differ, as they routinely do here.
    """
    rings = ((row.get("_geometry") or {}).get("rings")) or []
    pts = [p for r in rings for p in r]
    if not pts:
        return None, None
    return (sum(p[0] for p in pts) / len(pts), sum(p[1] for p in pts) / len(pts))


_SUFFIXES = {"AVE", "AV", "AVENUE", "ST", "STREET", "RD", "ROAD", "DR", "DRIVE",
             "BLVD", "BOULEVARD", "TER", "TERRACE", "CT", "COURT", "PL", "PLACE",
             "LN", "LANE", "CIR", "CIRCLE", "WAY", "HWY", "HIGHWAY", "PKWY"}
_DIRS = {"N", "S", "E", "W", "NE", "NW", "SE", "SW", "NORTH", "SOUTH", "EAST", "WEST"}


def _split_address(addr: str):
    """(street number, bare street name) for the Palm Beach parcel query.

    Palm Beach stores the direction and the suffix in their own columns, so
    "1865 North Flagler Drive" has to be reduced to 1865 and FLAGLER before it
    will match anything.
    """
    parts = str(addr).upper().replace(",", " ").split()
    if not parts or not parts[0].isdigit():
        return None, None
    num = parts[0]
    rest = [p for p in parts[1:] if p not in _DIRS and p not in _SUFFIXES]
    return (num, rest[0]) if rest else (num, None)


def _md_addr(addr: str) -> str:
    """Miami-Dade's address spelling: number, direction, name, short suffix."""
    parts = str(addr).upper().replace(",", " ").split()
    long_to_short = {"AVENUE": "AVE", "STREET": "ST", "ROAD": "RD", "DRIVE": "DR",
                     "BOULEVARD": "BLVD", "TERRACE": "TER", "COURT": "CT",
                     "PLACE": "PL", "NORTH": "N", "SOUTH": "S", "EAST": "E",
                     "WEST": "W", "HIGHWAY": "HWY"}
    out = [long_to_short.get(p, p) for p in parts]
    # Cut at the first token the county would not have (a unit, a city name).
    while out and out[-1] in ("FL", "FLORIDA"):
        out.pop()
    return " ".join(out).replace("'", "''")


def ingest(city: str, verbose=True):
    init_db()
    records = load_records(city)
    sess = sc.session()

    accepted, rejected = [], []
    for rec in records:
        reason = check_record(rec)
        (rejected if reason else accepted).append(
            {"reason": reason, "name": rec.get("name"), "address": rec.get("address")}
            if reason else rec)
    if verbose:
        print(f"{city}: {len(accepted)} in scope, {len(rejected)} dropped at intake")
        for r in rejected:
            print(f"   dropped: {r['name']} ({r['address']}) -- {r['reason']}")

    nbhd = None
    layer = NEIGHBORHOOD_LAYERS.get(city)
    if layer:
        try:
            feats = sc.arcgis_query(layer[0], out_fields="*", geometry=True, sess=sess)
            nbhd = sc.PolygonIndex(feats, layer[1])
            if verbose:
                print(f"  {len(nbhd)} neighborhood polygons for {city}")
        except RuntimeError as exc:
            if verbose:
                print(f"  neighborhood layer unavailable: {exc}")

    parcels = _resolve_parcels(city, accepted, sess, verbose=verbose)

    db = get_session()
    written = updated = 0
    try:
        for rec in accepted:
            if _upsert(db, city, rec, nbhd, parcels):
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


def _upsert(db, city, rec, nbhd, parcels) -> bool:
    values = dict(rec.get("values") or {})
    sources = rec.get("sources") or {}

    resolved = parcels.get(rec.get("address") or "", {})
    parcel_id = _norm_parcel(city, rec.get("parcel_id") or resolved.get("parcel_id"))
    aliases = [_norm_parcel(city, a) for a in (rec.get("parcel_id_aliases") or [])]
    aliases = [a for a in aliases if a and a != parcel_id]

    key = f"sofl://{city.lower().replace(' ', '-')}/" + (
        parcel_id or rec["name"].lower().replace(" ", "-"))

    row = None
    if parcel_id:
        row = db.query(Project).filter(Project.parcel_id == parcel_id,
                                       Project.city == city).one_or_none()
    if row is None:
        row = db.query(Project).filter(Project.bpda_url == key).one_or_none()
    # This loader owns every field of the records it writes and clears what a
    # record omits, which would delete the contractor, permitted area, cost and
    # certificate that a City of Miami permit row carries. A record that lands
    # on a permit row is a mistake in the record file, not something to apply:
    # that project belongs in data/sofl/miami_overlay.json, which adds to a
    # permit row instead of replacing it.
    if row is not None and row.permit_number:
        raise ValueError(
            f"{rec['name']}: parcel {parcel_id or key} already holds the permit-derived "
            f"row {row.name!r} (permit {row.permit_number}). Move this project to "
            f"data/sofl/miami_overlay.json so the permit fields survive.")
    is_new = row is None
    if is_new:
        row = Project(bpda_url=key)
        db.add(row)

    row.city = city
    row.name = rec["name"]
    row.address = rec["address"]
    row.alt_addresses = ", ".join(rec.get("alt_addresses") or []) or None
    row.parcel_id = parcel_id or None
    row.parcel_id_aliases = ",".join(aliases) or None
    row.status = rec["status"]
    row.description = rec.get("description") or None
    row.notes = rec.get("notes") or None
    row.requires_extraction = False
    row.excluded = False
    row.out_of_scope = False
    row.entry_type = "development"
    row.filing_type = rec.get("filing_type") or None
    row.review_scale = None
    row.review_scale_basis = "not_applicable"
    row.asset_class_raw = rec.get("asset_class_raw") or None

    # Dead is the existing withdrawn/denied field, not a sixth stage: the row
    # keeps the stage it died at and drops out of pipeline totals through the
    # filter Boston's withdrawn projects already use.
    row.project_status_filing = rec.get("dead") or None

    # Delivery and construction evidence. completion_stage is what makes the
    # data layer override a filing status, so it is set only from evidence that
    # can establish it -- check_record has already refused anything else.
    ev = rec.get("stage_evidence") or {}
    if rec["status"] == sc.STAGE_COMPLETE:
        row.completion_stage = "Complete"
        row.completion_basis = ev.get("basis") or "news_confirmed"
        row.completion_evidence = ev.get("note") or ev.get("kind") or None
        row.completion_source_url = ev.get("url") or None
        row.completion_date = ev.get("date") or None
        if ev.get("date"):
            # The date is stored at the precision its source actually had, so
            # "June 2026" renders as June 2026 rather than as a false 1 June.
            from scraper.delivery_dates import parse_date_phrase
            parsed = parse_date_phrase(ev["date"])
            if parsed:
                row.delivered_date, row.delivered_precision = parsed[0], parsed[1]
    elif rec["status"] == sc.STAGE_UNDER_CONSTRUCTION:
        row.completion_stage = "Under Construction"
        row.completion_basis = ev.get("basis") or "news_confirmed"
        row.completion_evidence = ev.get("note") or ev.get("kind") or None
        row.completion_source_url = ev.get("url") or None
        row.completion_date = None
        row.delivered_date = None
        row.delivered_precision = None
    else:
        row.completion_stage = None
        row.completion_basis = None
        row.completion_evidence = None
        row.completion_source_url = None
        row.completion_date = None
        row.delivered_date = None
        row.delivered_precision = None

    # An owner of record recovered from the county is written only where the
    # record did not already name one, and never into `developer`: a
    # single-purpose LLC is who holds the land, not who is building.
    if resolved.get("owner") and "owner_or_agency" not in values:
        values["owner_or_agency"] = resolved["owner"]
        sources.setdefault("owner_or_agency", {
            "name": f"{'Palm Beach County' if city == 'West Palm Beach' else 'Miami-Dade'} "
                    f"property appraiser parcel record",
            "url": resolved["source"],
        })

    # Coordinates come from the parcel the project sits on, which is both the
    # map position and the point the neighborhood join is made at.
    lat = rec.get("latitude") or resolved.get("lat")
    lon = rec.get("longitude") or resolved.get("lon")

    # Neighborhood: a polygon where the city publishes one, otherwise the area
    # name the source document itself used. Never a guess.
    if nbhd and lat and lon:
        found = nbhd.name_at(lon, lat)
        if found:
            values["neighborhood"] = found
            sources.setdefault("neighborhood", {
                "name": f"{city} neighborhoods layer", "url": NEIGHBORHOOD_LAYERS[city][0]})

    row.latitude = lat
    row.longitude = lon
    row.coords_approximate = bool(rec.get("coords_approximate"))

    # Clear the columns a record governs before writing, so a value removed
    # from a record file is removed from the row rather than left behind.
    #
    # status, neighborhood, parcel_id and the aliases are set explicitly above
    # and must NOT be cleared here. parcel_id used to be in FIELD_COLUMNS and
    # so was nulled a few lines after being resolved, which silently emptied
    # the column the whole South Florida dedupe keys on -- every record-loaded
    # project came out with no folio or PCN at all.
    _keep = {"status", "neighborhood", "parcel_id", "parcel_id_aliases"}
    for col in set(FIELD_COLUMNS.values()) - _keep:
        setattr(row, col, None)
    row.neighborhood = ""
    row.total_gsf_source = None

    for field, value in values.items():
        if value in (None, "", "not found"):
            continue
        setattr(row, FIELD_COLUMNS[field], value)
    if values.get("total_gsf"):
        row.total_gsf_source = (rec.get("total_gsf_source") or "web")

    db.flush()

    # Provenance: one citation per filled value. A value whose record names no
    # source is an error, not a value -- record_sources refuses it.
    db.query(ExtractionSource).filter(
        ExtractionSource.project_id == row.id).delete(synchronize_session=False)
    # A record may name one "_default" source -- the article the project was
    # read from -- and override it per field where a value came from a
    # different piece. Every value still resolves to exactly one URL; the
    # default only removes the need to repeat it eight times for a project
    # whose whole programme was published in one place.
    missing = [f for f, v in values.items()
               if v not in (None, "", "not found")
               and not (sources.get(f) or sources.get("_default") or {}).get("url")]
    if missing:
        raise ValueError(f"{rec['name']}: no source URL for {missing}")
    for field, value in values.items():
        if value in (None, "", "not found"):
            continue
        src = sources.get(field) or sources["_default"]
        sc.record_sources(db, row.id, {FIELD_COLUMNS[field]: value},
                          src.get("name") or "", src["url"], src.get("date") or "")
    return is_new


if __name__ == "__main__":
    for _city in sys.argv[1:]:
        ingest(_city)
