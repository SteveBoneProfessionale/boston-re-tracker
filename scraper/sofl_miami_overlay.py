"""Name the City of Miami permit rows and fill the fields a permit has not got.

scraper/sofl_miami.py builds Miami's pipeline from the City's permit feed, which
gives the contractor, the folio, the permitted area, the cost, the use, the
status and the certificate of occupancy -- and no project name, no unit count,
no building height, no developer and no architect, because there are no such
fields on a permit.

This module fills those from named trade press, and it ADDS to a permit row
rather than replacing it. That distinction is the reason it is a separate
module from scraper/sofl_records.py: that loader owns every field of the
records it writes and clears what a record omits, which is right for a project
whose only source is the press and would be wrong here -- running it over a
Miami row would delete the contractor and the permitted area that the City
published.

A record matches a permit row by address, which is the City's own address of
record, with the folio accepted as an alternative where the two sources write
the address differently. A record that matches nothing is reported rather than
inserted: a project with no permit belongs in data/sofl/miami.json, which
scraper/sofl_records.py loads.
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.database import get_session, init_db       # noqa: E402
from db.models import Project, ExtractionSource    # noqa: E402
from scraper import sofl_common as sc              # noqa: E402

CITY = "Miami"
RECORDS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "data", "sofl", "miami_overlay.json")

# Only these columns may be overlaid. Everything the permit feed populates --
# the contractor, the permitted area, the cost, the status, the certificate --
# is deliberately absent, so an overlay cannot overwrite a government record
# with a press report.
OVERLAY_COLUMNS = {
    "developer": "developer",
    "architect": "architect",
    "lender": "lender",
    "residential_units": "residential_units",
    "hotel_rooms": "hotel_rooms",
    "height_ft": "building_height_ft",
    "stories": "num_stories",
    "commercial_gsf": "commercial_gsf",
    "parking_spaces": "parking_spaces",
    "civil_engineer": "civil_engineer",
}


def load() -> list[dict]:
    with open(RECORDS, encoding="utf-8") as fh:
        return json.load(fh)


def ingest(verbose=True):
    init_db()
    records = load()
    db = get_session()
    matched = unmatched = 0
    try:
        for rec in records:
            row = _find(db, rec)
            if row is None:
                unmatched += 1
                if verbose:
                    print(f"  unmatched (no Miami permit row): {rec['name']} "
                          f"-- {rec.get('match_address') or rec.get('match_parcel')}")
                continue
            _apply(db, row, rec)
            matched += 1
        db.commit()
    finally:
        db.close()
    if verbose:
        print(f"{CITY} overlay: {matched} permit row(s) named and filled, "
              f"{unmatched} record(s) matched nothing")
    return {"matched": matched, "unmatched": unmatched, "records": records}


def _find(db, rec):
    folio = sc.norm_folio(rec.get("match_parcel"))
    if folio:
        row = db.query(Project).filter(Project.parcel_id == folio,
                                       Project.city == CITY).one_or_none()
        if row is not None:
            return row
    addr = (rec.get("match_address") or "").strip()
    if not addr:
        return None
    return db.query(Project).filter(Project.address == addr,
                                    Project.city == CITY).one_or_none()


def _apply(db, row, rec):
    values = rec.get("values") or {}
    sources = rec.get("sources") or {}

    bad = [f for f in values if f not in OVERLAY_COLUMNS]
    if bad:
        raise ValueError(f"{rec['name']}: {bad} cannot be overlaid onto a permit row")
    missing = [f for f, v in values.items()
               if v not in (None, "", "not found")
               and not (sources.get(f) or sources.get("_default") or {}).get("url")]
    if missing:
        raise ValueError(f"{rec['name']}: no source URL for {missing}")

    # The project's real name replaces the address the permit ingest used as a
    # placeholder, and the address itself is left exactly as the City has it.
    row.name = rec["name"]
    if rec.get("alt_addresses"):
        row.alt_addresses = ", ".join(rec["alt_addresses"])
    if rec.get("notes"):
        # Appended, because sofl_miami may already have written the
        # "earlier completed permit on this parcel" caveat here and that is
        # not something a name change should erase -- but appended only once,
        # so re-running this module does not stack the same paragraph up.
        existing = (row.notes or "").strip()
        if rec["notes"] not in existing:
            row.notes = f"{existing} {rec['notes']}".strip() if existing else rec["notes"]
    if rec.get("asset_class_note"):
        row.description = f"{(row.description or '').strip()} {rec['asset_class_note']}".strip()

    for field, value in values.items():
        if value in (None, "", "not found"):
            continue
        setattr(row, OVERLAY_COLUMNS[field], value)

    db.flush()
    # Replace only this module's citations, leaving the permit citations alone.
    db.query(ExtractionSource).filter(
        ExtractionSource.project_id == row.id,
        ExtractionSource.filing_name.notlike("City of Miami building permit%"),
        ExtractionSource.filing_name.notlike("Miami-Dade property boundary%"),
    ).delete(synchronize_session=False)
    for field, value in values.items():
        if value in (None, "", "not found"):
            continue
        src = sources.get(field) or sources["_default"]
        sc.record_sources(db, row.id, {OVERLAY_COLUMNS[field]: value},
                          src.get("name") or "", src["url"], src.get("date") or "")


if __name__ == "__main__":
    ingest()
