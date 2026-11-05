"""Apply the dominant-use asset class to Miami's permit-derived rows.

Reads the work items already stored on each row (`asset_class_raw`) rather than
re-querying the City, because the work items are what the permit said and they
have not changed -- the rule that reads them has. Nothing is re-scraped.

Rows whose work items name no use this can read are LEFT ALONE and listed, not
blanked: four recreational-facility rows keep "Other", and four press-derived
rows that never had permit tokens are classified by hand below.

    python -m scraper.sofl_apply_asset_class --dry-run
    python -m scraper.sofl_apply_asset_class
"""

import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from db.database import get_session                      # noqa: E402
from db.models import Project, ExtractionSource          # noqa: E402
from scraper import sofl_changelog as clog               # noqa: E402
from scraper.sofl_asset_class import classify            # noqa: E402
from scraper import sofl_miami as sm                     # noqa: E402

# Four rows came from the press overlay and never carried permit work items, so
# the rule has nothing to read. Classified by hand from what the row already
# holds, each with its reason.
OVERLAY = {
    1373: ("Mixed-Use", "619 Brickell Avenue is a condominium tower with a hotel "
                        "component, which is a real second programme."),
    1374: ("Mixed-Use", "130 Biscayne Boulevard carries 414 condominium residences "
                        "and a 144-room hotel programme on levels 19 to 30, so the "
                        "hotel is a real second programme."),
    1375: ("Condo", "LILLI is a for-sale condominium tower with no second "
                    "programme reported."),
    1376: ("Condo", "75 Broadway is a for-sale condominium tower with no second "
                    "programme reported."),
}


def run(dry_run=False, verbose=True):
    db = get_session()
    entries = clog.load()
    changed = unchanged = 0
    skipped = []
    try:
        rows = db.query(Project).filter(Project.city == "Miami").order_by(Project.id).all()
        for row in rows:
            new, why = classify(row.asset_class_raw, row.id)
            if new is None:
                if row.id in OVERLAY:
                    new, why = OVERLAY[row.id]
                    why = f"no permit work items; {why}"
                else:
                    skipped.append((row.id, row.name, row.asset_class, row.asset_class_raw))
                    continue
            if new == row.asset_class:
                unchanged += 1
                continue
            old = row.asset_class
            # The citation is the master building permit already on the row: the
            # work items this rule reads are that permit's own field, so the
            # change needs a new reading of the same source, not a new source.
            url = (sm.permit_url({"PermitNumber": row.permit_number})
                   if row.permit_number else "")
            clog.record(entries, row.id, row.name, "asset_class", old, new, url, why)
            if not dry_run:
                row.asset_class = new
                db.flush()
                db.query(ExtractionSource).filter(
                    ExtractionSource.project_id == row.id,
                    ExtractionSource.field_name == "asset_class").delete(
                        synchronize_session=False)
                db.add(ExtractionSource(
                    project_id=row.id, field_name="asset_class", field_value=new,
                    filing_name=(f"City of Miami building permit {row.permit_number}, "
                                 "work items" if row.permit_number
                                 else "classified by hand; see notes"),
                    filing_date="", pdf_url=url))
            changed += 1
        if not dry_run:
            db.commit()
    finally:
        db.close()

    if not dry_run:
        clog.save(entries)
    if verbose:
        print(f"{'(dry run) ' if dry_run else ''}{changed} reclassified, "
              f"{unchanged} already correct, {len(skipped)} left alone")
        for pid, name, cur, raw in skipped:
            print(f"  left alone: {pid} {str(name)[:38]:<38} {cur!r}  raw={raw!r}")
    return {"changed": changed, "unchanged": unchanged, "skipped": skipped}


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    run(**vars(ap.parse_args()))
