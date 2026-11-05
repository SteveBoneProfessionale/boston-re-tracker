"""Apply the pinned asset classes for Miami Beach, West Palm Beach, Coral Gables and Doral.

Separate from the Miami applier because the evidence is a different kind: Miami
reads the City's use tokens mechanically, these rest on a sentence of the
record or on the Live Local Act's own terms. Each decision carries its reason
into the change log, so the table says WHY as well as what.

    python -m scraper.sofl_apply_asset_class_cities --dry-run
    python -m scraper.sofl_apply_asset_class_cities
"""

import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from db.database import get_session                               # noqa: E402
from db.models import Project, ExtractionSource                   # noqa: E402
from scraper import sofl_changelog as clog                        # noqa: E402
from scraper.sofl_asset_class_cities import DECISIONS, UNSTATED   # noqa: E402

CITIES = ("Miami Beach", "West Palm Beach", "Coral Gables", "Doral")


def run(dry_run=False, verbose=True):
    db = get_session()
    entries = clog.load()
    changed = unchanged = 0
    try:
        for pid, (new, why) in DECISIONS.items():
            row = db.get(Project, pid)
            if row is None:
                print(f"  MISSING ROW {pid}")
                continue
            if row.asset_class == new:
                unchanged += 1
                continue
            # The citation is whatever already sourced this row's programme --
            # the record file's own filing. A reclassification is a reading of
            # evidence already on the row, not a new document, so the existing
            # FIELD SOURCES entry stands and the reason carries the change.
            src = db.query(ExtractionSource).filter(
                ExtractionSource.project_id == pid).order_by(
                    ExtractionSource.id).first()
            url = src.pdf_url if src is not None else ""
            clog.record(entries, pid, row.name, "asset_class",
                        row.asset_class, new, url, why)
            if not dry_run:
                row.asset_class = new
                db.flush()
                db.query(ExtractionSource).filter(
                    ExtractionSource.project_id == pid,
                    ExtractionSource.field_name == "asset_class").delete(
                        synchronize_session=False)
                db.add(ExtractionSource(
                    project_id=pid, field_name="asset_class", field_value=new,
                    filing_name=(src.filing_name if src is not None
                                 else "classified from the record; see notes"),
                    filing_date=(src.filing_date if src is not None else ""),
                    pdf_url=url))
            changed += 1

        # Every remaining row in these cities should now hold a value the brief
        # accepts. Anything still "Residential" must be in UNSTATED by name.
        # In a dry run nothing has been written, so the rows DECISIONS covers
        # would all still read "Residential" and the check would be noise.
        # Excluding them makes the dry run report the same set the real run
        # will leave behind.
        q = db.query(Project).filter(Project.city.in_(CITIES),
                                     Project.asset_class == "Residential")
        if dry_run:
            q = q.filter(~Project.id.in_(list(DECISIONS)))
        # Materialised to plain tuples INSIDE the session: db.commit() expires
        # every ORM attribute, and reading one back after db.close() raises
        # DetachedInstanceError.
        stragglers = [(r.id, r.city, r.name) for r in q.order_by(Project.id).all()]
        if not dry_run:
            db.commit()
    finally:
        db.close()

    if not dry_run:
        clog.save(entries)
    if verbose:
        print(f"{'(dry run) ' if dry_run else ''}{changed} reclassified, "
              f"{unchanged} already correct")
        unexpected = [t for t in stragglers if t[0] not in UNSTATED]
        print(f"\n{len(stragglers)} row(s) still 'Residential' "
              f"({len(unexpected)} not accounted for):")
        for pid, city, name in stragglers:
            mark = "  " if pid in UNSTATED else "??"
            print(f"  {mark} {pid} {city[:13]:<14}{str(name)[:44]}")
        if unexpected:
            print("\n  '??' rows are NOT in the UNSTATED list and need a decision.")
    return {"changed": changed, "unchanged": unchanged,
            "still_residential": [t[0] for t in stragglers]}


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    run(**vars(ap.parse_args()))
