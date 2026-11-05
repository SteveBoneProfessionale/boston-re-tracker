r"""Write South Florida acquisitions into the transactions table.

    python -m scraper.acq_sofl_load --dry-run
    python -m scraper.acq_sofl_load

Idempotent on the DEED: a row is keyed by city, recorded book and page, and
sale date, so re-running replaces rather than duplicates. Where a deed carries
no book and page the key falls back to city, date, price and parcel list, which
is the same degenerate case deed_key handles upstream and for the same reason.

LINKING TO THE PROJECTS TABLE happens here because it is the only place that
can see both tables. A transaction is linked to a development project when they
share a parcel identifier -- Miami-Dade's folio, which the Projects rows carry
from the City of Miami permit feed. The match is on the DIGITS of the
identifier, because the permit feed drops the folio's leading zero and the
appraiser keeps it, so "0131360090420" and "131360090420" are the same parcel
written two ways; comparing the strings as given would match nothing at all.

A transaction deed can cover many parcels, so the link is recorded against the
first project any of its parcels matches, with link_basis naming the parcel
that made the match. That keeps the link checkable instead of asserting a
relationship whose grounds have been discarded.
"""

import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from sqlalchemy import text                          # noqa: E402

from db.database import engine, get_session, init_db  # noqa: E402

# Columns the builder produces that are not columns of the table. They carry
# the classification into the report and stop here.
INTERNAL = ("_asset_class", "_parcels")


def _link_map(conn):
    """{parcel digits -> (project id, project name)} for the five cities."""
    out = {}
    rows = conn.execute(text(
        "select id, name, parcel_id, city from projects "
        "where parcel_id is not null and trim(parcel_id) <> ''")).all()
    for pid, name, parcel, city in rows:
        digits = "".join(c for c in str(parcel) if c.isdigit())
        if len(digits) >= 10:
            # Normalised to the appraiser's 13-digit folio by left-padding,
            # because the permit feed drops the leading zero.
            out.setdefault(digits.zfill(13), (pid, name, city))
    return out


def _key(row):
    if row.get("deed_book") and row.get("deed_page"):
        return (row["city"], str(row["deed_book"]).lstrip("0"),
                str(row["deed_page"]).lstrip("0"), row["sale_date"])
    return (row["city"], row["sale_date"], row["price"], row["parcel_id"])


def write(rows, dry_run=False, verbose=True, replace=False):
    """Upsert the rows. With replace=True, clear the two sources first.

    REPLACE IS THE RIGHT MODE AFTER A RULE CHANGE. Upserting alone leaves
    ORPHANS: a deed that was in scope under the old rules and is not under the
    new ones is never visited by the loop, so it survives in the table and keeps
    being counted. That happened here -- tightening the assembly rule dropped
    five rows that stayed in the database and showed six more Retail deals than
    the builder found. Deleting the two sources' rows and re-inserting makes the
    table a function of the current rules rather than of every rule ever run.
    """
    init_db()
    inserted = updated = linked = deleted = 0

    with engine.begin() as conn:
        links = _link_map(conn)
        if replace:
            deleted = conn.execute(text(
                "delete from transactions where source in "
                "('miamidade_pa','pbcpao')")).rowcount or 0
        existing = {}
        if not replace:
            for r in conn.execute(text(
                    "select id, city, deed_book, deed_page, sale_date, price, "
                    "parcel_id from transactions where source in "
                    "('miamidade_pa','pbcpao')")).mappings():
                existing[_key(dict(r))] = r["id"]

        for row in rows:
            payload = {k: v for k, v in row.items() if k not in INTERNAL}

            # Link on any of the deed's parcels.
            for part in str(row.get("parcel_id") or "").split(";"):
                digits = "".join(c for c in part if c.isdigit()).zfill(13)
                hit = links.get(digits)
                if hit:
                    payload["linked_project_id"] = hit[0]
                    payload["link_basis"] = (
                        f"folio {part.strip()} matches the {hit[2]} project "
                        f"record {hit[1]!r}")
                    linked += 1
                    break

            key = _key(row)
            if key in existing:
                sets = ", ".join(f"{k} = :{k}" for k in payload)
                payload["_id"] = existing[key]
                if not dry_run:
                    conn.execute(text(f"update transactions set {sets} "
                                      f"where id = :_id"), payload)
                updated += 1
            else:
                cols = ", ".join(payload)
                vals = ", ".join(f":{k}" for k in payload)
                if not dry_run:
                    conn.execute(text(f"insert into transactions ({cols}) "
                                      f"values ({vals})"), payload)
                inserted += 1

        if dry_run:
            conn.rollback()

    if verbose:
        print(f"{'(dry run) ' if dry_run else ''}"
              + (f"{deleted} cleared, " if replace else "")
              + f"{inserted} inserted, {updated} updated, "
              f"{linked} linked to a project record")
    return inserted, updated, linked


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--replace", action="store_true",
                    help="clear the two county sources before loading, so the "
                         "table reflects the current rules and not every rule "
                         "ever run")
    args = ap.parse_args()
    from scraper import acq_sofl_build
    rows, _dropped = acq_sofl_build.build()
    write(rows, dry_run=args.dry_run, replace=args.replace)
    return 0


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.exit(main())
