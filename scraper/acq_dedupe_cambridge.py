r"""One deed, one transaction -- the dedup the Cambridge loader never got.

I identified this trap in the MassGIS loader, where raw parcel rows summed to
$62.2B against $27.9B deduplicated, and wrote it up as the reason that loader
groups on the registry citation. I then identified it again for press-reported
portfolios, which is why Brighton Avenue is one $36M row and not six. I never
went back and applied it to Cambridge, which had been loaded earlier and had no
dedup at all.

The cost of that omission: 20 groups of rows sharing a price, a date AND a
book/page -- one deed written once per parcel it conveyed -- inflating the
Cambridge spine by $7,937,470,807. That is 39% of its stated volume, and it was
inside every Cambridge total previously reported, including the $50.0B headline.

    $750,000,000  2024-01-26  x3
    $555,000,000  2016-11-08  x4
    $180,000,000  2021-09-09  x9
    $445,000,000  2020-12-01  x3
    $291,040,269  2015-08-21  x4   (book 65949, page 156, four NorthPoint parcels)

Grouping is on (deed_book, deed_page, price, sale_date), which is stricter than
the MassGIS version because every Cambridge row carries a book and page. Two
different sales coinciding on all four is not a coincidence worth worrying
about; the same deed appearing on each parcel it conveyed is the documented
behaviour of an assessment roll.

The largest parcel by building area survives as the representative, the parcel
count is recorded on it, and the rest are deleted. Re-running the loader
reproduces the raw rows, so nothing is lost that cannot be rebuilt.

    python scraper/acq_dedupe_cambridge.py --apply
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from db.database import engine

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)


def main(dry_run: bool):
    conn = engine.connect()
    groups = conn.execute(text("""
        select deed_book, deed_page, price, sale_date, count(*) n
          from transactions
         where source = 'cambridge_socrata'
           and coalesce(deed_book,'') <> ''
         group by deed_book, deed_page, price, sale_date
        having count(*) > 1
         order by price * (count(*) - 1) desc""")).fetchall()

    before_n, before_v = conn.execute(text(
        "select count(*), sum(coalesce(price,0)) from transactions "
        "where source = 'cambridge_socrata'")).first()

    removed = 0
    for book, page, price, sd, n in groups:
        rows = conn.execute(text("""
            select id, address, coalesce(building_sf,0), coalesce(land_sf,0)
              from transactions
             where source = 'cambridge_socrata' and deed_book = :b
               and coalesce(deed_page,'') = coalesce(:p,'')
               and price = :pr and sale_date = :d
             order by coalesce(building_sf,0) desc, coalesce(land_sf,0) desc"""),
            {"b": book, "p": page, "pr": price, "d": sd}).fetchall()
        if len(rows) < 2:
            continue
        keep, drop = rows[0], rows[1:]
        addrs = ", ".join(r[1] for r in rows)
        log.info("$%-13s %s  x%-2d keep %-26s", f"{price:,}", sd, len(rows),
                 keep[1][:26])
        if not dry_run:
            conn.execute(text("""
                update transactions
                   set unit_count = coalesce(unit_count, null),
                       price_caveat = :cav,
                       notes = coalesce(notes,'') || :n
                 where id = :id"""), {
                "id": keep[0],
                "cav": (f"Whole-deed consideration across {len(rows)} parcels."),
                "n": (f" | MULTI-PARCEL DEED, DEDUPLICATED. Book {book}/page "
                      f"{page} conveyed {len(rows)} parcels and the assessment "
                      f"roll wrote the full price onto each: {addrs}. One "
                      f"transaction is recorded, not {len(rows)}, with the "
                      f"largest parcel standing as the representative address. "
                      f"Price is the whole-deed consideration, so price-per-SF "
                      f"reflects only this parcel's area and overstates the "
                      f"portfolio rate.")})
            for r in drop:
                conn.execute(text("delete from transactions where id = :id"),
                             {"id": r[0]})
        removed += len(drop)

    if not dry_run:
        conn.commit()

    after_n, after_v = conn.execute(text(
        "select count(*), sum(coalesce(price,0)) from transactions "
        "where source = 'cambridge_socrata'")).first()
    tot_n, tot_v = conn.execute(text(
        "select count(*), sum(coalesce(price,0)) from transactions")).first()
    log.info("\n%d duplicate parcel rows %s", removed,
             "would be removed" if dry_run else "removed")
    log.info("Cambridge spine  %d rows $%s  ->  %d rows $%s",
             before_n, f"{int(before_v):,}", after_n, f"{int(after_v):,}")
    log.info("whole table      %d rows, $%s", tot_n, f"{int(tot_v):,}")
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
