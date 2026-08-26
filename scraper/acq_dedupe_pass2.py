r"""Second dedup pass: catch the portfolio repeats that a book/page key misses.

The first pass grouped parcel rows on the registry citation, which is the right
key when a citation exists and is consistent. Two situations defeat it, and
between them they left $1.67 BILLION of residual overcount in the Boston spine:

MIXED REGISTERED AND RECORDED LAND. Massachusetts holds roughly a fifth of
Suffolk and Middlesex title as Torrens registered land, on a Certificate of
Title with a document number, in a physically separate registry section. When a
single deal conveys one registered parcel and one recorded parcel, the SAME
transaction carries two different citations -- "86538/0RL" against "57387/043"
for the Liberty Mutual Back Bay sale. Grouping on the citation therefore splits
one transaction in two and counts its price twice. This is the registered-land
problem showing up somewhere I had not looked for it: I built the schema to
represent Torrens correctly and then wrote a dedup key that Torrens breaks.

MISSING CITATIONS. Other rows carry no book or page at all, so every one of them
lands in its own group.

The fix is a second key: same BUYER, same PRICE, same DATE. One party paying an
identical sum on an identical day across several parcels is one deal, not
several. At $673M or $270M the odds of coincidence are nil; the smallest group
this catches is $7.5M across five parcels, which is still plainly a portfolio.

Ordering matters: this pass runs AFTER the citation pass, so where a citation is
present and consistent it has already done the work, and this only sweeps the
residue.

    python scraper/acq_dedupe_pass2.py --apply
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

SPINE = ("massgis_l3", "cambridge_socrata")


def main(dry_run: bool):
    conn = engine.connect()
    before_n, before_v = conn.execute(text(
        "select count(*), sum(coalesce(price,0)) from transactions")).first()

    removed = 0
    for src in SPINE:
        groups = conn.execute(text("""
            select price, sale_date, buyer, count(*) n
              from transactions
             where source = :s and price > 0 and coalesce(buyer,'') <> ''
             group by price, sale_date, buyer
            having count(*) > 1
             order by price * (count(*) - 1) desc"""), {"s": src}).fetchall()
        for price, sd, buyer, n in groups:
            rows = conn.execute(text("""
                select id, address, coalesce(building_sf,0), coalesce(land_sf,0),
                       coalesce(deed_book,''), coalesce(document_number,'')
                  from transactions
                 where source = :s and price = :p and sale_date = :d
                   and buyer = :b
                 order by coalesce(building_sf,0) desc, coalesce(land_sf,0) desc"""),
                {"s": src, "p": price, "d": sd, "b": buyer}).fetchall()
            if len(rows) < 2:
                continue
            keep, drop = rows[0], rows[1:]
            addrs = ", ".join(r[1] for r in rows)
            cites = ", ".join(sorted({(r[4] or r[5] or "none") for r in rows}))
            mixed = any(r[5] for r in rows) and any(r[4] for r in rows)
            log.info("%-16s $%-13s %s x%-2d  %s", src[:16], f"{price:,}", sd,
                     len(rows), keep[1][:26])
            if not dry_run:
                conn.execute(text("""
                    update transactions
                       set price_caveat = :cav,
                           notes = coalesce(notes,'') || :n
                     where id = :id"""), {
                    "id": keep[0],
                    "cav": f"Whole-deed consideration across {len(rows)} parcels.",
                    "n": (f" | MULTI-PARCEL DEAL, DEDUPLICATED ON BUYER+PRICE+DATE. "
                          f"{len(rows)} parcels conveyed to the same buyer for the "
                          f"same price on the same day: {addrs}. Registry "
                          f"citations differ ({cites})"
                          + (", because the deal mixes Torrens REGISTERED land "
                             "with recorded land and the two are cited "
                             "differently -- which is why grouping on book and "
                             "page did not catch it"
                             if mixed else
                             " or are absent, so grouping on book and page did "
                             "not catch it")
                          + f". One transaction is recorded, not {len(rows)}, "
                          f"with the largest parcel as the representative.")})
                for r in drop:
                    conn.execute(text("delete from transactions where id = :id"),
                                 {"id": r[0]})
            removed += len(drop)

    if not dry_run:
        conn.commit()
    after_n, after_v = conn.execute(text(
        "select count(*), sum(coalesce(price,0)) from transactions")).first()
    log.info("\n%d duplicate parcel rows %s", removed,
             "would be removed" if dry_run else "removed")
    log.info("%d rows $%s  ->  %d rows $%s", before_n, f"{int(before_v):,}",
             after_n, f"{int(after_v):,}")
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
