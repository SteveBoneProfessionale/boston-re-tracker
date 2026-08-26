r"""Set the transaction price floor to $2M, which is the owner's call, not mine.

I picked $3M from the Cambridge distribution because it kept 99.6% of dollar
volume against 99.3% at $5M. Three tenths of a point is exactly the size of gap
where the owner's preference should beat the optimiser, and the stated range was
"somewhere between $2M and $5M". The floor went briefly to $5M and then to $2M,
which is where it stands.

WHAT THE FLOOR DOES NOT EXPLAIN, and this is the reason the script exists rather
than the floor just being a constant somewhere:

The floor was raised while asking whether it was hiding 2026 activity. It was
not. At $5M the number of 2026 rows removed was ZERO, because the smallest
priced 2026 transaction was $23.7M. Lowering it to $2M then added zero 2026
rows for the same reason. The floor moves the historical spines by hundreds of
rows and moves 2026 by none.

The thin 2026 count is a SOURCE problem: Boston's assessment file has no sale
price at all, Cambridge's stops 6 August 2025, MassGIS Level 3 for Boston is
FY2023 and stops 28 October 2022, and the registry is behind a bot block. Every
2026 row comes from press or an SEC filing.

Rows below the floor are deleted rather than flagged, because the floor is an
inclusion rule for the tab and not a judgement about the transactions. Rerunning
the loaders with a lower --floor restores them.

    python scraper/acq_set_floor.py --apply
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

FLOOR = 2_000_000


def main(dry_run: bool):
    conn = engine.connect()
    rows = conn.execute(text(
        "select id, address, city, sale_date, price, source from transactions "
        "where price is not null and price < :f order by price desc"),
        {"f": FLOOR}).fetchall()
    log.info("%d rows below the $%s floor", len(rows), f"{FLOOR:,}")
    for r in rows[:8]:
        log.info("   %-34s %-10s %s  $%s  [%s]", r[1][:34], r[2], r[3],
                 f"{r[4]:,}", r[5])
    if len(rows) > 8:
        log.info("   ... and %d more", len(rows) - 8)

    in26 = [r for r in rows if str(r[3]) >= "2026-01-01"]
    log.info("\nof which dated 2026: %d", len(in26))

    if not dry_run:
        conn.execute(text("delete from transactions where price is not null "
                          "and price < :f"), {"f": FLOOR})
        conn.commit()
        n, v = conn.execute(text(
            "select count(*), sum(coalesce(price,0)) from transactions")).first()
        log.info("\nafter: %d transactions, $%s", n, f"{int(v or 0):,}")
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    main(dry_run=not a.apply)
