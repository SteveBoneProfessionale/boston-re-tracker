r"""Restrict the transaction table to 2025 and 2026.

The original brief was "January 1 2026 to present". When Cambridge's assessment
file turned out to hold no 2026 sales and Boston's held no prices at all, I
loaded both cities' history back to 2015 and justified it as a price-per-SF
comparison set. That was my call, and I never came back and asked. It left 765
of 804 rows outside the stated window, so every headline the tab produced --
804 transactions, $40.42B, the buyer rankings -- was 95% out of scope.

The window is now 2025-01-01 onward.

WHAT SURVIVES: 40 rows. 39 dated 2026 and exactly ONE dated 2025 -- 847
Massachusetts Avenue, Cambridge, $3.4M, 1 July 2025. That single row is not an
artefact of the floor or the commercial filter; the Cambridge source itself
contains exactly one commercial sale at or above $2M for the whole of 2025, and
Boston's MassGIS layer stops in October 2022. Checked before deleting anything.

WHAT IS LOST, and it is worth naming rather than discovering later:

  The price-per-SF comparison set. "Median $/SF by asset class" was computed off
  ~760 historical rows and is now computed off 40, most of which have no square
  footage because they are press-sourced. That chart is effectively dead.

  The buyer and seller rankings. 117 resolved buyers become 20. Alexandria's
  $4.48B was 2015-2022 and goes entirely. What remains is a ranking of one
  eight-month window, which is honest but thin.

  The entity-resolution work on pre-2025 rows -- roughly 100 sponsor
  resolutions, several of which cost a web search each.

NOTHING IS UNRECOVERABLE. Every loader takes a --since argument and is
idempotent, so `acq_massgis.py --since 2015-01-01 --apply` and its siblings
rebuild the history, and the resolution scripts re-apply over it. The ownership
chain that derives sellers reads Boston's and Cambridge's annual files directly
and does not depend on these rows existing.

    python scraper/acq_set_window.py --apply
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

WINDOW_START = "2025-01-01"


def main(dry_run: bool):
    conn = engine.connect()
    before_n, before_v = conn.execute(text(
        "select count(*), sum(coalesce(price,0)) from transactions")).first()
    keep_n, keep_v = conn.execute(text(
        "select count(*), sum(coalesce(price,0)) from transactions "
        "where sale_date >= :d"), {"d": WINDOW_START}).first()

    log.info("before   %d rows  $%.2fB", before_n, before_v / 1e9)
    log.info("in window %d rows  $%.2fB", keep_n, (keep_v or 0) / 1e9)
    log.info("to remove %d rows  $%.2fB",
             before_n - keep_n, (before_v - (keep_v or 0)) / 1e9)

    for y, n in conn.execute(text(
            "select substr(sale_date,1,4), count(*) from transactions "
            "where sale_date >= :d group by 1 order by 1"), {"d": WINDOW_START}):
        log.info("   %s  %d rows", y, n)

    if dry_run:
        conn.close()
        return

    conn.execute(text("delete from transactions where sale_date < :d "
                      "or sale_date is null"), {"d": WINDOW_START})
    conn.commit()

    n, v = conn.execute(text(
        "select count(*), sum(coalesce(price,0)) from transactions")).first()
    log.info("\nafter    %d rows  $%.2fB", n, (v or 0) / 1e9)
    for side in ("buyer", "seller"):
        a = conn.execute(text(
            f"select count(*) from transactions "
            f"where coalesce({side},'') <> ''")).scalar()
        s = conn.execute(text(
            f"select count(*) from transactions "
            f"where coalesce({side}_canonical,'') <> ''")).scalar()
        log.info("%-7s entity on %d rows, sponsor on %d", side, a, s)
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
