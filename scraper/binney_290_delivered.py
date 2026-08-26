r"""290 Binney Street: the delivery date, settled by the owner's own filing.

This one was asserted, retracted, and is now being reinstated on better
evidence, so the trail matters.

It was first recorded as delivered on a certificate of occupancy. The permit
read "19 story core/shell building", BXP still listed the property under
development, and a further partial CO followed a month later. A core-and-shell
CO says the envelope is closed, not that the building is finished, and the
owner disagreeing with our reading should have stopped the record cold. It did
not, and the user's own site visit caught it. Retracted.

What settles it is BXP's Q2 2026 report: "BXP fully placed in-service 290 Binney
Street in Cambridge, Massachusetts." The Q1 2026 supplemental still carried the
property in CONSTRUCTION IN PROGRESS with initial occupancy and stabilization
both estimated Q2 2026; the Q2 2026 supplemental no longer carries it there at
all. An owner telling its own investors a building is in service, in an audited
filing, and dropping it out of the construction schedule, is the strongest
evidence available short of standing in the lobby.

PRECISION IS QUARTER, NOT DAY. "Fully placed in-service" during Q2 2026 gives a
three-month window, not a date. The stored value is the period start, 1 April
2026, so chronological sorting works, and the precision field carries the truth
that the day is not known.

    python scraper/binney_290_delivered.py --apply
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from db.database import engine
from scraper.backfill_delivery_dates import _prov

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

PASSAGE = (
    'BXP Q2 2026 earnings release, Development section: "BXP fully placed '
    'in-service 290 Binney Street in Cambridge, Massachusetts. 290 Binney Street '
    'is a 16-story, 572,578 square foot laboratory/life sciences property that is '
    '100% leased to AstraZeneca." '
    'Corroborated structurally by the supplementals: the Q1 2026 CONSTRUCTION IN '
    'PROGRESS table lists "290 Binney Street (55% ownership) Q2 2026 Q2 2026 '
    'Cambridge, MA 573,000 ... 100%" for initial occupancy and stabilization, and '
    'the Q2 2026 CONSTRUCTION IN PROGRESS table no longer lists the property.'
)
URL = ("https://www.sec.gov/Archives/edgar/data/1037540/"
       "000103754026000031/q22026pressrelease.htm")


def main(dry_run: bool):
    conn = engine.connect()
    row = conn.execute(text(
        "select id, name, delivered_date from projects "
        "where address like '%290 Binney%'")).first()
    if not row:
        log.error("290 Binney not found")
        return
    pid, name, current = row
    log.info("project %d  %s\n  delivered_date before: %s", pid, name, current)

    if not dry_run:
        conn.execute(text(
            "update projects set delivered_date = :d, delivered_precision = :p "
            "where id = :id"),
            {"d": "2026-04-01", "p": "quarter", "id": pid})
        _prov(conn, pid, "delivered_date", value="2026-04-01 (quarter)",
              outcome="resolved", tier="document_confirmed",
              source_type="sec_filing", source_url=URL,
              source_name="BXP Q2 2026 earnings release and quarterly supplemental",
              source_date="2026-08-03", passage=PASSAGE,
              reason="Reinstates a delivery previously retracted after a "
                     "core-and-shell CO was misread as completion. The owner's own "
                     "audited filing supersedes the permit record.",
              step=7)
        conn.commit()
        after = conn.execute(text(
            "select delivered_date, delivered_precision from projects where id = :id"),
            {"id": pid}).first()
        log.info("  delivered_date after:  %s (%s)", after[0], after[1])
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    main(dry_run=not a.apply)
