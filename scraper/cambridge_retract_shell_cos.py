r"""Retract two Cambridge deliveries a certificate of occupancy did not support.

A site visit contradicted the data, and the data was wrong. Both retractions
come from the same mistake: treating "a certificate of occupancy exists against
this permit" as "the building is finished". Two different things break that
equivalence, and the certificate record alone shows neither of them.

290 BINNEY STREET (Building C) -- a CORE AND SHELL certificate.
The building permit behind it, issued 11 January 2024, reads "19 story
core/shell building for commercial use with 8 levels of below grade parking".
A core-and-shell certificate says the base building is legally occupiable, not
that anyone can occupy it: the interiors are unbuilt and every tenant fits out
under a later permit. That later permit is visible in the same dataset -- a
Floors/Units certificate issued 1 July 2026 under permit 262069, a month after
the whole-building one. BXP's own property list still called 290 Binney "under
development" while our table called it delivered, and that disagreement should
have been resolved in BXP's favour the first time rather than noted and passed
over.

THE GALLERIA RESIDENTIAL, 57 JFK STREET -- one partial certificate of six.
57 JFK has six certificates going back to 2019, every one of them Floors/Units
and not one of them Entire Building, issued against six different alteration
permits of 35,015, 35,000, 34,106 and 28,504 square feet. It is a large Harvard
Square building being renovated in pieces, and two of its permits are still
Active. The April 2025 certificate is the fifth partial sign-off in a running
sequence, not a completion. The exact permit join was correct and the
conclusion drawn from it was not.

WHAT SURVIVED THE RE-CHECK
  2 Garden Street        one Entire Building certificate, three floors, on a
                         7,712 sf permit matching the row's 7,721 sf
  Metropolitan Storage   one Entire Building certificate, five floors, on a
                         conversion whose permits are closed out
  New Tobin School       opened for the 2025-26 school year
  150 Richmond           minutes record the certificate received and the tenant
                         moving in
  55 George M Cohan      Pennrose states phase one complete; RIHousing 100%
  220 and 228 Broad St   Copley II and III, both confirmed at their addresses,
                         RIHousing 100% complete, leasing

THE RULE THIS LEAVES
Before a certificate of occupancy is read as a delivery, two things have to be
checked that were not: whether the permit behind it is core-and-shell, and
whether the certificate is the only one at that address or one of a series.

    python scraper/cambridge_retract_shell_cos.py --dry-run
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

CO_DATASET = "https://data.cambridgema.gov/resource/qwvv-deed.json"

RETRACT = [
    dict(project_id=414, name="MXD Infill - 290 Binney Street (Building C)",
         was="2026-06-01",
         reason="RETRACTED. The certificate of occupancy issued 1 June 2026 "
                "against permit 222345 is for a CORE AND SHELL building -- the "
                "permit reads '19 story core/shell building for commercial use "
                "with 8 levels of below grade parking' -- so it establishes "
                "that the base building is legally occupiable, not that it is "
                "finished. A further Floors/Units certificate followed on 1 "
                "July 2026 under permit 262069, which is fit-out. BXP's own "
                "property list still describes 290 Binney as under "
                "development, and a site visit found it visibly unfinished. "
                "The shell date is kept here rather than in the column, where "
                "it read as a completion. Also note the GFA disagrees: the "
                "permit says 807,817 sf against 503,904 on this row, probably "
                "because the Development Log excludes the eight below-grade "
                "parking levels."),
    dict(project_id=410, name="The Galleria Residential",
         was="2025-04-11",
         reason="RETRACTED. 57 JFK Street carries six certificates of "
                "occupancy going back to 2019, every one Floors/Units and none "
                "Entire Building, issued against six separate alteration "
                "permits of 35,015, 35,000, 34,106 and 28,504 sf among others, "
                "with two permits still Active. The April 2025 certificate is "
                "one partial sign-off in a running renovation of a large "
                "Harvard Square building, not the delivery of a 38-unit "
                "project. The permit join was exact; the inference from it was "
                "wrong."),
]


def main(dry_run: bool = False):
    conn = engine.connect()
    for r in RETRACT:
        log.info("retract  [%s] %-46s was %s", r["project_id"],
                 r["name"][:46], r["was"])
        if dry_run:
            continue
        conn.execute(text(
            "update projects set delivered_date = null, delivered_precision = null "
            "where id = :id"), {"id": r["project_id"]})
        # The superseded row keeps the certificate date on the record, so the
        # retraction is auditable and the date is not simply lost.
        _prov(conn, r["project_id"], "delivered_date", value=None,
              outcome="null", tier=None, source_type="certificate_of_occupancy",
              source_url=CO_DATASET,
              source_name="City of Cambridge certificates of occupancy, re-read",
              source_date=r["was"], passage=None, reason=r["reason"], step=3)
    if not dry_run:
        conn.commit()
    conn.close()
    log.info("\n%d retracted", len(RETRACT))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    main(**vars(ap.parse_args()))
