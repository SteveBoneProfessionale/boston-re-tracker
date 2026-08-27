r"""Forty-sixth pass. A row I wrote is contradicted by a primary filing. Clearing it.

100 BROADWAY'S SELLER READ "RLJ LODGING TRUST", written on the SELF-IDENTIFYING
rule: the entity is RLJ CAMBRIDGE HOTEL LLC, and RLJ Lodging Trust is
independently confirmed elsewhere in this table as the seller of the Fairmont
Copley Plaza. That looked safe.

RLJ LODGING TRUST'S OWN 2020 FORM 10-K SAYS OTHERWISE. For the year ended 31
December 2020 the REIT reports selling ONE hotel property -- the Residence Inn
Houston Sugarland, in Stafford, Texas, on 1 December 2020, for about $4.9
million. A $98,991,934 Cambridge disposition in October 2020 is not in it, and a
listed REIT does not omit a hundred-million-dollar sale from its annual report.

SO THE SELF-IDENTIFYING READ WAS TOO COARSE. "RLJ" is a family, not a company:

    RLJ Development / RLJ Urban Lodging Fund   Robert L. Johnson's private
                                               funds, which bought the Residence
                                               Inn Boston Cambridge at 120
                                               Broadway for $63 million in 2005
    RLJ Lodging Trust                          the listed REIT formed out of
                                               those funds in 2011

An entity named RLJ CAMBRIDGE HOTEL LLC could belong to either, and the REIT's
own filing rules out the one that was written. THE CANONICAL IS CLEARED rather
than swapped, because "probably the private fund instead" is a guess, and a
blank is correct where a wrong name would rank.

    This is the first time in this project a written sponsor has been REMOVED on
    contradicting evidence rather than refined. The self-identifying rule has
    been reliable -- Verizon New England, Trustees of Boston University, Gillette,
    Norwich Partners, Park Square Revival -- but it assumes the name on the
    entity maps to exactly one company. Where a name spans a REIT and the
    private funds it grew out of, it does not.

AND IT VINDICATES A METHOD FROM THE PREVIOUS PASS. Marketplace Center's seller
was found by chasing the party with a DISCLOSURE OBLIGATION after four
consumer-facing outlets failed to name it. Reading the counterparty's own filing
is the same move -- and here it removed an error instead of filling a gap. Both
directions are worth the trip.

    python scraper/acq_press46.py --apply
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

CLEAR_NOTE = (
    " | SELLER CLEARED ON CONTRADICTING PRIMARY EVIDENCE. This row previously "
    "read RLJ LODGING TRUST, written on the self-identifying rule from the entity "
    "RLJ CAMBRIDGE HOTEL LLC, with RLJ independently confirmed elsewhere in this "
    "table at the Fairmont Copley Plaza. RLJ LODGING TRUST'S OWN 2020 FORM 10-K "
    "CONTRADICTS IT: for the year ended 31 December 2020 the REIT reports selling "
    "ONE hotel property, the Residence Inn Houston Sugarland in Stafford, Texas, "
    "on 1 December 2020 for about $4.9 million. A $98,991,934 Cambridge "
    "disposition in October 2020 does not appear, and a listed REIT does not omit "
    "a nine-figure sale from its annual report. THE SELF-IDENTIFYING READ WAS TOO "
    "COARSE: 'RLJ' spans RLJ Development and its RLJ Urban Lodging Fund -- Robert "
    "L. Johnson's private vehicles, which bought the Residence Inn Boston "
    "Cambridge at 120 Broadway for $63 million in 2005 -- and RLJ Lodging Trust, "
    "the REIT formed out of those funds in 2011. An entity called RLJ CAMBRIDGE "
    "HOTEL LLC could belong to either, and the filing rules out the one that was "
    "written. The canonical is CLEARED rather than swapped to the private fund, "
    "because that would be a guess. NOTE ALSO the nearby deal this must not be "
    "conflated with: Xenia sold the 221-room Residence Inn Boston Cambridge at "
    "120 Broadway for $107.5 million to TPG in the SAME MONTH. Different address, "
    "different price, different parties. The buyer entity ODYSSEY PROPCO LLC "
    "belongs to a numbered series and remains undecoded after four searches."
)

NOTES = [
    (1027, "131-147 SEAPORT BOULEVARD SELLER: FIFTH SEARCH, AND COTTONWOOD IS "
           "CONFIRMED AS THE DEVELOPER WITHOUT BEING CONFIRMED AS THE SELLER. "
           "REBusinessOnline, Bisnow and Shopping Center Business all cover "
           "COTTONWOOD MANAGEMENT breaking ground on the $900 million Echelon "
           "Seaport -- 733 condominiums and apartments across three towers with "
           "125,000 SF of retail -- and WS Development's own site confirms WS "
           "would own and manage the retail component. WS is on the buy side of "
           "this row. But every one of those pieces is from 2017 groundbreaking "
           "coverage, and NOTHING reports the 2021 conveyance of the retail "
           "condominium at $94,500,000. The seller of record is 131 149 SEAPORT "
           "PRIMARY CONDOMINIUM TRUST -- a condominium trust, not a sponsor. "
           "Cottonwood is the obvious answer and 'obvious' is what the BP3 case "
           "disposed of: there, the obvious reading of an entity was Boston "
           "Properties and the answer was Phase 3 Real Estate Partners, a San "
           "Diego firm with no connection to it."),
]


def main(dry_run: bool):
    conn = engine.connect()
    cur = conn.execute(text(
        "select seller, coalesce(seller_canonical,'') from transactions "
        "where id = 1643")).first()
    log.info("id=1643 seller %-34s -> (CLEARED, was %s)", (cur[0] or "")[:34],
             cur[1] or "null")
    if not dry_run:
        conn.execute(text("""
            update transactions
               set seller_canonical = null,
                   seller_confidence = null,
                   seller_resolution_basis = null,
                   notes = coalesce(notes,'') || :n
             where id = 1643"""), {"n": CLEAR_NOTE})
        for rid, note in NOTES:
            conn.execute(text(
                "update transactions set notes = coalesce(notes,'') || :n "
                "where id = :id"), {"n": " | " + note, "id": rid})
        conn.commit()

    tot = conn.execute(text(
        "select count(*) from transactions where coalesce(quarantined,0)=0")).scalar()
    for side in ("buyer", "seller"):
        v = conn.execute(text(
            f"select count(*) from transactions where coalesce(quarantined,0)=0 "
            f"and coalesce({side}_canonical,'') <> ''")).scalar()
        log.info("%s_canonical: %d of %d (%.0f%%)", side, v, tot, v / tot * 100)
    # Every other row still resting on the self-identifying rule, for review.
    log.info("\nrows still resolved on the self_identifying basis:")
    for side in ("buyer", "seller"):
        for r in conn.execute(text(
                f"select id, {side}, {side}_canonical, price from transactions "
                f"where coalesce(quarantined,0)=0 "
                f"and {side}_resolution_basis = 'self_identifying' "
                f"order by price desc")):
            log.info("  %-6s id=%-5s $%-12s %-34s -> %s", side, r[0],
                     f"{r[3] or 0:,}", (r[1] or "")[:34], r[2])
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
