r"""Forty-third pass. One buyer, and three rows whose STRUCTURE is now settled
even though the parties are not.

368 CONGRESS STREET. CoStar: "120-suite Residence Inn Boston Downtown Seaport
Trades for $64 Million" -- in August 2018 CLAREMONT COMPANIES reached a deal with
Norwich Partners to buy the hotel, redeveloped in 2013 out of a century-old sugar
and molasses warehouse. This row is $64,000,000 in August 2018 with Norwich
already on the sell side, and its buyer entity is 370 CONGRESS STREET LLC --
which is the hotel's own street address, one door from the parcel's 368.

THREE ROWS WHERE THE SEARCHING IS FINISHED AND THE ANSWER IS STRUCTURAL. Each
has now been searched three or four times, and what the searches established is
not a party but a REASON there is no party to find. Writing that down is worth
more than another attempt, because it tells the next reader -- or the next pass
-- to stop.

    643-653 Summer St   Massport's own South Boston real-estate portfolio lists
                        643-653 Summer Street as an Industrial and Lab property.
                        Combined with the LEI record showing BOSTON HARBOR
                        INDUSTRIAL DEVELOPMENT LLC holding a Massport ground
                        lease to 2085, the $282.5M conveyance is a LEASEHOLD in
                        the Raymond L. Flynn Marine Park. Press on that park
                        covers leases, board votes and RFQs -- not trades.
    80-90 First St      The buyer entity is 20 CAMBRIDGE PLACE GROUND OWNER LLC.
                        20 CambridgeSide is New England Development's 366,000 SF
                        LEED Platinum lab and office building within the
                        CambridgeSide redevelopment. The word GROUND in the
                        entity, against a building that was developed rather
                        than traded, points the same way: a ground-lease
                        interest, not a fee sale.
    1416 Mass Ave       The seller is Piedmont, established from its own Q4 2022
                        results. The buyer is not named by Piedmont's release,
                        by Connect CRE, or by BLDUP's write-up of the pair --
                        which is normal. A REIT announcing a disposition reports
                        its own proceeds and gain; it has no reason to name who
                        bought.

    python scraper/acq_press43.py --apply
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

RESOLVE = [
    (1282, "buyer", "Claremont Companies",
     'CoStar: "120-suite Residence Inn Boston Downtown Seaport Trades for $64 '
     'Million" -- in August 2018 CLAREMONT COMPANIES reached a deal with Norwich '
     'Partners to buy the hotel, which had been redeveloped in 2013 out of a '
     'century-old sugar and molasses warehouse and opened that June under a '
     'Norwich affiliate. This row is $64,000,000 in August 2018 and the seller '
     'side already read Norwich Partners, written from the entity NORWICH PARTNERS '
     'BOSTON LLC and corroborated by press naming Norwich as seller of the Envoy '
     'Hotel to Hersha. The buyer entity 370 CONGRESS STREET LLC is the hotel\'s '
     'own address, one door from this parcel\'s 368. NEREJ separately records '
     'Norwich Partners selling a 108-unit hotel to AAM 15 Management, a different '
     'deal that should not be conflated with this one.'),
]

NOTES = [
    (1165, "643-653 SUMMER STREET: THE SEARCHING IS FINISHED AND THE ANSWER IS "
           "STRUCTURAL. MASSPORT'S OWN SOUTH BOSTON REAL ESTATE PORTFOLIO LISTS "
           "643-653 SUMMER STREET as an Industrial and Lab property. Combined with "
           "the LEI registration showing BOSTON HARBOR INDUSTRIAL DEVELOPMENT LLC "
           "holding a Massport ground lease running to 30 March 2085, the "
           "$282,500,000 recorded here is a LEASEHOLD INTEREST in the Raymond L. "
           "Flynn Marine Park rather than a fee conveyance. That is why four "
           "searches have found nothing: press coverage of that park runs to "
           "leases, Massport board votes and RFQs, not trades. A quarter-billion "
           "dollars of leasehold moves without a headline. NO FURTHER SEARCHING IS "
           "WARRANTED on the current sources; this needs a registry feed or a "
           "Massport records request."),
    (1592, "80-90 FIRST STREET: SAME CONCLUSION, SAME REASON. The buyer entity is "
           "20 CAMBRIDGE PLACE GROUND OWNER LLC, and 20 CAMBRIDGESIDE is New "
           "England Development's 366,000 SF lab and office building -- WiredScore "
           "Platinum, LEED Platinum v4 -- within the CambridgeSide mixed-use "
           "redevelopment in East Cambridge. The building was DEVELOPED, not "
           "traded, and the word GROUND in the entity points at a ground-lease "
           "interest. Three searches have produced the project in full and no "
           "party to this $156,435,584 conveyance. Inferring the owner of a "
           "complex from its brand name is not resolution, and a ground lease is "
           "not reported as a sale."),
    (1577, "1416 MASSACHUSETTS AVENUE BUYER: NOT FOUND AFTER THREE SEARCHES, AND "
           "THE REASON IS ORDINARY. The seller is established as Piedmont Office "
           "Realty Trust from its own Q4 2022 results, which report roughly $160 "
           "million of combined proceeds and a $102.6 million gain on the two "
           "Cambridge assets. NEITHER PIEDMONT'S RELEASE, NOR CONNECT CRE, NOR "
           "BLDUP'S WRITE-UP OF THE PAIR NAMES THE BUYER -- which is normal rather "
           "than suspicious. A REIT announcing a disposition reports its proceeds, "
           "its gain and what it did with the money; it has no reason to name the "
           "counterparty, and the counterparty has no filing obligation. The buyer "
           "entity 1414 MASSACHUSETTS AVENUE LLC is the address."),
]


def main(dry_run: bool):
    conn = engine.connect()
    n = 0
    for rid, side, sponsor, why in RESOLVE:
        cur = conn.execute(text(
            f"select {side}, coalesce({side}_canonical,'') from transactions "
            f"where id = :id"), {"id": rid}).first()
        if not cur:
            log.warning("id %s missing", rid)
            continue
        if cur[1]:
            log.info("id=%-5s %-6s already %s, skipped", rid, side, cur[1])
            continue
        log.info("id=%-5s %-6s %-34s -> %s", rid, side, (cur[0] or "")[:34], sponsor)
        if not dry_run:
            conn.execute(text(f"""
                update transactions
                   set {side}_canonical = :s, {side}_confidence = 'web_corroborated',
                       {side}_resolution_basis = 'web',
                       notes = coalesce(notes,'') || :n
                 where id = :id"""), {
                "s": sponsor, "id": rid,
                "n": f" | {side.upper()} RESOLVED. " + why})
            n += 1

    if not dry_run:
        for rid, note in NOTES:
            conn.execute(text(
                "update transactions set notes = coalesce(notes,'') || :n "
                "where id = :id"), {"n": " | " + note, "id": rid})
        conn.commit()

    log.info("\n%d sides written", n)
    tot = conn.execute(text(
        "select count(*) from transactions where coalesce(quarantined,0)=0")).scalar()
    for side in ("buyer", "seller"):
        v = conn.execute(text(
            f"select count(*) from transactions where coalesce(quarantined,0)=0 "
            f"and coalesce({side}_canonical,'') <> ''")).scalar()
        log.info("%s_canonical: %d of %d (%.0f%%)", side, v, tot, v / tot * 100)
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
