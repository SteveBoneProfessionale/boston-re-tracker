r"""Forty-fifth pass. Marketplace Center's seller, found in a trade wire.

200 STATE STREET RETAIL, after four attempts. S&P Global Market Intelligence,
"Gazit-Globe unit buys Boston retail property for $82M": Gazit Horizons Inc
purchased the retail property beside Faneuil Hall Marketplace FROM CLARENDON
GROUP USA INC on 24 April 2019 for $81.8 million. This row is $81,800,000 in
April 2019 with Gazit already on the buy side.

    The Globe covered it as "Israeli company buys Quincy Market retail space",
    The Real Reporter as "Marketplace Center in Faneuil Hall Trades to Gazit
    Horizon for $81.8M", and Gazit's own portfolio page carries it -- none of
    those three named the seller. A wire service summarising the buyer's filing
    did. Where four consumer-facing outlets report a deal and none names the
    counterparty, the party that has to disclose is the one to chase.

    Clarendon Group was already a canonical in this table from separate press,
    so the name and the entity now agree without either being used to prove the
    other.

AND THE FANEUIL HALL CONFUSION IS SETTLED IN BOTH DIRECTIONS. An earlier pass
warned that Marketplace Center and Faneuil Hall Marketplace are different assets
and that Ashkenazy -- present in this table as the Fairmont Copley Plaza buyer --
could easily be attached here in error. Confirmed: Ashkenazy held the GROUND
LEASE on Faneuil Hall Marketplace, sold it to J. Safra Group in 2024, and was
never a party to Marketplace Center.

    python scraper/acq_press45.py --apply
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
    (1213, "seller", "Clarendon Group",
     'S&P Global Market Intelligence, "Gazit-Globe unit buys Boston retail '
     'property for $82M": Gazit Horizons Inc purchased the retail property close '
     'to Faneuil Hall Marketplace FROM CLARENDON GROUP USA INC on 24 April 2019 '
     'for $81.8 million -- this row exactly. FOUR ATTEMPTS AND A LESSON ABOUT '
     'SOURCES: the Boston Globe ("Israeli company buys Quincy Market retail '
     'space"), The Real Reporter ("Marketplace Center in Faneuil Hall Trades to '
     'Gazit Horizon for $81.8M"), Boston Real Estate Times and Gazit\'s own '
     'portfolio page all covered this deal and NONE of them named the seller. A '
     'wire service summarising the listed buyer\'s disclosure did. Where several '
     'consumer-facing outlets report a trade and none names the counterparty, the '
     'party with a disclosure obligation is the one to chase. Clarendon Group was '
     'already a canonical in this table from separate reporting, so the entity and '
     'the name agree independently. THE ASSET is a 62,000 SF retail condominium on '
     'the Rose Kennedy Greenway, 100% let to a mix including Banana Republic, LOFT '
     'and American Eagle -- and it is NOT Faneuil Hall Marketplace itself, whose '
     'ground lease Ashkenazy held and sold to J. Safra Group in 2024. Ashkenazy '
     'appears in this table as the Fairmont Copley Plaza buyer and was never a '
     'party here.'),
]

NOTES = [
    (1540, "1345 BOYLSTON STREET SELLER: SAMUELS & ASSOCIATES IS A LEAD AFTER "
           "THREE SEARCHES, AND THE ADDRESS IS SLIGHTLY OFF TOO. The buyer is "
           "certain: the entity is TARGET CORPORATION, buying its own store. The "
           "store is the three-level, 170,000 SF CityTarget inside a thirteen-storey "
           "mixed-use building with 172 apartments that opened in 2015, developed "
           "by SAMUELS & ASSOCIATES -- which the coverage addresses as 1325 "
           "BOYLSTON STREET while this parcel record says 1345. That makes Samuels "
           "the obvious seller of the retail condominium and it is still not "
           "written: no source names a party to this conveyance, the seller field "
           "is EMPTY rather than merely undecoded, and \"the developer of the "
           "building probably sold the unit in it\" is the same inference refused "
           "at 131 Seaport Boulevard, where Cottonwood is the equivalent lead."),
    (1430, "CAMBRIDGE STREET SELLER, $128,878,288, November 2016: THIRD SEARCH, "
           "NOT FOUND, AND THE GEOGRAPHY NOW MAKES SENSE EVEN IF THE PARTY DOES "
           "NOT. The buyer is Harvard University and the parcel is Commercial "
           "Land. Harvard Magazine and the Crimson establish that CAMBRIDGE STREET "
           "IS THE ALLSTON CORRIDOR -- Harvard's 91-acre Massachusetts Turnpike "
           "Authority parcel, bought for $75 million, lies south of Cambridge "
           "Street -- so a nine-figure Harvard land purchase on that street in "
           "2016 is entirely consistent with the assembly the Crimson calls a "
           "decades-long land grab. THE NEAR MISS STANDS: Harvard finalised a CSX "
           "TRANSPORTATION deal in 2016 at $147.4 million, a different figure from "
           "this row. Close enough to tempt, far enough apart to be a different "
           "conveyance or a different allocation, and the seller field is empty."),
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
