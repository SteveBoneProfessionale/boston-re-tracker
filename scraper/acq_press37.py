r"""Thirty-seventh pass. An ambiguity resolved, and three traps written down.

35-45 MORRISSEY BOULEVARD: the earlier refusal was right and the answer was the
entity. That note said the source described Star Market and Beasley as "under
long term lease agreements", which was "equally consistent with them being
SALE-LEASEBACK VENDORS or SITTING TENANTS whose landlord sold over their heads",
and left the seller blank rather than choose.

They were sitting tenants. The Dorchester Reporter, 8 August 2018: "Center Court
Properties acquired two major parcels through its CC 35-55 Morrissey LLC ... for
a combined $56 million on Aug. 6, 2018, according to recorded deeds. The
development company purchased the lots from MORRISSEY HOLDINGS." The seller of
record on this row is MORRISSEY HOLDINGS LLC -- it named itself the whole time,
and the risk was writing a tenant over it.

THREE TRAPS RECORDED SO THEY ARE NOT WALKED INTO LATER.

    MARKETPLACE CENTER IS NOT FANEUIL HALL MARKETPLACE. They adjoin, they sound
    alike, and they have different owners and different deal histories. Gazit
    Horizons bought Marketplace Center -- 62,000 SF of retail let to Banana
    Republic, LOFT and American Eagle -- for $81.8M in 2019. Ashkenazy held the
    GROUND LEASE on Faneuil Hall Marketplace, bought from General Growth in
    2011 and sold to J. Safra Group in 2024. Ashkenazy is separately in this
    table as the buyer of the Fairmont Copley Plaza, which makes the collision
    live rather than hypothetical.

    ONE DALTON'S HOTEL HAS 215 ROOMS AND THIS ROW IS $215,000,000. Those are
    unrelated numbers that will look like corroboration to anyone skimming.
    Carpenter & Company developed the 61-storey tower in partnership with Four
    Seasons, which makes FSBOS (US) LLC more tempting, not less -- and the BP3
    case, where the obvious reading was flatly wrong, is why it stays refused.

    50 POST OFFICE SQUARE CARRIES A THIRD PRICE. This table records
    $285,000,000; LaSalle's acquisition was reported at "more than $290
    million". Eighth row in this table with two published figures.

    python scraper/acq_press37.py --apply
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
    (1284, "seller", "Morrissey Holdings",
     'RESOLVES AN AMBIGUITY AN EARLIER PASS DELIBERATELY LEFT OPEN. That note said '
     'the reporting described Star Market and Beasley Media Group as "under long '
     'term lease agreements", which was equally consistent with their being '
     'sale-leaseback vendors or sitting tenants, and refused to choose. They were '
     'TENANTS. The Dorchester Reporter, 8 August 2018: "Center Court Properties '
     'acquired two major parcels through its CC 35-55 Morrissey LLC, purchasing '
     'the Star Market at 35 Morrissey Blvd. and the Beasley Media Group Boston '
     'building at 55 Morrissey Blvd. for a combined $56 million on Aug. 6, 2018, '
     'according to recorded deeds. The development company purchased the lots from '
     'MORRISSEY HOLDINGS." The seller of record here is MORRISSEY HOLDINGS LLC, '
     'which named itself all along; the risk was writing a tenant over it. The '
     'buyer entity POB CC 35-55 MORRISSEY LLC matches the vehicle the paper names.'),
]

NOTES = [
    (1213, "MARKETPLACE CENTER IS NOT FANEUIL HALL MARKETPLACE, and this row is "
           "the former. The two adjoin, sound alike and have entirely separate "
           "ownership. The Real Reporter, \"Marketplace Center in Faneuil Hall "
           "Trades to Gazit Horizon for $81.8M\", and Gazit's own portfolio page: "
           "Gazit bought the 62,000 SF retail building in April 2019, 100% let to "
           "a mix including Banana Republic, LOFT and American Eagle. FANEUIL HALL "
           "MARKETPLACE ITSELF is a GROUND LEASE that Ashkenazy Acquisition Corp "
           "bought from General Growth Properties in 2011 and sold to J. Safra "
           "Group in early 2024, after a public dispute with the City. THE "
           "COLLISION IS LIVE, NOT HYPOTHETICAL: Ashkenazy is separately in this "
           "table as the buyer of the Fairmont Copley Plaza, so a future pass "
           "could easily attach it here. SELLER STILL NOT FOUND: no source names "
           "the party behind MARKETPLACE CENTER ASSOC LLC."),
    (1199, "1 DALTON STREET, $215,000,000, May 2019: STILL REFUSED, AND THERE IS "
           "A NUMERICAL TRAP ON THIS ROW. The Four Seasons Hotel at One Dalton has "
           "215 ROOMS. This row records $215,000,000. Those figures are unrelated "
           "and will look like corroboration to anyone skimming. Carpenter & "
           "Company developed the 61-storey, 742-foot tower IN PARTNERSHIP WITH "
           "FOUR SEASONS as part of a $700 million project, which makes the record "
           "entity FSBOS (US) LLC more tempting rather than less. It stays "
           "undecoded for the reason the BP3 case just demonstrated: at 5 Channel "
           "Street the obvious reading of BP3 was Boston Properties and the answer "
           "was Phase 3 Real Estate Partners, a San Diego firm with no connection "
           "to it. A partnership between two firms does not establish which of "
           "them an entity belongs to."),
    (1502, "50 POST OFFICE SQUARE CARRIES TWO PUBLISHED PRICES. This row records "
           "$285,000,000 for December 2015; LaSalle's acquisition on behalf of a "
           "US separate-account client is reported at \"more than $290 million\". "
           "Eighth row in this table with a registry figure and a press figure "
           "describing the same deal, and consistent with the pattern the AC Hotel "
           "row explains: recorded consideration versus total consideration. THE "
           "SELLER REMAINS UNWRITTEN. Bentall Kennedy is still a lead only -- "
           "successor to Kennedy Associates, which bought the tower from Verizon "
           "for $192M in 2008, and named by NEREJ selecting Suffolk to renovate "
           "the building before the 2015 trade -- but four searches have now "
           "failed to find any source reporting it SELLING."),
    (1296, "100-170 MEADOW ROAD, $64,000,000, July 2018: SEARCHED AGAIN, STILL "
           "NOT REPORTED, AND THE INTERNAL-TRANSFER QUESTION HARDENS. National "
           "Development's own case study records it acquiring the 450,000 SF "
           "industrial complex on 72+ acres in Readville -- Boston Business Park, "
           "addressed 200 Meadow Road -- in 2015, three years before this "
           "conveyance. Both entities on this row name the same asset: CPT BOSTON "
           "BUSINESS PARK LLC bought from 100 BBP LLC. A 2018 transfer between two "
           "Boston Business Park vehicles, with National Development already the "
           "owner, is most likely a partner buyout or an intra-sponsor "
           "restructuring. NOT QUARANTINED, because the shared element is the "
           "ASSET NAME -- the collision proven innocent at Porter Square and "
           "American Twine -- and no source names a party."),
    (1538, "50-60 STANIFORD STREET BUYER, $123,300,000, April 2015: SEARCHED "
           "TWICE, NOT FOUND. The seller is established as Equity Residential from "
           "NEREJ, which reports Cushman & Wakefield acting on its behalf in the "
           "$123.3 million sale of the ten-storey, 193,230 SF class A medical "
           "office building on the Massachusetts General Hospital campus -- 100% "
           "let, anchored by MGH at 74%, with Ophthalmic Consultants of Boston and "
           "Boston Eye Surgery. NEITHER OF THE TWO ARTICLES FOUND NAMES THE BUYER, "
           "and the entity RAR2 50 STANIFORD LLC is a fund series that is not "
           "decoded."),
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
        conn.execute(text(
            "update transactions "
            "set price_caveat = coalesce(price_caveat || ' ', '') || :c "
            "where id = 1502"), {
            "c": ("LaSalle's December 2015 acquisition of 50 Post Office Square "
                  "is reported at more than $290 million; this row records "
                  "$285,000,000.")})
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
