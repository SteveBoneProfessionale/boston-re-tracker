r"""Seventh pass. A JV that kept a partner on both sides, and a $/SF fault.

535-545 BOYLSTON: BRICKMAN IS ON BOTH SIDES, AND IT IS NOT A QUARANTINE. The
row read buyer "Brickman" and an unresolved seller. The press is more
interesting than that. Shimizu Corporation's own release, "Capital Security
Advisors, Brickman Associates, and Shimizu Jointly Acquire Two Office Buildings
in Boston", and DLA Piper's note on the $148M acquisition, establish:

    buyer   Shimizu Realty Development (USA) + Capital Security Advisors
            + Brickman Associates      -- entity SCB BOYLSTON PO LLC, whose
                                          initials are those three firms
    seller  Investcorp International Realty + Brickman Associates

Brickman sold and stayed. That is not one company selling to itself -- Investcorp
genuinely exited and two new firms genuinely entered -- so it is not quarantined.
But it is not a clean whole-asset trade either, and recording only "Brickman" as
the buyer hid the fact entirely. Both ventures are now recorded in full, which is
the joint-venture rule doing exactly the work it exists to do.

TWO PRICES, BOTH REAL, AGAIN. Newmark's release says it arranged a $128 million
sale; DLA Piper says a US$148 million acquisition; this row records $148,000,000.
The row is not adjusted. This is the third row in this table carrying two
published figures, after Fairmont Copley ($163M/$170M) and 125 Broadway
($592M/$603M), and the pattern is consistent enough to be worth naming: the
brokerage reports the asset price and the buyer's counsel reports total
consideration.

1 HAMPSHIRE STREET IS PART OF A BUILDING, NOT A BUILDING. Bisnow, "Alexandria
Buys Chunk Of Kendall Square Lab Building For $120M": Alexandria bought part of
the third floor and all of the fourth floor from Schlumberger on 28 June 2022.
The row is typed as a whole asset_sale.

AND THAT EXPOSES A FAULT IN price_per_sf THAT IS WORTH MORE THAN THE TWO ROWS.
building_sf on a spine row is the PARCEL's recorded area. On a condominiumised
or fragmented parcel that is not the asset's area, so the derived $/SF is
meaningless. Two proofs in this batch alone:

    1 Hampshire St        20,718 SF -> $5,792/SF
    160-170 N Washington   7,200 SF -> $20,833/SF   (the Converse headquarters
                                                     at Lovejoy Wharf is 214,000
                                                     SF; press puts the deal at
                                                     roughly $800/SF)

This pass flags every row whose $/SF is implausible rather than silently leaving
them to distort any per-foot comparison.

    python scraper/acq_press7.py --apply
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

# $/SF above this cannot describe a real Boston or Cambridge asset trade. The
# record high is roughly $1,100/SF for trophy office and about $1,800/SF for
# the very best lab; 2,500 leaves a wide margin and still catches the faults.
PSF_CEILING = 2500

RESOLVE = [
    (1471, "buyer", "Union Investment",
     'NorthEndWaterfront, "One Lovejoy Wharf Building, Converse Headquarters, '
     'Sold by Related Beal to Union Investment", and Boston Office Spaces, "New '
     'Converse Building Sold for $150M": the 214,000 SF Converse world '
     'headquarters at 160 North Washington Street traded in spring 2016 at about '
     '$800 per square foot. This is Union Investment\'s SECOND Boston purchase in '
     'this table within a month -- 101 Seaport closed in April 2016 for $452M -- '
     'which is a useful independent check on both rows.', True),
    (1471, "seller", "Related Beal",
     'Same reporting: Related Beal developed One Lovejoy Wharf and sold it. This '
     'also corroborates the RREF correction from the opposite direction: Related '
     'Beal is demonstrably an active Boston seller in exactly this period, which '
     'the Rialto attribution would have obscured.', True),

    (1160, "buyer", "Nuveen Real Estate",
     'NEREJ, "Nuveen Real Estate acquires 147,273 s/f retail asset at 350 '
     'Washington", and BLDUP, "Downtown Crossing Building Sells for $134.21 '
     'Million", which matches this row to the dollar at $134,211,350 on 11 '
     'October 2019. The asset is the class A flagship retail anchored by '
     'Marshalls, HomeGoods, TJ Maxx and Boston Sports Club. This also CONFIRMS '
     'THE TREA PREFIX against a named property: the entity is TREA 350 WASHINGTON '
     'STREET LLC and Nuveen is the manager of the TIAA Real Estate Account. '
     'Confirmed, not decoded -- the press named the firm at this address first.',
     True),

    (971, "buyer",
     "Shimizu Realty Development / Capital Security Advisors / Brickman Associates",
     'CORRECTION AND COMPLETION. This row read "Brickman" alone. Shimizu '
     'Corporation\'s own release is titled "Capital Security Advisors, Brickman '
     'Associates, and Shimizu Jointly Acquire Two Office Buildings in Boston", and '
     'DLA Piper records representing Shimizu Realty Development (U.S.A.) Inc in '
     'its US$148 million acquisition of 535-545 Boylston Street. The record entity '
     'SCB BOYLSTON PO LLC carries all three initials. Under the rule that a joint '
     'venture resolves to every partner rather than the most visible one, all '
     'three are recorded. BrightSpire Capital provided $100.2M of acquisition '
     'financing; the partners planned to convert up to three floors to lab.', True),
    (971, "seller", "Investcorp International Realty / Brickman Associates",
     'Same reporting, plus Citybizlist. The selling venture was Investcorp '
     'International Realty and Brickman Associates. NOTE THAT BRICKMAN IS ON BOTH '
     'SIDES: it sold as Investcorp\'s partner and stayed in as Shimizu\'s and '
     'Capital Security\'s. This is NOT quarantined as an affiliated transfer, '
     'because Investcorp genuinely exited and two unrelated firms genuinely '
     'entered, so real value changed hands at arm\'s length. But it is a partial '
     'continuation rather than a clean whole-asset trade, and recording only '
     '"Brickman" as the buyer concealed that completely.', True),

    (1587, "seller", "Schlumberger",
     'Bisnow, "Alexandria Buys Chunk Of Kendall Square Lab Building For $120M", '
     'and Traded: Alexandria Real Estate Equities acquired part of the third floor '
     'and all of the fourth floor of 1 Hampshire Street from Schlumberger on 28 '
     'June 2022 for $120,000,000. The buyer side already read Alexandria, from the '
     'entity ARE-MA REGION NO 103 HOLDING LLC.', False),

    (1585, "seller", "CPI / Brickman Associates",
     'REBusinessOnline, "Brickman Associates Takes Blackstone Science Square in '
     'Cambridge", and Brickman\'s own site, which carries the Blackstone Square '
     'assets: 237 Putnam Avenue is Blackstone Science Square, and the record '
     'entity is CPI/BRICKMAN BSS OWNER LLC, where BSS is Blackstone Science '
     'Square. Brickman is confirmed against the named property. CPI IS LEFT AS THE '
     'RECORD RENDERS IT, not expanded: no source found names what it stands for, '
     'and inventing an expansion is the RREF error. Both partners appear because a '
     'joint venture resolves to all of them, even when one of them can only be '
     'given as its initials. Brickman also appears on both sides of 535-545 '
     'Boylston Street in this table, two Februaries apart.', False),
]

NOTES = [
    (1459, "36-44 BROAD STREET, $150,300,000, June 2016. NOT WRITTEN, BECAUSE THE "
           "EVIDENCE CONFLICTS. The seller of record is TRANSWESTERN BROAD ST LLC, "
           "which names Transwestern on its face and would normally resolve on "
           "the self-identifying rule. But the reporting found says Transwestern "
           "Investment Co bought 40 Broad Street in 2006 for $50M and SOLD it to "
           "TIAA-CREF in 2013 for $110M, which would make TIAA, not Transwestern, "
           "the owner in 2016. Either the 2013 report describes a different Broad "
           "Street building, or Transwestern retained or reacquired an interest, "
           "or the entity kept its name through a change of control the way FELCOR "
           "COPLEY PLAZA OWNER did after the RLJ merger. A self-identifying name "
           "is only safe when nothing contradicts it, and here something does."),
    (1160, "350 WASHINGTON STREET SELLER: TWO LEADS, NEITHER WRITTEN. The record "
           "entity is TR 350 WASHINGTON CORP. Both Kingston Investors Corp and "
           "Eastern Real Estate carry 350 Washington Street on their own project "
           "pages, and neither page states a disposition date, so either could be "
           "the 2019 seller or a former owner or a development partner. Two "
           "candidates with equal support is not a resolution."),
]


def main(dry_run: bool):
    conn = engine.connect()
    n = 0
    for rid, side, sponsor, why, force in RESOLVE:
        cur = conn.execute(text(
            f"select {side}, coalesce({side}_canonical,'') from transactions "
            f"where id = :id"), {"id": rid}).first()
        if not cur:
            log.warning("id %s missing", rid)
            continue
        if cur[1] and not force:
            log.info("id=%-5s %-6s already %s, skipped", rid, side, cur[1])
            continue
        tag = f"(WAS {cur[1]}) " if cur[1] else ""
        log.info("id=%-5s %-6s %-34s -> %s%s", rid, side, (cur[0] or "")[:34],
                 tag, sponsor)
        if not dry_run:
            conn.execute(text(f"""
                update transactions
                   set {side}_canonical = :s, {side}_confidence = 'web_corroborated',
                       {side}_resolution_basis = 'web',
                       notes = coalesce(notes,'') || :n
                 where id = :id"""), {
                "s": sponsor, "id": rid,
                "n": f" | {side.upper()} RESOLVED FROM PRESS. " + why})
            n += 1

    # --- the $/SF fault sweep -------------------------------------------------
    bad = conn.execute(text(f"""
        select id, address, price, building_sf, price_per_sf
          from transactions
         where coalesce(quarantined,0) = 0
           and coalesce(price_per_sf,0) > {PSF_CEILING}
         order by price_per_sf desc""")).fetchall()
    log.info("\n%d rows carry a $/SF above $%s, which no Boston or Cambridge "
             "asset trade reaches:", len(bad), f"{PSF_CEILING:,}")
    for r in bad[:15]:
        log.info("   id=%-5s $%-13s %8s SF  $%9s/SF  %s", r[0], f"{r[2]:,}",
                 f"{r[3]:,}", f"{r[4]:,.0f}", r[1][:30])

    if not dry_run:
        for rid, note in NOTES:
            conn.execute(text(
                "update transactions set notes = coalesce(notes,'') || :n "
                "where id = :id"), {"n": " | " + note, "id": rid})
        conn.execute(text(
            "update transactions "
            "set price_caveat = coalesce(price_caveat || ' ', '') || :c "
            "where id = 1587"), {
            "c": ("Price is for part of a building: part of the third floor and "
                  "all of the fourth. Recorded area and $/SF do not describe "
                  "the whole asset.")})
        for r in bad:
            conn.execute(text("""
                update transactions
                   set psf_unreliable = 1,
                       notes = coalesce(notes,'') || :n
                 where id = :id"""), {
                "id": r[0],
                "n": (" | PRICE PER SQUARE FOOT IS UNRELIABLE ON THIS ROW. It "
                      "computes to $%s/SF from a recorded area of %s SF, and no "
                      "Boston or Cambridge asset has traded near that. "
                      "building_sf on a spine row is the PARCEL's recorded area, "
                      "which on a condominiumised or fragmented parcel is a "
                      "portion of the asset rather than the asset. The price is "
                      "not in question; the denominator is. Proven on this row's "
                      "neighbours: 160-170 N Washington records 7,200 SF for the "
                      "214,000 SF Converse headquarters, which press puts at "
                      "about $800/SF, not $20,833."
                      % (f"{r[4]:,.0f}", f"{r[3]:,}"))})
        conn.commit()

    log.info("\n%d sides written", n)
    tot = conn.execute(text(
        "select count(*) from transactions where coalesce(quarantined,0)=0")).scalar()
    for side in ("buyer", "seller"):
        v, d = conn.execute(text(
            f"select count(*), sum(coalesce(price,0)) from transactions "
            f"where coalesce(quarantined,0)=0 and coalesce({side}_canonical,'') <> ''"
        )).first()
        log.info("%s_canonical: %d of %d (%.0f%%), $%.2fB", side, v, tot,
                 v / tot * 100, (d or 0) / 1e9)
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
