r"""Nineteenth pass. Boston Landing, and two leads recorded rather than written.

Both Guest Street rows resolve from the same corner of Brighton, and both are
joint ventures that a single-partner record would have flattened.

    20 Guest St   Griffith Properties / Artemis Real Estate Partners  $72,000,000
                  Bisnow, "New Balance Sells Former World HQ For $72M": NB
                  Development sold the 228,000 SF building -- New Balance's
                  headquarters from 2000 until it moved across the Turnpike in
                  2015 -- to a joint venture of Boston-based Griffith Properties
                  and Washington-based Artemis Real Estate Partners. Fully let,
                  to Harvard Business Publishing, Major League Lacrosse and
                  ButcherBox among others.
    60 Guest St   Lendlease / Ivanhoe Cambridge                       $67,000,000
                  NEREJ, "NB Development sells Boston Landing life science site
                  for $67 million", and Boston Real Estate Times naming the
                  buyers: a fully approved life-science development parcel,
                  320,000 SF, sold through JLL.

Griffith Properties was ALREADY in this table, written by the care-of pass as
"Griffith Properties Llc" on this very row. The press both confirms it and shows
it was incomplete -- Artemis was the other half and the care-of line could never
have said so. That is the case for the whole cleanup in one row.

343 CONGRESS IS A LEG OF A $205 MILLION PORTFOLIO. Berkeley Investments sold its
Fort Point portfolio to BGO in 2015; this row is one building at $63,500,000,
and the buyer side already read MEPT / BentallGreenOak. The Real Reporter's
later brief, "BGO Sells 343 Congress to North Colony", closes the loop from the
other end a decade on.

TWO LEADS DELIBERATELY NOT WRITTEN. Both would raise the resolution rate and
both rest on an inference rather than a statement, which is the distinction this
table is built on.

    350 Washington  Invesco bought it in 2011 for $128M from Real Estate Capital
                    Partners, and Nuveen bought it in 2019 -- but Nuveen released
                    neither seller nor price, and "the 2011 buyer is probably the
                    2019 seller" is the two-step inference that produced the
                    RREF/Rialto error.
    Cambridge St    Harvard finalised a CSX Transportation land deal in 2016 at
                    $147.4M; this row is $128,878,288. Close, and not the same
                    number.

    python scraper/acq_press19.py --apply
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
    (1098, "buyer", "Griffith Properties / Artemis Real Estate Partners", "web",
     'Bisnow, "New Balance Sells Former World HQ For $72M": NB Development sold '
     'the 228,000 SF building at 20 Guest Street -- New Balance\'s world '
     'headquarters from its development in 2000 until the company moved to its '
     '250,000 SF home overlooking the Turnpike in 2015 -- to a joint venture of '
     'Boston-based GRIFFITH PROPERTIES and Washington-based ARTEMIS REAL ESTATE '
     'PARTNERS. The building was fully let, to Harvard Business Publishing, the '
     'Major League Lacrosse headquarters and ButcherBox among others. THIS ROW '
     'PREVIOUSLY READ "Griffith Properties Llc" ALONE, written from the '
     'assessment roll\'s care-of line: the firm was right and the venture was '
     'half missing, which a care-of line can never tell you. The seller side '
     'already read NB Development Group.'),

    (1063, "buyer", "Lendlease / Ivanhoe Cambridge", "web",
     'NEREJ, "NB Development sells Boston Landing life science site for $67 '
     'million", and Boston Real Estate Times, "Boston Landing life science '
     'development site trades for $67 million, bought by Lendlease and Ivanhoe '
     'Cambridge partnership": JLL Capital Markets closed the sale of 60 Guest '
     'Street, a parcel fully approved for life-science development on the Boston '
     'Landing campus, enabling 320,000 SF. Recorded as the venture, not one '
     'partner. The seller side already read NB Development Group.'),

    (1490, "seller", "Berkeley Investments", "web",
     'Berkeley Investments sold its Fort Point portfolio to BGO in 2015 in a $205 '
     'million portfolio acquisition, and Berkeley had held 343 Congress Street '
     'since 2003, converting the garage on the property to Class A office. This '
     'row is $63,500,000 in December 2015 and the buyer side already read MEPT / '
     'BentallGreenOak, which is BGO. PORTFOLIO ALLOCATION, not a standalone '
     'trade. The Real Reporter closes the loop from the far end: "BGO Sells 343 '
     'Congress to North Colony" at roughly $48M in 2025, and North Colony Asset '
     'Management is separately present in this table.'),
]

NOTES = [
    (1160, "350 WASHINGTON STREET SELLER: A LEAD, NOT A RESOLUTION. The buyer is "
           "confirmed -- NEREJ and BLDUP both have Nuveen Real Estate acquiring "
           "the 147,273 SF Downtown Crossing flagship retail asset for "
           "$134,211,350 on 11 October 2019. But NUVEEN RELEASED NEITHER THE "
           "SELLER NOR THE PRICE in its own announcement. The available chain is "
           "that Invesco bought 350 Washington in 2011 for $128 million from Real "
           "Estate Capital Partners, which makes Invesco the likely 2019 seller. "
           "LIKELY IS NOT RESOLVED: inferring a seller from an earlier buyer plus "
           "the absence of a known intervening trade is exactly the two-step "
           "inference that produced the RREF/Rialto error, and the record entity "
           "TR 350 WASHINGTON CORP matches neither name. Kingston Investors Corp "
           "and Eastern Real Estate also carry the building on their own pages, "
           "which is three candidates, not one."),
    (1372, "254 SUMMER STREET SELLER, $62,200,000, June 2017: NOT FOUND. The "
           "buyer side already reads Morgan Stanley Prime Property Fund. City "
           "assessment records confirm CHANNEL HOLDINGS LLC as the owner of both "
           "254 and 256-260 Summer Street, matching the seller entity on this row, "
           "but nothing found names the firm behind it. Fort Point coverage from "
           "2017 is dominated by the larger Congress Street and A Street trades."),
    (962, "55 SUMMER STREET BUYER, $106,646,350, March 2022: NOT FOUND. The "
          "seller is established -- BLDUP names Hive Property Owner LLC as a "
          "Synergy Investments subsidiary -- but the same article does not name "
          "the buyer, and BLDUP now returns HTTP 403 to direct fetches so the "
          "full text could not be read. The entity BP3-BOS5 55 SUMMER STREET LLC "
          "remains undecoded: BP3 and BOS5 read as a fund number and an asset "
          "number, and although Boston Properties is confirmed elsewhere in this "
          "table under a BP <ASSET> LLC convention, nothing names it at 55 "
          "Summer Street."),
]


def main(dry_run: bool):
    conn = engine.connect()
    n = 0
    for rid, side, sponsor, basis, why in RESOLVE:
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
