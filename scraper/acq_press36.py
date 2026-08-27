r"""Thirty-sixth pass. BP3 IS NOT BOSTON PROPERTIES, and the refusal proved it.

This is the strongest vindication of the confirm-first rule in the whole project,
because here the obvious reading was WRONG rather than merely unproven.

An earlier pass wrote, in full:

    55 SUMMER STREET BUYER: BP3-BOS5 IS NOT DECODED, AND DELIBERATELY SO. Boston
    Properties is confirmed in this table under the BP <ASSET> LLC convention,
    and the temptation to read BP3 the same way is exactly the RREF error in a
    new costume. The shape is different -- BP3 and BOS5 read as a fund number
    and an asset number within a series -- and nothing found names Boston
    Properties at 55 Summer Street.

BP3 IS PHASE 3 REAL ESTATE PARTNERS, a San Diego developer with no connection to
Boston Properties whatsoever. BLDUP states the entity and the firm in one
sentence: "SPRT Owner 55, LLC -- a subsidiary of Westbrook Partners -- sold 5
Channel Center Street to BP3-BOS3 5 CHANNEL STREET, LLC -- AN AFFILIATE OF
CALIFORNIA-BASED PHASE 3 REAL ESTATE PARTNERS -- for $35,000,000."

    Had "BP3 = Boston Properties" been written, four rows and $193.6 million
    would now be credited to a company that had nothing to do with any of them.
    The read on the SHAPE was right too: BP3 is the fund and BOS3/5/7/8 are
    Boston asset numbers within it.

THE WHOLE SERIES RESOLVES, and it is a coherent Fort Point lab-conversion play:

    BOS3   5 Channel Street     $ 35,000,000   Dec 2020   <- Westbrook Partners
    BOS5   55 Summer Street     $106,646,350   Mar 2022   <- Synergy Investments
    BOS7   12 Farnsworth St     $ 49,611,200   Nov 2021   <- BentallGreenOak
    BOS8   11 Sleeper Street    $  2,388,800   Nov 2021   <- BentallGreenOak

Bisnow and BLDUP: Phase 3 bought 12 Farnsworth -- a 104-year-old six-storey
brick-and-beam office with a ground-floor Flour Bakery, renovated 2016 -- TOGETHER
WITH 11 Sleeper Street, a 27-space car park, from BentallGreenOak. It converted
55 Summer, a ten-storey 115,000 SF tower, to life science by summer 2025.

RECP IS DLJ REAL ESTATE CAPITAL PARTNERS. CoStar: "DLJ Real Estate Capital
Partners Pays $77.5 Million for 18 Tremont", bought from Equus Capital Partners,
and DLJ sold to Jamestown for roughly $103 million in 2019. The seller entity on
that row is RECP V 18 TREMONT OWNER LLC -- fund V of that firm.

745 ATLANTIC AVENUE CLOSES A CHAIN THIS TABLE HAD ONLY GUESSED AT. A previous
note said the 2015 seller was "very probably Beacon, Charter Hall, or their
venture" and refused to write it. Bisnow, "Oxford Adds 745 Atlantic Ave to
Boston Portfolio": Oxford acquired it FROM BEACON CAPITAL for $114.5 million.
Beacon it was.

    python scraper/acq_press36.py --apply
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

BP3 = (
    "BP3 IS PHASE 3 REAL ESTATE PARTNERS, CONFIRMED AGAINST A NAMED PROPERTY, AND "
    "IT IS NOT BOSTON PROPERTIES. BLDUP names the entity and the firm in one "
    "sentence: \"SPRT Owner 55, LLC -- a subsidiary of Westbrook Partners -- sold "
    "5 Channel Center Street to BP3-BOS3 5 CHANNEL STREET, LLC -- an affiliate of "
    "California-based PHASE 3 REAL ESTATE PARTNERS -- for $35,000,000.\" An "
    "earlier pass in this table refused to decode BP3, writing that \"the "
    "temptation to read BP3 the same way as the confirmed BP <ASSET> LLC "
    "convention is exactly the RREF error in a new costume\" and that BP3 and "
    "BOS5 read as a fund number and an asset number. Both halves of that were "
    "right: the reading was wrong AND the shape was a fund-and-asset series. Had "
    "Boston Properties been written, four rows and $193.6 million would now be "
    "credited to a firm with no involvement in any of them. THE SERIES: BOS3 is 5 "
    "Channel Street, BOS5 is 55 Summer Street, BOS7 is 12 Farnsworth Street and "
    "BOS8 is 11 Sleeper Street -- a Fort Point and Downtown lab-conversion "
    "programme running from December 2020 to March 2022."
)

RESOLVE = [
    (1080, "buyer", "Phase 3 Real Estate Partners", BP3),
    (1080, "seller", "Westbrook Partners",
     'BLDUP, "Fort Point Office Building Acquired for $35 Million": "SPRT Owner '
     '55, LLC -- A SUBSIDIARY OF WESTBROOK PARTNERS -- sold 5 Channel Center '
     'Street" to Phase 3 for $35,000,000. The asset is a 50,000 SF historic '
     'rehabilitation. Westbrook now sells twice in this table, having also sold '
     '211 Congress Street to Drucker Associates in 2018.'),

    (962, "buyer", "Phase 3 Real Estate Partners", BP3 +
     " THIS ROW IS BOS5, 55 Summer Street at $106,646,350 in March 2022. Phase 3 "
     "completed the conversion of the ten-storey, 115,000 SF tower to Level 1 / "
     "Level 2 life-science use by summer 2025. The seller side already read "
     "Synergy Investments, written from BLDUP naming Hive Property Owner LLC as a "
     "Synergy subsidiary."),

    (1009, "buyer", "Phase 3 Real Estate Partners", BP3 +
     " THIS ROW IS BOS7, 12 Farnsworth Street at $49,611,200 in November 2021 -- "
     "a 104-year-old six-storey brick-and-beam office with a ground-floor Flour "
     "Bakery & Cafe, renovated in 2016, bought for conversion to life science."),
    (1009, "seller", "BentallGreenOak",
     'Bisnow\'s Boston deal sheet of 12 November 2021 and BLDUP, "Developer '
     'Acquires Fort Point Office Building for $49.6 Million": Phase 3 Real Estate '
     'Partners bought 12 Farnsworth Street TOGETHER WITH 11 Sleeper Street, a '
     '27-space car park, FROM BENTALLGREENOAK. Two rows in this table, one deal.'),

    (1008, "buyer", "Phase 3 Real Estate Partners", BP3 +
     " THIS ROW IS BOS8, 11 Sleeper Street at $2,388,800 in November 2021 -- the "
     "27-space car park bought alongside 12 Farnsworth Street in the same "
     "transaction. Written although the row is small, because a confirmed decode "
     "costs nothing to apply and the two rows are one deal."),
    (1008, "seller", "BentallGreenOak",
     'Same reporting: 11 Sleeper Street was acquired from BentallGreenOak '
     'alongside 12 Farnsworth Street.'),

    (1225, "seller", "DLJ Real Estate Capital Partners",
     'RECP IS DLJ REAL ESTATE CAPITAL PARTNERS, confirmed against the property. '
     'CoStar: "DLJ Real Estate Capital Partners Pays $77.5 Million for 18 '
     'Tremont", buying the twelve-storey building from Equus Capital Partners; '
     'Banker & Tradesman and Connect CRE then record Jamestown paying roughly $103 '
     'million for it in 2019, which is this row. The seller entity is RECP V 18 '
     'TREMONT OWNER LLC -- fund V of that firm. The chain now reads Equus -> DLJ '
     '($77.5M) -> Jamestown ($102.75M, 2019) -> Kendall Capital ($29.5M, 2026).'),

    (1533, "seller", "Beacon Capital Partners",
     'CLOSES A CHAIN THIS TABLE PREVIOUSLY REFUSED TO GUESS AT. The earlier note '
     'said the 2015 seller was "very probably Beacon, Charter Hall, or their '
     'venture" and declined to write any of them. Bisnow, "Oxford Adds 745 '
     'Atlantic Ave to Boston Portfolio": Oxford Properties acquired the building '
     'FROM BEACON CAPITAL for $114.5 million, closing 29 May 2015 -- the Globe '
     'covered it as "745 Atlantic Ave. purchased for $114.5 million". The '
     'eleven-storey building holds 166,000 SF of creative office and 8,000 SF of '
     'retail, let to WeWork and Cambridge Consultants. Charter Hall Office REIT '
     'had been Beacon\'s co-owner from the 2008 $1.7 billion portfolio deal but is '
     'not named in the 2015 reporting, so only Beacon is written.'),
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
            basis = "prefix_confirmed" if "BP3 IS PHASE 3" in why else "web"
            conf = ("registry_confirmed" if basis == "prefix_confirmed"
                    else "web_corroborated")
            conn.execute(text(f"""
                update transactions
                   set {side}_canonical = :s, {side}_confidence = :c,
                       {side}_resolution_basis = :b,
                       notes = coalesce(notes,'') || :n
                 where id = :id"""), {
                "s": sponsor, "id": rid, "c": conf, "b": basis,
                "n": f" | {side.upper()} RESOLVED. " + why})
            n += 1

    for side in ("buyer", "seller"):
        for rid, ent in conn.execute(text(f"""
                select id, {side} from transactions
                 where coalesce(quarantined,0) = 0
                   and upper(coalesce({side},'')) like 'BP3-%'
                   and coalesce({side}_canonical,'') = ''""")):
            log.info("id=%-5s %-6s %-34s -> Phase 3 Real Estate Partners "
                     "[family sweep]", rid, side, (ent or "")[:34])
            if not dry_run:
                conn.execute(text(f"""
                    update transactions
                       set {side}_canonical = 'Phase 3 Real Estate Partners',
                           {side}_confidence = 'registry_confirmed',
                           {side}_resolution_basis = 'prefix_confirmed',
                           notes = coalesce(notes,'') || :n
                     where id = :id"""),
                    {"id": rid, "n": f" | {side.upper()} RESOLVED. " + BP3})
                n += 1

    if not dry_run:
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
