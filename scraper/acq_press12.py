r"""Twelfth pass. Two more prefixes confirmed, and a nine-property portfolio leg.

IRE-RE = INVESCO REAL ESTATE, confirmed against a named property. NEREJ,
"Newmark Knight Frank brokers $107 million sale of 226 Causeway to Rockpoint
Group", with CPE and REBusinessOnline reporting the same October 2018 deal:
Rockpoint bought the 192,890 SF retail and office condominium -- TripAdvisor's
headquarters, Oxfam America and the Boston Celtics were the tenants -- from
Invesco, which had paid $91.7M for it in 2015. The seller entity on this row is
IRE-RE CAUSEWAY LLC. Invesco is independently in this table at 179 Lincoln
Street under a different convention, INVESCO IF IV, which names the firm
outright.

TA <ASSET> LLC = TA REALTY, confirmed against a named property. Cambridge Day
and The Real Reporter both report TA Realty buying Porter Square Shopping Center
in May 2022 as one of NINE sites in a $390 million portfolio from Gravestar,
with Wilder of Boston as its partner and operator. This row is $112,476,000 in
May 2022 with a buyer entity of TA PORTER SQUARE LLC. TA Realty was already in
this table at 131 Dartmouth Street, established there from the ownership record
rather than from a name, so the two agree.

    PORTFOLIO ALLOCATION AGAIN. $112,476,000 is Porter Square's share of a $390
    million nine-property deal. That is the third portfolio-allocation row found
    in this table -- after Canal Park ($304M across three) and Club Quarters
    ($410M across a national portfolio) -- and it is worth saying plainly that a
    registry price is the price of a PARCEL, not of a DEAL.

AMERICAN TWINE IS RECORDED AT THE WRONG STREET, AND THE ROW IS STILL RIGHT.
Connect CRE and Bisnow report New England Development buying the American Twine
Office Park in East Cambridge from Transatlantic Investment Management for $87
million, deed registered 3 June 2019, at about $758/SF. The press calls the
complex 222 Third Street; this row is addressed 165 Second Street, which is the
historic American Net and Twine Company Factory. Same complex, several street
frontages, one parcel record. The entities on both sides say AMERICAN TWINE, and
the price and month match exactly.

    python scraper/acq_press12.py --apply
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
    (1271, "buyer", "Rockpoint Group", "web",
     'NEREJ, "Newmark Knight Frank brokers $107 million sale of 226 Causeway to '
     'Rockpoint Group", with CPE ("Boston Mixed-Use Building Commands $107M") and '
     'REBusinessOnline reporting the same October 2018 deal. The asset is a '
     '192,890 SF retail and office condominium let to TripAdvisor, Oxfam America '
     'and the Boston Celtics. Rockpoint is separately established in this table at '
     '75-101 Federal Street.'),
    (1271, "seller", "Invesco Real Estate", "prefix_confirmed",
     'Same reporting names Invesco as the seller; Invesco had bought the building '
     'from Spear Street Capital in 2015 for $91.7 million. THIS CONFIRMS IRE-RE AS '
     'INVESCO REAL ESTATE against a named property -- the seller entity is IRE-RE '
     'CAUSEWAY LLC -- and Invesco is independently present in this table at 179 '
     'Lincoln Street under the different convention INVESCO IF IV, which names the '
     'firm outright.'),

    (1589, "buyer", "TA Realty / Wilder", "prefix_confirmed",
     'Cambridge Day, "Porter Square Shopping Center has been sold off by '
     'Gravestar, one of nine sites in a $390M deal", and The Real Reporter, "TA '
     'Realty With Massive $390M 9-Building Shopping Center Purchase from '
     'Gravestar": TA Realty of Boston bought the centre in May 2022 for '
     '$112,476,000 -- this row to the dollar -- with Wilder of Boston as partner '
     'and operator of the retail. Recorded as the venture, not one partner. THIS '
     'CONFIRMS TA <ASSET> LLC AS TA REALTY against a named property: the entity is '
     'TA PORTER SQUARE LLC. PORTFOLIO ALLOCATION: this price is Porter Square\'s '
     'share of a nine-property, $390 million deal, not a standalone trade.'),
    (1589, "seller", "Gravestar", "web",
     'Same reporting: Gravestar had owned Porter Square Shopping Center since its '
     'formation in 1994 and sold nine properties to TA Realty together.'),

    (1263, "seller", "TA Realty", "prefix_confirmed",
     'The entity is TA NEWBURY & DARTMOUTH STREET, on the TA <ASSET> LLC '
     'convention confirmed as TA Realty in this same pass at Porter Square, where '
     'Cambridge Day and The Real Reporter name the firm alongside the property. '
     'Written even though the row is small, because applying a confirmed decode '
     'costs nothing.'),

    (1660, "buyer", "New England Development", "web",
     'Connect CRE, "New England Development Acquires Kendall Square Office Complex '
     'for $87M", and Bisnow reporting the same: New England Development bought the '
     'American Twine Office Park in East Cambridge for $87 million, deed '
     'registered 3 June 2019, at about $758 per square foot. ADDRESS NOTE: the '
     'press calls the complex 222 Third Street and this row is addressed 165 '
     'Second Street, the historic American Net and Twine Company Factory. It is '
     'one complex with several street frontages on one parcel record; the entities '
     'on both sides read AMERICAN TWINE and the price and month match exactly. New '
     'England Development is separately in this table as part of the consortium '
     'that owned the Taj Boston.'),
    (1660, "seller", "Transatlantic Investment Management", "web",
     'Same reporting names Transatlantic Investment Management as the seller, and '
     'Transatlantic carries the property on its own past-properties page. The '
     'record entity is AMERICAN TWINE LIMITED PARTNERSHIP.'),

    (1473, "buyer", "Goldman Sachs", "web",
     'Universal Hub, reporting from land records on the 2026 conversion proposal: '
     'the eleven-storey building at 294 Washington Street, across from School '
     'Street, was bought for $94.5 million in 2016 by an affiliate of Goldman '
     'Sachs. This row is $94,500,000 in March 2016 with a buyer entity of 294 '
     'WASHINGTON OWNER LLC.'),
    (1473, "seller", "Synergy Investments", "web",
     'Same land-records reporting lists Synergy as the seller in that 2016 '
     'transaction. WRINKLE RECORDED: Synergy\'s own site currently carries 294 '
     'Washington Street among its properties, which would mean it either '
     'reacquired the building later or manages it for the owner. The 2016 seller '
     'is what this row records and the source for it is the land record itself, '
     'but the present-day listing is noted rather than ignored. Synergy appears '
     'three other times in this table: selling 55 Summer Street in 2022, operating '
     'Two Drydock alongside KKR, and buying 179 Lincoln Street in 2024.'),
]

NOTES = [
    (1662, "1028 MASSACHUSETTS AVENUE, $128,000,000, April 2019. SEARCHED AND NOT "
           "FOUND. The seller of record is CAMBRIDGE 1030 MASS AVE LLC, an "
           "address-named vehicle that says nothing about its sponsor. Searches on "
           "the address, price and year returned only residential sales on "
           "Massachusetts Avenue, which dominate all coverage of that street."),
    (1681, "25-27 LAND BOULEVARD, $81,750,000, February 2018. SEARCHED AND NOT "
           "FOUND, AND THE OBVIOUS LEAD IS THE WRONG BUILDING. The entities are "
           "CAMBRIDGE LLC and CAMBRIDGE HOTEL LLC, which point at the Royal "
           "Sonesta. The Royal Sonesta is at 40 EDWIN LAND BOULEVARD, a different "
           "address on the same road, and nothing found reports it selling in "
           "2018. Two entities that say nothing but 'Cambridge' plus a nearby "
           "famous hotel is not evidence."),
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
        log.info("id=%-5s %-6s %-34s -> %-38s [%s]", rid, side,
                 (cur[0] or "")[:34], sponsor, basis)
        if not dry_run:
            conf = ("web_corroborated" if basis == "web" else "registry_confirmed")
            conn.execute(text(f"""
                update transactions
                   set {side}_canonical = :s, {side}_confidence = :c,
                       {side}_resolution_basis = :b,
                       notes = coalesce(notes,'') || :n
                 where id = :id"""), {
                "s": sponsor, "id": rid, "c": conf, "b": basis,
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
