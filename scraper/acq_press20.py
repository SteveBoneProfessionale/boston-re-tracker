r"""Twentieth pass. The last of the $50M+ work, and a fifth incomplete venture.

327-333 SUMMER STREET WAS RECORDED AS ASB ALONE AND IT WAS A JOINT VENTURE. The
Boston Globe, "Fort Point flip: 333 Summer sells for more than $74 million":
Synergy Investments sold the building, home to LogMeIn, for $74.25 million to an
arm of ASB Capital Management, and ASB's own release is titled "ASB AND LINCOLN
Acquire Prime Seaport District Office Building" -- a joint venture with Lincoln
Property Company, bought on behalf of ASB's $5.1 billion Allegiance Fund. That
is the fifth venture in this table found to have been recorded as one partner:
after Brickman at 535 Boylston, King Street at 733 Concord, Synergy at 100
Franklin and Griffith at 20 Guest Street.

AND ITS SELLER CARRIES A CONFLICT THAT IS WRITTEN DOWN RATHER THAN SMOOTHED. The
seller of record is W2005 BWH II REALTY LLC -- a Whitehall Street fund vehicle --
while the Globe names SYNERGY INVESTMENTS as the seller, and says Synergy and
DivcoWest paid $11 million for the building in 2013 before renovating it and
letting 117,000 SF to LogMeIn on a twelve-year lease. The press is specific and
matches this row on price and month, so Synergy is written; but the entity does
not match, and on this spine the seller is DERIVED from the previous ownership
snapshot, which can lag a conveyance. The conflict is recorded on the row.

ONE WINTHROP SQUARE IS A CLEAN REPEAT SALE. NEREJ names it outright: "Nan Fung
Life Sciences Real Estate acquires One Winthrop Sq. from MAPFRE INSURANCE for $75
million", March 2020, a five-storey 115,667 SF building. MAPFRE had paid $55
million for it in 2015, so $20 million in five years.

    python scraper/acq_press20.py --apply
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
    (1026, "seller", "Zurich Alternative Asset Management", "web", False,
     'NEREJ, "Zurich sells 51 Melcher St. to GI Partners for $74.6 million", with '
     'Bisnow, Boston Real Estate Times and Commercial Real Estate Direct '
     'reporting the same September 2021 deal: Newmark represented Zurich '
     'Alternative Asset Management in the sale of the nine-storey, 102,727 SF '
     'creative office building, 21% let to Life is Good with more than 80,000 SF '
     'available. Zurich had paid $53 million for the 105-year-old building in '
     '2014 after a renovation. The buyer side already read GI Partners, which '
     'bought for its Real Estate Essential Tech + Science Fund -- the same fund '
     'that bought Blackstone Science Square, confirming the GI ETS entity family '
     'a second time.'),

    (1128, "seller", "MAPFRE Insurance", "web", False,
     'NEREJ, "Nan Fung Life Sciences Real Estate acquires One Winthrop Sq. from '
     'MAPFRE Insurance for $75 million", with Boston Real Estate Times and Traded '
     'reporting the same March 2020 deal: a five-storey, 115,667 SF office '
     'building in the Financial District beside Millennium\'s Winthrop Center. '
     'REPEAT SALE: MAPFRE USA Insurance had bought it in 2015 for $55 million, so '
     'the building gained $20 million in five years. The record entity MM REAL '
     'ESTATE LLC is MAPFRE\'s vehicle. The buyer side already read Nan Fung.'),

    (1480, "seller", "Synergy Investments", "web", False,
     'The Boston Globe, "Fort Point flip: 333 Summer sells for more than $74 '
     'million": Synergy Investments sold the building -- home to LogMeIn -- for '
     '$74.25 million, per Suffolk County property records, matching this row on '
     'price and month. Synergy and partner DivcoWest had paid $11 million for it '
     'in 2013, before extensive renovations and before LogMeIn signed a '
     'twelve-year lease on 117,000 SF. CONFLICT RECORDED, NOT SMOOTHED: the '
     'seller of record on this row is W2005 BWH II REALTY LLC, a Whitehall Street '
     'fund vehicle, which does not match Synergy. On this spine the seller is '
     'DERIVED from the previous ownership snapshot and can lag a conveyance, so '
     'the entity may simply be the owner before Synergy. The press is specific '
     'and matches price and month, so it is written, but the mismatch is on the '
     'row.'),

    (1480, "buyer", "ASB Real Estate Investments / Lincoln Property Company",
     "web", True,
     'CORRECTION AND COMPLETION. This row read "ASB Real Estate Investments" '
     'alone, written from the entity ASB SUMMER STREET VENTURE LLC naming its own '
     'owner -- correct as far as it went, and the word VENTURE in that entity was '
     'the clue it did not go far enough. ASB\'s own release is titled "ASB and '
     'Lincoln Acquire Prime Seaport District Office Building", and a second one, '
     '"ASB and Lincoln Secure $63 Million Financing on Seaport District Office '
     'Properties", confirms the pairing. ASB bought on behalf of its $5.1 billion '
     'Allegiance Fund, in joint venture with Lincoln Property Company. Fifth '
     'venture in this table recorded as a single partner.'),
]

NOTES = [
    (1016, "ALBANY STREET, $86,950,000, October 2021: BUYER CONFIRMED, SELLER NOT "
           "FOUND, AND A PRICE GAP. RLJ Lodging Trust's own release, \"RLJ Lodging "
           "Trust Acquires AC Hotel by Marriott Boston Downtown\", 25 October "
           "2021, with Connect CRE and Commercial Real Estate Direct: RLJ bought "
           "the FEE SIMPLE interest in the 205-room hotel for $89.0 million, about "
           "$434,000 per key. The purpose-built hotel opened in 2018 inside the "
           "mixed-use INK BLOCK development on Albany Street, which corroborates "
           "the buyer this row already carried from its self-identifying entity "
           "RLJ MACH BOSTON LLC. PRICE: the release says $89.0M and this row "
           "records $86,950,000 -- the fifth instance in this table of a registry "
           "figure and a press figure differing on the same deal. SELLER: not "
           "named in any source found. National Development built and owns Ink "
           "Block, which makes it a lead and not a fact; the record entity is "
           "ALBANY STREET HOTEL LLC."),
]


def main(dry_run: bool):
    conn = engine.connect()
    n = 0
    for rid, side, sponsor, basis, force, why in RESOLVE:
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
        log.info("id=%-5s %-6s %-32s -> %s%s", rid, side, (cur[0] or "")[:32],
                 tag, sponsor)
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
            "where id = 1016"), {
            "c": ("RLJ's own release puts the AC Hotel acquisition at $89.0M; "
                  "this row records $86,950,000.")})
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
