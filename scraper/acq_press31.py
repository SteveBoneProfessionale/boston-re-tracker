r"""Thirty-first pass. UMNV is a portmanteau, and the Seaport's biggest land deal.

UMNV IS URBAN MERITAGE AND NOVAYA VENTURES, confirmed against the property, and
the entity is a portmanteau of both partners rather than an abbreviation of one.
Commercial Property Executive and Foxfield's write-up of the Urban Meritage
platform: Urban Meritage and Novaya Ventures bought 126 Newbury Street for $54.2
million in 2015 -- a six-storey, 50,000 SF office and retail building on the
third block -- all cash, through a partnership launched in early 2013. The
previous owner was RUDIN MANAGEMENT, which had paid $9.3 million for it.

    The seller entity on that row is BEEKMAN TOWN HOUSE LLC, which fits: Rudin
    is a New York family firm and Beekman Place is in Manhattan. The entity
    corroborates the press without being the reason for it.

The decode unlocks a second row, 8 Newbury Street, where UMNV sits on the other
side of the deal five years later.

SEAPORT BOULEVARD IS A LEG OF THE $359 MILLION BLOCK. Boston Magazine, "Last
Block of Seaport Land Sells for $359 Million", and WS Development's own release:
in October 2015 WS acquired the remaining 12.5-acre block of Seaport land for
$359 million FROM MORGAN STANLEY AND BOSTON GLOBAL INVESTORS, taking leadership
of the whole project. This row is $53,391,425 in that month with WS Development
already recorded as buyer -- one parcel of that block, not a standalone trade.

    Boston Global Investors appears twice in this table now, on opposite sides:
    selling here, and as CIM Group's local partner on the Motor Mart Garage
    tower a year later.

    python scraper/acq_press31.py --apply
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

UMNV = (
    "UMNV IS URBAN MERITAGE AND NOVAYA VENTURES, confirmed against a named "
    "property. Commercial Property Executive and Foxfield's account of the Urban "
    "Meritage platform both record Urban Meritage and Novaya Ventures buying 126 "
    "Newbury Street for $54.2 million in 2015 -- a six-storey, 50,000 SF office "
    "and retail building on the third block of Newbury Street, an all-cash "
    "purchase through a partnership launched in early 2013. THE ENTITY IS A "
    "PORTMANTEAU OF BOTH PARTNERS, not an abbreviation of one: UM for Urban "
    "Meritage, NV for Novaya Ventures. Recorded as the venture under the rule "
    "that a joint venture resolves to all partners."
)

RESOLVE = [
    (1552, "buyer", "Urban Meritage / Novaya Ventures", UMNV),
    (1552, "seller", "Rudin Management",
     'Same reporting: "The previous owner was RUDIN MANAGEMENT, who had paid $9.3 '
     'million for 126 Newbury St." The seller entity on this row is BEEKMAN TOWN '
     'HOUSE LLC, which fits without being the evidence: Rudin is a New York family '
     'firm and Beekman Place is in Manhattan. Entity and press corroborate each '
     'other independently.'),
    (1107, "seller", "Urban Meritage / Novaya Ventures", UMNV +
     " This row is 8 Newbury Street in July 2020, where the same venture is the "
     "SELLER five years after buying 126 Newbury Street -- the entity is UMNV 8 "
     "NEWBURY LLC, the identical convention."),

    (1511, "seller", "Morgan Stanley / Boston Global Investors",
     'Boston Magazine, "Last Block of Seaport Land Sells for $359 Million", and WS '
     'Development\'s own release, "$359M Sale May Speed Seaport\'s Completion": in '
     'October 2015 WS Development acquired the remaining 12.5-acre block of Seaport '
     'land for $359 million from MORGAN STANLEY and BOSTON GLOBAL INVESTORS, and '
     'assumed leadership of the project. PORTFOLIO ALLOCATION: this row is '
     '$53,391,425 in that month, one parcel of that block rather than a separate '
     'deal. The buyer side already read WS Development. Boston Global Investors '
     'now appears twice in this table on opposite sides -- selling here, and as '
     'CIM Group\'s local partner on the Motor Mart Garage tower at 201 Stuart '
     'Street the following year.'),
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
            basis = "prefix_confirmed" if "UMNV IS" in why else "web"
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
                   and upper(coalesce({side},'')) like 'UMNV %'
                   and coalesce({side}_canonical,'') = ''""")):
            log.info("id=%-5s %-6s %-34s -> Urban Meritage / Novaya Ventures "
                     "[family sweep]", rid, side, (ent or "")[:34])
            if not dry_run:
                conn.execute(text(f"""
                    update transactions
                       set {side}_canonical = 'Urban Meritage / Novaya Ventures',
                           {side}_confidence = 'registry_confirmed',
                           {side}_resolution_basis = 'prefix_confirmed',
                           notes = coalesce(notes,'') || :n
                     where id = :id"""),
                    {"id": rid, "n": f" | {side.upper()} RESOLVED. " + UMNV})
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
