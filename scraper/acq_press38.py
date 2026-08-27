r"""Thirty-eighth pass. A conflict resolved the way the other two were.

374 CONGRESS STREET HELD A CONFLICT SINCE THE FOURTH PASS. The note read: NEREJ
says the seven-property Fort Point portfolio came from "a separate account
advised by CLARION PARTNERS", while the seller of record is AG/ND FORT POINT
LLC. "Both can be true -- a Clarion separate account may hold title through a
vehicle named for an earlier joint venture -- but choosing between them without
a source that names the entity alongside the firm is guessing."

A second retrieval of the NEREJ piece confirms Clarion and adds HFF as broker,
and the portfolio itemisation matches this table exactly: 263 Summer St, 332 and
374 Congress St, and 33-41, 34, 38 and 44 Farnsworth St, 408,342 SF for $224
million, April 2016. Clarion is written and the AG/ND entity is kept verbatim
with the mismatch on the row.

    THAT IS THE THIRD ROW WHERE THE PRESS AND THE DERIVED SELLER DISAGREE, and
    all three resolve the same way. 327-333 Summer (W2005 entity, Synergy in
    press), 6-10 Oliver (P6/Saracen entity, Synergy in press), and now 374
    Congress. On this spine the seller is reconstructed from the previous ANNUAL
    ownership snapshot, so a property that changed hands twice between snapshots
    reports an owner one conveyance stale. AG/ND -- Angelo Gordon and National
    Development -- is very plausibly the owner BEFORE the Clarion account.

BEACON CAPITAL SELLS A THIRD TIME IN THIS TABLE, and the headline is explicit:
The Real Reporter, "Beacon Reaps $77M in 230 Congress St. Sale to NY Co." The
twelve-storey 1931 art deco building by Ralph T. Walker went to Northwood
Investors, which the buyer side already carried. Beacon now appears at 160
Federal, Canal Park, 745 Atlantic, 135 Morrissey, One Brattle Square and here.

    python scraper/acq_press38.py --apply
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
    (1516, "seller", "Beacon Capital Partners",
     'The Real Reporter\'s headline is explicit: "Beacon Reaps $77M in 230 '
     'Congress St. Sale to NY Co." -- this row\'s exact price, September 2015. The '
     'asset is a twelve-storey art deco office, retail and telecommunications '
     'building of about 151,000-155,000 SF, designed by Ralph T. Walker and built '
     'in 1931, walking distance from South Station and the Greenway. The buyer '
     'side already read Northwood Investors, from Connect CRE and Northwood\'s own '
     'portfolio page, and "NY Co." fits Northwood\'s New York origins. BEACON NOW '
     'SELLS SIX TIMES IN THIS TABLE -- 160 Federal, the Canal Park complex, 745 '
     'Atlantic Avenue, 135 Morrissey Boulevard, One Brattle Square and here -- '
     'which is a disposition record rather than six coincidences.'),

    (1468, "seller", "Clarion Partners",
     'RESOLVES A CONFLICT THIS TABLE HELD OPEN SINCE THE FOURTH PASS. That note '
     'said NEREJ named "a separate account advised by CLARION PARTNERS" as seller '
     'while the record entity is AG/ND FORT POINT LLC, and refused to choose. A '
     'second retrieval of NEREJ, "TIAA purchases seven-property Fort Point '
     'portfolio for $224 million", confirms Clarion as the seller with HFF acting, '
     'and itemises the portfolio exactly as this table holds it: 263 Summer '
     'Street; 332 and 374 Congress Street; and 33-41, 34, 38 and 44 Farnsworth '
     'Street -- 408,342 SF for $224 million, April 2016. THE ENTITY MISMATCH IS '
     'KEPT ON THE ROW: AG/ND reads as Angelo Gordon and National Development, very '
     'plausibly the owner BEFORE the Clarion separate account. This is the third '
     'row where press and the derived seller disagree, after 327-333 Summer Street '
     'and 6-10 Oliver Street, and all three resolve to the press for the same '
     'reason -- the seller here is reconstructed from the previous ANNUAL ownership '
     'snapshot and comes out one conveyance stale when a property trades twice '
     'between snapshots.'),
]

NOTES = [
    (1394, "201 NEWBURY STREET SELLER, $75,000,000, April 2017: NOT FOUND, BUYER "
           "CORROBORATED A SECOND WAY. Nuveen Real Estate carries 201 Newbury on "
           "its own retail portfolio page, which independently supports the TRPF "
           "decode written on the buy side. The asset is a multi-floor RETAIL "
           "CONDOMINIUM at the front of the former Prince School -- a 19th-century "
           "school converted in 1987 to the mixed-use Prince on Newbury at the "
           "corner of Newbury and Exeter. The seller entity TWO 01 "
           "NEWBURY-PRINCE LLC names that building and nothing else, and no source "
           "found reports the 2017 conveyance."),
    (1560, "350 MAIN STREET SELLER, $53,000,000, June 2024: SEARCHED TWICE, NOT "
           "FOUND. The asset is confirmed as the Kendall Hotel, which occupies the "
           "restored Engine 7 Firehouse and is listed with Historic Hotels of "
           "America on that basis -- so FIREHOUSE INN LLC names the building "
           "correctly. Coverage of MIT in Kendall Square in this period is "
           "dominated by its $750 million Volpe Center acquisition, which closed 26 "
           "January 2024 and is a separate row in this table. Nothing found "
           "reports the hotel conveyance."),
    (1643, "100 BROADWAY BUYER, $98,991,934, October 2020: SEARCHED THREE TIMES, "
           "NOT FOUND. ODYSSEY PROPCO LLC belongs to a numbered series -- Odyssey "
           "Propco III and Odyssey Propco IV both exist as registered entities, "
           "the latter operating the Capitol Hill Hotel in Washington DC -- which "
           "makes it a hotel fund vehicle rather than a firm name. Odyssey Hotel "
           "Group is the obvious reading and is NOT written, because no source "
           "names it at this address. The seller side reads RLJ Lodging Trust from "
           "the self-identifying entity RLJ CAMBRIDGE HOTEL LLC."),
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
