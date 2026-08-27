r"""Twenty-eighth pass. Two more, and a 63% loss recorded on the row.

99 BEDFORD STREET IS A COMPLETE ROUND TRIP AND THE TABLE SHOULD HOLD BOTH ENDS.
Banker & Tradesman and CPE: Plymouth Rock sold the Bedford Block -- an 1899
building of roughly 86,000-98,000 SF near South Station -- to an affiliate of
Credit Suisse for $50.25 million in November 2019, this row exactly. Then:

    2019-11   Plymouth Rock -> Credit Suisse          $50,250,000   this row
    2025-11   foreclosure auction, $19M credit bid
    2025-12   lenders       -> Chevron Partners       $19,000,000

38% of the 2019 price six years later, and the lenders took it at auction before
selling on. Bisnow headlined it "Bedford Street Office Building Sold For 63%
Discount". The second leg is not in this table, so it is written on the row.

35-45 MORRISSEY: THE BUYER'S INITIALS ARE IN THE ENTITY. The Dorchester Reporter
records Center Court adding to its Morrissey Boulevard holdings by buying the
Star Market building at No. 35, the Beasley Media Group building at No. 55 and
adjacent land for a combined $56 million. The buyer entity is POB CC 35-55
MORRISSEY LLC -- CC for Center Court, and the address range 35-55 matching the
reporting exactly.

    THE SELLER IS NOT WRITTEN, and the reason is a genuine ambiguity in the
    source rather than an absence. The reporting names Star Market and Beasley
    Media Group as being "under long term lease agreements which Center Court
    has said will be honored". That is consistent with them being SALE-LEASEBACK
    VENDORS, and equally consistent with them being sitting TENANTS whose
    landlord sold over their heads. Those are opposite facts about who the
    counterparty was, and the sentence does not settle which.

    python scraper/acq_press28.py --apply
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
    (1156, "seller", "Plymouth Rock",
     'Banker & Tradesman and Commercial Property Executive ("Historic Boston '
     'Office Asset Fetches $50M"): PLYMOUTH ROCK sold the Bedford Block, an 1899 '
     'building near South Station, to an affiliate of Credit Suisse for about '
     '$50.3 million in November 2019 -- this row records $50,250,000 in that '
     'month. Five tenants, and a lobby renovation completed in 2020. The buyer '
     'side already read Credit Suisse, and The Real Reporter later refers to "owner '
     'Credit Suisse" when the building was listed, which corroborates it '
     'independently.'),

    (1284, "buyer", "Center Court Partners",
     'The Dorchester Reporter: Center Court "added to its Morrissey Boulevard '
     'holdings, buying the Star Market building, the next-door Beasley Media Group '
     'Boston buildings, and adjacent land for a combined $56 million" -- the Star '
     'Market at 35 Morrissey and the Beasley building at No. 55. This row is '
     '$56,000,000 in August 2018 and the buyer entity is POB CC 35-55 MORRISSEY '
     'LLC: CC for Center Court, and the address range 35-55 matching the reporting. '
     'THE SELLER IS DELIBERATELY LEFT BLANK: the same passage says both buildings '
     'are "under long term lease agreements" that Center Court will honour, which '
     'is equally consistent with Star Market and Beasley being SALE-LEASEBACK '
     'VENDORS or being SITTING TENANTS whose landlord sold over their heads. Those '
     'are opposite claims about who the counterparty was and the source does not '
     'settle it.'),
]

REPEAT = (
    " | ROUND TRIP COMPLETED, SECOND LEG NOT IN THIS TABLE. 99 Bedford Street "
    "sold at a FORECLOSURE AUCTION in November 2025 on a $19 million credit bid "
    "by the lenders, and Chevron Partners closed on the $19,000,000 acquisition "
    "on 8 December 2025 -- 38% of the $50,250,000 recorded here, six years on. "
    "Bisnow headlined it \"Bedford Street Office Building Sold For 63% "
    "Discount\"; Banker & Tradesman covered the Chevron close; the Boston Globe "
    "covered the lenders taking the building. Recorded here so the pair survives "
    "whether or not the 2025 leg ever loads from a registry source. This is the "
    "third such round trip written onto a row in this table, after 18 Tremont "
    "Street (-71%) and 380 E Street (roughly -50%)."
)


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
        conn.execute(text(
            "update transactions set notes = coalesce(notes,'') || :n "
            "where id = 1156"), {"n": REPEAT})
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
