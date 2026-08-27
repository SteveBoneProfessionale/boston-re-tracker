r"""Thirty-fourth pass, and the price-gap pattern finally explains itself.

THE AC HOTEL ROW SETTLES WHAT SIX OTHER ROWS ONLY HINTED AT. RLJ's own release
puts the acquisition at $89.0 million. This table records $86,950,000, and an
earlier pass flagged that as the fifth instance of a registry figure and a press
figure disagreeing about one deal. BLDUP now supplies both halves in one
sentence: "National Development sold the AC Hotel in Boston's Ink Block
neighborhood to a Bethesda, MD-based equity management firm for $86,950,000" --
the registry number -- while RLJ's release says $89.0 million.

So the two figures are not a discrepancy to reconcile. They are two different
quantities that both belong to the deal: the recorded consideration on the deed,
and the total the buyer reports paying. Every row in this table carrying a pair
of prices fits that shape:

    Fairmont Copley Plaza   $163,000,000   press $170.0M
    125 Broadway            $602,840,000   BXP $592M, Biogen ~$603M
    535-545 Boylston        $148,000,000   Newmark $128M
    The Envoy Hotel         $ 70,789,300   Hersha $112.5M
    AC Hotel                $ 86,950,000   RLJ $89.0M
    Onyx Hotel              $ 46,755,000   Pebblebrook $58.3M
    Revere Hotel garage     $ 91,000,000   Pebblebrook $95.0M

The standing rule -- keep the recorded price, note the other -- is right, and it
now has a reason rather than just a policy behind it.

    python scraper/acq_press34.py --apply
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
    (1016, "seller", "National Development",
     'BLDUP, "Downtown Boston Hotel Sold for $87 Million": "National Development '
     'sold the AC Hotel in Boston\'s Ink Block neighborhood to a Bethesda, '
     'MD-based equity management firm for $86,950,000" -- this row to the dollar, '
     'and the Bethesda firm is RLJ Lodging Trust, which the buyer side already '
     'carries. RLJ\'s own release puts its purchase at $89.0 million for the fee '
     'simple interest in the 205-room, 95,000 SF hotel, which opened in 2018 '
     'inside the Ink Block development. THAT PAIR OF FIGURES EXPLAINS SIX OTHER '
     'ROWS: BLDUP is quoting the recorded consideration and RLJ the total it '
     'reports paying, which is why this table\'s price and the press price differ '
     'on the Fairmont Copley Plaza, 125 Broadway, 535-545 Boylston, the Envoy '
     'Hotel, the Onyx Hotel and the Revere Hotel garage. National Development is '
     'independently in this table buying Boston Business Park.'),
]

NOTES = [
    (1213, "200 STATE STREET RETAIL SELLER, $81,800,000, April 2019: NOT FOUND, "
           "AND THE BUYER IS NOW TRIPLY SOURCED. Gazit Globe's own announcement "
           "(\"Gazit Horizons acquires Marketplace Center\"), Gazit Horizons' "
           "portfolio page and Boston Real Estate Times all record the purchase of "
           "MARKETPLACE CENTER -- a 62,000 SF retail condominium on the Rose "
           "Kennedy Greenway beside Faneuil Hall -- for $81.8 million in April "
           "2019. None of them names the SELLER, which is normal: a buyer's "
           "announcement has no reason to. The record entity MARKETPLACE CENTER "
           "ASSOC LLC names the asset, not a sponsor."),
    (1692, "767 MEMORIAL DRIVE, $82,000,000, July 2017: SEARCHED TWICE, NOT "
           "FOUND. Every search on this address returns the other Memorial Drive "
           "trades that dominate Cambridge coverage -- One Memorial Drive at "
           "$825.1M, 640 Memorial at $260M, 780 and 790 Memorial inside a $365M "
           "portfolio. Neither 777 MEMORIAL OWNER LP nor CMC9 OWNER LLC is "
           "decoded. 777 Memorial Drive is the Courtyard by Marriott Boston "
           "Cambridge, on the site of a former Radisson, which makes the buyer "
           "entity an ADDRESS rather than a firm."),
    (1592, "80-90 FIRST STREET, $156,435,584, February 2022: SEARCHED TWICE, NOT "
           "FOUND. The project is identified: 80 First Street is part of the "
           "CambridgeSide complex, which New England Development is redeveloping "
           "as 20 CAMBRIDGESIDE -- a 350,000 SF life science building within six "
           "interconnected buildings totalling two million SF of office, lab, "
           "residential and hotel. The buyer entity 20 CAMBRIDGE PLACE GROUND "
           "OWNER LLC matches that name and the word GROUND points at a ground "
           "lease, which would explain the silence: ground leases are not reported "
           "as trades. But no source found names a party to this conveyance at "
           "this price, and inferring the owner of a complex from its brand is not "
           "resolution."),
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
