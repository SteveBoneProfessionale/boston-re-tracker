r"""Fortieth pass. A lead upgraded on better evidence, and Synergy's ninth row.

350 WASHINGTON STREET WAS A LEAD AND IS NOW A RESOLUTION, on a specific fact
rather than a stronger hunch. The earlier note refused Invesco because the only
available chain was "Invesco bought it in 2011, therefore Invesco probably sold
it in 2019" -- the two-step inference that produced the RREF error. What the
refusal was waiting for has now turned up: THE ASSET WAS LISTED ON INVESCO REAL
ESTATE ADVISORS' OWN WEBSITE AS A PROPERTY SET FOR DISPOSITION AS OF SEPTEMBER
2019, and Nuveen bought it on 11 October 2019.

    That is not "the 2011 buyer is probably the 2019 seller". It is the owner
    publicly marketing the asset for sale weeks before the sale. The Real
    Reporter separately records Invesco closing on 350 Washington in a $128
    million purchase in 2011, from Real Estate Capital Partners acting for two
    German funds, negotiated by CBRE/NE.

254 SUMMER STREET IS SYNERGY'S NINTH ROW. BLDUP: "Morgan Stanley acquires 250
Summer Street, 104,728-square-foot Fort Point office building, for $62.5 million"
-- in cash, FROM SYNERGY INVESTMENTS. This row is 254 Summer Street at
$62,200,000 in June 2017, one door along in the parcel record, with Morgan
Stanley Prime Property Fund already on the buy side. Synergy now appears at 2
Center Plaza, 100 Franklin, 55 Summer, 179 Lincoln, 294 Washington, 327-333
Summer, 6-10 Oliver, 11 Beacon and here.

    python scraper/acq_press40.py --apply
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
    (1160, "seller", "Invesco Real Estate",
     'UPGRADED FROM LEAD TO RESOLUTION ON A NEW FACT. An earlier pass refused '
     'Invesco here, writing that "inferring a seller from an earlier buyer plus '
     'the absence of a known intervening trade is exactly the two-step inference '
     'that produced the RREF/Rialto error". The new fact is direct: the asset was '
     'LISTED ON THE WEBSITE OF INVESCO REAL ESTATE ADVISORS AS A PROPERTY SET FOR '
     'DISPOSITION AS OF SEPTEMBER 2019, and Nuveen Real Estate bought it on 11 '
     'October 2019 for $134,211,350. An owner publicly marketing an asset weeks '
     'before it sells is evidence of a different kind from an old purchase. The '
     'Real Reporter separately records Invesco closing on 350 Washington Street in '
     'a $128 million purchase in 2011 -- from Real Estate Capital Partners acting '
     'for two German funds, transferring from Boston Retail Development LP, '
     'negotiated by CBRE/NE -- so the holding period is eight years. THE ENTITY '
     'STILL DOES NOT MATCH: TR 350 WASHINGTON CORP is neither name, which is the '
     'derived-seller lag this table has now documented at 327-333 Summer, 6-10 '
     'Oliver, 374 Congress and 36-44 Broad.'),

    (1372, "seller", "Synergy Investments",
     'BLDUP: "Morgan Stanley acquires 250 Summer Street, 104,728-square-foot Fort '
     'Point office building, for $62.5 million" -- in cash, FROM SYNERGY '
     'INVESTMENTS, 2017. This row is 254 Summer Street at $62,200,000 in June '
     '2017; 250 and 254 Summer are the same eight-storey brick-and-beam building '
     'over Fort Point Channel, addressed one door along in the parcel record, and '
     'Synergy carries 250 Summer Street on its own portfolio page. The buyer side '
     'already read Morgan Stanley Prime Property Fund. SYNERGY NOW APPEARS NINE '
     'TIMES IN THIS TABLE -- 2 Center Plaza, 100 Franklin, 55 Summer, 179 Lincoln, '
     '294 Washington, 327-333 Summer, 6-10 Oliver, 11 Beacon and here -- on both '
     'sides, which is a portfolio history rather than nine coincidences.'),
]

NOTES = [
    (1027, "131-147 SEAPORT BOULEVARD SELLER, $94,500,000, August 2021: STILL NOT "
           "FOUND, AND THE DEVELOPER IS NOW IDENTIFIED WITHOUT BEING WRITTEN. The "
           "asset is the retail condominium at ECHELON SEAPORT, the $900 million, "
           "1.3 million SF three-tower scheme with 733 homes that COTTONWOOD "
           "MANAGEMENT broke ground on, and whose 125,000 SF of retail -- branded "
           "The Superette, about 40 stores and restaurants across the first two "
           "levels -- WS DEVELOPMENT manages. WS is already on the buy side of "
           "this row. Cottonwood is therefore the obvious seller, and it is NOT "
           "written: the seller of record is 131 149 SEAPORT PRIMARY CONDOMINIUM "
           "TRUST, a condominium trust rather than a sponsor, and no source names "
           "a party to this conveyance. Being the developer of a building does not "
           "establish who conveyed a condominium unit within it -- the same "
           "distinction that keeps Samuels & Associates a lead at 1345 Boylston "
           "Street."),
    (1201, "711 ATLANTIC AVENUE, $68,500,000, May 2019: BOTH SIDES STILL REFUSED. "
           "The buyer of record, ASSET PRESERVATION INC, is a 1031 qualified "
           "intermediary and not an owner -- already documented on this row. The "
           "seller entity I&G DIRECT REAL ESTATE 39 belongs to a numbered fund "
           "series that does exist as a registered entity (I&G Direct Real Estate 3 "
           "LP is Bloomberg-listed), but nothing found names the manager behind "
           "it, and a fund series number is not a sponsor name. Ashforth carries "
           "711 Atlantic Avenue on its own portfolio page with no acquisition "
           "date, which remains a lead and not a fact."),
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
