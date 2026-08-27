r"""Thirty-third pass. Two earlier refusals, both now confirmed, both hotels.

90 TREMONT STREET was refused twice in this project. The note read: "neither KHP
BOSTON HOTEL LLC nor THI VI BOSTON LLC is decoded; both have plausible readings
in the hotel-fund world and neither is confirmed against this address." Both
plausible readings were correct, and now they are sourced. CoStar and Boston
Real Estate Times: THAYER LODGING GROUP, owned by Brookfield, bought the Nine
Zero Hotel for $85.1 million in 2016 from San Francisco-based KHP CAPITAL
PARTNERS -- about $450,000 per room. This row is $85,100,300 in September 2016.

    THI VI = Thayer Hotel Investors VI.  KHP = KHP Capital Partners.

A THIRD INSTANCE OF THE FELCOR/RLJ PATTERN, and it is now frequent enough to be
a rule about hotel REITs rather than a curiosity. The Onyx Hotel's seller entity
is LHO ONYX HOTEL ONE LLC -- LaSalle Hotel Properties -- but PEBBLEBROOK HOTEL
TRUST ACQUIRED LASALLE IN DECEMBER 2018, and it is Pebblebrook that announced and
closed this sale on 29 May 2019. The entity kept the dead brand.

    FelCor  -> RLJ         Fairmont Copley Plaza, Dec 2017
    Wells   -> Piedmont    1416 Mass Ave and 1 Brattle Sq, Dec 2022
    LaSalle -> Pebblebrook Onyx Hotel, May 2019

TWO PRICES ON THE ONYX, AND THE ROW HAS THE SMALLER ONE AGAIN. Pebblebrook's
8-K and Business Wire put the disposition at $58.3 million for the 112-room
hotel; BLDUP reports "Boylston Properties Acquires Onyx Hotel for $46.755
Million", which is this row to the dollar, five days later. Sixth instance in
this table of a registry figure and a press figure describing one deal.

AND ONE FREE NAME THE AUDIT WALKED PAST. 173 Boston Street's buyer entity is
VERIZON NEW ENGLAND INC, which names its own owner. The canonical audit cleared
this row's value -- correctly, since it was "Kroll", a restructuring adviser --
but clearing the wrong answer is not the same as looking for the right one, and
the right one was written on the record all along.

    python scraper/acq_press33.py --apply
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
    (1443, "buyer", "Thayer Lodging Group (Brookfield)", "web",
     'REVERSES AN EARLIER REFUSAL ON THIS ROW. CoStar, "Kimpton Acquires Nine Zero '
     'Hotel in Boston", and Boston Real Estate Times both trace the ownership: '
     'Thayer Lodging Group, owned by Brookfield, bought the Nine Zero Hotel for '
     '$85.1 million in 2016, about $450,000 per room. This row is $85,100,300 in '
     'September 2016. The entity THI VI BOSTON LLC is Thayer Hotel Investors VI. '
     'An earlier pass declined to decode it, saying it "had a plausible reading in '
     'the hotel-fund world" that was not confirmed against this address; the '
     'reading was right and it is now sourced. The hotel later became Hotel AKA '
     'Boston Common under Electra America, which this table records separately.'),
    (1443, "seller", "KHP Capital Partners", "web",
     'Same reporting: Thayer bought from San Francisco-based KHP CAPITAL PARTNERS, '
     'the entity here being KHP BOSTON HOTEL LLC. Kimpton had originally acquired '
     'the property in 2006, and KHP is the Kimpton principals\' successor '
     'investment firm, which is why the hotel kept the Kimpton flag after the '
     'ownership changed.'),

    (1197, "buyer", "Boylston Properties", "web",
     'BLDUP, "Boylston Properties Acquires Onyx Hotel for $46.755 Million" -- this '
     'row to the dollar, June 2019. The 112-room Onyx sits at 155 Portland Street '
     'in the West End. PRICE NOTE: Pebblebrook\'s own 8-K and Business Wire release '
     'put the disposition at $58.3 million on 29 May 2019, five days earlier. Both '
     'figures are published and the row keeps the recorded one, as at the Fairmont '
     'Copley Plaza, 125 Broadway, 535 Boylston, the Envoy and the AC Hotel.'),
    (1197, "seller", "Pebblebrook Hotel Trust", "web",
     'A THIRD INSTANCE OF AN ENTITY KEEPING ITS PRE-MERGER NAME. The seller of '
     'record is LHO ONYX HOTEL ONE LLC, naming LaSalle Hotel Properties -- but '
     'PEBBLEBROOK HOTEL TRUST ACQUIRED LASALLE IN DECEMBER 2018, and it is '
     'Pebblebrook that announced and completed this sale, in its own 8-K and a '
     'Business Wire release headed "Pebblebrook Hotel Trust Completes Sale of Onyx '
     'Hotel", 29 May 2019. Writing LaSalle would name a company that no longer '
     'existed when the deed was signed. Same situation as FELCOR COPLEY PLAZA '
     'OWNER after RLJ, and WELLS OPERATING PARTNERSHIP after Piedmont. Pebblebrook '
     'is independently in this table selling the Revere Hotel garage at 200 Stuart '
     'Street to CIM Group in 2017.'),

    (995, "buyer", "Verizon", "self_identifying",
     'The record entity is VERIZON NEW ENGLAND INC, which names its own owner -- '
     'the New England operating company of Verizon Communications. No decoding '
     'step is involved. NOTE ON PROVENANCE: the canonical audit cleared this row\'s '
     'previous value, "Kroll", correctly -- Kroll is a restructuring and valuation '
     'adviser, not an owner -- but removing a wrong answer is not the same as '
     'looking for the right one, and the right one was on the face of the record. '
     'Verizon New England also appears in this table as the original owner of 185 '
     'Franklin Street, the former New England Telephone tower.'),
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
        conn.execute(text(
            "update transactions "
            "set price_caveat = coalesce(price_caveat || ' ', '') || :c "
            "where id = 1197"), {
            "c": ("Pebblebrook's 8-K puts the Onyx Hotel disposition at $58.3M on "
                  "29 May 2019; this row records $46,755,000 on 3 June 2019.")})
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
