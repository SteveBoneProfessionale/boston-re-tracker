r"""Thirty-ninth pass. The 36-44 Broad Street conflict, explained by an old entity.

THAT ROW WAS FLAGGED, NOT RESOLVED, IN THE SEVENTH PASS. The note read: the
seller of record is TRANSWESTERN BROAD ST LLC, which names its firm outright and
would normally resolve on the self-identifying rule -- but the reporting says
Transwestern Investment Co bought 40 Broad Street in 2006 for $50M and SOLD it
to TIAA-CREF in 2013 for $110M, "which would make TIAA, not Transwestern, the
owner in 2016. Either the 2013 report describes a different Broad Street
building, or Transwestern retained or reacquired an interest, or the entity kept
its name through a change of control the way FELCOR COPLEY PLAZA OWNER did."

IT WAS THE THIRD OPTION. The Real Reporter: "Invesco Takes 40 Broad St from TIAA
CREF". Transwestern sold in 2013, TIAA-CREF took title through the vehicle
Transwestern had named, and the LLC carried that dead name forward to the 2016
sale. So:

    2006   -> Transwestern Investment Co.        $ 50,000,000
    2013   Transwestern -> TIAA-CREF             $110,000,000
    2016   TIAA-CREF    -> Invesco Real Estate   $150,300,000   this row

That is the FOURTH entity in this table found carrying a name its owner had
shed -- after FelCor/RLJ, LaSalle/Pebblebrook and Wells/Piedmont -- and the
first where the stale name belonged to a SELLER rather than an acquired REIT. A
self-identifying entity is only safe when nothing contradicts it, and here
something did, and the refusal held for thirty passes until the contradiction
explained itself.

A NINTH HALF-RECORDED VENTURE, and the biggest one yet. Allston LabWorks read
"King Street Properties" alone. NEREJ, REBusinessOnline and King Street's own
release all describe the $915 million project as a joint venture of KING STREET
PROPERTIES, BROOKFIELD AND MUGAR ENTERPRISES, which paid about $181.6 million
for the land in March 2022 -- this row exactly -- across 250, 280 and 305
Western Avenue on roughly four acres beside Harvard's Allston campus, at the
former Stadium Auto Body site at Western Avenue and Everett Street.

    AND THE SELLER ENTITY IS WESTERN AVENUE JOINT VENTURE, which raises a
    possibility worth recording and not writing: Mugar Enterprises is a Boston
    family firm with long-held Allston land, and a landowner joining the
    development venture that buys its own site is a partner contribution rather
    than a sale. No source states it, so the seller stays blank.

    python scraper/acq_press39.py --apply
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

BROAD = (
    "RESOLVES A CONFLICT THIS TABLE FLAGGED AND REFUSED TO GUESS AT. The seventh "
    "pass noted that the seller entity TRANSWESTERN BROAD ST LLC names its firm "
    "outright, but that reporting had Transwestern Investment Co BUYING 40 Broad "
    "Street in 2006 for $50M and SELLING it to TIAA-CREF in 2013 for $110M -- "
    "which would make TIAA the 2016 owner. It listed three possible explanations "
    "and wrote none of them. The third was right: The Real Reporter, \"INVESCO "
    "TAKES 40 BROAD ST FROM TIAA CREF\". TIAA-CREF took title through the vehicle "
    "Transwestern had named and the LLC carried the dead name forward. The chain "
    "is Transwestern (2006, $50M) -> TIAA-CREF (2013, $110M) -> Invesco (this "
    "row, $150,300,000). FOURTH ENTITY IN THIS TABLE CARRYING A NAME ITS OWNER "
    "HAD SHED, after FELCOR COPLEY PLAZA OWNER under RLJ, LHO ONYX HOTEL under "
    "Pebblebrook and WELLS OPERATING PARTNERSHIP under Piedmont -- and the first "
    "where the stale name came from an ordinary sale rather than a REIT merger."
)

RESOLVE = [
    (1459, "buyer", "Invesco Real Estate", False, BROAD +
     " Invesco is separately confirmed in this table at 179 Lincoln Street, 226 "
     "Causeway Street and 10 Fawcett Street, and appears again as ASVRF alongside "
     "Cabot, Cabot & Forbes on Mooney Street."),
    (1459, "seller", "TIAA (Nuveen)", False, BROAD +
     " TIAA is separately confirmed in this table buying the seven-property Fort "
     "Point portfolio in April 2016 -- the same year, the same buyer-side family, "
     "and its disposal of 40 Broad Street in the same period is consistent with "
     "portfolio rotation rather than coincidence."),

    (967, "buyer", "King Street Properties / Brookfield / Mugar Enterprises", True,
     'CORRECTION AND COMPLETION. This row read "King Street Properties" alone. '
     'NEREJ ("King Street Properties breaks ground on $915m Allston Labworks -- a '
     '4.27-acre mixed-use project"), REBusinessOnline and King Street\'s own '
     'release all describe the scheme as a joint venture of KING STREET '
     'PROPERTIES, BROOKFIELD AND MUGAR ENTERPRISES, and CPE records the venture '
     'paying about $181.6 million for the land in March 2022 per Suffolk County '
     'records -- this row exactly. The site is 250, 280 and 305 Western Avenue, '
     'roughly four acres beside Harvard\'s Allston campus at the former Stadium '
     'Auto Body site, and the project later secured a $585 million loan. NINTH '
     'VENTURE IN THIS TABLE FOUND RECORDED AS A SINGLE PARTY, and the largest.'),
]

NOTES = [
    (967, "EVERETT STREET SELLER: A POSSIBILITY RECORDED AND NOT WRITTEN. The "
          "seller of record is WESTERN AVENUE JOINT VENTURE. MUGAR ENTERPRISES -- "
          "a Boston family firm with long-held Allston land -- is one of the three "
          "partners in the BUYING venture, per NEREJ and King Street's own "
          "release. A landowner joining the development venture that acquires its "
          "own site is a PARTNER CONTRIBUTION rather than a sale, and would put "
          "Mugar on both sides in the same way Brickman, King Street, Synergy and "
          "Griffith appear on both sides of other rows here. NO SOURCE STATES IT, "
          "so the seller stays blank rather than being inferred from the shape of "
          "the deal. Flagged for a licensed feed."),
]


def main(dry_run: bool):
    conn = engine.connect()
    n = 0
    for rid, side, sponsor, force, why in RESOLVE:
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
