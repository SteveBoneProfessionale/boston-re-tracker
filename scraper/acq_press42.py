r"""Forty-second pass. Two buyers, and an article whose DATE was the trap.

399 WASHINGTON STREET. BLDUP: "LaSalle and L3 acquire long-vacant Downtown
Crossing building for $63.25 million" -- the 76,000 SF retail and office block
that had stood empty since 2006, the old Barnes & Noble building. This row is
$63,000,000 in June 2017 addressed 395-403 Washington Street. Both partners
recorded. L3 Capital was already a canonical in this table from a care-of line;
LaSalle is confirmed at 50 Post Office Square and 160 Federal Street.

    AND IT IS A FIFTH ROUND TRIP. The venture spent millions more on working
    lifts and lavatories, new mechanicals and a glass entrance, finished in 2021,
    and no tenants came. Hudson Group and Assembly Investments bought it in 2025
    for $13 million -- 80% below this row.

THE PADDOCK BUILDING, AND A LESSON ABOUT READING SEARCH RESULTS. CPE reports
that 101 Tremont Street "changed hands Sept. 22 for a little over $50.1 million
when the 11-story asset was acquired by GLL REAL ESTATE PARTNERS", and that it
had last traded in 2013 for $9.7 million. The summarised search output dated
that to 2020 by inferring from the article's publication date.

    IT IS THIS ROW: $50,150,000, 22 September, an eleven-storey building, and
    the registry says 2016. Price to the nearest $50,000 and the day of the
    month both match. The YEAR in a search summary is often the summariser's
    inference; the price and the day came from the article. GLL Real Estate
    Partners is independently in this table selling 200 State Street to Carr
    Properties, and the buyer entity 101BOS LLC is 101 Tremont, Boston.

The Paddock Building fronts both Tremont and Bromfield Streets, which is why
this row is addressed 59-75 Bromfield -- the same parcel-versus-asset address
mismatch as 131 Dartmouth, 250/254 Summer and 40 Edwin Land Boulevard.

    python scraper/acq_press42.py --apply
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
    (1379, "buyer", "LaSalle Investment Management / L3 Capital",
     'BLDUP: "LaSalle and L3 acquire long-vacant Downtown Crossing building for '
     '$63.25 million", and Bisnow, "Downtown Crossing\'s Barnes & Noble Building '
     'Finally Sold" -- a 76,000 SF retail and office building empty since 2006. '
     'The Globe\'s later coverage confirms "Chicago-based L3 Capital and LaSalle '
     'Investment Management purchased the property for about $63.3 million" in '
     '2017. This row is $63,000,000 in June 2017. Recorded as the venture, not one '
     'partner. LaSalle is separately confirmed in this table at 50 Post Office '
     'Square and 160 Federal Street; L3 Capital was already present, written from '
     'a care-of line and now corroborated by press. ROUND TRIP: the owners spent '
     'millions more on working lifts and lavatories, new mechanical and HVAC '
     'systems and a new glass entrance, completing in 2021, and tenants never '
     'came; Hudson Group and Assembly Investments bought it in 2025 for $13 '
     'million, 80% below this row.'),

    (1442, "buyer", "GLL Real Estate Partners",
     'Commercial Property Executive, "Office Building Sells in the Heart of '
     'Boston": 101 Tremont Street -- the PADDOCK BUILDING -- "changed hands Sept. '
     '22 for a little over $50.1 million when the 11-story asset was acquired by '
     'GLL REAL ESTATE PARTNERS", having last traded in 2013 for $9.7 million per '
     'Suffolk County records. THIS ROW IS $50,150,000 ON 22 SEPTEMBER, an '
     'eleven-storey building, and the registry dates it 2016. A DATE CAVEAT WORTH '
     'STATING: summarised search output placed the article in 2020 by inferring '
     'from its publication date, but the price to the nearest $50,000 and the day '
     'of the month both come from the article itself and both match this row. GLL '
     'Real Estate Partners is independently in this table as the seller of 200 '
     'State Street to Carr Properties, and the buyer entity 101BOS LLC is 101 '
     'Tremont, Boston. ADDRESS NOTE: the Paddock Building fronts both Tremont and '
     'Bromfield Streets, which is why this row is addressed 59-75 Bromfield -- the '
     'same parcel-versus-asset mismatch as 131 Dartmouth, 250/254 Summer and 40 '
     'Edwin Land Boulevard. The seller, PADDOCK LIMITED PARTNERSHIP, is the '
     'long-standing owner that bought at $9.7 million in 2013 and is not named by '
     'any source found.'),
]

NOTES = [
    (1379, "395-403 WASHINGTON STREET SELLER: A NAMED INDIVIDUAL, WHICH IS AN "
           "ANSWER RATHER THAN A GAP. The seller of record is POSNER, ROBERT A -- "
           "a natural person holding property directly, not a vehicle concealing a "
           "sponsor. seller_canonical stays null because there is no firm to "
           "resolve to, the same as 325 Binney Street, whose seller is Albert and "
           "Austin Brown. The building had stood empty since 2006 under that "
           "ownership, which fits a long-held private asset rather than an "
           "institutional one."),
    (1007, "70-76 EVERETT STREET, $51,500,000, November 2021: THE CLOSING IS NOW "
           "CONFIRMED, THE PARTY STILL IS NOT. New England Development's release "
           "on the finalisation of financing states that the partnership announced "
           "in NOVEMBER 2021 THE CLOSING ON PARCEL A AND PARCEL B -- this row's "
           "month, and the buyer entity is ALLSTON YARDS PARCEL B DEVELOPMENT LLC. "
           "The venture is documented: a project of STOP & SHOP with NEW ENGLAND "
           "DEVELOPMENT as master developer, in partnership with BOZZUTO and "
           "SOUTHSIDE INVESTMENT PARTNERS. Bozzuto's own vehicle for the "
           "residential Building A is BDC ALLSTON LLC, and Parcel B is the "
           "commercial office and life-science building, which suggests a "
           "different partner mix -- SUGGESTS being the operative word. Nothing "
           "states which partners hold Parcel B. THE SELLER ENTITY IS EMPTY, and "
           "with Stop & Shop both the landowner and a project partner, this may be "
           "a land contribution into the venture rather than a purchase, which is "
           "the pattern already documented at 63 Sprague, 20 Guest Street and "
           "Allston LabWorks."),
    (1503, "116 WEST FIRST STREET, $52,000,000, December 2015: A SECOND LEAD, "
           "NEITHER WRITTEN. The asset is the One Channel Center Garage. TISHMAN "
           "SPEYER carries \"One Channel & Channel Center Garage\" on its own "
           "property page with no acquisition date -- already recorded. NEW THIS "
           "PASS: COMMONWEALTH VENTURES / CV PROPERTIES developed the garage, "
           "commissioning Spalding Tougias Architects to design it alongside its "
           "eleven-storey, 521,000 SF One Channel Center office building let to "
           "State Street. CV Properties is separately in this table selling 105 "
           "West First Street to Tishman Speyer with Ares Management. So the "
           "developer and the present owner are both identified and NEITHER is "
           "established as a party to the December 2015 conveyance."),
    (964, "326-330 DORCHESTER AVENUE, $50,000,000, March 2022: THE SELLER ENTITY "
          "RESOLVES TO A PERSON, AND THAT COMPLICATES RATHER THAN SETTLES IT. "
          "Universal Hub identifies BOLD COLONY LLC as \"an entity registered to "
          "MICHAEL GARDNER\", covering a five-acre South Boston assemblage at "
          "Dorchester and Old Colony avenues that includes the Castle Self Storage "
          "facility, a car wash and low-rise buildings. Bisnow records COTTONWOOD "
          "GROUP lending $130 million across seven parcels in 2022 to buy debt, "
          "refinance mortgages and fund new acquisitions, for a scheme of up to 2 "
          "million SF. WHY IT IS NOT WRITTEN: Gardner appears to have still "
          "controlled the assemblage in 2026, when Universal Hub reported it "
          "heading to auction, so a March 2022 conveyance OUT of Bold Colony may "
          "be an intra-assemblage transfer or a financing step rather than a sale. "
          "The buyer entity LMDE8 LLC is not decoded."),
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
