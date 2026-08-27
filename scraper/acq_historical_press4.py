r"""Fourth press pass. A THIRD MIS-TYPED PARTIAL INTEREST, and a successor error.

THE TAJ BOSTON IS A 95% STAKE. 15 Arlington Street, $196M, April 2018, sat in
the table as a whole-building asset sale. Eastern Real Estate's own property
page states that the ownership partnership "opportunistically sold 95% of the
asset to Iconiq Capital" in 2018. That is the third partial interest found by
press-checking large rows, after 101 Federal (50%) and Congress Square (95%),
and it is the same failure each time: the registry records what was paid for a
stake with nothing to say it was a stake.

TWO NEAR-COLLISIONS OF NAMES, BOTH RESOLVED THE SLOW WAY.

    ICONIQ vs ICONIC.  The Taj buyer is ICONIQ CAPITAL. The Fairmont Copley
                       Plaza buyer's entity is ICONIC COPLEY PLAZA HOTEL LLC.
                       One letter apart, two different firms, two Boston hotels
                       trading four months apart. The Copley buyer was NOT
                       inferred from its entity: RLJ announced the sale with the
                       buyer undisclosed, and Ashkenazy Acquisition Corp is
                       established because it carries the coverage on its own
                       press page and BLDUP names it outright.
    FELCOR vs RLJ.     The Copley seller read FelCor Lodging Trust, taken from
                       the entity FELCOR COPLEY PLAZA OWNER. But RLJ Lodging
                       Trust acquired FelCor in 2017 and it is RLJ that
                       announced and completed this sale in December 2017. The
                       entity kept its old name after the merger, which is
                       normal and is exactly why an entity name is not a
                       sponsor. Corrected to RLJ.

A PRICE THAT DISAGREES WITH ITSELF, AND THE ROW IS RIGHT. RLJ reported the
Copley Plaza sale at $170.0M; BLDUP reported $163M; this row records
$163,000,000. Both figures are real and the gap is the usual one between
announced consideration and recorded price. The row is not adjusted.

    python scraper/acq_historical_press4.py --apply
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
    (1313, "buyer", "Iconiq Capital",
     'Eastern Real Estate\'s own property page for the Taj Boston: the '
     'partnership "opportunistically sold 95% of the asset to Iconiq Capital" in '
     '2018. Lubert-Adler lists the same asset in its portfolio. NOT DECODED FROM '
     'THE ENTITY. The record entity is IREP NEWBURY HOTEL LLC, and the hotel was '
     'rebranded The Newbury Boston in 2019, but the sponsor comes from the '
     'seller\'s own account of the deal. Highgate Hotels appears in coverage as '
     'the operator that repositioned and rebranded the hotel; whether it also '
     'took equity is not stated anywhere found, so it is not recorded as a '
     'partner.', False),
    (1313, "seller", "New England Development / Eastern Real Estate / Lubert-Adler",
     'New England Development\'s release "Consortium Buys the Historic Taj '
     'Boston" records the 2016 purchase at $125M by New England Development, '
     'Eastern Real Estate and partners. Eastern Real Estate and Lubert-Adler both '
     'carry the asset on their own portfolio pages. Recorded as the consortium, '
     'not one member.', False),

    (1336, "buyer", "Ashkenazy Acquisition Corp.",
     'BLDUP, "Ashkenazy acquires Fairmont Copley Plaza hotel for $163 million", '
     'matching this row exactly on price and month. Ashkenazy Acquisition Corp '
     'carries the transaction coverage on its own press page, and it already '
     'holds the South Station and Faneuil Hall leaseholds, so the Boston '
     'portfolio fits. Deutsche Bank provided $95M of mortgage financing. RLJ '
     'announced the sale with the buyer undisclosed, so this is established from '
     'the buyer\'s side, not the seller\'s. THE ENTITY WAS NOT USED: ICONIC '
     'COPLEY PLAZA HOTEL LLC is one letter from Iconiq Capital, which bought a '
     'different Boston hotel four months later. They are different firms.', False),
    (1336, "seller", "RLJ Lodging Trust",
     'CORRECTION. This row read FelCor Lodging Trust, taken from the entity '
     'FELCOR COPLEY PLAZA OWNER. RLJ Lodging Trust acquired FelCor in 2017, and '
     'it is RLJ that announced and completed the sale of the 383-room Fairmont '
     'Copley Plaza on 15 December 2017, at roughly $444,000 per key, in its own '
     'release and its FY2017 10-K. The entity kept the FelCor name after the '
     'merger, which is ordinary and is the reason an entity name is not a '
     'sponsor. FelCor remains correct for pre-2017 rows.', True),

    (1356, "buyer", "Fortis Property Group",
     'NEREJ, "Fortis Property Group acquires Dock Sq. Garage for $170 million", '
     'with REBusinessOnline, Boston Real Estate Times and The Real Reporter '
     'reporting the same September 2017 deal: 698 spaces plus 16,100 SF of '
     'retail let to Hard Rock Cafe, beside Faneuil Hall.', False),
    (1356, "seller", "Sullivan Properties",
     'The Real Reporter, "Fortis Buying Dock Square Garage from Sullivan '
     'Properties via Newmark for Near $170M". Newmark Knight Frank acted for '
     'Dock Square Parking Associates LLC, which is the record entity on this '
     'row, and the press names Sullivan Properties as the principal behind it.',
     False),

    (1141, "buyer", "Blackstone Real Estate",
     'Connect CRE, "Blackstone Pays $156M for Creative Offices Near South '
     'Station", and Boston Real Estate Times, "Blackstone Real Estate Buys 179 '
     'Lincoln Street in Downtown Boston": Blackstone bought the 221,474 SF '
     'five-storey building from Invesco Real Estate in January 2020 for '
     '$155.65M, this row\'s exact price. The seller side already read Invesco, '
     'written from the entity INVESCO IF IV naming its own manager, so entity '
     'and press agree here. Blackstone later sold the building to Synergy for '
     '$76.5M in March 2024, a 51% decline, which is a repeat-sale pair worth '
     'having if a licensed feed ever supplies the 2024 leg.', False),

    (1624, "seller", "Clarion Partners",
     'Cambridge Day, 13 September 2021: Clarion Partners sold roughly 185,000 SF '
     'of office and industrial space plus developable land in Alewife for $180 '
     'million, bounded by Concord Avenue and Fawcett and Moulton streets in West '
     'Cambridge and centred on 617 Concord Ave. The portfolio last traded in '
     '2013 for $61M. The record entity CV PORTFOLIO WEST CAMBRIDGE LLC is '
     'consistent with a portfolio vehicle. The buyer side already read '
     'Healthpeak.', False),
]

STAKE = (1313, 95,
         "Price is for a 95% interest; the selling consortium retained 5%.",
         " | TYPE CORRECTED TO PARTIAL INTEREST. Eastern Real Estate's own "
         "property page states the ownership partnership sold 95% of the Taj "
         "Boston to Iconiq Capital in 2018. Recorded as an asset sale it reads "
         "as a whole-building trade. This is the THIRD mis-typed partial "
         "interest found by press-checking large rows, after 101 Federal (50%) "
         "and Congress Square (95%), and the pattern is consistent: the registry "
         "records the price paid for a stake and carries nothing to indicate it "
         "was a stake. `price` stays at $196,000,000 because that is what "
         "changed hands; no implied whole-asset value is written because no "
         "source states one.")

NOTES = [
    (1158, "99 SUMMER STREET: A STRONG AFFILIATED-TRANSFER LEAD, NOT QUARANTINED. "
           "The buyer of record is 99 SUMMER OWNER II LLC and the seller of "
           "record is 99 SUMMER OWNER LLC -- the same vehicle name with a "
           "successor numeral. Rockpoint Group bought the 20-storey, ~270,000 SF "
           "building from Cornerstone Real Estate Advisers in December 2015 "
           "(Business Wire, GlobeSt, The Real Reporter) and it is still managed "
           "by Rockhill Management, a Rockpoint affiliate, so no intervening "
           "third-party owner is known. A $198M conveyance in October 2019 "
           "between OWNER and OWNER II reads as a fund-to-fund transfer or a "
           "recapitalisation rather than a sale. IT IS NOT QUARANTINED because a "
           "shared name stem was deliberately rejected as a quarantine signal: "
           "address-named vehicles collide by construction. The 'II' convention "
           "is stronger than a bare stem but it is still not proof, and no press "
           "reports a 2019 trade or recap here. Flagged for a licensed feed."),
    (1701, "21 THORNDIKE STREET, $202,500,000, February 2017. SEARCHED AND NOT "
           "FOUND. The record entity is DAVENPORT OWNER (DE) LLC. DO NOT DECODE "
           "IT: 21 Thorndike sits in the A.H. Davenport Co. furniture works "
           "complex in East Cambridge, so 'Davenport' names the BUILDING, not a "
           "firm -- the same trap as PUTNAM CIRCLE ASSOCIATES being a street. "
           "Searches returned only the separate Sullivan Courthouse "
           "redevelopment at 40 Thorndike by Leggat McCall Properties, which is "
           "a different address and a different deal."),
]


def main(dry_run: bool):
    conn = engine.connect()
    n = 0
    for rid, side, sponsor, why, force in RESOLVE:
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
        log.info("id=%-5s %-6s %-36s -> %s%s", rid, side, (cur[0] or "")[:36],
                 tag, sponsor)
        if not dry_run:
            conn.execute(text(f"""
                update transactions
                   set {side}_canonical = :s, {side}_confidence = 'web_corroborated',
                       {side}_resolution_basis = 'web',
                       notes = coalesce(notes,'') || :n
                 where id = :id"""), {
                "s": sponsor, "id": rid,
                "n": f" | {side.upper()} RESOLVED FROM PRESS. " + why})
            n += 1

    if not dry_run:
        rid, pct, caveat, note = STAKE
        conn.execute(text("""
            update transactions
               set transaction_type = 'partial_interest', pct_acquired = :p,
                   price_caveat = :c, notes = coalesce(notes,'') || :n
             where id = :id"""),
            {"p": pct, "c": caveat, "n": note, "id": rid})
        log.info("\nid=%s re-typed to partial_interest, %d%%", rid, pct)
        for rid, note in NOTES:
            conn.execute(text(
                "update transactions set notes = coalesce(notes,'') || :n "
                "where id = :id"), {"n": " | " + note, "id": rid})
        conn.commit()

    log.info("%d sides written", n)
    tot = conn.execute(text(
        "select count(*) from transactions where coalesce(quarantined,0)=0")).scalar()
    for side in ("buyer", "seller"):
        v, d = conn.execute(text(
            f"select count(*), sum(coalesce(price,0)) from transactions "
            f"where coalesce(quarantined,0)=0 and coalesce({side}_canonical,'') <> ''"
        )).first()
        log.info("%s_canonical: %d of %d (%.0f%%), $%.2fB", side, v, tot,
                 v / tot * 100, (d or 0) / 1e9)
    for t, c, d in conn.execute(text(
            "select transaction_type, count(*), sum(coalesce(price,0)) "
            "from transactions where coalesce(quarantined,0)=0 "
            "group by transaction_type order by 2 desc")):
        log.info("  %-18s %4d  $%.2fB", t, c, (d or 0) / 1e9)
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
