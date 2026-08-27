r"""Second historical press pass, and the two misses that mark where it stops.

Fourteen transactions searched across both passes, twelve resolved. The two
failures are as informative as the hits, so they are recorded on the rows rather
than left as silent blanks.

    595 Memorial Drive, $227.3M, Oct 2019     no coverage found
    643-653 Summer Street, $282.5M, Oct 2019  no coverage found

Both are large. Neither returned anything: 595 Memorial is swamped in search by
One Memorial Drive's $825M and $1.05B trades, and 643-653 Summer sits inside the
Massport marine industrial park where the press covers leases and Massport board
votes rather than conveyances. Size does not guarantee coverage.

A THIRD CORRECTION OF THE SAME KIND AS 101 SEAPORT. 200 State Street appears
twice, once as OFFICE and once as RETAIL, because the office condominium and the
retail condominium share a parcel. Both rows carried GAZIT HORIZONS as buyer,
because the buyer on a spine row comes from the parcel's CURRENT owner and Gazit
owns the retail. But the $222M November 2018 conveyance is the OFFICE
condominium, and Newmark's release and NEREJ both state it plainly: Carr
Properties bought it from GLL Real Estate Partners. Gazit's own $81.8M purchase
of the retail is the other row, five months later.

That is now three rows where the current-owner derivation has attached the wrong
party on a condominiumised parcel -- 101 Seaport, 200 State office, and by
extension any parcel holding several condominium units. It is a systematic
weakness of the method, not three coincidences.

CONGRESS SQUARE IS A 95% STAKE. Hana Financial Group and KTB Asset Management
acquired 95% for $342M with Related Fund Management retaining 5%. Recorded as an
asset sale it reads as a whole-building trade. That is the second mis-typed
partial interest found this way, after 101 Federal.

    python scraper/acq_historical_press2.py --apply
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
    (1567, "seller", "U.S. Government (GSA)",
     'Banker & Tradesman: "MIT Completes $750M Kendall Square Acquisition." MIT '
     'agreed in 2017 to pay $750 million for the Volpe campus from the federal '
     'government and closed on 26 January 2024, matching this row on price and '
     'month. The counterparty already recorded here is MIT.'),

    (983, "buyer", "Hana Financial Group / KTB Asset Management",
     'CommercialCafe, "Congress Square Office Tower in Boston Recapitalized": '
     'Hana Financial Group, on behalf of a Korean investor group, and KTB Asset '
     'Management acquired a 95% ownership stake in Congress Square for $342 '
     'million in December 2021, with Related Fund Management retaining 5%. KKR '
     'provided financing. The entity RFM-KTB CSQ PROPCO LLC carries both the '
     'Related and KTB initials and the Congress Square abbreviation.'),
    (983, "seller", "Related Fund Management",
     'Same reporting. Related sold 95% and retained 5%, so it is the disposing '
     'party. Its vehicle RFM BLOCK ON CONGRESS I LLC is the seller of record.'),

    (1652, "seller", "The Davis Cos. / Invesco Real Estate",
     'The Davis Companies\' own release: "The Davis Companies Sells Alewife '
     'Research Center in Cambridge to HealthPeak for $332.5 Million." The '
     '224,305 SF LEED Gold lab at 35 CambridgePark Drive was developed and held '
     'by a partnership of The Davis Companies and Invesco Real Estate. '
     'Corroborated by NEREJ and GlobeSt. Recorded as the venture, not one '
     'partner.'),

    (1265, "buyer", "Carr Properties",
     'CORRECTION, NOT A FILL. This row previously read GAZIT HORIZONS, taken '
     'from the parcel\'s current owner. NEREJ: "GLL Real Estate sells 200 State '
     'St. to Carr Properties for $222 million", and Boston Real Estate Times '
     'reports the same, for the 304,178 SF office condominium plus a 120-space '
     'garage, November 2018. Gazit owns the RETAIL condominium on the same '
     'parcel and bought it separately for $81.8M in April 2019, which is the '
     'other 200 State Street row. The buyer derivation cannot distinguish '
     'condominium units sharing a parcel.'),
    (1265, "seller", "GLL Real Estate Partners",
     'Same reporting: Newmark Knight Frank acted for GLL Real Estate Partners.'),
]

MISSES = [
    (1653, "595 Memorial Drive, $227,300,000, October 2019. SEARCHED AND NOT "
           "FOUND. Repeated searches on address, price and year returned only "
           "One Memorial Drive's $825M and $1.05B trades, which dominate every "
           "result for the street. The record entity HMC CAMBRIDGE LLC is not "
           "decoded: HMC could plausibly stand for Harvard Management Company, "
           "and that is precisely the inference that produced the RREF/Rialto "
           "error, so it is not written."),
    (1165, "643-653 Summer Street, $282,500,000, October 2019. SEARCHED AND NOT "
           "FOUND. The property sits inside the Massport marine industrial park, "
           "where press coverage runs to leases, board votes and RFQs rather "
           "than conveyances. Size is no guarantee of coverage: this is a "
           "quarter-billion-dollar trade with no findable reporting."),
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
        log.info("id=%-5s %-6s %-34s %s-> %s", rid, side, (cur[0] or "")[:34],
                 f"(was {cur[1]}) " if cur[1] else "", sponsor)
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
        # Congress Square is a 95% stake, not a whole-building sale.
        conn.execute(text("""
            update transactions
               set transaction_type = 'partial_interest', pct_acquired = 95,
                   price_caveat = :cav,
                   notes = coalesce(notes,'') || :n
             where id = 983"""), {
            "cav": ("Price is for a 95% interest; Related Fund Management "
                    "retained 5%."),
            "n": (" | TYPE CORRECTED TO PARTIAL INTEREST. Hana Financial Group "
                  "and KTB Asset Management acquired 95% of Congress Square for "
                  "$342M in December 2021; Related Fund Management retained 5%. "
                  "Recorded as an asset sale it reads as a whole-building trade. "
                  "This is the second mis-typed partial interest found by press "
                  "checking, after 101 Federal Street, so the assessment roll "
                  "records stake conveyances at their stake price with no "
                  "indication that is what they are.")})
        for rid, note in MISSES:
            conn.execute(text(
                "update transactions set notes = coalesce(notes,'') || :n "
                "where id = :id"), {"n": " | " + note, "id": rid})
        conn.commit()

    tot = conn.execute(text(
        "select count(*) from transactions where coalesce(quarantined,0)=0")).scalar()
    log.info("\n%d sides written", n)
    for side in ("buyer", "seller"):
        v, d = conn.execute(text(
            f"select count(*), sum(coalesce(price,0)) from transactions "
            f"where coalesce(quarantined,0)=0 and coalesce({side}_canonical,'') <> ''"
        )).first()
        log.info("%s_canonical: %d of %d (%.0f%%), $%.2fB", side, v, tot,
                 v / tot * 100, (d or 0) / 1e9)
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
