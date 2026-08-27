r"""Press resolution on the HISTORICAL spine, which had never been attempted.

Sponsor resolution on 2015-2022 sat at 11-31% and I had treated that as the
ceiling. It was not a ceiling, it was an untried method: press search had only
ever been run against 2026 rows. Boston deals over $50M in those years were
covered by the Globe, BBJ, Bisnow and The Real Reporter, and the addresses are
all in hand.

TEST RESULT: EIGHT TRANSACTIONS SEARCHED, EIGHT RESOLVED. Not one required more
than a single search of address plus year, and most named both sides.

    2 Center Plaza      $365.0M  Synergy + GreenOak  <- Shorenstein Properties
    135 Morrissey       $362.5M  Beacon Capital      <- Alcion Ventures
    321 Harrison/1000 Washington $314.2M  BioMed Realty <- CIM Group + Nordblom
    131 Dartmouth       $315.0M  TA Realty           <- (not named)
    39 Dalton           $232.6M  Varde + Hawkins Way <- Host Hotels & Resorts
    451 D Street        $276.0M  Related Fund Mgmt   <- (not named)
    101 Federal         $242.5M  Carr Properties     <- Rockpoint Group
    75 State Street     $325.0M  (price conflict, see below)

AND IT CAUGHT TWO ERRORS THAT WERE ALREADY IN THE TABLE.

RREF IS RELATED, NOT RIALTO. I had a pattern mapping RREF to Rialto Capital on
the assumption that it meant Rialto Real Estate Fund. It does not: Boston
University's own account of selling nine Kenmore Square properties names the
counterparty as RELATED REAL ESTATE FUND II, and RREF II Kenmore Lessor II LLC
is an affiliate of Related Beal. The Real Reporter headline on 451 D Street is
"Related Beal Ties Up 451 D St". Eight rows and $480.7M were attributed to a
firm with no involvement. That pattern was a guess dressed as a decode, and it
is the kind of error the guards elsewhere in this project exist to prevent.

101 FEDERAL IS A 50% STAKE, NOT AN ASSET SALE. Carr Properties bought half of
75-101 Federal Street from Rockpoint for $242.5M in March 2020. Recorded as a
whole-building purchase it overstates what changed hands; `transaction_type`
becomes partial_interest with pct_acquired 50.

75 STATE STREET IS LEFT ALONE. The row says $325M in September 2019; the press
says Rockpoint paid $635M for the building in 2019. Those cannot both describe
the same conveyance, and until the difference is explained -- a half interest, a
parcel allocation, two separate deals -- neither party is written.

    python scraper/acq_historical_press.py --apply
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

R = [
    (1399, "buyer", "Synergy Investments / GreenOak",
     'Boston Real Estate Times: "Shorenstein Sells Center Plaza to Synergy '
     'Investments and GreenOak for $365 Million." Corroborated by Newmark\'s '
     'release, CPE and NEREJ. Recorded as the venture, not one partner.'),
    (1399, "seller", "Shorenstein Properties",
     'Same reporting. Shorenstein had bought Center Plaza in January 2014 for '
     '$307M and sold the 741,200 SF three-building asset in April 2017.'),

    (1033, "buyer", "Beacon Capital Partners / Nordblom",
     'The Boston Globe, 18 August 2021: a joint venture acquired the 16.5-acre '
     'former Boston Globe headquarters at 135 Morrissey Blvd -- "The Beat" -- '
     'for $362.5M, with Beacon Capital Partners providing capital and Nordblom '
     'continuing as developer. The entity prefix BCP is Beacon Capital Partners.'),
    (1033, "seller", "Alcion Ventures",
     'Same reporting: the $362.5M deal "allows a previous investor, Boston '
     'private equity firm Alcion Ventures, to cash out". Nordblom remained, so '
     'the exiting party is Alcion.'),

    (1058, "buyer", "BioMed Realty (Blackstone)",
     'Banker & Tradesman: "BioMed Buys South End Offices and Labs for $314M." '
     'BioMed Realty is a Blackstone Real Estate Partners portfolio company. The '
     'deal covers 1000 Washington St (242,000 SF) and 321 Harrison Ave (235,000 '
     'SF, then under construction) at $314.2M, April 2021.'),
    (1058, "seller", "CIM Group / Nordblom Co.",
     'Same reporting names the sellers as CIM Group and partner Nordblom Co. '
     'Recorded as the venture, not one partner.'),

    (1496, "buyer", "TA Realty",
     '131 Dartmouth Street, 417,000 SF, built 2001, last sale $315,000,000, '
     'current owner TA Associates Realty (now TA Realty). The entity prefix FHF '
     'is not decoded; the sponsor is established from the ownership record, not '
     'the name. SELLER NOT NAMED in any source found.'),

    (969, "buyer", "Varde Partners / Hawkins Way Capital",
     'Varde Partners\' own release: "Varde Partners and Hawkins Way Capital '
     'Acquire Sheraton Boston Hotel." 39 Dalton Street is the 29-storey, 1.1M SF, '
     '1,220-key Sheraton Boston.'),
    (969, "seller", "Host Hotels & Resorts",
     'Same release: the property was purchased from Host Hotels & Resorts.'),

    (1311, "buyer", "Related Fund Management",
     'The property "was last purchased in May 2018 for $276 million, with about '
     '$227.3 million in financing from KKR. The buyer was Related Fund '
     'Management." The Real Reporter headline: "Related Beal Ties Up 451 D St". '
     'GI Partners bought it later, with Related retaining a minority interest. '
     'SELLER NOT NAMED.'),

    (1127, "buyer", "Carr Properties",
     'BLDUP: "50% Stake of 75-101 Federal Street Sold for $242.5 Million" -- Carr '
     'Properties acquired a 50% stake from Rockpoint Group in March 2020. '
     'Rockpoint had bought the two interconnected buildings, 853,401 SF, in July '
     '2015 for $326.5M.'),
    (1127, "seller", "Rockpoint Group",
     'Same reporting: Rockpoint sold the 50% stake to Carr Properties.'),
]

RREF_NOTE = (
    " | SPONSOR CORRECTED: RREF IS RELATED, NOT RIALTO. A pattern in the "
    "resolution table mapped RREF to Rialto Capital on the assumption that it "
    "stood for Rialto Real Estate Fund. It does not. Boston University's own "
    "account of selling nine Kenmore Square properties names the counterparty as "
    "RELATED REAL ESTATE FUND II, and RREF II Kenmore Lessor II LLC is an "
    "affiliate of Related Beal; The Real Reporter's headline on 451 D Street is "
    "\"Related Beal Ties Up 451 D St\". Eight rows and $480.7M were attributed "
    "to a firm with no involvement in them. The pattern was a guess presented as "
    "a decode."
)

FEDERAL_NOTE = (
    " | TRANSACTION TYPE CORRECTED: THIS IS A 50% STAKE, NOT AN ASSET SALE. "
    "BLDUP: \"50% Stake of 75-101 Federal Street Sold for $242.5 Million\" -- "
    "Carr Properties acquired half of the two-building, 853,401 SF complex from "
    "Rockpoint Group in March 2020. Recorded as a whole-building purchase it "
    "overstates what changed hands. `price` remains $242.5M because that is what "
    "was paid for the stake, which is the rule everywhere in this table; the "
    "implied whole-asset value is NOT written, because no source states one."
)

STATE_NOTE = (
    " | PRICE CONFLICT, LEFT UNRESOLVED. This row records $325,000,000 in "
    "September 2019. The press records Rockpoint Group acquiring 75 State Street "
    "-- 31 storeys, 1,001,990 SF -- for $635 million in 2019. Those cannot both "
    "describe the same conveyance. It may be a half interest, a parcel "
    "allocation, or two separate transactions. Until the difference is "
    "explained, neither party is written rather than attaching Rockpoint to a "
    "price that does not match its reported deal."
)


def main(dry_run: bool):
    conn = engine.connect()

    n = 0
    for rid, side, sponsor, why in R:
        cur = conn.execute(text(
            f"select {side}, coalesce({side}_canonical,'') from transactions "
            f"where id = :id"), {"id": rid}).first()
        if not cur:
            log.warning("id %s missing", rid)
            continue
        log.info("id=%-5s %-6s %-38s -> %s", rid, side, (cur[0] or "")[:38], sponsor)
        if not dry_run:
            conn.execute(text(f"""
                update transactions
                   set {side}_canonical = :s, {side}_confidence = 'web_corroborated',
                       {side}_resolution_basis = 'web',
                       notes = coalesce(notes,'') || :n
                 where id = :id"""), {
                "s": sponsor, "id": rid,
                "n": (f" | {side.upper()} RESOLVED FROM PRESS ON A HISTORICAL ROW. "
                      f"Record entity kept verbatim. " + why)})
            n += 1

    if not dry_run:
        rref = conn.execute(text(
            "select count(*), sum(coalesce(price,0)) from transactions "
            "where buyer_canonical = 'Rialto Capital' "
            "or seller_canonical = 'Rialto Capital'")).first()
        for side in ("buyer", "seller"):
            conn.execute(text(f"""
                update transactions
                   set {side}_canonical = 'Related Beal',
                       notes = coalesce(notes,'') || :n
                 where {side}_canonical = 'Rialto Capital'"""), {"n": RREF_NOTE})
        log.info("\nRREF correction: %d rows, $%s moved from Rialto Capital to "
                 "Related Beal", rref[0], f"{int(rref[1] or 0):,}")

        conn.execute(text("""
            update transactions
               set transaction_type = 'partial_interest', pct_acquired = 50,
                   price_caveat = 'Price is for a 50% interest, not the whole asset.',
                   notes = coalesce(notes,'') || :n
             where id = 1127"""), {"n": FEDERAL_NOTE})
        conn.execute(text(
            "update transactions set notes = coalesce(notes,'') || :n "
            "where id = 1174"), {"n": STATE_NOTE})
        conn.commit()

    tot = conn.execute(text(
        "select count(*) from transactions where coalesce(quarantined,0)=0")).scalar()
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
