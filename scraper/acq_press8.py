r"""Eighth pass. A prefix confirmed, a near-identical prefix REFUSED, and hotels.

BP <ASSET> LLC = BOSTON PROPERTIES, CONFIRMED. Boston Properties' own SEC filing
records that on 1 February 2016 it completed the sale of 415 Main Street,
Cambridge -- roughly 231,000 NRSF -- to the tenant for a gross price of about
$105.4 million under a fixed-price purchase option granted in the 2004 lease and
exercised in October 2014. This row records $105,360,329 in February 2016, and
its seller entity is BP SEVEN CAMBRIDGE CENTER LLC. That is a source naming the
firm alongside the property, which is what the rule requires.

AND THE SAME PASS REFUSES BP3-BOS5. The buyer of 55 Summer Street is BP3-BOS5 55
SUMMER STREET LLC. Two letters of that look like the prefix just confirmed. It is
NOT written as Boston Properties: the shape is different -- a fund number and an
asset number, BP3 and BOS5 -- and nothing found names Boston Properties at 55
Summer Street. Confirming "BP SEVEN CAMBRIDGE CENTER" does not license reading
every entity that starts with those letters, which is the whole substance of the
RREF lesson.

175 FEDERAL STREET IS LEFT AS A LEAD FOR THE SAME REASON, even though its entity
IS the confirmed shape. BP-175 FEDERAL STREET LLC follows the convention exactly,
Deka Immobilien is confirmed as the $139M May 2016 buyer, and nothing contradicts
Boston Properties as the seller. But no source found names Boston Properties at
175 Federal Street, and the rule is a source naming the firm ALONGSIDE THE
PROPERTY, not a convention that fits. 36-44 Broad Street in the previous pass is
the cautionary case: TRANSWESTERN BROAD ST LLC names its firm outright and is
still contradicted by the reporting.

HOTELS RESOLVE WELL, because hotel owners are usually public REITs that announce
their own dispositions. Three of the four hotel sides in this pass come from the
seller's own investor-relations release.

HOTEL COMMONWEALTH CARRIES A PRICE GAP WORTH RECORDING. Xenia's release says it
completed the disposition at $113.0 million in November 2020; this row records
$84,904,000. Hotel Commonwealth is a condominium within a larger Kenmore Square
building and this row is one parcel of it -- its building_sf is the 1 SF
placeholder -- so the recorded figure is very likely the allocation to the hotel
parcel rather than the deal. The row keeps its recorded price and gains a caveat.

    python scraper/acq_press8.py --apply
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
    (1735, "seller", "BXP (Boston Properties)", "prefix_confirmed",
     'Boston Properties\' own SEC filing: on 1 February 2016 it completed the '
     'sale of 415 Main Street, Cambridge -- an office property of roughly 231,000 '
     'net rentable SF -- for a gross price of about $105.4 million, net proceeds '
     'about $104.9M, a gain of about $60.8M. The buyer was the TENANT, exercising '
     'a fixed-price purchase option granted in its 14 July 2004 lease and '
     'exercised on 22 October 2014. This row records $105,360,329 in February '
     '2016. THIS CONFIRMS THE BP <ASSET> LLC CONVENTION as Boston Properties, '
     'against a named property: the seller entity is BP SEVEN CAMBRIDGE CENTER '
     'LLC and 415 Main Street is Seven Cambridge Center. NOTE ON TYPE: a '
     'pre-agreed fixed-price option exercised eleven years into a lease is not a '
     'marketed trade, so this price reflects 2004 terms, not the 2016 market.'),

    (962, "seller", "Synergy Investments", "web",
     'BLDUP, "Downtown Boston High Rise Trades Hands for $106.6 Million": Hive '
     'Property Owner LLC, a subsidiary of Synergy Investments, sold the '
     'ten-storey office building at 55 Summer Street, on the corner of Chauncy '
     'Street near Downtown Crossing, for $106,646,350 -- this row to the dollar. '
     'Synergy also appears in this table as the operating partner alongside KKR '
     'at Two Drydock and as the 2024 buyer of 179 Lincoln Street.'),

    (1088, "buyer", "Ohana Real Estate Investors", "web",
     'Xenia Hotels & Resorts\' own release, "Xenia Hotels & Resorts Completes '
     'Dispositions Of Hotel Commonwealth And Renaissance Austin Hotel", November '
     '2020, with the Globe reporting the same: the 245-room Hotel Commonwealth in '
     'Kenmore Square went to luxury hotel operator Ohana Real Estate Investors. '
     'The record entity here is KENMORE SQUARE HOTEL LLC.'),
    (1088, "seller", "Xenia Hotels & Resorts", "prefix_confirmed",
     'Same release. THIS CONFIRMS THE XHR PREFIX against a named property: the '
     'record entity is XHR BOSTON COMMONWEALTH and XHR is Xenia Hotels & Resorts, '
     'which acquired Hotel Commonwealth in 2015 for $136 million and sold it in '
     'November 2020 -- a pandemic-era loss the Globe covered as "a landmark Boston '
     'hotel is sold at a loss".'),

    (1643, "seller", "RLJ Lodging Trust", "self_identifying",
     'The record entity is RLJ CAMBRIDGE HOTEL LLC, which names its own owner. '
     'RLJ Lodging Trust is independently established in this table as the seller '
     'of the Fairmont Copley Plaza in December 2017, confirmed there by its own '
     'release and its FY2017 10-K, so this is not a bare name match. NOT THE '
     'RESIDENCE INN AT 120 BROADWAY: Xenia sold that hotel for $107.5M in the same '
     'month, October 2020, and the two are easy to conflate. They are different '
     'assets with different owners -- this row is 100 Broadway at $98,991,934 with '
     'an RLJ entity, that one is 120 Broadway at $107.5M with a Xenia entity.'),

    (1016, "buyer", "RLJ Lodging Trust", "self_identifying",
     'The record entity is RLJ MACH BOSTON LLC, which names its own owner, and '
     'RLJ Lodging Trust is independently established in this table at the Fairmont '
     'Copley Plaza. The MACH element is NOT decoded -- no source found says what '
     'it stands for -- but it does not need to be, because the sponsor is on the '
     'face of the name without it.'),

    (1213, "buyer", "Gazit Horizons", "web",
     'The record entity is GAZIT HORIZONS (MARKETPLACE) LLC, which names its own '
     'owner, and the press confirms the deal directly: Gazit bought the RETAIL '
     'condominium at 200 State Street for $81.8 million in April 2019, five months '
     'after Carr Properties bought the OFFICE condominium on the same parcel for '
     '$222 million. This row is $81,800,000 in April 2019, so it is the Gazit '
     'purchase and the Gazit entity belongs on it. That pairing is what exposed '
     'the condominium fault: the OTHER 200 State Street row had also been stamped '
     'with Gazit, because both rows share a parcel and Gazit is its current owner.'),
]

NOTES = [
    (1464, "175 FEDERAL STREET SELLER: BOSTON PROPERTIES IS A LEAD, NOT THE "
           "SELLER. The record entity is BP-175 FEDERAL STREET LLC, which follows "
           "the BP <ASSET> LLC convention confirmed as Boston Properties in this "
           "same pass, at 415 Main Street, from Boston Properties' own SEC filing. "
           "The buyer side is solid -- NEREJ and the BBJ both report Deka "
           "Immobilien buying the 17-storey, 227,360 SF tower for $139 million in "
           "May 2016, its first Boston purchase, part of about $250m of Boston "
           "deals that year. But NO SOURCE FOUND NAMES BOSTON PROPERTIES AT 175 "
           "FEDERAL STREET, and the standing rule is a source naming the firm "
           "alongside the property, not a naming convention that fits. 36-44 Broad "
           "Street is why: TRANSWESTERN BROAD ST LLC names its firm outright and "
           "is still contradicted by the reporting."),
    (962, "55 SUMMER STREET BUYER: BP3-BOS5 IS NOT DECODED, AND DELIBERATELY SO. "
          "The record entity is BP3-BOS5 55 SUMMER STREET LLC. Boston Properties "
          "is confirmed in this table under the BP <ASSET> LLC convention, and the "
          "temptation to read BP3 the same way is exactly the RREF error in a new "
          "costume. The shape is different -- BP3 and BOS5 read as a fund number "
          "and an asset number within a series -- and nothing found names Boston "
          "Properties at 55 Summer Street. Left blank."),
    (1443, "90 TREMONT STREET, $85,100,300, September 2016. SEARCHED AND NOT "
           "FOUND. The property is the Nine Zero Hotel, now Hotel AKA Boston "
           "Common. Coverage found is of a LATER trade -- Electra America "
           "Hospitality Group acquiring it for $82,630,000 around 2021 -- not of "
           "the 2016 conveyance. Neither KHP BOSTON HOTEL LLC nor THI VI BOSTON "
           "LLC is decoded; both have plausible readings in the hotel-fund world "
           "and neither is confirmed against this address."),
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
        log.info("id=%-5s %-6s %-34s -> %-42s [%s]", rid, side,
                 (cur[0] or "")[:34], sponsor, basis)
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
        for rid, note in NOTES:
            conn.execute(text(
                "update transactions set notes = coalesce(notes,'') || :n "
                "where id = :id"), {"n": " | " + note, "id": rid})
        conn.execute(text(
            "update transactions "
            "set price_caveat = coalesce(price_caveat || ' ', '') || :c "
            "where id = 1088"), {
            "c": ("Xenia's own release puts the Hotel Commonwealth disposition at "
                  "$113.0M; this row records $84,904,000. The hotel is a "
                  "condominium within a larger Kenmore Square building and this "
                  "row is one parcel of it, so the recorded figure is most likely "
                  "the allocation to that parcel rather than the whole deal.")})
        conn.commit()

    log.info("\n%d sides written", n)
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
