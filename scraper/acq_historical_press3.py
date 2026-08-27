r"""Third historical press pass, and it found two WRONG BUYERS as well as fills.

Eleven transactions searched above $50M, nine resolved on at least one side.
More importantly, checking the counterpart side before writing -- rather than
only the blank side -- caught two rows that already carried a confident sponsor
and carried the wrong one. Both are the current-owner derivation failing in the
same way the condominium sweep described, and neither would have been found by
looking only at what was empty.

101 SEAPORT IS UNION INVESTMENT, NOT WS DEVELOPMENT. The row read WS SEAPORT
L-1 LLC -> WS Development, taken from the parcel. But Skanska's own press
release, Goodwin's deal announcement and Connect CRE all state that Skanska USA
sold 101 Seaport to Union Investment for $452M, closing April 2016 -- the exact
price and month on this row. WS Development is the Seaport master developer and
owns land on that block; it did not buy this tower.

TWO DRYDOCK IS KKR WITH SYNERGY, NOT SYNERGY. The row read Synergy alone.
Skanska's release is titled "KKR and Synergy acquire Two Drydock" -- KKR is the
capital, Synergy participates alongside and operates. Under the joint-venture
rule both partners are recorded. The record entity KRE 2DD OWNER LLC is
consistent with that, but the press is what establishes it.

TWO ROWS ARE DELIBERATELY LEFT BLANK, and the reasons are different.

    185 Franklin seller   Bentall Kennedy is a LEAD, not the seller. It is the
                          successor to Kennedy Associates, which bought the
                          tower for $192M in 2008, and NEREJ has it selecting
                          Suffolk to renovate 50 Post Office Square before the
                          2015 sale -- an owner's act. But no source reports it
                          selling. That is a two-step inference of exactly the
                          shape that produced the RREF/Rialto error.
    374 Congress seller   A genuine CONFLICT. NEREJ says the seven-property
                          Fort Point portfolio came from a separate account
                          advised by Clarion Partners. The record entity is
                          AG/ND FORT POINT LLC. Those may be reconcilable, and
                          guessing which is not resolution.

    python scraper/acq_historical_press3.py --apply
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

# (id, side, sponsor, note, force). `force` overwrites an existing canonical.
RESOLVE = [
    (1583, "buyer", "BXP (Boston Properties)",
     'BXP\'s own release, "BXP Expands Life Sciences Portfolio in Kendall '
     'Square", with Bisnow and Connect CRE reporting the same: BXP acquired the '
     'six-storey, 271,000 SF lab at 125 Broadway in September 2022. NOTE ON '
     'PRICE. BXP reported roughly $592M and Biogen reported gross proceeds of '
     'roughly $603M; this row records $602,840,000, which is the Biogen figure. '
     'The difference is the two sides accounting for the same conveyance '
     'differently, not two transactions.', False),
    (1583, "seller", "Biogen",
     'Biogen\'s own statement, "Biogen Completes Corporate Real Estate '
     'Transaction": a sale and leaseback of Building 8 at 125 Broadway, with '
     'Biogen leasing the whole property back through April 2028. Recorded as an '
     'arm\'s-length asset sale because it is one; the leaseback is a term of the '
     'deal, not an affiliation between the parties.', False),

    (1469, "buyer", "Union Investment",
     'CORRECTION, NOT A FILL. This row read WS Development, derived from the '
     'parcel\'s current owner. Skanska\'s own release ("Skanska divests office '
     'development in Boston, for USD 452M"), Goodwin\'s deal announcement and '
     'Connect CRE all name Union Investment Real Estate GmbH as the buyer of 101 '
     'Seaport for $452M, closing April 2016 -- this row\'s exact price and month. '
     'WS Development is the Seaport master developer and holds land on the same '
     'block, which is why its entity sits on the parcel; it did not buy this '
     'tower. Fourth instance of the current-owner derivation naming the wrong '
     'party.', True),
    (1469, "seller", "Skanska USA Commercial Development",
     'Same reporting. Skanska developed the 17-storey, ~440,000 SF LEED tower '
     'and booked the divestment in Q1 2016. The Globe recorded it as a '
     'price-per-square-foot record for Boston at the time.', False),

    (1502, "buyer", "LaSalle Investment Management",
     'LaSalle\'s own release on the 110 High Street lobby states it acquired 50 '
     'Post Office Square in December 2015 on behalf of a US separate-account '
     'client, and the record entity CSHV 50 POST OFFICE SQUARE LLC is registered '
     'in Boston. 50 Post Office Square is the same building as 185 Franklin '
     'Street, the former New England Telephone tower. Corroborated by Bisnow and '
     'CPE.', False),

    (1205, "buyer", "Related Beal",
     'Caught In Southie\'s development report and BLDUP both state that Related '
     'Beal bought 244-284 A Street from Procter & Gamble for $218 million in '
     'spring 2019, matching this row on price and month. The seller side of this '
     'row already read Procter & Gamble (Gillette), written independently from '
     'the entity GILLETTE COMPANY naming its own owner, so the two methods '
     'corroborate each other here.', False),

    (988, "buyer", "Nan Fung Life Sciences Real Estate",
     'Bisnow, "Life Sciences Player Nan Fung Pays Credit Suisse $238M For '
     'Independence Wharf", and Banker & Tradesman reporting the same: a '
     'subsidiary of Hong Kong-based Nan Fung Group closed on the 14-storey, '
     '337,000 SF tower at 470 Atlantic Avenue in December 2021.', False),
    (988, "seller", "Credit Suisse",
     'Same reporting names the seller as an affiliate of Credit Suisse; Jones '
     'Day records advising the Credit Suisse affiliate on its earlier '
     'acquisition of the building.', False),

    (991, "buyer", "KKR / Synergy Investments",
     'CORRECTION. This row read Synergy alone. Skanska\'s release is titled "KKR '
     'and Synergy Acquire Two Drydock in the Seaport\'s Raymond L. Flynn Marine '
     'Park from Skanska", with Bisnow and REBusinessOnline reporting the same: '
     'KKR is the capital and Synergy Investments participates alongside it and '
     'operates the property. Under the rule that a joint venture resolves to all '
     'partners rather than the most visible one, both are recorded.', True),
    (991, "seller", "Skanska USA Commercial Development",
     'Same release: Skanska divested the 13-storey, 235,000 SF Class A building '
     'for $234.5M, booked Q4 2021. The record entity SCD DRYDOCK Q1 LLC carries '
     'the Skanska Commercial Development initials and the Parcel Q1 designation, '
     'consistent with the reporting.', False),

    (1468, "buyer", "TIAA (Nuveen)",
     'NEREJ, "TIAA purchases seven-property Fort Point portfolio for $224 '
     'million": an affiliate of TIAA bought 408,342 SF across 263 Summer St, 332 '
     'and 374 Congress St and 33-41, 34, 38 and 44 Farnsworth St in April 2016, '
     'matching this row on price and month. HFF acted for the seller. The parcel '
     'is still owned by Nuveen, TIAA\'s asset manager, which corroborates the '
     'buyer independently of the entity name.', False),

    (1029, "seller", "Lincoln Property Co. / ASB Real Estate Investments",
     'Institutional Real Estate: Nan Fung Life Sciences Real Estate bought Two '
     'Financial Center at 60 South Street from Lincoln Property Co. and ASB Real '
     'Estate Investments for $210 million. Recorded as the venture, not one '
     'partner. The buyer side of this row already read Nan Fung.', False),
]

LEADS = [
    (1502, "185 FRANKLIN STREET SELLER: BENTALL KENNEDY IS A LEAD, NOT THE "
           "SELLER. Two facts point at it. Kennedy Real Estate Counsel / Kennedy "
           "Associates bought the tower from Verizon for $192M in September 2008 "
           "(CoStar, NEREJ), and Bentall Kennedy is its successor. NEREJ then "
           "reports Bentall Kennedy selecting Suffolk Construction to renovate 50 "
           "Post Office Square before the 2015 trade, which is an owner's act "
           "rather than an adviser's. But NO SOURCE REPORTS BENTALL KENNEDY "
           "SELLING, and inferring the 2015 seller from the 2008 buyer plus the "
           "absence of a known intervening trade is a two-step inference of "
           "exactly the shape that produced the RREF/Rialto error. Stored as a "
           "lead so a licensed feed can confirm or kill it."),
    (1468, "374 CONGRESS STREET SELLER: A CONFLICT, NOT A BLANK. NEREJ reports "
           "the seven-property Fort Point portfolio was sold by a separate "
           "account advised by CLARION PARTNERS. The record entity on this row is "
           "AG/ND FORT POINT LLC, whose initials suggest a different pairing "
           "entirely. Both can be true -- a Clarion separate account may hold "
           "title through a vehicle named for an earlier joint venture -- but "
           "choosing between them without a source that names the entity "
           "alongside the firm is guessing. Left null."),
    (1199, "1 DALTON STREET COMMERCIAL UNIT, $215,000,000, May 2019. SEARCHED "
           "AND NOT FOUND. The record entity is FSBOS (US) LLC and the building "
           "is the Four Seasons at One Dalton, so the prefix has an obvious "
           "reading. That is precisely why it is not written: an obvious reading "
           "is what RREF looked like too. Searches on the address, price and year "
           "returned only residential condominium resales, which dominate all "
           "coverage of this tower."),
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
        for rid, note in LEADS:
            conn.execute(text(
                "update transactions set notes = coalesce(notes,'') || :n "
                "where id = :id"), {"n": " | " + note, "id": rid})
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
