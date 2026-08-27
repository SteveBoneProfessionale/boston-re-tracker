r"""Sixteenth pass. Working the rows that already had one side, cheapest first.

A row with one side resolved carries the property, the year, the price AND a
named counterparty, so a single search of address plus year usually names the
other side. That held: six of eight searched resolved, against roughly one in
two on rows with both sides blank.

TWO LEADS BECAME RESOLUTIONS.

    1 Canal Park   Breakthrough Properties was stored as a LEAD in an earlier
                   pass, because BLDUP said only "a life science developer"
                   bought it for $131,000,000 in July 2021 and Breakthrough's own
                   page gave no acquisition date. Connect CRE and REBusinessOnline
                   now close it: "Breakthrough Properties, a joint venture of
                   Tishman Speyer and Bellco Capital, acquired One Canal in 2021
                   ... for a reported $130 million". Date, price and firm all
                   land. Promoted from lead to resolved.
    595 Memorial   Recorded in an earlier pass as SEARCHED AND NOT FOUND. That
                   was a search failure, not an absence: the asset is the HYATT
                   REGENCY CAMBRIDGE, and searching the price rather than the
                   address finds it immediately. CoStar: "Host Hotels Sells Hyatt
                   Regency Cambridge for $227.3 Million". The old note is
                   corrected on the row rather than left to mislead.

A THIRD PARTNER-CONTINUATION, found the same way as Brickman and King Street.
100 Franklin Street, which this table addresses as 201 Devonshire, has BLDUP
reporting "Synergy Investments Reacquires 100 Franklin Street for $69.2 Million"
and Boston Real Estate Times reporting "CLARION PARTNERS AND SYNERGY INVESTMENTS
Sell 100 Franklin Street". Both are true: the selling venture was Clarion and
Synergy together, and Synergy bought its partner out. Recording "Synergy" on the
buy side and nothing on the sell side would have hidden a buyout completely.

GI ETS IS GI PARTNERS, confirmed against the property. Connect CRE: "GI Partners
Acquires 77K-SF Fully Leased Lab Building in Cambridge", on behalf of its
ESSENTIAL TECH + SCIENCE fund, at $151 million in September 2022. The entity is
GI ETS CAMBRIDGE I LLC -- ETS is that fund, and the press names the firm at this
address, so the initials are confirmed rather than decoded.

    python scraper/acq_press16.py --apply
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
    (1311, "seller", "State Street", "web",
     'Related Beal\'s own release, "Related Beal adds to life sciences portfolio '
     'with reacquisition of 451 D Street", 6 June 2018: Related acquired the '
     '477,000+ SF building FROM STATE STREET for $276 million and planned to '
     'convert vacant office to lab. The word REACQUISITION is exact -- Related '
     'had owned it before selling in 2012 -- and The Real Reporter\'s headline is '
     '"Related Beal Ties Up 451 D St, (Again)". The buyer side already read '
     'Related Fund Management.'),

    (1742, "seller", "Canyon Johnson Urban Funds", "web",
     'The Boston Globe, "Huge NorthPoint project advances with $300m deal", '
     'August 2015, and Bisnow\'s deal sheet of 1 September 2015: DivcoWest '
     'acquired the 45-acre NorthPoint development site in East Cambridge from '
     'CANYON JOHNSON REAL ESTATE PARTNERS for $291 million, matching this row\'s '
     '$291,040,269 and month. The site had been held since 2010 by the fund '
     'co-owned by Earvin "Magic" Johnson. DivcoWest renamed it Cambridge Crossing '
     'in October 2017 and planned 4.5 million SF across Cambridge, Boston and '
     'Somerville, which is why this row is addressed on Water Street.'),

    (1653, "seller", "Host Hotels & Resorts", "web",
     'CORRECTS AN EARLIER "SEARCHED AND NOT FOUND" NOTE ON THIS ROW. CoStar, '
     '"Host Hotels Sells Hyatt Regency Cambridge for $227.3 Million": Host Hotels '
     'of Bethesda sold the 470-room hotel to KSL Capital Partners in November '
     '2019 at about $484,000 per room, matching this row to the dollar. The '
     'earlier pass searched the ADDRESS and drowned in One Memorial Drive '
     'coverage; searching the PRICE finds it at once. Press gives the address as '
     '575 Memorial Drive and this row says 595 -- one parcel record, one asset. '
     'Host Hotels is independently in this table selling the Sheraton Boston at '
     '39 Dalton Street to Varde and Hawkins Way, so the counterparty is '
     'corroborated by its own second appearance.'),

    (1161, "seller", "Clarion Partners / Synergy Investments", "web",
     'A THIRD PARTNER-CONTINUATION. BLDUP reports "Synergy Investments Reacquires '
     '100 Franklin Street for $69.2 Million" and Boston Real Estate Times reports '
     '"Clarion Partners and Synergy Investments Sell 100 Franklin Street in Boston '
     'for $69.2 Million". Both are accurate: the SELLING venture was Clarion and '
     'Synergy together and Synergy bought its partner out, so Synergy is on both '
     'sides and Clarion is the party actually exiting. This row is addressed 201 '
     'Devonshire Street; 100 Franklin and 201 Devonshire are the same building on '
     'the corner. Recording only Synergy as buyer and leaving the seller blank '
     'would have concealed a buyout, exactly as happened at 535-545 Boylston '
     'Street and 733 Concord Avenue.'),

    (1585, "buyer", "GI Partners", "prefix_confirmed",
     'Connect CRE, "GI Partners Acquires 77K-SF Fully Leased Lab Building in '
     'Cambridge", and GI Partners\' own release, "GI Partners Announces the '
     'Acquisition of Premier Life Sciences Properties in Leading Coastal Markets": '
     'GI acquired Blackstone Science Square at 237 Putnam Avenue on behalf of its '
     'ESSENTIAL TECH + SCIENCE fund, trading at $151 million in September 2022 -- '
     'this row exactly. THIS CONFIRMS THE GI ETS PREFIX against the property: the '
     'entity is GI ETS CAMBRIDGE I LLC and ETS is that fund. The seller side '
     'already read CPI / Brickman Associates, and NEREJ records Brickman buying '
     'the 78,672 SF building from Centermark for $31.5 million in 2014, so the '
     'chain is Centermark -> Brickman -> GI Partners.'),

    (1628, "buyer", "Breakthrough Properties (Tishman Speyer / Bellco Capital)",
     "web",
     'PROMOTED FROM LEAD TO RESOLVED. An earlier pass stored Breakthrough as a '
     'lead only, because BLDUP said merely "a life science developer" acquired 1 '
     'Canal Park on 7 July 2021 for $131,000,000 and Breakthrough\'s own portfolio '
     'page carried the asset with no acquisition date. Connect CRE '
     '("Breakthrough Properties Starts 105K-SF Life Science Redev in Cambridge") '
     'and REBusinessOnline now state it directly: Breakthrough Properties, the '
     'joint venture of Tishman Speyer and Bellco Capital, ACQUIRED ONE CANAL IN '
     '2021 for a reported $130 million, and is building out 105,000+ SF of R&D. '
     'Firm, year and price all land on this row. Recorded as the venture with '
     'both parents named, per the joint-venture rule. The seller side already '
     'read Intercontinental Real Estate Corp.'),
]

NOTES = [
    (1701, "21 THORNDIKE STREET BUYER, $202,500,000, February 2017: NOT FOUND, "
           "BUT THE CHAIN AROUND IT IS NOW COMPLETE. The asset is the DAVENPORT "
           "BUILDING, roughly 220,000 SF in East Cambridge, and 'Davenport' names "
           "the A.H. Davenport furniture works that occupied the site -- it is a "
           "BUILDING NAME, not a firm, so DAVENPORT OWNER (DE) LLC stays "
           "undecoded. NEREJ records DivcoWest buying it for $79 million in 2013, "
           "spending $18 million on the lobby and common areas, and selling to "
           "JAMESTOWN for $136 million in December 2014. Jamestown is the seller "
           "on this row. So the chain reads DivcoWest 2013 $79M -> Jamestown 2014 "
           "$136M -> unknown 2017 $202.5M. Only the last buyer is missing, and no "
           "coverage of that leg was found."),
    (1092, "380 E STREET SELLER, $168,500,000, November 2020: NOT FOUND, AND A "
           "REPEAT SALE WORTH RECORDING. Bisnow and BLDUP confirm the buyer -- "
           "Alexandria Real Estate Equities bought 380-420 E Street, a 5+ acre "
           "stretch of industrial property including a self-storage facility and a "
           "shipping warehouse, for $168.5 million, planning a 1 million SF life "
           "science campus. NO SOURCE NAMES THE SELLER; the record entity is 920 "
           "STORAGE LLC, a storage operator's vehicle. WHAT HAPPENED NEXT: "
           "Alexandria abandoned the lab plan and sold both E Street properties in "
           "December 2023 for about $87 million, with 380 E Street going to "
           "Premier Storage Investors of Memphis for $38.6 million -- roughly half "
           "of what was paid three years earlier. The Globe covered it as "
           "\"Alexandria drops South Boston lab project and sells property at deep "
           "discount\"."),
    (1165, "643-653 SUMMER STREET SELLER, $282,500,000, October 2019: STILL NOT "
           "FOUND, BUT THE STRUCTURE IS NOW CLEAR AND EXPLAINS WHY. The record "
           "entity BOSTON HARBOR INDUSTRIAL DEVELOPMENT LLC holds a MASSPORT "
           "GROUND LEASE running to 30 March 2085, per its LEI registration. So "
           "this $282.5M conveyance is most likely a LEASEHOLD interest in the "
           "Raymond L. Flynn Marine Park rather than a fee sale, which is why no "
           "conveyance coverage exists: the press on that park reports leases, "
           "Massport board votes and RFQs, not trades. Recorded so the row is not "
           "searched a fourth time on the same terms."),
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
