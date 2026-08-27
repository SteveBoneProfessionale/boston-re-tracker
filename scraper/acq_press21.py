r"""Twenty-first pass. And a systemic finding: the derived SELLER can be a lag.

TWO ROWS NOW WHERE THE PRESS AND THE SELLER OF RECORD NAME DIFFERENT FIRMS, and
they fail the same way.

    327-333 Summer St   record: W2005 BWH II REALTY LLC   press: Synergy sold
    6-10 Oliver St      record: P6/SARACEN 2 OLIVER REAL  press: Synergy sold

On this spine the SELLER is derived from the previous ownership snapshot -- the
owner recorded before the buyer appeared. That is sound only if the snapshots
are dense enough to catch every intervening owner. They are annual, so a
building that changed hands twice between two snapshots reports the owner from
BEFORE both trades, and the seller is one conveyance stale.

Both rows are corrected to the press and both keep the record entity verbatim,
with the conflict written on the row. The wider point is recorded here rather
than on any one row: A DERIVED SELLER IS A WEAKER FACT THAN A DERIVED BUYER,
because the buyer comes from the roll's CURRENT owner and the seller from a
reconstruction. The condominium sweep already showed the buyer derivation
failing on shared parcels; this is the seller derivation's own failure mode.

GRE IS GUGGENHEIM REAL ESTATE, confirmed against the property. KBS's own
release, "KBS Strategic Opportunity REIT Sells 50 Congress Street for $79
Million", and REBusinessOnline, "Jumbo Capital, Guggenheim Purchase 50 Congress
Street in Boston for $79M": a joint venture of Jumbo Capital Management and
Guggenheim Real Estate bought the ten-storey, 179,872 SF Financial District
building in May 2017. The buyer entity is GRE CONGRESS STREET LLC.

CIM GROUP BOUGHT BOTH STUART STREET GARAGES INSIDE A YEAR. Motor Mart at 201
Stuart in October 2016 with LAZ Parking, and the 826-space Revere Hotel garage
at 200 Stuart from Pebblebrook Hotel Trust in June 2017. Two rows that looked
unrelated are one buyer's strategy.

    python scraper/acq_press21.py --apply
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

LAG = (
    "SELLER CORRECTED AGAINST THE RECORD, AND THE RECORD ENTITY IS KEPT. On this "
    "spine the seller is DERIVED from the previous annual ownership snapshot, so "
    "a property that traded twice between two snapshots reports the owner from "
    "before both -- the seller comes out one conveyance stale. That is the "
    "likeliest explanation here, and it is the seller derivation's counterpart to "
    "the condominium fault in the buyer derivation."
)

RESOLVE = [
    (1378, "buyer", "CIM Group", False,
     'BLDUP, "CIM Group acquires Revere Hotel parking garage in Bay Village for '
     '$95 million", and Pebblebrook\'s own release, "Pebblebrook Hotel Trust '
     'Completes Sale of the Parking Garage at Revere Hotel Boston Common", '
     'announced 26 June 2017: an 826-space garage serving the adjacent Revere '
     'Hotel, plus 10,500 SF of vacant ground-floor retail. CIM GROUP BOUGHT BOTH '
     'STUART STREET GARAGES INSIDE A YEAR -- the Motor Mart at 201 Stuart in '
     'October 2016 with LAZ Parking, and this one. PRICE: the release says $95.0M '
     'and this row records $91,000,000.'),
    (1378, "seller", "Pebblebrook Hotel Trust", False,
     'Same release, from the seller\'s own investor-relations page. Pebblebrook '
     'owned the Revere Hotel Boston Common and sold the garage separately from '
     'the hotel, which is why this row is addressed as a garage unit.'),

    (923, "buyer", "Intercontinental Real Estate Corp.", False,
     'Cushman & Wakefield\'s own release, "Cushman & Wakefield Arranges $107.5M '
     'Sale of Industrial Portfolio Dubbed Yard 5": a three-building, 196,000 SF '
     'industrial portfolio on Industrial Drive in Readville, Boston, acquired by '
     'Intercontinental Real Estate Corporation, September 2022. The record entity '
     'is YARD 5 MA OWNER LLC, matching the portfolio name. PORTFOLIO ALLOCATION: '
     'this row is $104,750,000 against a $107.5M three-building total. '
     'Intercontinental is independently established in this table as the buyer of '
     'the Canal Park complex in Cambridge.'),
    (923, "seller", "First Highland Management & Development", False,
     'Same release names First Highland Management & Development as the seller.'),

    (1341, "seller", "New Boston Fund / Asian Community Development Corporation",
     False,
     'BLDUP, "One Greenway market-rate apartment component sold to Prudential for '
     '$144.5 million", and The Real Reporter: HFF represented the One Greenway '
     'developers -- NEW BOSTON FUND and the ASIAN COMMUNITY DEVELOPMENT '
     'CORPORATION -- in the 2017 sale of The Tower at One Greenway, 217 '
     'market-rate units, for $144.5 million. This row is $144,500,000 in November '
     '2017 and the buyer side already read PGIM Real Estate, which is Prudential\'s '
     'real estate arm. The seller entity PARCEL 24 NORTH LLC is exact: One '
     'Greenway sits on Parcel 24, and the North Building held the 217 market-rate '
     'rentals plus 95 affordable, while the South Building held 50 affordable '
     'condominiums. Recorded as the venture, not one partner -- and the non-profit '
     'half of it matters, because ACDC being a co-developer is the reason the '
     'scheme is mixed-income.'),

    (1667, "seller", "King Street Properties", False,
     'Healthpeak\'s own Q4 2018 results and Q3 2019 supplemental: in January 2019 '
     'Healthpeak acquired a 100%-leased, 64,000 SF life science facility on '
     'CambridgePark Drive for $71 million, and in February 2019 development rights '
     'on the adjacent parcel for up to $27 million, describing both as expanding '
     '"Healthpeak\'s relationship with leading local owner and operator, KING '
     'STREET PROPERTIES". This row is $71,000,000 in January 2019. ADDRESS '
     'CAVEAT: Healthpeak names 87 CambridgePark Drive for the $71M and 101 '
     'CambridgePark Drive for the development rights; this row is addressed 101. '
     'The parcel record appears to cover both. King Street is independently '
     'confirmed in this table selling 200 CambridgePark Drive for $165.5M in '
     '2015, so the counterparty is not in doubt even where the door number is.'),

    (1388, "buyer", "Jumbo Capital Management / Guggenheim Real Estate", False,
     'REBusinessOnline, "Jumbo Capital, Guggenheim Purchase 50 Congress Street in '
     'Boston for $79M", with BLDUP and The Real Reporter reporting the same May '
     '2017 deal: a joint venture of Jumbo Capital Management and Guggenheim Real '
     'Estate bought the ten-storey, 179,872 SF Financial District building at Post '
     'Office Square. THIS CONFIRMS GRE AS GUGGENHEIM REAL ESTATE against the '
     'property -- the entity is GRE CONGRESS STREET LLC. Recorded as the venture.'),
    (1388, "seller", "KBS Strategic Opportunity REIT", True,
     'REFINED FROM "KBS Realty Advisors". KBS\'s own release is titled "KBS '
     'Strategic Opportunity REIT Sells 50 Congress Street for $79 Million" -- the '
     'seller is the REIT, a non-traded vehicle based in Newport Beach; KBS Capital '
     'Advisors is its adviser and listed the building in September 2016. The '
     'adviser and the owner are different parties and the release names the owner. '
     'KBS had bought 50 Congress from Nordblom in July 2013 for $51 million.'),

    (1529, "buyer", "NTT Urban Development", False,
     'NEREJ, "Synergy Investments sells 11-story 2 Oliver St. to NTT Urban '
     'Development for $79 million": the Tokyo-based developer bought the building '
     'after a sweeping renovation, repositioning and re-letting, closing 96% '
     'leased. This row is $79,000,000 in July 2015.'),
    (1529, "seller", "Synergy Investments", True,
     'CORRECTED FROM "P6 / Saracen Properties". ' + LAG + ' NEREJ names SYNERGY '
     'INVESTMENTS as the seller of 2 Oliver Street to NTT for exactly this price. '
     'Saracen Properties does carry Two Oliver Street on its own portfolio page, '
     'which is consistent with it being an EARLIER owner or a development partner '
     'rather than the 2015 seller. The record entity P6/SARACEN 2 OLIVER REALTY is '
     'kept verbatim. Synergy now appears eight times in this table.'),
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
        conn.execute(text(
            "update transactions "
            "set price_caveat = coalesce(price_caveat || ' ', '') || :c "
            "where id = 1378"), {
            "c": ("Pebblebrook's release puts the garage sale at $95.0M; this row "
                  "records $91,000,000.")})
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
