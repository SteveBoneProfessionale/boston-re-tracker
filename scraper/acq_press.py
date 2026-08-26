r"""Press- and SEC-sourced transactions: the half of the market a deed feed misses.

masslandrecords is behind an Imperva bot block, so the registry spine does not
exist yet. That is a reason to build this path, not a substitute for it -- but
the two paths were never interchangeable, and this file is the argument why.

WHAT A DEED CANNOT SEE. A partial interest, an entity-level acquisition and a
Massachusetts nominee-trust assignment all move economic ownership while title
stands still. No amount of registry access surfaces them. Trade press and SEC
filings are the ONLY source for that layer, so this path would be needed even
with a Warren Group feed in hand.

WHY `source` MATTERS MORE THAN USUAL HERE. Every row carries its origin --
`sec_filing`, `press`, or a registry/assessment feed -- so that when a licensed
feed does arrive, reconciliation is a query and not an archaeology project. The
tab shows the mark on the face of the table. The three tiers are not equal:

  sec_filing   a REIT's own 10-Q disposition table, audited, exact to the day
               and dollar. Better than a deed for price, because it states the
               seller's share explicitly.
  press        a named publication reporting a named price. Good on the top of
               the market, structurally blind below it.
  registry     what we cannot get yet.

TWO WAYS THIS PATH GETS IT WRONG, both of which bit during collection:

UNDATED ARTICLES. A search for "2026 recapitalization" surfaced Newmark's $1.5
billion 401 Park recap and BXP's sale of a 45% interest in Kendall Square. Both
read as current. The first is February 2021 and the second is March 2024. Every
row here carries the date the SOURCE states, and where a source states no close
date the precision is widened rather than a date invented.

IMPLIED VALUATION IS NOT A PRICE. The NBIM deal below is the exact trap: the
$1.66 billion figure is a gross valuation across TWO buildings, 290 and 300
Binney, at a blended $2,050/SF. Multiplying that rate by 290 Binney's area would
produce a confident number that nobody paid and no source states. `price` stays
null and the reason is recorded.

    python scraper/acq_press.py --apply
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from db.database import engine

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

# Massachusetts deed excise: $2.28 per $500 of consideration above $100.
EXCISE_PER_1000 = 4.56


def _excise(price):
    return round(price / 1000.0 * EXCISE_PER_1000, 2) if price else None


# Every entry states its own source and the passage that supports it. `date`
# is the CLOSE date where a source gives one, and `precision` widens to month
# or year where it does not, rather than a day being fabricated.
DEALS = [
    # ── Boston, 2026 ────────────────────────────────────────────────
    dict(
        address="265 Franklin Street", city="Boston",
        transaction_type="asset_sale",
        date="2026-07-15", precision="day", price=116_000_000,
        buyer="Synergy / Axonic Capital", seller=None,
        building_sf=360_000, property_type="Office",
        source="press", source_name="The Boston Globe",
        source_url="https://www.bostonglobe.com/2026/07/17/business/downtown-boston-office-sale/",
        source_date="2026-07-17",
        passage='"Boston real estate firm Synergy partnered with New York-based '
                'alternative investment manager Axonic Capital to buy 265 Franklin '
                'Street for $116 million, in a deal that closed Wednesday." '
                '21 stories, 360,000 SF. The article was published Friday 17 July '
                '2026, so "Wednesday" is 15 July 2026.',
        notes="Sold for less than the $170M Clarendon Group paid roughly 20 years "
              "earlier, per the same article.",
    ),
    dict(
        address="The Lofts at Atlantic Wharf", city="Boston",
        transaction_type="asset_sale",
        date="2026-02-25", precision="day", price=55_500_000,
        buyer=None, seller="BXP",
        building_sf=87_000, property_type="Residential",
        confidence="registry_confirmed",
        source="sec_filing", source_name="BXP Q2 2026 Form 10-Q supplemental (Item 8-K)",
        source_url="https://www.sec.gov/Archives/edgar/data/1037540/000103754026000031/q22026supplemental.htm",
        source_date="2026-08-03",
        passage='From the Acquisitions and dispositions table, "For the period '
                'from January 1, 2026 through June 30, 2026": "Residential: The '
                'Lofts at Atlantic Wharf  Boston, MA  February 25, 2026  87,000  '
                '55,500  54,065  14,764" (dollars in thousands), the columns being '
                "BXP's Share of Gross Sales Price, Net Cash Proceeds and Book Gain. "
                'Cross-checked against Bisnow\'s 2 March 2026 deal sheet headline '
                '"BXP Sells Downtown Boston Residential Building For $55M".',
        notes="Highest-confidence row in the table: an audited disposition schedule "
              "gives the day and the dollar exactly, which is better than a deed "
              "for price. BUYER NOT NAMED — a 10-Q states the seller's proceeds, "
              "not the grantee, so the buyer is the one field this source cannot "
              "supply and a deed could. "
              "PRICE READ AS WHOLE: the column is 'BXP's Share of Gross Sales "
              "Price', and the table annotates fractional ownership inline for "
              "Gateway Commons (50%) and 7750 Wisconsin Avenue (50%) but carries no "
              "such annotation on this line, so $55.5M is read as the full price. "
              "Atlantic Wharf's OFFICE component is a 55%-owned JV, which is why "
              "this is flagged rather than assumed away.",
        price_caveat="Stated as BXP's share; no fractional-ownership annotation on "
                     "the line, so read as the whole price.",
    ),
    dict(
        address="31 St. James Avenue", city="Boston",
        transaction_type="distressed",
        date="2026-03-01", precision="month", price=95_000_000,
        buyer="LNR Partners", seller=None,
        building_sf=503_312, property_type="Office",
        arms_length=False, non_arms_length_reason="foreclosure",
        source="press", source_name="The Real Deal / Commercial Real Estate Direct",
        source_url="https://therealdeal.com/national/boston/2026/03/27/starwood-lending-arm-wins-auction-for-back-bay-office/",
        source_date="2026-03-27",
        passage='LNR Partners paid $95 million for the Park Square Building at 31 '
                'St. James Ave. at a foreclosure auction in March 2026. A $160 '
                'million loan moved to special servicing in 2024. The 1922-vintage '
                'Class B building is 503,312 SF.',
        notes="The Park Square Building. Kept rather than dropped: a lender taking "
              "title at auction is a deal in progress, and Eastdil was retained to "
              "re-market it by May 2026. Not arm's-length, so it is excluded from "
              "arm's-length pricing reads but counted as a transaction.",
        price_caveat="Foreclosure auction bid, not an arm's-length negotiated price.",
    ),
    dict(
        address="10-20 Channel Center Street", city="Boston",
        transaction_type="asset_sale",
        date="2026-01-01", precision="month", price=52_000_000,
        buyer="North Colony Asset Management", seller="LaSalle Investment Management",
        building_sf=251_000, property_type="Office",
        source="press", source_name="Bisnow Boston Deal Sheet",
        source_url="https://www.bisnow.com/boston/news/deal-sheet/the-boston-deal-sheet-132558",
        source_date="2026-01-05",
        passage='"The sale is less than half its 2016 price of nearly $119M." '
                'North Colony Asset Management acquired the 251K SF building at '
                '10-20 Channel Center St. for $52M from LaSalle Investment '
                'Management, which had acquired it in 2016 from Callahan Capital '
                'Properties.',
        notes="A later search summary attributed the sale to Equity Residential as "
              "seller; the article itself names LaSalle Investment Management, and "
              "the article is what is recorded here. EQR was the seller on 929 Mass, "
              "which is the likely source of that conflation.",
    ),
    dict(
        address="250 Summer Street", city="Boston",
        transaction_type="asset_sale",
        date="2026-06-01", precision="month", price=37_000_000,
        buyer="North Colony Asset Management", seller="Morgan Stanley",
        building_sf=100_000, property_type="Office",
        source="press", source_name="Bisnow Boston Deal Sheet",
        source_url="https://www.bisnow.com/boston/news/deal-sheet/jv-acquires-financial-district-building-at-69-discount-boston-deal-sheet-135158",
        source_date="2026-06-24",
        passage='250 Summer Street, Fort Point, 100,000 SF, $37M, buyer North '
                'Colony Asset Management, seller Morgan Stanley, June 2026 per '
                'public records.',
    ),
    dict(
        address="230 Congress Street", city="Boston",
        transaction_type="asset_sale",
        date="2026-06-01", precision="month", price=23_700_000,
        buyer="Hudson Assembly", seller="Northwood Investors",
        building_sf=151_000, property_type="Office",
        source="press", source_name="Bisnow Boston Deal Sheet",
        source_url="https://www.bisnow.com/boston/news/deal-sheet/jv-acquires-financial-district-building-at-69-discount-boston-deal-sheet-135158",
        source_date="2026-06-24",
        passage='"Hudson Assembly, a joint venture between Evan Papanastasiou and '
                'Noam Ron, backed by capital partner Time Equities, acquired 230 '
                'Congress St. for $23.7M from Denver-based Northwood Investors." '
                'Northwood acquired the building in 2015 for $77M.',
        notes="Buyer is itself a joint venture, but the asset changed hands whole, "
              "so this is an asset_sale and not a partial_interest.",
    ),
    dict(
        address="Found Hotel Boston Common", city="Boston",
        transaction_type="asset_sale",
        date="2026-01-01", precision="month", price=24_000_000,
        buyer="Giri Hospitality", seller="Hawkins Way Capital",
        property_type="Hotel",
        source="press", source_name="Bisnow Boston Deal Sheet",
        source_url="https://www.bisnow.com/boston/news/deal-sheet/the-boston-deal-sheet-132558",
        source_date="2026-01-05",
        passage='Found Hotel Boston Common sold for $24M to Quincy-based Giri '
                'Hospitality from Hawkins Way Capital, which had acquired it for '
                '$17.9M in 2016.',
        notes="ADDRESS UNCONFIRMED — the source names the hotel, not a street "
              "address, so the property name stands in the address field until a "
              "parcel can be matched. Not linked to a project for that reason.",
    ),
    dict(
        address="1000 Washington Street / 321 Harrison Avenue", city="Boston",
        transaction_type="distressed",
        date="2026-06-03", precision="day", price=None,
        buyer="KKR Real Estate Finance Trust / AllianceBernstein",
        seller="BioMed Realty (Blackstone)",
        building_sf=490_000, property_type="Lab/Office",
        arms_length=False, non_arms_length_reason="deed_in_lieu",
        source="press", source_name="Bisnow Boston",
        source_url="https://www.bisnow.com/boston/news/life-sciences/biomed-loses-south-end-lab-office-property-to-lender-135556",
        source_date="2026-06",
        passage='"BioMed had relinquished ownership of the asset on June 3 to the '
                'entity that had provided the 2021 loan." The lenders were a KKR '
                'Real Estate Finance Trust and AllianceBernstein partnership; the '
                'original floating-rate loan was $322 million from April 2021. The '
                'South End campus is two buildings, 490 KSF.',
        notes="NO PRICE RECORDED. A transfer to the lender states a loan balance, "
              "not a consideration, and $322M is the 2021 loan rather than a 2026 "
              "price. Recording the loan as the price would put a fictitious $322M "
              "into dollar-volume totals. The source does not say whether this was "
              "a foreclosure deed or a deed in lieu; deed_in_lieu is recorded as "
              "the reason with that ambiguity noted here.",
        price_caveat="Consideration not stated; $322M 2021 loan balance is not a price.",
    ),

    # ── Cambridge, 2026 ─────────────────────────────────────────────
    dict(
        address="929 Massachusetts Avenue", city="Cambridge",
        transaction_type="asset_sale",
        date="2026-01-01", precision="month", price=53_600_000,
        buyer="John M. Corcoran & Co. / Stars REI", seller="Equity Residential",
        unit_count=127, property_type="Multifamily",
        source="press", source_name="Bisnow Boston Deal Sheet",
        source_url="https://www.bisnow.com/boston/news/deal-sheet/the-boston-deal-sheet-132558",
        source_date="2026-01-05",
        passage='929 Mass, a 127-unit, two-building multifamily development in '
                'Cambridge. John M. Corcoran & Co. and partner Stars REI acquired '
                'the property for $53.6M from Equity Residential, with a $37.1M '
                'Fannie Mae loan arranged by Walker & Dunlop.',
    ),
    dict(
        address="1 Hampshire Street", city="Cambridge",
        transaction_type="asset_sale",
        date="2026-01-01", precision="year", price=45_000_000,
        buyer="Draper Laboratory", seller="Alexandria Real Estate Equities",
        property_type="Office/Lab",
        source="press", source_name="Banker & Tradesman",
        source_url="https://bankerandtradesman.com/alexandria-sells-kendall-square-properties-for-45m/",
        source_date="2026",
        passage='Draper Laboratories bought four commercial units at 1 Hampshire '
                'St. in Kendall Square from Alexandria Real Estate Equities for '
                '$45 million. Alexandria acquired the condos in 2022 for $120 '
                'million from Schlumberger Technology Corp.',
        notes="CLOSE DATE NOT STATED by the source, so precision is year, not a "
              "fabricated day. Alexandria's Q2 10-Q reports only $4.8M of real "
              "estate sale proceeds for all of 1H 2026 against $170M 'completed as "
              "of the date of this report' (3 August), which places this close "
              "after 30 June 2026 but is inference, not a stated date.",
    ),
    dict(
        address="Twenty20 (Cambridge Crossing)", city="Cambridge",
        transaction_type="asset_sale",
        date="2026-02-01", precision="month", price=None,
        buyer="Mesirow", seller=None,
        unit_count=355, property_type="Multifamily",
        source="press", source_name="Boston Real Estate Times",
        source_url="https://bostonrealestatetimes.com/cbre-arranges-sale-of-355-unit-twenty20-high-rise-in-east-cambridge/",
        source_date="2026-02-12",
        passage='CBRE announced the sale of Twenty20, a 355-unit high-rise '
                'apartment community in the Cambridge Crossing development in East '
                'Cambridge, representing the institutional seller and procuring the '
                'buyer, Mesirow. Units average 807 SF with 8,625 SF of ground-floor '
                'retail.',
        notes="PRICE NOT DISCLOSED and the seller is named only as 'the "
              "institutional seller'. Carried with a null price rather than dropped, "
              "because the transaction is real and a later feed can fill the price. "
              "It contributes nothing to dollar totals.",
    ),

    # ── Out of the 2026 window, loaded deliberately ─────────────────
    # The single best-documented partial interest in Cambridge, and the one that
    # explains 290 Binney's ownership. Dated correctly so the year filter keeps
    # it out of 2026 reads.
    dict(
        address="290 Binney Street", city="Cambridge",
        transaction_type="partial_interest",
        date="2024-03-25", precision="day", price=None,
        pct_acquired=45.0, implied_valuation=None,
        buyer="Norges Bank Investment Management", seller="BXP",
        building_sf=570_000, property_type="Lab/Life Sciences",
        source="press", source_name="BXP / BusinessWire",
        source_url="https://www.businesswire.com/news/home/20240325314914/en/BXP-Completes-Sale-of-a-45-Interest-in-Kendall-Square-Life-Sciences-Property",
        source_date="2024-03-25",
        passage='BXP completed the sale of a 45% interest in 290 Binney Street, a '
                '16-story, 570,000 SF laboratory/life sciences property in Kendall '
                'Square 100% pre-leased to AstraZeneca, to Norges Bank Investment '
                'Management. BXP retains 55% and provides development, property '
                'management and leasing services. "The gross valuation for NBIM\'s '
                'two-building investment in Cambridge, Massachusetts is '
                'approximately $1.66 billion or $2,050 per square foot."',
        notes="NOT A 2026 DEAL — completed 25 March 2024, and it surfaced in a "
              "search for 2026 recapitalizations reading as current. Dated to 2024 "
              "so it cannot contaminate a 2026 read. "
              "NO PRICE AND NO IMPLIED VALUATION RECORDED: the $1.66B gross "
              "valuation covers TWO buildings, 290 and 300 Binney, 810,000 SF "
              "combined at a blended $2,050/SF. Applying that blended rate to 290 "
              "Binney alone would manufacture a single-asset figure no source "
              "states. The $533.5M by which NBIM's investment reduces BXP's share "
              "of development spend is also not a price paid.",
        price_caveat="Gross valuation stated only across 290 + 300 Binney combined; "
                     "no single-asset price or valuation is derivable.",
    ),
]


def main(dry_run: bool = False):
    conn = engine.connect()
    loaded = skipped = 0
    for d in DEALS:
        price = d.get("price")
        sf = d.get("building_sf")
        units = d.get("unit_count")
        exists = conn.execute(text(
            "select id from transactions where address = :a and sale_date = :d "
            "and transaction_type = :t"),
            {"a": d["address"], "d": d["date"], "t": d["transaction_type"]}).first()
        if exists:
            skipped += 1
            continue
        if dry_run:
            log.info("would load %-46s %-10s %s", d["address"], d["city"],
                     f"${price:,}" if price else "(no price)")
            loaded += 1
            continue
        conn.execute(text("""
            insert into transactions
              (address, city, transaction_type, sale_date, sale_date_precision,
               price, price_caveat, buyer, seller, pct_acquired, implied_valuation,
               property_type, building_sf, unit_count, price_per_sf, price_per_unit,
               arms_length, non_arms_length_reason, excise_implied_price,
               source, source_url, source_name, source_date, passage, confidence,
               notes, created_at)
            values
              (:a, :city, :t, :d, :prec, :p, :caveat, :buyer, :seller, :pct, :implied,
               :ptype, :sf, :units, :ppsf, :ppu, :arms, :reason, :excise,
               :src, :url, :sname, :sdate, :passage, :conf, :notes, :now)"""), {
            "a": d["address"], "city": d["city"], "t": d["transaction_type"],
            "d": d["date"], "prec": d["precision"], "p": price,
            "caveat": d.get("price_caveat"),
            "buyer": d.get("buyer"), "seller": d.get("seller"),
            "pct": d.get("pct_acquired"), "implied": d.get("implied_valuation"),
            "ptype": d.get("property_type"), "sf": sf, "units": units,
            "ppsf": round(price / sf, 2) if (price and sf) else None,
            "ppu": round(price / units, 2) if (price and units) else None,
            "arms": d.get("arms_length"), "reason": d.get("non_arms_length_reason"),
            "excise": _excise(price),
            "src": d["source"], "url": d["source_url"], "sname": d["source_name"],
            "sdate": d.get("source_date"), "passage": d.get("passage"),
            # A named publication reporting a named price is web_corroborated only
            # where two independent hosts carry it; single-host stays low.
            "conf": d.get("confidence", "web_low_confidence"),
            "notes": d.get("notes"), "now": datetime.utcnow(),
        })
        loaded += 1
    if not dry_run:
        conn.commit()

    rows = conn.execute(text(
        "select source, count(*), sum(coalesce(price,0)) from transactions "
        "group by source order by 3 desc")).fetchall()
    conn.close()
    log.info("\n%d loaded, %d already present", loaded, skipped)
    log.info("\n%-22s %6s  %s", "SOURCE", "ROWS", "VOLUME")
    for s, n, v in rows:
        log.info("%-22s %6d  $%s", s, n, f"{int(v or 0):,}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    main(dry_run=a.dry_run or not a.apply)
