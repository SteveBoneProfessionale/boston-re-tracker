r"""Transactions from the systematic archive read: Bisnow, Connect CRE, TRR.

METHOD, because it is the finding. Reading all 31 of 2026's Bisnow Boston Deal
Sheets issue by issue, plus 163 Connect CRE Metro Boston items and 85 Real
Reporter articles, roughly doubled the 2026 set again. Type-led searching had
found 14 transactions, a first archive pass found 20, and this brings it to the
high thirties. The deals that searching missed are not obscure: a $169M
Northeastern dormitory purchase, a $99M Gillette site, a $56M Fort Point
portfolio.

WHAT EACH SOURCE ACTUALLY CONTRIBUTED, which decides whether more sources help:

  Bisnow      31 issues, the dense source. Lead story plus a sectioned SALES
              list, prices usually cited to deed or public records.
  Connect CRE 163 items in 2026, and after filtering, ESSENTIALLY NOTHING new --
              every Boston/Cambridge sale it carried was already in Bisnow. Its
              "Metro Boston" section is mostly suburban.
  TRR         paywalled past the lead paragraph, so only headline, dateline and
              first sentence are readable. Within that limit it is the only
              source that surfaced genuine mid-market deals Bisnow skipped:
              493 Concord Avenue at $4.2M, an $11.9M Boston apartment trade.

TWO SPECIFIC TRAPS HANDLED HERE:

PRE-CLOSE ESTIMATES. TRR headlines routinely price deals before they close --
"Newmark Deal 'Near' $118M" for 265 Franklin, which recorded at $116M; "Newmark
Exclusive Tops $25M" for a Newbury Street portfolio that sold for $50M. A
headline hedged with "seen", "near" or "tops" is broker guidance, not a price.
Where a deed-record figure exists it wins.

PORTFOLIO AGGREGATES. TRR reported TIAA's exit from a 410,000 SF Seaport office
portfolio as "$175M delivered". The components are already here separately --
$56M to Davis Cos. in January, $28M to Eastern Real Estate in July -- so
recording $175M as a transaction would double-count $84M of it. It is NOT
loaded. It is, however, evidence: $175M realised against $84M identifiable
means roughly $91M of Seaport office trading that the press record does not let
me name.

    python scraper/acq_archive.py --apply
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

BN = "https://www.bisnow.com/news/boston/deal-sheet/"

DEALS = [
    # ── Boston ──────────────────────────────────────────────────────
    dict(address="291 St. Botolph Street (East Village)", city="Boston",
         date="2026-06-30", precision="month", price=169_000_000,
         buyer="Northeastern University", seller="Phoenix Property Co.",
         unit_count=723, property_type="Student housing",
         src_name="Bisnow Boston Deal Sheet", src_date="2026-06-30",
         url=BN + "northeastern-acquires-fenway-dorm-tower-for-169m-the-boston-deal-sheet",
         passage='"The university acquired a 17-story, 723-bed dormitory at 291 '
                 'St. Botolph St. for $169M from Phoenix Property Co., according '
                 'to public records." Named East Village, opened 2015, developed '
                 'in a joint venture with Lincoln Property Co.',
         notes="Unit count is BEDS, not apartments, so price-per-unit here is "
               "price per bed and is not comparable with a multifamily $/unit."),

    dict(address="232 A Street", city="Boston",
         date="2026-08-01", precision="month", price=99_000_000,
         buyer="Procter & Gamble (Gillette)", seller="Breakthrough Properties",
         property_type="Development site",
         src_name="Bisnow Boston Deal Sheet", src_date="2026-08-25",
         url=BN + "city-realty-buys-coolidge-corner-portfolio-for-23m-the-boston-deal-sheet",
         passage='"Breakthrough Properties sold its 232 A St. development site in '
                 'South Boston back to P&G Gillette for $99M. The life sciences '
                 'firm acquired the site in 2021 from P&G for $80M." 2.4 acres; '
                 'P&G plans roughly $1B of investment in Fort Point.',
         notes="A repurchase: P&G sold the site to Breakthrough in 2021 for $80M "
               "and bought it back for $99M. Arm's-length between unrelated "
               "parties, so it counts, but the round trip is worth seeing."),

    dict(address="34, 38 and 44 Farnsworth Street and 332 Congress Street",
         city="Boston",
         date="2026-01-15", precision="month", price=56_000_000,
         buyer="The Davis Cos.", seller="Nuveen", property_type="Office",
         src_name="Bisnow Boston", src_date="2026-01-15",
         url="https://www.bisnow.com/news/boston/office/davis-cos-acquires-fort-point-portfolio-for-56m-132750",
         passage='"The Fort Point portfolio sold for $56M, according to deed '
                 'records. The Davis Cos. acquired the properties from investment '
                 'manager Nuveen." Nuveen acquired the properties in 2016 as part '
                 'of a five-property portfolio sale for $224M.',
         notes="FOUR BUILDINGS AT ONE PRICE, recorded once. The fifth building of "
               "Nuveen's 2016 portfolio, 374 Congress Street, sold separately to "
               "Eastern Real Estate in July 2026 and is its own row.",
         caveat="Combined price across four buildings."),

    dict(address="505 Washington Street (The Godfrey Hotel)", city="Boston",
         date="2026-05-01", precision="month", price=50_000_000,
         buyer="Elliott Management", seller="Union Investment",
         unit_count=242, property_type="Hotel",
         src_name="Bisnow Boston Deal Sheet", src_date="2026-05-19",
         url=BN + "alpha-management-continues-buying-spree-the-boston-deal-sheet",
         passage='"West Palm Beach, Florida-based Elliott Management acquired the '
                 '242-room Godfrey Hotel in downtown Boston for $50M, according to '
                 'public records. The seller in the deal was German asset manager '
                 'Union Investment."',
         notes="Unit count is HOTEL KEYS. Address 505 Washington Street is the "
               "Godfrey's known location and is not stated in the report."),

    dict(address="93-97 Massachusetts Avenue, 375 Newbury Street and 10 other suites",
         city="Boston",
         date="2026-05-08", precision="day", price=50_000_000,
         buyer="Treeco (The Real Estate Equity Co.)",
         seller="Kensington Investment Co.", property_type="Retail",
         src_name="Bisnow Boston", src_date="2026-05-11",
         url="https://www.bisnow.com/news/boston/retail/new-jersey-investor-acquires-back-bay-retail-portfolio-50m-134504",
         passage='"Treeco acquired 93-97 Massachusetts Ave., 375 Newbury St. and 10 '
                 'other retail and office suites across Newbury Street, according '
                 'to May 8 public records. The firm paid $50M for the properties."',
         notes="A TWELVE-PLUS-ADDRESS PORTFOLIO at one price, recorded once. "
               "The Real Reporter headlined the same deal 'Newmark Exclusive Tops "
               "$25M' on 11 May — that is pre-close broker guidance on the "
               "listing, not the $50M the deed records show. The deed figure is "
               "what is stored.",
         caveat="Combined portfolio price across twelve-plus addresses."),

    dict(address="11-19 Peterborough Street and 19 Queensberry Street", city="Boston",
         date="2026-08-03", precision="month", price=37_500_000,
         buyer="Egeria Group (affiliate entity)", seller="The Davis Cos.",
         broker="Boston Realty Advisors", unit_count=93,
         property_type="Multifamily",
         src_name="Bisnow Boston", src_date="2026-08-03",
         url="https://www.bisnow.com/news/boston/multifamily/the-davis-cos-sells-two-fenway-apartments-for-37m",
         passage='"An entity linked to investment firm Egeria Group acquired a '
                 '93-unit apartment portfolio at 11-19 Peterborough St. and 19 '
                 'Queensberry St. for $37.5M, according to public records. The '
                 'seller in the deal was The Davis Cos." Two four-storey buildings.',
         caveat="Combined price across two buildings."),

    dict(address="320 Summer Street", city="Boston",
         date="2026-02-01", precision="month", price=26_300_000,
         buyer="HC 320 Summer St. (entity)",
         seller="ASB Real Estate Investments (affiliate)",
         building_sf=122_000, property_type="Office",
         src_name="Bisnow Boston Deal Sheet", src_date="2026-02-09",
         url=BN + "foxrock-secures-128m-loan-for-465-unit-quincy-apartment-community",
         passage='"An entity named HC 320 Summer St. acquired a 122K SF office '
                 'building at 320 Summer St. for $26.3M, Banker & Tradesman '
                 'reported. The seller in the deal was an affiliate of ASB Real '
                 'Estate Investments, which acquired the property for $17.8M in '
                 '2007." Former LogMeIn office.',
         notes="The Real Reporter covered the same asset on 10 February as "
               "'312-320 Summer St.' with Lincoln Property Co. among the "
               "long-term ownership that paid $17.7M for two Summer Street assets "
               "in 2007. Same property, slightly different address range and "
               "ownership description."),

    dict(address="1848-1850 Commonwealth Avenue", city="Boston",
         date="2026-05-18", precision="day", price=23_500_000,
         buyer="Alpha Management", seller="Mohnsen Vessali",
         property_type="Multifamily",
         src_name="Bisnow Boston Deal Sheet", src_date="2026-05-19",
         url=BN + "alpha-management-continues-buying-spree-the-boston-deal-sheet",
         passage='"Brookline-based Alpha Management acquired two apartment '
                 'buildings in two different deals totaling $37M, according to '
                 'public records. The properties are located at 1848-1850 '
                 'Commonwealth Ave. and 10-18 Brainerd Road. The deeds for both '
                 'deals were posted on May 18. Mohnsen Vessali was the seller in '
                 'the Commonwealth Avenue deal." The article image caption states '
                 '"Alpha Management Acquired 1846-1850 Commonwealth Ave. In Boston '
                 'For $23.5M."',
         notes="TWO SEPARATE DEEDS, so two rows rather than one $37M row. This "
               "one's $23.5M is stated directly in the caption."),

    dict(address="10-18 Brainerd Road", city="Boston",
         date="2026-05-18", precision="day", price=13_500_000,
         buyer="Alpha Management", seller="The Mount Vernon Co.",
         property_type="Multifamily",
         src_name="Bisnow Boston Deal Sheet", src_date="2026-05-19",
         url=BN + "alpha-management-continues-buying-spree-the-boston-deal-sheet",
         passage='Same report: two deals totalling $37M, deeds posted 18 May, with '
                 'The Mount Vernon Co. the seller in the Brainerd Road deal. The '
                 'caption prices the Commonwealth Avenue building at $23.5M.',
         notes="PRICE DERIVED BY SUBTRACTION: $37M total less the $23.5M stated "
               "for Commonwealth Avenue. No source states $13.5M directly. Kept "
               "because both inputs are stated in the same report, but it is "
               "arithmetic and is flagged as such rather than presented as a "
               "reported price.",
         caveat="Derived: $37M two-deal total less the $23.5M stated for the "
                "Commonwealth Avenue building. Not a directly reported price.",
         conf="web_low_confidence"),

    dict(address="340 Bremen Street and 319-327 Chelsea Street", city="Boston",
         date="2026-06-30", precision="month", price=18_000_000,
         buyer="True North Legacy Holdings (Jeffrey R. Bruce)",
         seller="MG2 Group", unit_count=38, property_type="Multifamily",
         src_name="Bisnow Boston Deal Sheet / The Real Reporter",
         src_date="2026-06-30",
         url=BN + "northeastern-acquires-fenway-dorm-tower-for-169m-the-boston-deal-sheet",
         passage='Bisnow: "An entity linked to real estate investor Jeff Bruce '
                 'acquired a multifamily property at 319-327 Chelsea St. and 340 '
                 'Bremen St. in East Boston for $18M. The seller in the deal was '
                 'Boston-based MG2 Group. The property, known as 319 & Park, '
                 'includes 38 units, 34 parking spaces and ground-floor retail." '
                 'The Real Reporter, 30 June: "luxury apartments ... under new '
                 'ownership to True North Legacy Holdings whose founding principal '
                 'Jeffrey R. Bruce spent $18 million buying 340 Bremen St. from '
                 'Joseph Donovan of EB3 Holdings".',
         notes="TWO SOURCES, SAME PRICE, DIFFERENT SELLER NAME — Bisnow says MG2 "
               "Group, TRR says Joseph Donovan of EB3 Holdings. Both may be true "
               "if one is the record entity and the other the principal; they are "
               "not reconciled here, and Bisnow's is stored with TRR's noted. "
               "Recorded once, not twice: same price, same month, same submarket.",
         conf="web_corroborated"),

    dict(address="88 Constitution Road", city="Boston",
         date="2026-03-01", precision="month", price=12_800_000,
         buyer="Sunrise Capital Investors / Parking Advisors",
         seller="National Development", broker="Colliers",
         unit_count=297, property_type="Parking garage",
         src_name="Bisnow Boston Deal Sheet", src_date="2026-03-16",
         url=BN + "worcester-office-conversion-project-secures-51m-loan-the-boston-deal-sheet",
         passage='"Sunrise Capital Investors and Parking Advisors acquired a '
                 '297-space parking garage at 88 Constitution Road in Charlestown '
                 'for $12.8M. The seller was National Development."',
         notes="Unit count is PARKING SPACES."),

    dict(address="Boston mixed-income apartments (address not stated)", city="Boston",
         date="2026-06-08", precision="month", price=11_900_000,
         buyer="ARX", seller=None, broker="Walker & Dunlop",
         property_type="Multifamily",
         src_name="The Real Reporter", src_date="2026-06-08",
         url="https://therealreporter.com/article/arx_buys_hub_apartments_trade_via_walker_dunlop",
         passage='"BOSTON—Mixed-income apartments in a dynamic setting have '
                 'transacted for $11.9 million through Walker & Dunlop and financed '
                 'by a $7 million loan from Camden National Bank of Portland, ME."',
         notes="ADDRESS NOT OBTAINABLE. The Real Reporter is paywalled past the "
               "lead paragraph and the lead names neither the address nor the "
               "seller. City, date and price are stated, so the row is real; it "
               "cannot be linked to a parcel and contributes to counts and volume "
               "only. This is what the mid-market looks like in the press record."),

    dict(address="33-41 West Street", city="Boston",
         date="2026-06-09", precision="month", price=9_500_000,
         buyer="Embrace Boston", seller="Kendall Capital",
         broker="Avison Young", building_sf=35_000, property_type="Office",
         src_name="Bisnow Boston Deal Sheet", src_date="2026-06-09",
         url=BN + "davis-cos-lands-107k-sf-lease-at-taunton-industrial-property",
         passage='"Embrace Boston acquired a 35K SF property at 33-41 West St. in '
                 'Downtown Crossing for $9.5M. The property will act as a civic '
                 'commons for Boston... The seller was Kendall Capital."'),

    dict(address="343 Congress Street", city="Boston",
         date="2026-01-05", precision="month", price=None,
         buyer="North Colony Asset Management", seller=None,
         building_sf=115_000, property_type="Office",
         src_name="Bisnow Boston Deal Sheet", src_date="2026-01-05",
         url=BN + "the-boston-deal-sheet-132558",
         passage='"North Colony Asset Management also acquired another 115K SF '
                 'office building at 343 Congress St. in the Seaport. The building '
                 'is home to pet company Chewy, barbecue restaurant The Smoke Shop '
                 'and pizzeria Pastoral Fort Point."',
         notes="NO PRICE STATED and no seller named. Reported only as an aside to "
               "the 10-20 Channel Center story. Carried at a null price rather "
               "than dropped, because the transaction is real and a feed can fill "
               "it later; it contributes nothing to dollar volume."),

    dict(address="Dorchester Avenue warehouse portfolio (10 sites)", city="Boston",
         ttype="distressed",
         date="2026-03-24", precision="day", price=75_000_000,
         buyer="J.T. Magen & Co. / Extell Development", seller=None,
         property_type="Industrial",
         arms=False, reason="affiliated_parties",
         src_name="Bisnow Boston Deal Sheet", src_date="2026-03-23",
         url=BN + "south-boston-property-owners-submit-75m-bid-keep-development-rights",
         passage='"J.T. Magen & Co. and Extell Development retained ownership of a '
                 'portfolio of vacant warehouses and seafood plants along '
                 'Dorchester Avenue with a $75M auction bid on the properties, The '
                 'Boston Globe reported."',
         notes="NOT ARM'S-LENGTH: the existing owners bid to KEEP the portfolio at "
               "auction, so no third party acquired anything and the $75M is a "
               "credit-style bid rather than a negotiated price between unrelated "
               "parties. Kept because a $75M auction on ten South Boston sites is "
               "a real market event, but flagged so it never enters an "
               "arm's-length pricing read.",
         caveat="Auction bid by the existing owners to retain ownership; not a "
                "negotiated price between unrelated parties."),

    # ── Cambridge ───────────────────────────────────────────────────
    dict(address="160 Fawcett Street", city="Cambridge",
         date="2026-01-12", precision="month", price=12_000_000,
         buyer="Hines (affiliate entity)", seller="Cabot, Cabot & Forbes",
         property_type="Commercial",
         src_name="Bisnow Boston Deal Sheet", src_date="2026-01-12",
         url=BN + "developers-acquire-588-unit-apartment-complex-for-245m-the-boston-deal-sheet",
         passage='"An entity linked to Houston-based Hines acquired 160 Fawcett St. '
                 'in Cambridge for $12M from Cabot, Cabot & Forbes, according to '
                 'public records. The parcel is home to a single-story commercial '
                 'property." Near Healthpeak\'s proposed $4.5B Alewife development, '
                 'which Hines joined as a development partner.',
         conf="web_corroborated",
         notes="Corroborated by The Real Reporter, 19 January: \"Alewife Site "
               "Yields CC&F $12M; C&W Tabs Healthpeak, Hines JV\"."),

    dict(address="493 Concord Avenue", city="Cambridge",
         date="2026-05-27", precision="month", price=4_200_000,
         buyer=None, seller="Sozio family", broker="Hulteen",
         property_type="Retail",
         src_name="The Real Reporter", src_date="2026-05-27",
         url="https://therealreporter.com/article/4.2m_wins_sozio_cambridge_hulteen_brokers_legacy_trade",
         passage='"CAMBRIDGE — Octagonal 493 Concord Ave. is touted as \'one of '
                 'Cambridge\'s most unique and recognizable buildings\'... thanks to '
                 'the flair of retail savant Angelo \'Chuck\' Sozio whose enduring '
                 'Sozio Appliances..." Headline: "$4.2M Wins Sozio Cambridge; '
                 'Hulteen Brokers Legacy Trade".',
         notes="BUYER NOT OBTAINABLE — TRR is paywalled past the lead. The price "
               "is in the headline and the address and seller are in the lead. "
               "One of only two genuinely mid-market Cambridge trades the entire "
               "archive read surfaced, which is the point about coverage below "
               "$10M."),
]

UPDATES = [
    dict(match="Twenty20", price=218_000_000, seller="PGIM Real Estate",
         date="2026-02-10", precision="day", broker="CBRE",
         note=" | PRICE AND SELLER FILLED FROM THE ARCHIVE READ. Bisnow, 12 "
              "February 2026: \"Mesirow Financial acquired the Twenty20 apartment "
              "complex in Cambridge Crossing... The Chicago-based investment firm "
              "bought the 20-story, 355-unit apartment complex from PGIM Real "
              "Estate for $218M, according to public records. The sale was listed "
              "on Feb. 10.\" A $139M mortgage came through Walker & Dunlop; CBRE's "
              "Simon Butler, Biria St. John, John McLaughlin and Brian Bowler "
              "represented the seller and procured the buyer. The row previously "
              "carried a null price because the brokerage announcement withheld "
              "it — this moves it into the $100M+ band."),
    dict(match="10 units, opposite Harvard",
         address="23-25 Hammond Street", buyer="Jeremy Seeger",
         seller="Cafasso Properties", building_sf=13_000, date="2026-04-16",
         precision="month", broker="Marcus & Millichap",
         note=" | ADDRESS, BUYER AND SELLER FILLED. Bisnow, 20 April 2026: "
              "\"Jeremy Seeger purchased a 10-unit multifamily asset at 23-25 "
              "Hammond St. in Cambridge for $6.5M, the first time the property — "
              "across the street from Harvard University — has sold in more than a "
              "century. It spans more than 13K SF... Marcus & Millichap's Evan "
              "Griffith and Tony Pepdjonovic marketed the property on behalf of "
              "the seller, Cafasso Properties, and procured the buyer.\""),
    dict(match="265 Franklin", seller="Clarendon Group",
         note=" | SELLER FILLED: Bisnow, 21 July 2026 — \"The seller was Clarendon "
              "Group, which had purchased the property in 2006 for $170M.\" The "
              "Real Reporter had trailed the deal on 22 May as \"Newmark Deal "
              "'Near' $118M\"; the deed figure of $116M is what is stored, because "
              "a hedged pre-close headline is broker guidance and not a price."),
    dict(match="31 St. James", seller="Capital Properties",
         note=" | SELLER FILLED: Bisnow, 30 March 2026 — \"Capital Properties was "
              "the owner of the building before the property went into special "
              "servicing and auction.\" Note a size discrepancy across sources: "
              "Bisnow and a later listing say 540K SF, while Commercial Real "
              "Estate Direct and the Eastdil marketing say 503,312 SF. The "
              "503,312 figure is retained as the more precise of the two and the "
              "conflict is recorded rather than resolved."),
    dict(match="250 Summer", buyer="North Colony Asset Management / Paradigm Properties",
         note=" | CO-BUYER ADDED. The Real Reporter, 18 June 2026: \"Having admired "
              "250 Summer St. for years, Paradigm Properties has bought the "
              "110,000-square-foot office building with Cambridge-based North "
              "Colony Asset Management at a sharply lower $37.6 million basis "
              "replacing Morgan Stanley Real Estate Advisors.\" TRR gives $37.6M "
              "and 110,000 SF against Bisnow's $37M and 100K SF from public "
              "records. The public-records figures are kept and the variance is "
              "recorded."),
    dict(match="11 Beacon", buyer="Synergy Investments",
         note=" | BUYER IDENTIFIED, and the transaction reads more clearly as a "
              "stake purchase. The Real Reporter headlined it on 9 March 2026 as "
              "\"Synergy Buys 11 Beacon Street; Cambridge Savings Bank Funds "
              "Trade\", against Bisnow's \"recapitalizes\". Both fit if Synergy "
              "bought out its 2016 partner GreenOak and refinanced: Synergy is "
              "acquiring an interest it did not already hold while keeping the "
              "ownership it did. price stays NULL because neither source states "
              "what was paid for the stake or how large the stake was; $23M "
              "remains an implied whole-asset valuation."),
    dict(match="1 Hampshire", date="2026-08-01", precision="month",
         note=" | DATE TIGHTENED from year to August 2026. Bisnow's 25 August 2026 "
              "deal sheet reports the sale as done: \"Alexandria Real Estate "
              "Equities sold four lab condo units at 1 Hampshire St. in Kendall "
              "Square for $45M to Draper Labs.\" This is consistent with "
              "Alexandria's Q2 10-Q showing only $4.8M of sale proceeds for all of "
              "1H 2026 against $170M completed as of 3 August."),
]


def main(dry_run: bool):
    conn = engine.connect()
    loaded = skipped = 0
    for d in DEALS:
        exists = conn.execute(text(
            "select id from transactions where address = :a and sale_date = :d"),
            {"a": d["address"], "d": d["date"]}).first()
        if exists:
            skipped += 1
            continue
        price, sf, units = d.get("price"), d.get("building_sf"), d.get("unit_count")
        log.info("%-58s %-10s %s", d["address"][:58], d["city"],
                 f"${price:,}" if price else "(no price)")
        if dry_run:
            loaded += 1
            continue
        conn.execute(text("""
            insert into transactions
              (address, city, transaction_type, sale_date, sale_date_precision,
               price, price_caveat, buyer, seller, broker, property_type,
               building_sf, unit_count, price_per_sf, price_per_unit, arms_length,
               non_arms_length_reason, excise_implied_price, source, source_url,
               source_name, source_date, passage, confidence, notes, created_at)
            values
              (:a, :city, :t, :d, :prec, :p, :caveat, :buyer, :seller, :broker,
               :ptype, :sf, :units, :ppsf, :ppu, :arms, :reason, :ex, 'press',
               :url, :sname, :sdate, :passage, :conf, :notes, :now)"""), {
            "a": d["address"], "city": d["city"], "t": d.get("ttype", "asset_sale"),
            "d": d["date"], "prec": d["precision"], "p": price,
            "caveat": d.get("caveat"), "buyer": d.get("buyer"),
            "seller": d.get("seller"), "broker": d.get("broker"),
            "ptype": d.get("property_type"), "sf": sf, "units": units,
            "ppsf": round(price / sf, 2) if (price and sf) else None,
            "ppu": round(price / units, 2) if (price and units) else None,
            "arms": d.get("arms"), "reason": d.get("reason"),
            "ex": round(price / 1000.0 * 4.56, 2) if price else None,
            "url": d["url"], "sname": d["src_name"], "sdate": d["src_date"],
            "passage": d["passage"], "conf": d.get("conf", "web_low_confidence"),
            "notes": d.get("notes"), "now": datetime.utcnow(),
        })
        loaded += 1

    updated = 0
    for u in UPDATES:
        row = conn.execute(text(
            "select id, price, building_sf, unit_count from transactions "
            "where address like :m"), {"m": f"%{u['match']}%"}).first()
        if not row:
            log.warning("no row matching %r", u["match"])
            continue
        rid, cur_price, cur_sf, cur_units = row
        sets, params = [], {"id": rid, "n": u["note"]}
        price = u.get("price", cur_price)
        sf = u.get("building_sf", cur_sf)
        for col, key in (("price", "price"), ("seller", "seller"),
                         ("buyer", "buyer"), ("broker", "broker"),
                         ("address", "address"), ("building_sf", "building_sf"),
                         ("sale_date", "date"), ("sale_date_precision", "precision")):
            if key in u:
                sets.append(f"{col} = :{col}")
                params[col] = u[key]
        if u.get("price"):
            sets.append("excise_implied_price = :ex")
            params["ex"] = round(price / 1000.0 * 4.56, 2)
            if cur_units:
                sets.append("price_per_unit = :ppu")
                params["ppu"] = round(price / cur_units, 2)
        if price and sf:
            sets.append("price_per_sf = :ppsf")
            params["ppsf"] = round(price / sf, 2)
        sets.append("notes = coalesce(notes,'') || :n")
        if not dry_run:
            conn.execute(text(
                f"update transactions set {', '.join(sets)} where id = :id"), params)
        log.info("updated %-28s %s", u["match"][:28],
                 f"${u['price']:,}" if u.get("price") else "")
        updated += 1

    if not dry_run:
        conn.commit()
    n26, v26 = conn.execute(text(
        "select count(*), sum(coalesce(price,0)) from transactions "
        "where sale_date >= '2026-01-01'")).first()
    conn.close()
    log.info("\n%d loaded, %d already present, %d updated", loaded, skipped, updated)
    log.info("2026 now: %d transactions, $%s", n26, f"{int(v26 or 0):,}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    main(dry_run=not a.apply)
