r"""Second press sweep: the deals the first sweep missed.

The first sweep found 14 transactions for 2026 and I reported that as what press
coverage yields. That was wrong -- not in method but in effort. Searching by
deal TYPE ("recapitalization", "office tower sold") finds the deals that match
those words. Searching the publication's own deal-sheet SERIES, issue by issue,
finds what actually ran. The second approach roughly doubled the set and turned
up the largest 2026 transaction in either city, which the first had missed
entirely: the Kensington at $234 million.

The lesson is about sampling, not sources. Bisnow's Boston Deal Sheet runs
weekly and each issue carries five to ten transactions; the first sweep had read
four issues. There is no reason to think this sweep is complete either, and the
tab's coverage note says so.

THE UNDATED-PRESS TRAP KEEPS FIRING. Four more articles surfaced from 2026
queries and read as current:

    Urban Spaces / Flats on First, $103M       May 2022
    30 Hampshire Street, $25.1M                November 2025
    2400 Massachusetts Avenue, $12.5M          February 2024
    Berklee / Charlesgate West, $28.1M         November 2025

None are loaded. Every entry below states the date its source gives.

    python scraper/acq_press_round2.py --apply
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

DS_JUL14 = "https://www.bisnow.com/boston/news/deal-sheet/large-office-to-residential-conversion-project-proposed-watertown-boston-deal-sheet-135419"
DS_APR27 = "https://www.bisnow.com/boston/news/deal-sheet/the-boston-deal-sheet-134308"
DS_MAR02 = "https://www.bisnow.com/boston/news/deal-sheet/bxp-sells-downtown-boston-apartment-building-boston-deal-sheet-133474"

DEALS = [
    dict(
        address="665 Washington Street (The Kensington)", city="Boston",
        transaction_type="asset_sale",
        date="2026-07-01", precision="month", price=234_000_000,
        buyer="Pontegadea (Amancio Ortega)",
        seller="Kensington Investment Management",
        unit_count=381, building_sf=488_000, property_type="Multifamily",
        source="press", source_name="Bisnow Boston Deal Sheet",
        source_url=DS_JUL14, source_date="2026-07-14",
        passage='"Spanish billionaire Amancio Ortega, founder of fashion chain '
                'Zara, bought the Kensington apartment tower" in Boston\'s Theater '
                'District for $234M from Kensington Investment Management. '
                '27-story, 381 units, developed 2011-2013 by a partnership between '
                'Kensington and National Development.',
        notes="THE LARGEST 2026 TRANSACTION IN EITHER CITY, and the first sweep "
              "missed it entirely — it was found only by reading the deal-sheet "
              "series issue by issue rather than searching deal types. Building "
              "area of 488,000 SF and the 665 Washington Street address come from "
              "property listings, not the deal report, which names only the "
              "Theater District.",
    ),
    dict(
        address="4-6 and 28 Newbury Street", city="Boston",
        transaction_type="asset_sale",
        date="2026-04-01", precision="month", price=113_500_000,
        buyer="Acadia Realty Trust / Osiris Ventures", seller=None,
        broker="Newmark", building_sf=29_000, property_type="Retail",
        source="press", source_name="Bisnow Boston Deal Sheet",
        source_url=DS_APR27, source_date="2026-04-27",
        passage='Acadia Realty Trust and Osiris Ventures acquired 4-6 and 28 '
                'Newbury Street for $113.5M. 4-6 Newbury is 10,000 SF of retail '
                'across two floors leased to Chanel; 28 Newbury is 19,000 SF '
                'leased to Cartier, its only New England location. Newmark (Robert '
                'Griffin, Geoffrey Millerd, Paul Penman) brokered. "Custom-designed '
                'for Chanel and Cartier, the properties each command an unmatched '
                'presence along the first block of Newbury Street."',
        notes="ONE TRANSACTION COVERING TWO BUILDINGS at a single combined price, "
              "so it is recorded once rather than split into two invented prices. "
              "Building area is the sum of the two stated areas (10,000 + 19,000), "
              "which makes $/SF a blended figure across both. Seller not named.",
        price_caveat="Combined price for two buildings; $/SF is blended.",
    ),
    dict(
        address="Brighton Avenue apartment portfolio", city="Boston",
        transaction_type="asset_sale",
        date="2026-04-01", precision="month", price=36_000_000,
        buyer="Alpha Management", seller="The Mount Vernon Co.",
        property_type="Multifamily",
        source="press", source_name="Bisnow Boston Deal Sheet",
        source_url=DS_APR27, source_date="2026-04-27",
        passage='Alpha Management acquired an apartment portfolio from The Mount '
                'Vernon Co. for $36M, comprising 61-77 Brighton Ave., 81, 83 and 87 '
                'Brighton Ave., 6 Camelot Court, 110 Warren St. and 66 Chester St.',
        notes="A SIX-ADDRESS PORTFOLIO in Allston/Brighton at one combined price. "
              "Recorded as a single transaction with the portfolio named in the "
              "address field, because splitting $36M across six buildings would "
              "require inventing six prices no source states. Unit count not "
              "disclosed, so no $/unit.",
        price_caveat="Combined portfolio price across six addresses.",
    ),
    dict(
        address="374 Congress Street", city="Boston",
        transaction_type="asset_sale",
        date="2026-07-02", precision="day", price=28_000_000,
        buyer="Eastern Real Estate", seller=None,
        building_sf=105_000, property_type="Office",
        source="press", source_name="Bisnow Boston Deal Sheet",
        source_url=DS_JUL14, source_date="2026-07-14",
        passage='Eastern Real Estate acquired the 105,000 SF office building at 374 '
                'Congress St. in Fort Point for $28M on 2 July 2026. The property '
                'was part of a five-building portfolio acquired by Nuveen in 2016 '
                'for $225M.',
    ),
    dict(
        address="31 Buttonwood Street and 14 Willis Street", city="Boston",
        transaction_type="asset_sale",
        date="2026-08-01", precision="month", price=4_100_000,
        buyer="Groma", seller=None,
        property_type="Multifamily",
        source="press", source_name="Bisnow Boston Deal Sheet",
        source_url="https://www.bisnow.com/news/boston/deal-sheet/bell-partners-buys-reading-multifamily-for-96m-the-boston-deal-sheet",
        source_date="2026-08",
        passage='"Tech real estate company Groma acquired two multifamily '
                'properties in Boston for $4.1M at 31 Buttonwood St. and 14 Willis '
                'St. in Dorchester."',
        notes="Two Dorchester properties at one combined price. Included because "
              "the floor is $2M; it would have been excluded at $5M. This is the "
              "size of deal press reports only occasionally, which is why the "
              "sub-$20M band cannot be treated as covered.",
        price_caveat="Combined price for two properties.",
    ),
    dict(
        address="Cambridge apartment building (10 units, opposite Harvard)",
        city="Cambridge",
        transaction_type="asset_sale",
        date="2026-06-01", precision="month", price=6_500_000,
        buyer=None, seller=None, broker="Marcus & Millichap",
        unit_count=10, property_type="Multifamily",
        source="press", source_name="REBusinessOnline / Connect CRE",
        source_url="https://rebusinessonline.com/marcus-millichap-brokers-6-5m-sale-of-cambridge-apartment-building/",
        source_date="2026-06",
        passage='Marcus & Millichap brokered the $6.5 million sale of a 10-unit '
                'apartment building located across from Harvard University in '
                'Cambridge. Connect CRE headlined the same deal "Cambridge '
                'Apartments Across from Harvard Fetch $655K Per Unit", consistent '
                'with $6.5M over 10 units.',
        notes="ADDRESS NOT STATED by either source, and neither names the buyer or "
              "seller — a brokerage announcement, not a deal report. Carried with "
              "the description in the address field and not linked to a parcel. "
              "The $655K/unit headline corroborates the price against the unit "
              "count, which is the only internal check available.",
    ),
]

# The Lofts row already exists from the SEC filing; press supplies what a 10-Q
# structurally cannot -- the grantee and the street address.
LOFTS_UPDATE = dict(
    address="530 Atlantic Avenue (The Lofts at Atlantic Wharf)",
    buyer="AEW Capital Management (affiliate entity)",
    broker="Newmark",
    unit_count=86,
    note=(" | BUYER AND ADDRESS ADDED FROM PRESS. Bisnow's 2 March 2026 deal "
          "sheet: BXP \"sold The Lofts at Atlantic Wharf, comprising 86 units, to "
          "an entity linked to AEW Capital Management\" at 530 Atlantic Ave., "
          "brokered by Newmark, per a deed recorded 26 February 2026. The 10-Q "
          "gives the close date as 25 February 2026 and states the price; the deed "
          "record is dated the following day. The SEC close date is kept as "
          "sale_date. This pairing is the argument for keeping both sources: the "
          "filing is authoritative on price and date, the press on the grantee.")
)


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
        log.info("%-52s %-10s %s", d["address"][:52], d["city"],
                 f"${price:,}" if price else "(no price)")
        if dry_run:
            loaded += 1
            continue
        conn.execute(text("""
            insert into transactions
              (address, city, transaction_type, sale_date, sale_date_precision,
               price, price_caveat, buyer, seller, broker, property_type,
               building_sf, unit_count, price_per_sf, price_per_unit,
               excise_implied_price, source, source_url, source_name, source_date,
               passage, confidence, notes, created_at)
            values
              (:a, :city, :t, :d, :prec, :p, :caveat, :buyer, :seller, :broker,
               :ptype, :sf, :units, :ppsf, :ppu, :ex, :src, :url, :sname, :sdate,
               :passage, 'web_low_confidence', :notes, :now)"""), {
            "a": d["address"], "city": d["city"], "t": d["transaction_type"],
            "d": d["date"], "prec": d["precision"], "p": price,
            "caveat": d.get("price_caveat"), "buyer": d.get("buyer"),
            "seller": d.get("seller"), "broker": d.get("broker"),
            "ptype": d.get("property_type"), "sf": sf, "units": units,
            "ppsf": round(price / sf, 2) if (price and sf) else None,
            "ppu": round(price / units, 2) if (price and units) else None,
            "ex": round(price / 1000.0 * 4.56, 2) if price else None,
            "src": d["source"], "url": d["source_url"], "sname": d["source_name"],
            "sdate": d.get("source_date"), "passage": d.get("passage"),
            "notes": d.get("notes"), "now": datetime.utcnow(),
        })
        loaded += 1

    # ── enrich the SEC-sourced Lofts row ────────────────────────────
    row = conn.execute(text(
        "select id, price from transactions where address like '%Lofts at Atlantic%'"
    )).first()
    if row and not dry_run:
        u = LOFTS_UPDATE
        conn.execute(text("""
            update transactions set address = :a, buyer = :b, broker = :br,
                   unit_count = :u, price_per_unit = :ppu,
                   notes = coalesce(notes,'') || :n
            where id = :id"""), {
            "a": u["address"], "b": u["buyer"], "br": u["broker"],
            "u": u["unit_count"], "ppu": round(row[1] / u["unit_count"], 2),
            "n": u["note"], "id": row[0]})
        log.info("\nLofts at Atlantic Wharf: buyer -> %s, address -> %s",
                 u["buyer"], u["address"])

    if not dry_run:
        conn.commit()
    n26, v26 = conn.execute(text(
        "select count(*), sum(coalesce(price,0)) from transactions "
        "where sale_date >= '2026-01-01'")).first()
    conn.close()
    log.info("\n%d loaded, %d already present", loaded, skipped)
    log.info("2026 now: %d transactions, $%s", n26, f"{int(v26 or 0):,}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    main(dry_run=not a.apply)
