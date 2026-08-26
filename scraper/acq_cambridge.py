r"""Load Cambridge commercial transactions from the city's assessment file.

waa7-ibdu carries saleprice and saledate directly, which Boston's file does
not. What it does NOT carry is anything recent: the FY2026 file's newest sale
date is 6 August 2025, and it holds 1,462 sales dated 2023 and 1,581 dated 2024
against 54 dated 2025 and ZERO dated 2026. It records the sale that established
current ownership as of the assessment snapshot, so the 2026 window this tab is
built for is entirely absent. Cambridge is not the easy half after all.

So this does two jobs, neither of them "supply 2026 Cambridge deals":

  it loads the historical commercial sales it does hold, which gives the tab a
  real spine to be built and tested against, and gives price-per-SF baselines
  by asset class that 2026 deals can be read against;

  it builds the property-type and size LOOKUP -- state class, property class,
  land area, living area, unit count -- which is what a deed needs joined to it,
  since a deed states neither.

    python scraper/acq_cambridge.py --floor 3000000
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text
from db.database import engine

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

URL = "https://data.cambridgema.gov/resource/waa7-ibdu.json"
UA = {"User-Agent": "Mozilla/5.0 (compatible; boston-re-tracker/1.0)"}

# State class 300-399 commercial, 400-499 industrial. The 000-199 residential
# bands and the 900 exempt band are out of scope by definition.
COMMERCIAL = "stateclasscode >= '300' AND stateclasscode <= '499'"

# Property classes that are residential condominiums or single families even
# when they carry a commercial-ish class code.
_RESIDENTIAL_WORDS = ("CONDO", "SNGL-FAM", "SINGLE FAM", "TWO-FAM", "THREE-FM",
                      "MULTIPLE HOUSES", "RES ")


def fetch(where: str, limit: int = 50000) -> list:
    rows, offset = [], 0
    with httpx.Client(headers=UA, timeout=90) as c:
        while True:
            r = c.get(URL, params={"$where": where, "$limit": 5000,
                                   "$offset": offset, "$order": "saledate DESC"})
            r.raise_for_status()
            page = r.json()
            rows.extend(page)
            if len(page) < 5000 or len(rows) >= limit:
                return rows
            offset += 5000


def main(floor: int, since: str = "2015-01-01"):
    where = (f"{COMMERCIAL} AND saleprice > 0 AND "
             f"saledate >= '{since}T00:00:00.000'")
    rows = fetch(where)
    log.info("%d Cambridge commercial/industrial sales since %s with a price",
             len(rows), since)

    kept, skipped_small, skipped_res = [], 0, 0
    for r in rows:
        price = int(float(r.get("saleprice") or 0))
        pc = (r.get("propertyclass") or "").upper()
        if any(w in pc for w in _RESIDENTIAL_WORDS):
            skipped_res += 1
            continue
        if price < floor:
            skipped_small += 1
            continue
        kept.append(r)
    log.info("  %d below the $%s floor, %d residential by property class, %d kept",
             skipped_small, f"{floor:,}", skipped_res, len(kept))

    conn = engine.connect()
    loaded = 0
    for r in kept:
        price = int(float(r.get("saleprice") or 0))
        sf = int(float(r.get("interior_livingarea") or 0)) or None
        units = int(float(r.get("interior_numunits") or 0)) or None
        book_page = (r.get("book_page") or "").split("/")
        book = book_page[0] if book_page and book_page[0] else None
        page = book_page[1] if len(book_page) > 1 else None
        sale_date = (r.get("saledate") or "")[:10] or None

        # Massachusetts excise implies a price and vice versa: $4.56 per
        # $1,000 of consideration. Stored so a stated price can be checked
        # against a stamp when one is available from the registry.
        excise = round(price / 1000.0 * 4.56, 2) if price else None

        # arms_length is left UNKNOWN, not asserted. The assessment file carries
        # no document type, so it cannot tell a bargain-and-sale deed from a
        # trustee's deed, a correction deed or a foreclosure. What the price
        # floor does remove is nominal consideration: a quarter of the sales in
        # this file are $100 or less, which is why the floor is doing real
        # arm's-length work before any flag is set.
        exists = conn.execute(text(
            "select id from transactions where address = :a and sale_date = :d "
            "and price = :p and transaction_type = 'asset_sale'"),
            {"a": r.get("address"), "d": sale_date, "p": price}).first()
        if exists:
            continue
        conn.execute(text("""
            insert into transactions
              (address, parcel_id, city, latitude, longitude, transaction_type,
               sale_date, sale_date_precision, price, property_type, building_sf,
               unit_count, land_sf, price_per_sf, price_per_unit, deed_book,
               deed_page, doc_type, arms_length, excise_implied_price, source,
               source_url, source_name, confidence, created_at)
            values
              (:a, :parcel, 'Cambridge', :lat, :lon, 'asset_sale',
               :d, 'day', :p, :ptype, :sf, :units, :land, :ppsf, :ppu, :book,
               :page, null, null, :excise, 'cambridge_socrata',
               :url, 'City of Cambridge assessment file (waa7-ibdu)',
               'registry_confirmed', :now)"""), {
            "a": r.get("address"), "parcel": r.get("map_lot") or r.get("gisid"),
            "lat": float(r["latitude"]) if r.get("latitude") else None,
            "lon": float(r["longitude"]) if r.get("longitude") else None,
            "d": sale_date, "p": price,
            "ptype": r.get("propertyclass"), "sf": sf, "units": units,
            "land": int(float(r.get("landarea") or 0)) or None,
            "ppsf": round(price / sf, 2) if sf else None,
            "ppu": round(price / units, 2) if units else None,
            "book": book, "page": page, "excise": excise,
            "url": URL, "now": datetime.utcnow(),
        })
        loaded += 1
    conn.commit()

    total = conn.execute(text(
        "select count(*), sum(price) from transactions where city='Cambridge'")).first()
    conn.close()
    log.info("\nloaded %d new; Cambridge now %d transactions, $%s total volume",
             loaded, total[0], f"{int(total[1] or 0):,}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--floor", type=int, default=3_000_000)
    ap.add_argument("--since", default="2015-01-01")
    main(ap.parse_args().floor, ap.parse_args().since)
