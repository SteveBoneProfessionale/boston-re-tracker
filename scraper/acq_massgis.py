r"""Boston's transaction spine, from MassGIS Level 3 standardized parcels.

WHY THIS EXISTS. Boston's own assessment file has 68 fields and not one of them
is a sale price -- OWNER is the only ownership field in it. So while Cambridge
had a Socrata file with saleprice and saledate, Boston had nothing at all, and
the tab held exactly two Boston rows, both from press. This closes that.

MassGIS Level 3 is the Commonwealth's standardized assessors' parcel layer. It
carries LS_PRICE, LS_DATE, LS_BOOK and LS_PAGE -- an actual book and page, which
is the registry citation a deed feed would give -- served from a public ArcGIS
REST endpoint with no access control to work around.

WHAT IT DOES NOT DO, AND THIS IS THE POINT: it does not reach 2026.

    Boston   L3 is FY2023. Newest sale in the layer: 28 October 2022.
    Cambridge L3 is FY2026. Newest sale in the layer: 31 July 2025.

Cambridge's L3 agrees with its Socrata file, which stops 6 August 2025. Boston's
is three and a half years stale. Between the two cities' own files, the state
layer and the blocked registry, there is NO open dataset in Massachusetts that
carries a 2026 Boston or Cambridge sale. The 2026 window is press-and-SEC-only
not because the collection was lazy but because the public record, as published,
stops before it.

So this loads history, and history is worth having: 886 Boston commercial sales
at or above $5M since 2015 give the Boston price-per-SF baselines that 2026
press deals have to be read against, plus the book and page to reconcile
against a licensed feed later.

ONE PARCEL IS NOT ONE DEED. L3 stores the last sale on EVERY parcel it touched,
so a deed conveying eleven parcels writes its price eleven times. Summing the
raw rows would inflate dollar volume by the size of every portfolio deal in the
set. Rows are therefore deduplicated on (book, page, date, price) before
loading, the largest parcel of a group is kept as the representative, and the
number of parcels the deed covered is recorded so a multi-parcel sale is not
mistaken for a single-building trade.

    python scraper/acq_massgis.py --apply --floor 5000000
"""

import argparse
import logging
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from db.database import engine

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

URL = ("https://arcgisserver.digital.mass.gov/arcgisserver/rest/services/AGOL/"
       "L3_Parcels_FeatureService_4326/FeatureServer/1/query")
UA = {"User-Agent": "boston-re-tracker/1.0 (13silonergan@gmail.com)"}

FIELDS = ("LOC_ID,PROP_ID,SITE_ADDR,CITY,LS_DATE,LS_PRICE,LS_BOOK,LS_PAGE,"
          "USE_CODE,BLD_AREA,UNITS,LOT_SIZE,OWNER1,YEAR_BUILT,FY,TOTAL_VAL")

# DOR use codes: 300-399 commercial, 400-499 industrial. Same banding the
# Cambridge loader uses, so the two cities classify alike.
COMMERCIAL = "USE_CODE >= '300' AND USE_CODE < '500'"

# Registered land (Torrens) carries a certificate rather than a book and page.
# L3 has a REG_ID field but it is not populated consistently, so registered-land
# parcels mostly arrive with a null book -- which is itself the signal.
_ASSET = {
    "3": "Commercial",
    "4": "Industrial",
}


def _use_label(code: str) -> str:
    c = (code or "").strip()
    return _ASSET.get(c[:1], "Commercial") + (f" ({c})" if c else "")


def fetch(city: str, floor: int, since: str) -> list:
    out, offset = [], 0
    where = (f"CITY='{city}' AND {COMMERCIAL} AND LS_PRICE >= {floor} "
             f"AND LS_DATE >= '{since}'")
    with httpx.Client(headers=UA, timeout=120) as c:
        while True:
            r = c.get(URL, params={
                "where": where, "outFields": FIELDS, "returnGeometry": "false",
                "orderByFields": "LS_DATE DESC", "resultOffset": offset,
                "resultRecordCount": 2000, "f": "json"})
            r.raise_for_status()
            d = r.json()
            if "error" in d:
                raise RuntimeError(d["error"])
            feats = d.get("features", [])
            out.extend(f["attributes"] for f in feats)
            if not d.get("exceededTransferLimit") or not feats:
                return out
            offset += len(feats)
            time.sleep(0.4)          # deliberate throttle on a public endpoint


def dedupe(rows: list) -> list:
    """One deed, one transaction.

    L3 writes the sale onto every parcel the deed conveyed, so a portfolio deal
    appears once per parcel at full price. Group on the registry citation plus
    date and price; keep the largest-building parcel as the representative and
    carry the parcel count.
    """
    groups = defaultdict(list)
    for r in rows:
        key = (r.get("LS_BOOK"), r.get("LS_PAGE"), r.get("LS_DATE"),
               r.get("LS_PRICE"))
        groups[key].append(r)
    kept = []
    for key, g in groups.items():
        if key[0] in (None, "", "0") and len(g) > 1:
            # No book/page to group on: without a citation these could be
            # genuinely distinct sales that merely share a date and price.
            # Keep them all rather than silently collapsing real transactions.
            kept.extend((r, 1) for r in g)
            continue
        rep = max(g, key=lambda x: (x.get("BLD_AREA") or 0, x.get("LOT_SIZE") or 0))
        kept.append((rep, len(g)))
    return kept


def main(floor: int, since: str, city: str, dry_run: bool):
    rows = fetch(city, floor, since.replace("-", ""))
    log.info("%s: %d parcel rows >= $%s since %s", city, len(rows),
             f"{floor:,}", since)
    kept = dedupe(rows)
    multi = sum(1 for _, n in kept if n > 1)
    raw_sum = sum(r.get("LS_PRICE") or 0 for r in rows)
    ded_sum = sum(r.get("LS_PRICE") or 0 for r, _ in kept)
    log.info("  %d distinct deeds after dedup (%d covered multiple parcels)",
             len(kept), multi)
    log.info("  volume raw $%s -> deduped $%s  (raw overstates by $%s)",
             f"{raw_sum:,}", f"{ded_sum:,}", f"{raw_sum - ded_sum:,}")

    newest = max((r.get("LS_DATE") or "") for r in rows) if rows else ""
    log.info("  newest sale date in the layer: %s", newest or "n/a")

    if dry_run:
        return

    conn = engine.connect()
    loaded = 0
    for r, nparc in kept:
        price = int(r.get("LS_PRICE") or 0)
        d = str(r.get("LS_DATE") or "")
        sale_date = f"{d[:4]}-{d[4:6]}-{d[6:8]}" if len(d) == 8 else None
        if not sale_date or not price:
            continue
        sf = int(r.get("BLD_AREA") or 0) or None
        units = int(r.get("UNITS") or 0) or None
        addr = (r.get("SITE_ADDR") or "").strip()
        book = (r.get("LS_BOOK") or "").strip() or None
        page = (r.get("LS_PAGE") or "").strip() or None

        exists = conn.execute(text(
            "select id from transactions where address = :a and sale_date = :d "
            "and price = :p"), {"a": addr, "d": sale_date, "p": price}).first()
        if exists:
            continue

        note = None
        if nparc > 1:
            note = (f"MULTI-PARCEL DEED — book {book}/page {page} conveyed "
                    f"{nparc} parcels at this price. One transaction is recorded, "
                    f"not {nparc}, and the largest parcel stands as the "
                    f"representative address. Price is the whole-deed "
                    f"consideration, so price-per-SF here reflects only the "
                    f"representative parcel's building area and understates the "
                    f"portfolio's true area.")
        conn.execute(text("""
            insert into transactions
              (address, parcel_id, city, transaction_type, sale_date,
               sale_date_precision, price, property_type, building_sf, unit_count,
               land_sf, price_per_sf, price_per_unit, deed_book, deed_page,
               seller, excise_implied_price, source, source_url, source_name,
               source_date, confidence, notes, price_caveat, created_at)
            values
              (:a, :pid, :city, 'asset_sale', :d, 'day', :p, :ptype, :sf, :units,
               :land, :ppsf, :ppu, :book, :page, null, :ex, 'massgis_l3', :url,
               :sname, :fy, 'registry_confirmed', :note, :caveat, :now)"""), {
            "a": addr, "pid": r.get("LOC_ID") or r.get("PROP_ID"), "city": city.title(),
            "d": sale_date, "p": price, "ptype": _use_label(r.get("USE_CODE")),
            "sf": sf, "units": units,
            "land": int(r.get("LOT_SIZE") or 0) or None,
            "ppsf": round(price / sf, 2) if sf else None,
            "ppu": round(price / units, 2) if units else None,
            "book": book, "page": page,
            "ex": round(price / 1000.0 * 4.56, 2),
            "url": URL, "sname": "MassGIS Level 3 standardized assessors' parcels",
            "fy": str(r.get("FY") or ""), "note": note,
            "caveat": ("Whole-deed consideration across multiple parcels."
                       if nparc > 1 else None),
            "now": datetime.utcnow(),
        })
        loaded += 1
    conn.commit()
    tot = conn.execute(text(
        "select count(*), sum(coalesce(price,0)) from transactions "
        "where city = :c"), {"c": city.title()}).first()
    conn.close()
    log.info("\nloaded %d; %s now %d transactions, $%s",
             loaded, city.title(), tot[0], f"{int(tot[1] or 0):,}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", default="BOSTON")
    ap.add_argument("--floor", type=int, default=5_000_000)
    ap.add_argument("--since", default="2015-01-01")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    main(a.floor, a.since, a.city, dry_run=not a.apply)
