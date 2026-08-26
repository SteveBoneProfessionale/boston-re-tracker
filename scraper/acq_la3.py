r"""LA3 sales parcels, and the $2.7B of intra-sponsor transfers it exposed.

WHAT LA3 IS. Every Massachusetts municipality files an annual LA3 with the
Department of Revenue listing the sales used for assessment ratio studies.
MassGIS publishes it as a feature service carrying something no other free
source in this project has: SELLER and BUYER by name, plus NAL_CODE, DOR's
non-arm's-length classification.

WHAT IT DOES NOT DO, which was the hope. It does not fill 2023-2025. The
published layer is a single fiscal year and covers 1 JULY 2021 TO 31 DECEMBER
2021 -- six months. Boston 4,411 sales, Cambridge 709, of which 136 are
commercial at or above $2M. Only one LA3 item exists in ArcGIS Online; there is
no per-year archive, and Boston's Level 3 parcel layer is likewise FY2023 in
every MassGIS service checked. So the 2023-2025 hole is not closeable from open
state data, and the remaining route is a public records request to DOR's Bureau
of Local Assessment for the LA3 filings of later years.

WHAT IT DID EXPOSE, which is worth more than the six months. LA3's top
Boston/Cambridge commercial row is ARE-MA REGION NO. 40 LLC selling to ARE-MA
REGION NO. 102 OWNER, LLC for $1,185,241,320, flagged NAL_CODE 'U'. That is
Alexandria conveying to Alexandria. Checking the whole table for the same
pattern -- one resolved sponsor on BOTH sides -- finds twelve rows and
$2,703,664,773 recorded as arm's-length acquisitions:

    $1,185,241,320  54-64 Binney St     ARE-MA 40 -> ARE-MA 102
    $1,020,000,000  100 Binney St       ARE-MA 45 -> ARE-MA 107
    $  125,264,160  225 Binney St       ARE-MA 34 -> ARE MA 54
    $   87,664,797  Cambridge St        Harvard -> Harvard
    ... and eight more, several with the SAME entity name on both sides

Alexandria's buy-side total was $4.60B. $2.33B of that is Alexandria buying from
itself. A "most active buyers" chart built on that is not measuring acquisition,
it is measuring internal restructuring.

So `arms_length` is set false with reason 'affiliated_parties' on every row where
the resolved sponsor matches on both sides, and the rankings exclude them. The
rows are kept, because an intra-sponsor conveyance at a stated price is real
information about how an owner values its own asset -- it is simply not a
purchase.

    python scraper/acq_la3.py --apply
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from db.database import engine

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

UA = {"User-Agent": "boston-re-tracker/1.0 (13silonergan@gmail.com)"}
LA3 = ("https://services1.arcgis.com/hGdibHYSPO59RG1h/arcgis/rest/services/"
       "LA3_Sales_Parcels_gdb/FeatureServer/0/query")

AFFILIATED_NOTE = (
    "NOT ARM'S-LENGTH: THE SAME SPONSOR IS ON BOTH SIDES. The resolved buyer and "
    "seller are the same firm conveying between its own vehicles, so this is an "
    "internal restructuring and not an acquisition. It was counted as a purchase "
    "in the buyer rankings until LA3 exposed the pattern: LA3's largest "
    "Boston/Cambridge commercial row is ARE-MA REGION NO. 40 LLC to ARE-MA REGION "
    "NO. 102 OWNER, LLC at $1,185,241,320, carrying DOR non-arm's-length code "
    "'U'. Twelve rows and $2,703,664,773 match the pattern across the table, of "
    "which $2.33B is Alexandria alone -- more than half its apparent buy-side "
    "volume. The row is kept because an intra-sponsor conveyance at a stated "
    "price says something about how an owner values its own asset; it is "
    "excluded from arm's-length reads and from acquisition rankings."
)


def fetch_la3() -> list:
    out, offset = [], 0
    where = ("MUNI IN ('BOSTON','CAMBRIDGE') AND USE_CODE >= '300' "
             "AND USE_CODE < '500' AND SALE_PRICE >= 1000000")
    with httpx.Client(headers=UA, timeout=120) as c:
        while True:
            r = c.get(LA3, params={
                "where": where,
                "outFields": ("LOC_ID,MUNI,SALE_DATE,SALE_PRICE,SELLER,BUYER,"
                              "ST_NUM,ST_NAME,NAL_CODE,USE_CODE"),
                "returnGeometry": "false", "resultOffset": offset,
                "resultRecordCount": 2000, "f": "json"})
            r.raise_for_status()
            d = r.json()
            feats = d.get("features", [])
            out.extend(f["attributes"] for f in feats)
            if not d.get("exceededTransferLimit") or not feats:
                return out
            offset += len(feats)
            time.sleep(0.4)


def main(dry_run: bool):
    conn = engine.connect()

    # ── 1. affiliated transfers, found via LA3's pattern ────────────
    rows = conn.execute(text("""
        select id, price from transactions
         where coalesce(buyer_canonical,'') <> ''
           and buyer_canonical = seller_canonical
           and coalesce(arms_length, 1) <> 0""")).fetchall()
    total = sum(r[1] or 0 for r in rows)
    log.info("%d rows have the same sponsor on both sides, $%s", len(rows),
             f"{total:,}")
    if not dry_run:
        for rid, _p in rows:
            conn.execute(text("""
                update transactions
                   set arms_length = 0,
                       non_arms_length_reason = 'affiliated_parties',
                       notes = coalesce(notes,'') || :n
                 where id = :id"""), {"n": " | " + AFFILIATED_NOTE, "id": rid})
        conn.commit()

    # ── 2. LA3: seller/buyer/NAL for the H2 2021 window ─────────────
    la3 = fetch_la3()
    log.info("\nLA3: %d Boston/Cambridge commercial sales >= $1M", len(la3))
    if la3:
        ds = [a["SALE_DATE"] for a in la3 if a.get("SALE_DATE")]
        f = lambda t: datetime.fromtimestamp(t / 1000, timezone.utc).date().isoformat()
        log.info("     covering %s to %s", f(min(ds)), f(max(ds)))

    by_loc, by_key = {}, {}
    for a in la3:
        ts = a.get("SALE_DATE")
        if not ts:
            continue
        d = datetime.fromtimestamp(ts / 1000, timezone.utc).date().isoformat()
        rec = dict(seller=(a.get("SELLER") or "").strip(),
                   buyer=(a.get("BUYER") or "").strip(),
                   nal=(a.get("NAL_CODE") or "").strip(),
                   date=d, price=int(a.get("SALE_PRICE") or 0))
        if a.get("LOC_ID"):
            by_loc[a["LOC_ID"]] = rec
        by_key[(rec["price"], d)] = rec

    cand = conn.execute(text("""
        select id, parcel_id, sale_date, price, coalesce(seller,''),
               coalesce(buyer,''), arms_length
          from transactions
         where sale_date >= '2021-07-01' and sale_date <= '2021-12-31'""")).fetchall()
    log.info("%d table rows fall inside the LA3 window", len(cand))

    filled_seller = tagged_nal = 0
    for rid, parcel, sd, price, seller, buyer, arms in cand:
        m = by_loc.get(str(parcel or "")) or by_key.get((price or 0, str(sd)[:10]))
        if not m:
            continue
        sets, params = [], {"id": rid}
        if not seller and m["seller"]:
            sets.append("seller = :s")
            params["s"] = m["seller"]
            filled_seller += 1
        if m["nal"]:
            sets.append("non_arms_length_reason = coalesce(non_arms_length_reason, :r)")
            params["r"] = f"LA3 NAL code '{m['nal']}'"
            tagged_nal += 1
        if sets and not dry_run:
            sets.append("notes = coalesce(notes,'') || :n")
            params["n"] = (
                f" | MATCHED TO THE DOR LA3 SALES FILE. LA3 is the annual "
                f"assessment-ratio return every Massachusetts municipality files "
                f"with the Department of Revenue, published by MassGIS. It names "
                f"seller '{m['seller'][:60]}' and buyer '{m['buyer'][:60]}' and "
                f"carries non-arm's-length code '{m['nal'] or '(blank)'}'. The "
                f"code is stored verbatim rather than interpreted: DOR's legend "
                f"was not consulted, so it marks a row for review and is not "
                f"itself an arm's-length determination.")
            conn.execute(text(
                f"update transactions set {', '.join(sets)} where id = :id"), params)
    if not dry_run:
        conn.commit()

    log.info("  %d sellers filled from LA3, %d rows tagged with a NAL code",
             filled_seller, tagged_nal)

    n = conn.execute(text(
        "select count(*) from transactions where arms_length = 0")).scalar()
    v = conn.execute(text(
        "select sum(coalesce(price,0)) from transactions where arms_length = 0")).scalar()
    log.info("\nnon-arm's-length rows: %d, $%s", n, f"{int(v or 0):,}")
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
