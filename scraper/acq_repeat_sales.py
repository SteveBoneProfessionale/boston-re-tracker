r"""Repeat sales: match every transaction against every other on the same asset.

A single trade states a price. A pair states what happened to a basis, which is
the thing no individual row can say and the thing this tracker is best placed to
show -- 803 transactions across two cities over eleven years is exactly the
shape of data that produces paired sales.

MATCHING IS ON TWO KEYS, parcel first and address second.

    parcel_id   authoritative where present. Boston spine rows carry MassGIS
                LOC_ID, Cambridge rows carry map_lot or gisid. Same parcel is
                the same asset by definition.
    address     normalised, for press rows which carry no parcel at all. This
                is the weaker key and it is where false pairs come from.

THREE WAYS A NAIVE MATCH INVENTS PAIRS, all guarded:

MULTI-PARCEL DEEDS. A portfolio row's address is its representative parcel, and
the same deed may appear under different representatives in different years.
Rows sharing a book/page are the same conveyance, not a repeat sale.

PORTFOLIO ROWS AGAINST SINGLE-ASSET ROWS. "Brighton Avenue apartment portfolio"
covers six addresses; pairing its $36M against one building's later trade would
compare a portfolio basis to a single asset. Rows whose price_caveat says the
price spans multiple parcels are excluded from address matching.

SAME-DAY DUPLICATES. Two rows on one parcel within 90 days are almost always
one transaction recorded twice, not a flip. Excluded, and counted separately so
the exclusion is visible.

WHAT IS STORED, on the LATER trade of each pair: prior_sale_date,
prior_sale_price, prior_sale_source and basis_change_pct. The earlier row is
left alone, so a property that traded three times produces two pairs and each
later row points at its immediate predecessor.

    python scraper/acq_repeat_sales.py            # report
    python scraper/acq_repeat_sales.py --apply
"""

import argparse
import logging
import re
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from db.database import engine

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

MIN_GAP_DAYS = 90

_SUFFIX = (r"\b(STREET|ST|AVENUE|AVE|ROAD|RD|DRIVE|DR|BOULEVARD|BLVD|PLACE|PL|"
           r"SQUARE|SQ|LANE|LN|COURT|CT|WAY|TERRACE|TER|PARKWAY|PKWY|ROW|WHARF|"
           r"CIRCLE|CIR|HIGHWAY|HWY|PARK|PK)\b")


def norm_addr(a: str) -> str:
    """Normalise for matching: strip unit/suite, street types, punctuation.

    Deliberately keeps the street NUMBER -- dropping it was what made an earlier
    address key collapse every Summer Street address into one cluster.
    """
    s = re.sub(r"[^A-Z0-9 ]", " ", (a or "").upper())
    s = re.sub(r"\b(UNIT|STE|SUITE|APT|FL|FLOOR|#)\b.*$", " ", s)
    s = re.sub(r"\b(COMMERCIAL|COMM|GARAGE|CONDOMINIUM|CONDO|TRUST|LLC)\b", " ", s)
    s = re.sub(_SUFFIX, " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    # 27 43 WORMWOOD -> keep the leading number only, ranges collapse to first
    m = re.match(r"^(\d+)(?:\s+\d+)*\s+(.*)$", s)
    return f"{m.group(1)} {m.group(2)}".strip() if m else s


def main(dry_run: bool):
    conn = engine.connect()
    rows = conn.execute(text("""
        select id, address, city, parcel_id, sale_date, price, source,
               coalesce(deed_book,''), coalesce(deed_page,''),
               coalesce(price_caveat,''), coalesce(property_type,''),
               building_sf, unit_count
          from transactions
         where price > 0 and sale_date is not null
         order by sale_date""")).fetchall()
    log.info("%d priced transactions to match", len(rows))

    by_parcel, by_addr = defaultdict(list), defaultdict(list)
    portfolio = 0
    for r in rows:
        rid, addr, city, parcel, sd, price, src, bk, pg, caveat, ptype, sf, units = r
        rec = dict(id=rid, addr=addr, city=city, parcel=parcel, date=str(sd)[:10],
                   price=price, src=src, cite=f"{bk}/{pg}", ptype=ptype,
                   sf=sf, units=units)
        if parcel:
            by_parcel[(city, str(parcel))].append(rec)
        # A portfolio price is not this building's basis.
        if re.search(r"multiple parcels|combined|portfolio|whole-deed", caveat, re.I):
            portfolio += 1
            continue
        key = norm_addr(addr)
        if len(key) > 4:
            by_addr[(city, key)].append(rec)

    log.info("%d rows excluded from address matching as portfolio prices", portfolio)

    pairs, seen, same_deed, too_close = {}, set(), 0, 0
    rejected = []

    def same_asset(a, b) -> str | None:
        """Reject pairs that share a key but are not the same thing.

        A parcel id and an address are both coarser than an asset. Under one
        parcel sit the land, the building, a garage condominium and a retail
        condominium, and the first verification run happily paired them:
        101 Seaport at $9.7M against $452M (+4567%) is a land conveyance
        against the tower built on it, and 160 Federal at $190M against $3.0M
        (-98%) is the tower against a single garage unit.
        """
        sa, sb = a.get("sf") or 0, b.get("sf") or 0
        if sa and sb:
            hi, lo = max(sa, sb), min(sa, sb)
            if hi / lo > 2.5:
                return "building area differs more than 2.5x"
        # A unit- or component-scoped address is not the whole asset.
        SCOPE = re.compile(r"\b(RETAIL|COMM|COMMERCIAL|UNIT|GARAGE|PARCEL|BLDG|"
                           r"G-?\d|PZ)\b", re.I)
        ma, mb = bool(SCOPE.search(a["addr"])), bool(SCOPE.search(b["addr"]))
        if ma != mb:
            return "one side is a unit/component, the other the whole asset"
        # An implausible move on a short hold is a scope mismatch, not a market.
        chg = (b["price"] - a["price"]) / a["price"] * 100
        if chg > 300 or chg < -85:
            return f"implausible basis move {chg:+.0f}% — near-certain scope mismatch"
        return None

    def consider(group, basis):
        nonlocal same_deed, too_close
        g = sorted(group, key=lambda x: x["date"])
        for a, b in zip(g, g[1:]):
            if a["id"] == b["id"]:
                continue
            if a["cite"] not in ("/",) and a["cite"] == b["cite"]:
                same_deed += 1
                continue
            d0 = date.fromisoformat(a["date"])
            d1 = date.fromisoformat(b["date"])
            if (d1 - d0).days < MIN_GAP_DAYS:
                too_close += 1
                continue
            why = same_asset(a, b)
            if why:
                rejected.append((b["addr"], a, b, why))
                continue
            if b["id"] in seen:
                continue
            seen.add(b["id"])
            pairs[b["id"]] = (a, b, basis)

    for k, g in by_parcel.items():
        if len(g) > 1:
            consider(g, "parcel")
    for k, g in by_addr.items():
        if len(g) > 1:
            consider(g, "address")

    log.info("\n%d repeat-sale pairs found", len(pairs))
    log.info("  %d rejected as the same deed recorded twice", same_deed)
    log.info("  %d rejected as under %d days apart", too_close, MIN_GAP_DAYS)
    log.info("  %d rejected as not the same asset:", len(rejected))
    for addr, a, b, why in rejected[:8]:
        log.info("      %-28s $%-12s -> $%-12s  %s", addr[:28],
                 f"{a['price']:,}", f"{b['price']:,}", why)

    ups, downs = [], []
    for later_id, (a, b, basis) in pairs.items():
        chg = (b["price"] - a["price"]) / a["price"] * 100
        (ups if chg >= 0 else downs).append(chg)
    if pairs:
        allc = sorted((b["price"] - a["price"]) / a["price"] * 100
                      for a, b, _ in pairs.values())
        med = allc[len(allc) // 2]
        log.info("  %d up, %d down, median change %+.1f%%", len(ups), len(downs), med)

    log.info("\nlargest basis moves:")
    ranked = sorted(pairs.values(),
                    key=lambda t: abs(t[1]["price"] - t[0]["price"]), reverse=True)
    for a, b, basis in ranked[:14]:
        chg = (b["price"] - a["price"]) / a["price"] * 100
        log.info("  %-30s %s $%-12s -> %s $%-12s %+7.1f%%  [%s]",
                 b["addr"][:30], a["date"][:7], f"{a['price']:,}",
                 b["date"][:7], f"{b['price']:,}", chg, basis)

    if not dry_run:
        n = 0
        for later_id, (a, b, basis) in pairs.items():
            chg = round((b["price"] - a["price"]) / a["price"] * 100, 1)
            conn.execute(text("""
                update transactions
                   set prior_sale_date = :pd, prior_sale_price = :pp,
                       prior_sale_source = :ps, basis_change_pct = :chg
                 where id = :id and coalesce(prior_sale_price,0) = 0"""), {
                "pd": a["date"], "pp": a["price"],
                "ps": f"matched on {basis} to transaction id {a['id']} ({a['src']})",
                "chg": chg, "id": later_id})
            n += 1
        conn.commit()
        tot = conn.execute(text(
            "select count(*) from transactions where prior_sale_price > 0")).scalar()
        log.info("\n%d pairs written; %d rows now carry a prior sale", n, tot)
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
