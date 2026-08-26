r"""Corrections and late additions to the press-sourced transaction set.

18 TREMONT STREET -- price corrected from $30.0M to $29.5M.

The row was loaded at $30M. Three independent sources state $29.5M: CoStar
("Kendall Capital bought 18 Tremont St. ... for $29.5 million"), Connect CRE,
and Bisnow's 16 June 2026 deal sheet. $30M appears to have been a rounded
figure. The buyer and seller were also worth re-checking, because a Bisnow
table read as though Jamestown were the BUYER; it is not. CoStar's headline is
"Jamestown disposes of Boston office building at steep discount to 2019 price",
and Jamestown is the party that paid roughly $103M in 2019. The direction as
originally recorded was right and stands; only the price and the date move, and
the buyer gains the Kendall Capital name that the original row lacked.

11 BEACON STREET -- a recapitalization, and the reason `price` stays null.

Synergy "recapitalized" 11 Beacon "for $23M" while keeping ownership. That
sentence does not say what anyone paid for a stake, and it does not give a
percentage. $23M against 146,000 SF is $157/SF, which reads as a whole-asset
capitalization rather than a cheque -- and the same publication describes the
2016 round as a recapitalization "at $63M", the same valuation phrasing.

So `price` is NULL and $23M goes to `implied_valuation`. If $23M were recorded
as a price, Synergy would appear in Most Active Buyers having "paid" $23M for
an asset it already owned and did not buy. That is precisely the error the
partial-interest handling exists to prevent, and it is worth one null field.

    python scraper/acq_press_fixups.py --apply
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

TREMONT_NOTE = (
    "PRICE CORRECTED from $30,000,000 to $29,500,000. CoStar: \"Kendall Capital "
    "bought 18 Tremont St., a 12-story office building across from Boston Common, "
    "for $29.5 million, paying 71% less than the roughly $103 million that "
    "Jamestown laid out for the tower back in 2019.\" Corroborated by Connect CRE "
    "(\"Downtown Boston Office Sells for 71% Discount on 2019 Price\") and Bisnow's "
    "16 June 2026 deal sheet. "
    "DIRECTION RE-VERIFIED: a Bisnow summary table read as though Jamestown were "
    "the buyer. It is not — Jamestown is the disposing party, having paid ~$103M "
    "in 2019, and the buyer is an affiliate of locally based Mai Luo, trading as "
    "Kendall Capital, which has a track record of office-to-apartment conversions "
    "(320 Summer Street, 145 apartments approved May). Institution for Savings "
    "provided $20.6M toward the purchase. "
    "DATE moved from 12 June to 9 June 2026, the only date any source states."
)

BEACON_11 = dict(
    address="11 Beacon Street", city="Boston",
    transaction_type="partial_interest",
    date="2026-03-01", precision="month",
    price=None, implied_valuation=23_000_000, pct_acquired=None,
    is_recapitalization=True,
    buyer=None, seller=None,
    building_sf=146_000, property_type="Office",
    source="press", source_name="Bisnow Boston Deal Sheet",
    source_url="https://www.bisnow.com/news/boston/deal-sheet/synergy-recapitalizes-beacon-hill-office-building-23m-boston-deal-sheet-133566",
    source_date="2026-03-09",
    passage='"Synergy Recapitalizes Beacon Hill Office Building For $23M." Synergy '
            'CEO David Greaney: "Synergy has owned 11 Beacon for more than 13 years, '
            'and we are pleased to recapitalize the asset while continuing our '
            'long-term ownership." 14-story, 146,000 SF, built 1922. Synergy '
            'acquired it in 2013 for $35M from DivcoWest and recapitalized in 2016 '
            'at $63M with GreenOak Real Estate as partner.',
    notes="NO PRICE AND NO PERCENTAGE RECORDED, deliberately. The source states a "
          "recapitalization amount, not a sum paid for a stake and not the size of "
          "the stake. $23M over 146,000 SF is $157/SF, consistent with a "
          "whole-asset capitalization rather than a cheque, and the same source "
          "describes the 2016 round as a recapitalization 'at $63M' — valuation "
          "phrasing, not price phrasing. Recording $23M as a price would credit "
          "Synergy with buying an asset it already owned and continues to own. "
          "INCOMING PARTNER NOT NAMED: the 2016 round names GreenOak; the 2026 "
          "round names no counterparty, which is the characteristic gap in "
          "press-sourced recapitalizations.",
    price_caveat="$23M is a stated recapitalization value, not a price paid; no "
                 "percentage interest disclosed.",
)


def main(dry_run: bool):
    conn = engine.connect()

    # ── 18 Tremont: price, date, buyer name ─────────────────────────
    row = conn.execute(text(
        "select id, price, price_per_sf, sale_date, building_sf from transactions "
        "where address like '%18 Tremont%'")).first()
    if row:
        tid, old_price, old_ppsf, old_date, sf = row
        new_price, new_ppsf = 29_500_000, round(29_500_000 / sf, 2) if sf else None
        log.info("18 Tremont  price %s -> %s   $/SF %s -> %s   date %s -> 2026-06-09",
                 f"${old_price:,}", f"${new_price:,}", old_ppsf, new_ppsf, old_date)
        if not dry_run:
            conn.execute(text("""
                update transactions set price = :p, price_per_sf = :ppsf,
                       sale_date = '2026-06-09', excise_implied_price = :ex,
                       buyer = 'Kendall Capital (Mai Luo affiliate)',
                       notes = coalesce(notes || ' | ', '') || :n
                where id = :id"""),
                {"p": new_price, "ppsf": new_ppsf,
                 "ex": round(new_price / 1000.0 * 4.56, 2),
                 "n": TREMONT_NOTE, "id": tid})
    else:
        log.warning("18 Tremont row not found")

    # ── 11 Beacon: the recapitalization ─────────────────────────────
    d = BEACON_11
    exists = conn.execute(text(
        "select id from transactions where address = :a and sale_date = :d"),
        {"a": d["address"], "d": d["date"]}).first()
    if exists:
        log.info("11 Beacon already present")
    else:
        log.info("11 Beacon Street  partial_interest  implied $%s, price NULL",
                 f'{d["implied_valuation"]:,}')
        if not dry_run:
            conn.execute(text("""
                insert into transactions
                  (address, city, transaction_type, sale_date, sale_date_precision,
                   price, price_caveat, implied_valuation, pct_acquired,
                   is_recapitalization, buyer, seller, property_type, building_sf,
                   source, source_url, source_name, source_date, passage,
                   confidence, notes, created_at)
                values
                  (:a, :city, :t, :d, :prec, null, :caveat, :implied, :pct,
                   :recap, :buyer, :seller, :ptype, :sf,
                   :src, :url, :sname, :sdate, :passage, 'web_low_confidence',
                   :notes, :now)"""), {
                "a": d["address"], "city": d["city"], "t": d["transaction_type"],
                "d": d["date"], "prec": d["precision"], "caveat": d["price_caveat"],
                "implied": d["implied_valuation"], "pct": d["pct_acquired"],
                "recap": d["is_recapitalization"], "buyer": d["buyer"],
                "seller": d["seller"], "ptype": d["property_type"],
                "sf": d["building_sf"], "src": d["source"], "url": d["source_url"],
                "sname": d["source_name"], "sdate": d["source_date"],
                "passage": d["passage"], "notes": d["notes"],
                "now": datetime.utcnow(),
            })

    if not dry_run:
        conn.commit()
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    main(dry_run=not a.apply)
