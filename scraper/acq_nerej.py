r"""The single 2026 Boston/Cambridge transaction NEREJ yielded, and why only one.

NEREJ carries exactly the mid-market deals the rest of the press record misses
-- eight-unit mixed-use buildings, neighbourhood retail, sub-$10M apartment
trades. It is also the worst undated-press hazard encountered anywhere in this
work, for two compounding reasons:

  Its URLs are flat and undated, and it has no chronological archive that can be
  paged the way Bisnow's section index can. So it cannot be read systematically;
  it can only be searched, which is the method that has already been shown to
  miss things.

  Its archive spans well over a decade with no date in the search result. Of the
  five candidates a single search returned, FOUR were out of window:

      1280 Massachusetts Ave, $45.15M      January 2018
      One Bowdoin Square, $28M             August 2025
      Cambridge Oxford Apartments, $22.25M May 2016
      727 Massachusetts Ave, $14M          February 2017
      294-302 Windsor Street, $4.92M       JUNE 2026  <- the only live one

An 80% false-positive rate. Every one of those four would have loaded as a 2026
transaction on the strength of the search result alone, and three of them are
large enough to have visibly distorted the bands.

    python scraper/acq_nerej.py --apply
"""
import argparse, logging, sys
from datetime import datetime
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from sqlalchemy import text
from db.database import engine

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

URL = "https://nerej.com/kelleher-and-pentore-of-horvath-tremblay-handle-492m-sale"
PASSAGE = (
    'NEREJ, 12 June 2026: "Kelleher and Pentore of Horvath & Tremblay handle '
    '$4.92 million sale of at 294-302 Windsor St. in Cambridge... have completed '
    'the sale of eight units consisting of seven residential apartments and one '
    'commercial space... The asset sold for $4.92 million." Two buildings, eight '
    'units, 7,765 SF of living space in 11,343 SF gross on a 0.15-acre parcel in '
    'The Port; the commercial unit houses Gypsy Cafe & Bar.'
)
NOTE = ("BUYER AND SELLER NOT NAMED — the report is a brokerage announcement and "
        "names only Horvath & Tremblay as exclusive agent for the seller. "
        "Included because it is one of only three sub-$10M Boston/Cambridge "
        "trades the entire archive read produced, and the shape of that band is "
        "the question the read was meant to answer.")


def main(dry_run: bool):
    conn = engine.connect()
    exists = conn.execute(text(
        "select id from transactions where address like '%294-302 Windsor%'")).first()
    if exists:
        log.info("already present")
    else:
        log.info("294-302 Windsor Street  Cambridge  $4,920,000")
        if not dry_run:
            conn.execute(text("""
                insert into transactions
                  (address, city, transaction_type, sale_date, sale_date_precision,
                   price, buyer, seller, broker, property_type, building_sf,
                   unit_count, price_per_sf, price_per_unit, excise_implied_price,
                   source, source_url, source_name, source_date, passage,
                   confidence, notes, created_at)
                values
                  (:a, 'Cambridge', 'asset_sale', '2026-06-12', 'day', :p, null,
                   null, 'Horvath & Tremblay', 'Mixed-use', 11343, 8, :ppsf, :ppu,
                   :ex, 'press', :url, 'New England Real Estate Journal',
                   '2026-06-12', :passage, 'web_low_confidence', :note, :now)"""), {
                "a": "294-302 Windsor Street", "p": 4_920_000,
                "ppsf": round(4_920_000 / 11343, 2),
                "ppu": round(4_920_000 / 8, 2),
                "ex": round(4_920_000 / 1000.0 * 4.56, 2),
                "url": URL, "passage": PASSAGE, "note": NOTE,
                "now": datetime.utcnow()})

    # 18 Tremont: record the unresolved direction conflict rather than hide it.
    row = conn.execute(text(
        "select id, notes from transactions where address like '%18 Tremont%'")).first()
    if row and "BISNOW REVERSES" not in (row[1] or ""):
        add = (" | BISNOW REVERSES THE PARTIES AND THE CONFLICT IS UNRESOLVED. "
               "Bisnow's 16 June 2026 deal sheet reads \"Jamestown acquired an "
               "11-story office building at 18 Tremont St. for $29.5M... with "
               "seller Mai Luo having bought it for $103M in 2019\", making "
               "Jamestown the BUYER. CoStar's headline is \"Jamestown disposes of "
               "Boston office building at steep discount to 2019 price\", and "
               "CommercialSearch reports \"Kendall Capital bought 18 Tremont St... "
               "paying 71% less than the roughly $103 million that Jamestown laid "
               "out for the tower back in 2019\", making Jamestown the SELLER. Two "
               "independent sources against one, and the 2019 purchaser is the "
               "pivot: whoever paid $103M in 2019 is the party selling now. "
               "Jamestown is stored as seller. If a deed feed ever arrives this is "
               "the first row to check.")
        if not dry_run:
            conn.execute(text(
                "update transactions set notes = coalesce(notes,'') || :n "
                "where id = :id"), {"n": add, "id": row[0]})
        log.info("18 Tremont: source conflict recorded")

    if not dry_run:
        conn.commit()
    n, v = conn.execute(text(
        "select count(*), sum(coalesce(price,0)) from transactions "
        "where sale_date >= '2026-01-01'")).first()
    conn.close()
    log.info("2026 now: %d transactions, $%s", n, f"{int(v or 0):,}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
