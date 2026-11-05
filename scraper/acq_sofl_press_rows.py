r"""Deals the county file does not carry, added from trade press.

    python -m scraper.acq_sofl_press_rows --dry-run
    python -m scraper.acq_sofl_press_rows

ONE ROW, AND IT IS MARKED AS WHAT IT IS. Every other South Florida row is a
recorded deed read from a county appraiser. This one is not: Commercial
Observer reports Starwood Asset Management buying Lafayette Square at 150 NE
79th Street for $29.75 million from the Gatehouse Group, and the Miami-Dade
sales file contains no 2026 sale at that address at any price above the floor.
It is a real transaction the registry has not yet published -- the same
recording lag that put the two neighbouring Gatehouse deeds into the file four
months after they were signed.

So it is written with source "press", not "miamidade_pa". That matters in three
visible places: the row carries the ◇ PRESS mark rather than ▥ DEED on the face
of the table, the coverage note's claim that every South Florida row is a
recorded deed stays true of the rest, and nobody can mistake a reported price
for a recorded consideration.

WHAT IS LEFT EMPTY IS AS DELIBERATE AS WHAT IS FILLED. There is no deed book,
no page, no folio, no grantor or grantee entity, because no instrument has been
read. The date is the article's, flagged as a publication proxy, because the
article gives no closing date. Nothing here is interpolated from the
neighbouring deals.
"""

import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from sqlalchemy import text                           # noqa: E402

from db.database import engine, init_db               # noqa: E402

URL = ("https://commercialobserver.com/2026/08/"
       "barry-sternlicht-starwood-miami-affordable-housing/")

ROWS = [
    {
        "address": "150 NE 79th Street",
        "city": "Miami",
        "transaction_type": "asset_sale",
        # The article carries no closing date, so its publication date stands
        # in and says so. Precision is "month" for the same reason.
        "sale_date": "2026-08-13",
        "sale_date_precision": "month",
        "sale_date_basis": "publication_proxy",
        "price": 29_750_000,
        "buyer": "Starwood Asset Management",
        "buyer_canonical": "Starwood Asset Management",
        "buyer_confidence": "press_reported",
        "buyer_resolution_basis": (
            "Commercial Observer on this deal: “Barry Sternlicht's Starwood "
            "Asset Management bought Lafayette Square, a 160-unit affordable "
            "housing property at 150 Northeast 79th Street, for $29.75 "
            "million from the Gatehouse Group.”"),
        "buyer_resolution_url": URL,
        "seller": "Gatehouse Group",
        "seller_canonical": "Gatehouse Group",
        "seller_confidence": "press_reported",
        "seller_resolution_basis": (
            "Commercial Observer on this deal names the Gatehouse Group as "
            "the seller."),
        "seller_resolution_url": URL,
        "property_type": "Multifamily",
        "asset_class": "Multifamily",
        "unit_count": 160,
        "price_per_unit": round(29_750_000 / 160, 2),
        "arms_length": 1,
        "quarantined": 0,
        "source": "press",
        "source_name": "Commercial Observer",
        "source_url": URL,
        "source_date": "2026-08-13",
        "confidence": "press_reported",
        "submarket": "Little River",
        "notes": (
            "NOT A COUNTY RECORD. Commercial Observer reports this sale "
            "citing property records, but the Miami-Dade appraiser's sales "
            "file carries no 2026 sale at this address above the $5M floor, "
            "so no deed could be read for it. Price, date and parties are the "
            "publication's. Deed book, page, folio and the grantor and "
            "grantee entities are empty because no instrument was read, and "
            "the date is the article's publication date rather than a "
            "closing. The two neighbouring Gatehouse sales reached the "
            "registry four months after signing, which is the likeliest "
            "reason this one has not appeared yet."),
    },
]

KEY = "select id from transactions where address = :address and city = :city " \
      "and price = :price"


def write(dry_run=False, verbose=True):
    init_db()
    inserted = updated = 0
    with engine.begin() as conn:
        for row in ROWS:
            found = conn.execute(text(KEY), {
                "address": row["address"], "city": row["city"],
                "price": row["price"]}).first()
            if found:
                sets = ", ".join(f"{k} = :{k}" for k in row)
                payload = dict(row, _id=found[0])
                if not dry_run:
                    conn.execute(text(f"update transactions set {sets} "
                                      f"where id = :_id"), payload)
                updated += 1
            else:
                cols = ", ".join(row)
                vals = ", ".join(f":{k}" for k in row)
                if not dry_run:
                    conn.execute(text(f"insert into transactions ({cols}) "
                                      f"values ({vals})"), row)
                inserted += 1
        if dry_run:
            conn.rollback()
    if verbose:
        print(f"{'(dry run) ' if dry_run else ''}{inserted} inserted, "
              f"{updated} updated")
    return inserted, updated


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    write(**vars(ap.parse_args()))
