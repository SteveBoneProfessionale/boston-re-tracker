r"""26 Hichborn Street, and three corrections to 294-302 Windsor Street.

26 HICHBORN STREET, BRIGHTON. The ARX row carried no address through four
resolution passes because I read only the rendered article text. The address was
in the page's <head> the entire time -- twitter:alt "26 Hichborn St., Boston"
and an og:image named 26HICHBORNsT.jpg. See scraper/paywall_meta.py; that method
now recovers an address from 62 of 85 cached Real Reporter articles.

THE SELLER STAYS NULL, and PhilMor is stored as a LEAD. PhilMor Real Estate
Investments announced acquiring 26 Hichborn for $14M in April 2021 and is the
recorded owner since. Horvath & Tremblay appear on both this trade and the 2021
one. That makes PhilMor the presumptive seller and does not make it the seller:
no source names a seller on the 2026 trade, and a five-year-old acquisition
release is not evidence about who conveyed in 2026.

PRIOR TRADE. $14,000,000 in April 2021 against $11,900,000 now, on 23 units and
42,544 SF:

        2021   $608,696 / unit    $329 / SF
        2026   $517,391 / unit    $280 / SF     -15.0%

A 15% decline in nominal basis over five years on a building completed in 2019.
That is the kind of fact a single transaction cannot state and a paired one can.

ARX KNEW THE BLOCK. Arx Urban's Stadia is 46-52 Hichborn Street, 46 units, where
Arx "provided advisory services to the sponsor, HICHBORN PARTNERS LLC, arranging
preferred equity for the project and investing alongside the sponsor" from 2019.
That is the building NEXT DOOR, not this one -- Arx had no position in 26
Hichborn itself, so this is not a buy-out of its own paper. Hichborn Partners
LLC is recorded as a related party at 46-52.

── 294-302 WINDSOR STREET, three corrections ───────────────────────────

DATE. Recorded as 12 June 2026. That is NEREJ's publication date. MLS 73375429
records the close on 10 FEBRUARY 2026 at $4,920,000, with a listing history of
$5.5M on 15 May 2025, cut to $5.3M on 29 September, pending 4 December, closed
10 February: 203 days on market and 11% below the original ask.

BROKERS, BOTH SIDES. I recorded Horvath & Tremblay and wrote in a note that the
article said H&T "sourced the buyer" without naming them. It does not say that.
I grepped the whole page and matched a sentence from a DIFFERENT article in the
related-items block -- the Jason Arms deal in Arlington, where H&T did source
both sides. The Windsor article says only "exclusively represented the seller",
and H&T use the longer phrase when it applies. Its absence is the signal: there
was a buy-side broker, and it was Keystone Homes Group at Keller Williams Realty
Boston Northwest. Listing side was the Kelleher-Pentore Team at H&T.

A RESIDENTIAL KW TEAM ON THE BUY SIDE OF A $4.92M MIXED-USE DEAL is itself
evidence about the buyer: private investor or small local operator, which is
exactly why no trade publication names them. Where the buy-side broker is
residential, stop spending searches on trade press.

FINANCIALS, STORED AS COMPONENTS AND NOT AS A CAP RATE. MLS gives $31,782
monthly rent, $58,010 opex and NOI of $326,974, which against $4.92M implies
6.6%. That opex is 15% of gross while the FY25 tax bill alone is $26,679, so the
figure appears to exclude taxes and the implied cap is therefore overstated.
Recorded as broker-stated and unverified.

    python scraper/acq_hichborn_windsor.py --apply
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from db.database import engine

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

HICHBORN_NOTE = (
    " | ADDRESS RECOVERED FROM PAGE METADATA, not from the article text. The "
    "Real Reporter gates the body but not the <head>: "
    '<meta property="twitter:alt" content="26 Hichborn St., Boston"> and an '
    "og:image named 26HICHBORNsT.jpg. This row sat with no address through four "
    "resolution passes because I only ever read rendered text. "
    "PROPERTY: 23 units, 42,544 SF, five storeys, completed 2019, adjacent to "
    "Boston Landing in Brighton. "
    "SELLER DELIBERATELY NULL, WITH A LEAD RECORDED. PhilMor Real Estate "
    "Investments announced acquiring 26 Hichborn for $14M in an April 2021 "
    "release and is the recorded owner since; Horvath & Tremblay appear on both "
    "that trade and this one. That makes PhilMor the PRESUMPTIVE seller and not "
    "the seller: no source names a seller on the 2026 transaction, and a "
    "five-year-old acquisition release is not evidence about who conveyed in "
    "2026. "
    "PRIOR TRADE: $14,000,000, April 2021 -- $608,696/unit and $329/SF against "
    "$517,391/unit and $280/SF here, a 15.0% decline in nominal basis over five "
    "years on a building completed in 2019. "
    "RELATED PARTY: Arx Urban's Stadia is 46-52 Hichborn Street, 46 units, where "
    "Arx 'provided advisory services to the sponsor, Hichborn Partners LLC, "
    "arranging preferred equity for the project and investing alongside the "
    "sponsor' from 2019. That is the building next door; Arx held no position in "
    "26 Hichborn itself, so this is not a buy-out of its own paper."
)

WINDSOR_NOTE = (
    " | DATE CORRECTED, 2026-06-12 -> 2026-02-10. The old value was NEREJ's "
    "PUBLICATION date, not the close. MLS 73375429 records the closing on 10 "
    "February 2026 at $4,920,000. Listing history: $5,500,000 on 15 May 2025, "
    "reduced to $5,300,000 on 29 September, pending 4 December, closed 10 "
    "February -- 203 days on market, 11% below the original ask. "
    "BROKERS CORRECTED ON BOTH SIDES. I previously recorded only Horvath & "
    "Tremblay and noted that the article said H&T 'sourced the buyer' without "
    "naming them. IT DOES NOT SAY THAT. I grepped the whole page and matched a "
    "sentence belonging to a DIFFERENT article in the related-items block -- the "
    "Jason Arms deal in Arlington, where H&T did work both sides. The Windsor "
    "text says only 'exclusively represented the seller'. H&T use the longer "
    "phrase when it applies, so its absence indicates a separate buy-side "
    "broker, and there was one: Keystone Homes Group at Keller Williams Realty "
    "Boston Northwest, Concord office. Listing side was the Kelleher-Pentore "
    "Team at Horvath & Tremblay. "
    "WHAT THE BUY-SIDE BROKER IMPLIES: a residential KW team on the buy side of "
    "a $4.92M mixed-use deal points to a private investor or small local "
    "operator, which is why the buyer never appears in trade press and why "
    "further trade-press searching on this row is wasted effort. "
    "FINANCIALS, BROKER-STATED AND UNVERIFIED: MLS reports $31,782 monthly rent "
    "($381,384 annualised), $58,010 operating expenses and NOI of $326,974, "
    "which against $4,920,000 implies a 6.6% cap. THAT CAP IS NOT LOADED AS A "
    "FACT. The stated opex is 15% of gross while the FY25 tax bill alone is "
    "$26,679, so the figure appears to exclude real estate taxes and the implied "
    "cap is overstated by an unknown margin."
)


def main(dry_run: bool):
    conn = engine.connect()

    # ── 26 Hichborn ─────────────────────────────────────────────────
    row = conn.execute(text(
        "select id, price from transactions where buyer_canonical = 'Arx Urban'"
    )).first()
    if row:
        rid, price = row
        units, sf = 23, 42_544
        prior = 14_000_000
        change = round((price - prior) / prior * 100, 1)
        log.info("26 Hichborn: %d units, %s SF, $%s/unit, $%s/SF, prior $%s (%+.1f%%)",
                 units, f"{sf:,}", f"{price//units:,}", f"{price//sf:,}",
                 f"{prior:,}", change)
        if not dry_run:
            conn.execute(text("""
                update transactions
                   set address = '26 Hichborn Street',
                       property_type = 'Multifamily',
                       unit_count = :u, building_sf = :sf,
                       price_per_unit = :ppu, price_per_sf = :ppsf,
                       prior_sale_date = '2021-04-01',
                       prior_sale_price = :prior,
                       prior_sale_source = 'PhilMor Real Estate Investments release, 30 April 2021',
                       basis_change_pct = :chg,
                       notes = coalesce(notes,'') || :n
                 where id = :id"""), {
                "u": units, "sf": sf,
                "ppu": round(price / units, 2), "ppsf": round(price / sf, 2),
                "prior": prior, "chg": change, "n": HICHBORN_NOTE, "id": rid})

    # ── 294-302 Windsor ─────────────────────────────────────────────
    w = conn.execute(text(
        "select id from transactions where address like '%294-302 Windsor%'")).first()
    if w:
        log.info("294-302 Windsor: date -> 2026-02-10, brokers both sides, "
                 "financials as components")
        if not dry_run:
            conn.execute(text("""
                update transactions
                   set sale_date = '2026-02-10', sale_date_precision = 'day',
                       broker = 'Horvath & Tremblay (Kelleher-Pentore Team)',
                       broker_buy_side = 'Keystone Homes Group, Keller Williams Realty Boston Northwest',
                       source_name = 'New England Real Estate Journal; MLS 73375429',
                       notes = coalesce(notes,'') || :n
                 where id = :id"""), {"n": WINDSOR_NOTE, "id": w[0]})

    if not dry_run:
        conn.commit()

    for r in conn.execute(text(
            "select address, sale_date, price, unit_count, building_sf, "
            "price_per_unit, price_per_sf, prior_sale_price, basis_change_pct, "
            "broker, broker_buy_side from transactions "
            "where address like '%Hichborn%' or address like '%294-302 Windsor%'")):
        log.info("\n  %s  %s  $%s", r[0], r[1], f"{r[2]:,}")
        log.info("     %s units, %s SF, $%s/unit, $%s/SF", r[3], f"{r[4]:,}" if r[4] else "-",
                 f"{r[5]:,.0f}" if r[5] else "-", f"{r[6]:,.0f}" if r[6] else "-")
        if r[7]:
            log.info("     prior $%s  basis %+.1f%%", f"{r[7]:,}", r[8])
        log.info("     listing broker: %s", r[9] or "-")
        log.info("     buy-side broker: %s", r[10] or "-")
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
