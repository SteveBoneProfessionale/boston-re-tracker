r"""Decode the five named fund prefixes, and their siblings.

Each was one search from a nine-figure row. Decoded:

    HART   28 State Street        Heitman America Real Estate Trust
    SPUS7  100 High Street        CBRE Global Investors (Strategic Partners US)
    FSP    Pier 4                 CommonWealth Partners' vehicle
    LS     Cambridge Discovery    Healthpeak Properties
    AP     Cambridge Discovery    Acorn Park, the Bulfinch/Harrison Street JV

THE SIBLING EFFECT IS THE POINT. SPUS7 was listed as one $370M row. It is
actually THREE: 100 High Street at $370M, 125 Cambridgepark Drive at $90.3M and
150 Cambridgepark Drive at $119.7M, all CBRE Global Investors on the sell side.
One lookup, $580M of coverage. That is the same effect CSP had when it brought
four IQHQ vehicles with it, and it is why every decoded prefix is folded back
into the pattern table rather than written as a one-off.

JOINT VENTURES RESOLVE TO ALL PARTNERS. 28 State Street was sold by Rockefeller
Group U.S. Premier Office Fund together with Mitsubishi Estate New York, and
Cambridge Discovery Park by Harrison Street with The Bulfinch Cos. and National
Real Estate Advisors. Both are recorded whole.

ONE PREFIX IS DELIBERATELY LEFT UNRESOLVED. IREP NEWBURY HOTEL LLC bought 15
Arlington Street -- the Taj Boston, now The Newbury -- for $196M on 3 April
2018. The consortium that owns the hotel is well documented (New England
Development, Eastern Real Estate, Rockpoint, Lubert-Adler, Highgate) but it
bought the hotel in JULY 2016 for $125M. The April 2018 event is reported as a
recapitalisation at $203M, which is a different figure from the $196M deed and a
different kind of event from a purchase. Who acquired what interest from whom is
not established by anything I can read, so both sides stay null. IREP itself
remains undecoded.

    python scraper/acq_resolve_prefixes.py --apply
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

RESOLUTIONS = [
    ("HART 28 STATE", "buyer", "Heitman",
     'CoStar, "28 State St. Tower Trades in Largest Boston Office Sale This '
     'Year", and Institutional Real Estate Inc. both record Heitman as the '
     'acquirer of the 40-storey Financial District tower in June 2018 at ~$418M. '
     'The entity prefix HART is Heitman America Real Estate Trust, the fund '
     'through which it holds. Matches the row on address, date and price.'),
    ("TWENTY-8 STATE STREET", "seller",
     "Rockefeller Group U.S. Premier Office Fund / Mitsubishi Estate New York",
     'Institutional Real Estate Inc.: "Rockefeller Group, Mitsubishi Estate New '
     'York sell 28 State Street in Boston." Business Wire and Boston Real Estate '
     'Times: "TA Realty Arranges ~$418 Million Sale of 28 State Street in '
     'Boston" on behalf of a joint venture between Rockefeller Group U.S. '
     'Premier Office Fund and Mitsubishi Estate New York. Recorded as the '
     'venture, not one partner.'),

    ("SPUS7", "seller", "CBRE Global Investors",
     'The Real Reporter: "CBRE Global Reaping $370M-Plus in Sale of Hub\'s 100 '
     'High St." CBRE Global Investors had bought the 28-storey tower for $282M '
     'roughly two years earlier. SPUS is CBRE Global Investors\' Strategic '
     'Partners U.S. fund series. THIS PREFIX APPEARS ON THREE ROWS, not one: 100 '
     'High Street ($370M), 125 Cambridgepark Drive ($90.3M) and 150 '
     'Cambridgepark Drive ($119.7M), all on the sell side.'),
    ("100 HIGH OWNER", "buyer", "Rockpoint Group",
     'The Real Reporter and CoStar: Rockpoint Group committed roughly $370 '
     'million for the 546,300 SF building, closing April 2017. Matches the row '
     'on address, date and price.'),

    ("FSP-PIER 4", "buyer", "CommonWealth Partners",
     'Connect CRE: "CommonWealth Claims Boston Seaport Building for Record $450M '
     'from Tishman Speyer." Corroborated by Institutional Real Estate Inc. and '
     'REBusinessOnline, both dating it August 2018 at $450M. The FSP prefix '
     'itself is not decoded; the sponsor is established from the transaction, '
     'not from the name.'),
    ("130 NORTHERN AVENUE LLC", "seller", "Tishman Speyer",
     'Same reporting: Tishman Speyer developed and sold the Pier 4 office '
     'building. The Boston Globe, 23 August 2018: "Seaport building sells for '
     'possibly record price." Note the press gives the address as 140 Northern '
     'Avenue / 200 Pier Four while the assessment roll carries 130 Northern '
     'Avenue; the date and the $450M price identify the transaction.'),

    ("LS 400/500 CDP", "buyer", "Healthpeak Properties",
     'Bisnow: "Bulfinch, Harrison Street Sell Cambridge Discovery Park For '
     '$720M" -- the buyer was Healthpeak Properties, closing December 2020 on '
     'the 620K SF three-building Acorn Park Drive campus. Healthpeak lists '
     'Cambridge Discovery Park in its own portfolio. This row is one parcel\'s '
     'allocation of that campus sale.'),
    ("AP CAMBRIDGE PARTNERS", "seller",
     "Harrison Street / The Bulfinch Cos. / National Real Estate Advisors",
     'Bisnow and Bulfinch\'s own release name the sellers as Harrison Street, '
     'Bulfinch Cos. and National Real Estate Advisors, with Bulfinch retaining a '
     'stake and continuing to manage. AP is Acorn Park, the JV\'s vehicle name. '
     'Recorded as the venture, not one partner.'),
]


def main(dry_run: bool):
    conn = engine.connect()
    n = 0
    for key, side, sponsor, passage in RESOLUTIONS:
        rows = conn.execute(text(
            f"select id, {side} from transactions "
            f"where upper(coalesce({side},'')) like :k "
            f"and coalesce({side}_canonical,'') = ''"), {"k": f"%{key}%"}).fetchall()
        if not rows:
            log.warning("no unresolved %s row matching %r", side, key)
            continue
        log.info("%-26s %-6s -> %-52s (%d rows)", key[:26], side, sponsor[:52],
                 len(rows))
        if not dry_run:
            for rid, _nm in rows:
                conn.execute(text(f"""
                    update transactions
                       set {side}_canonical = :s,
                           {side}_confidence = 'web_corroborated',
                           {side}_resolution_basis = 'web',
                           notes = coalesce(notes,'') || :n
                     where id = :id"""), {
                    "s": sponsor, "id": rid,
                    "n": (f" | {side.upper()} ENTITY RESOLVED TO SPONSOR. Record "
                          f"entity kept verbatim in `{side}`. Evidence: " + passage)})
                n += 1
    if not dry_run:
        conn.commit()
    tot = conn.execute(text("select count(*) from transactions")).scalar()
    log.info("\n%d rows resolved", n)
    for side in ("buyer", "seller"):
        v, d = conn.execute(text(
            f"select count(*), sum(coalesce(price,0)) from transactions "
            f"where coalesce({side}_canonical,'') <> ''")).first()
        log.info("%s_canonical: %d of %d rows, $%.2fB", side, v, tot,
                 (d or 0) / 1e9)
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
