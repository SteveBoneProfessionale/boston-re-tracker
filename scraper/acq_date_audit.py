r"""Audit every press-sourced date, and record what each one actually is.

Publication date substituting for closing date has now bitten repeatedly, most
recently on 294-302 Windsor Street, where NEREJ's 12 June publication date sat
in the table as the close for a deal that actually closed 10 February -- four
months out, and on the wrong side of a listing history showing 203 days on
market.

Auditing all 38 press and SEC rows: 28 carry a date that is either equal to the
publication date, or is month-precision, or is not tied to any stated closing
date in the source. The single worst case is ONE MARINA PARK DRIVE, the largest
transaction in the table at $435M, whose date is the publication date recorded
at DAY precision -- which asserts a specific day nobody reported.

So `sale_date_basis` is now recorded on every row, with four values:

    stated_close      the source names a closing date ("the deal closed on
                      July 8", "closed Wednesday")
    deed_record       the source cites a recorded deed or land record and its
                      date ("according to a Feb. 20 deed")
    filing            an SEC disposition schedule, which states the day
    publication_proxy no close date is given anywhere; the date shown is when
                      somebody wrote about it

A publication_proxy date at DAY precision is a false assertion, so those are
widened to month. The row keeps a usable date for sorting and stops claiming a
day it does not know.

WHAT THIS DOES NOT DO: it does not find the real dates. Each one needs a deed
record or an MLS entry, as Windsor did. It marks which rows need that work, and
which rows can be trusted as they stand.

    python scraper/acq_date_audit.py --apply
"""

import argparse
import logging
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from db.database import engine

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

# Phrases that indicate the source stated a closing or recording date.
STATED_CLOSE = re.compile(
    r"\b(closed on|the deal closed|closing on|sale closed|closed \w+day|"
    r"date disposed|completed the sale on)\b", re.I)
DEED_RECORD = re.compile(
    r"\b(according to (a|an) [A-Z][a-z]+\.? \d|per a [A-Z][a-z]+\.? \d|"
    r"[A-Z][a-z]+\.? \d{1,2},? \d{4} deed|deed record|deed records show|"
    r"was listed on [A-Z][a-z]+\.? \d|MLS \d)", re.I)


MONTHS = {m: i + 1 for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july", "august",
     "september", "october", "november", "december"])}
MONTHS.update({m[:3]: i + 1 for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july", "august",
     "september", "october", "november", "december"])})

DATE_IN_TEXT = re.compile(
    r"(?:(\d{1,2})\s+([A-Za-z]{3,9})|([A-Za-z]{3,9})\.?\s+(\d{1,2}))(?:,)?\s+(\d{4})")


def states_the_date(blob: str, sale_date: str) -> bool:
    """True when the source text contains a date equal to the stored sale date.

    An earlier version looked only for phrases like "closed on". That missed
    the commonest construction in this corpus -- "acquired ... for $28M ON 2
    JULY 2026" -- and so flagged rows as publication proxies when the source
    states the day plainly. Over-flagging is its own inaccuracy: it would send
    someone to re-research a date that was already sourced.
    """
    want = str(sale_date)[:10]
    for m in DATE_IN_TEXT.finditer(blob):
        d1, m1, m2, d2, yr = m.groups()
        mon = MONTHS.get((m1 or m2 or "").lower()[:3])
        day = int(d1 or d2 or 0)
        if not mon or not day:
            continue
        if f"{yr}-{mon:02d}-{day:02d}" == want:
            return True
    return False


def main(dry_run: bool):
    conn = engine.connect()
    try:
        conn.execute(text("select sale_date_basis from transactions limit 1"))
    except Exception:
        if not dry_run:
            conn.execute(text(
                "alter table transactions add column sale_date_basis VARCHAR"))
            conn.commit()
        log.info("added column sale_date_basis")

    rows = conn.execute(text(
        "select id, address, sale_date, sale_date_precision, source, source_date, "
        "coalesce(passage,''), coalesce(notes,'') from transactions "
        "where source in ('press','sec_filing') order by sale_date")).fetchall()

    counts, widened = {}, 0
    for rid, addr, sd, prec, src, srcdate, passage, notes in rows:
        blob = passage + " " + notes
        if src == "sec_filing":
            basis = "filing"
        elif DEED_RECORD.search(blob):
            basis = "deed_record"
        elif STATED_CLOSE.search(blob) or states_the_date(blob, sd):
            basis = "stated_close"
        else:
            basis = "publication_proxy"
        counts[basis] = counts.get(basis, 0) + 1

        new_prec = prec
        note = None
        if basis == "publication_proxy":
            if str(sd)[:10] == (srcdate or "")[:10]:
                note = ("DATE IS A PUBLICATION PROXY. No source states a closing "
                        "or recording date for this transaction; the date shown "
                        "equals the date the item was published. Publication "
                        "date has substituted for close date repeatedly in this "
                        "table -- 294-302 Windsor was four months out that way. "
                        "Treat as the month of report, not the day of sale.")
            else:
                note = ("DATE IS A PUBLICATION PROXY. No source states a closing "
                        "or recording date; the date shown was inferred from the "
                        "reporting window. Treat as approximate.")
            if prec == "day":
                new_prec = "month"
                widened += 1
                note += (" PRECISION WIDENED from day to month: a proxy date at "
                         "day precision asserts a specific day nobody reported.")

        if not dry_run:
            params = {"b": basis, "id": rid}
            sql = "update transactions set sale_date_basis = :b"
            if new_prec != prec:
                sql += ", sale_date_precision = :p"
                params["p"] = new_prec
            if note:
                sql += ", notes = coalesce(notes,'') || :n"
                params["n"] = " | " + note
            conn.execute(text(sql + " where id = :id"), params)

    if not dry_run:
        # Spine rows carry the assessment roll's own recorded sale date.
        conn.execute(text(
            "update transactions set sale_date_basis = 'deed_record' "
            "where source in ('massgis_l3','cambridge_socrata') "
            "and coalesce(sale_date_basis,'') = ''"))
        conn.commit()

    log.info("press/SEC date basis: %s", counts)
    log.info("%d day-precision proxy dates widened to month", widened)

    log.info("\nrows whose date is a publication proxy (need a deed or MLS date):")
    for rid, addr, sd, price in conn.execute(text(
            "select id, address, sale_date, coalesce(price,0) from transactions "
            "where sale_date_basis = 'publication_proxy' "
            "order by price desc limit 12")):
        log.info("  id=%-4s %s  $%-12s %s", rid, str(sd)[:10], f"{price:,}", addr[:38])
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
