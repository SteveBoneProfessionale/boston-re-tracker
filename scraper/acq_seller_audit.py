r"""Audit the press rows that carry no seller, and fix the one that should.

A blended "4% of rows have a seller" was hiding two unrelated facts. Split by
source it reads: press and SEC rows 79%, assessment-spine rows 0%. The spine
figure is structural -- an assessment roll records who owns a parcel now, never
the grantor -- and no work on that source changes it. The press figure is the
one worth auditing, so all eight sellerless press rows were re-read against the
cached article text.

ONE WAS AN EXTRACTION MISS. 374 Congress Street: Bisnow states "The property was
part of a five-building portfolio acquired by Nuveen in 2016 for $225M", and The
Real Reporter frames the whole deal as "TIAA's uneven exit from a
410,000-square-foot Seaport office portfolio finalized in homegrown Eastern Real
Estate ... spending $28 million buying repurposed warehouse 374 Congress St."
The grantor is named, just not in the sentence carrying the price. Fixed.

SEVEN ARE GENUINELY UNNAMED, and the reasons differ enough to record rather than
leave as bare nulls:

  Dorchester Avenue portfolio  no seller EXISTS -- the owners bid to retain it
  11 Beacon Street             a recapitalisation; the exiting partner is not
                               named. GreenOak was the 2016 partner and is the
                               obvious candidate, which is exactly why it is not
                               being written in
  4-6 and 28 Newbury Street    Newmark "represented the seller" without naming
                               them. ASG Equities is described as doing "its
                               third deal on the street" but whether that makes
                               it the seller here or a partner in the buying JV
                               is not stated
  343 Congress Street          reported as an aside with no grantor
  31 Buttonwood / 14 Willis    no grantor reported
  294-302 Windsor Street       brokerage announcement, seller-side only
  ARX apartments               The Real Reporter is paywalled past the lead

    python scraper/acq_seller_audit.py --apply
"""
import argparse, logging, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from sqlalchemy import text
from db.database import engine

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

# Matched by ID, not by address LIKE. The first run of this script wrote the
# seller onto the WRONG row: "374 Congress" also matches a 2016 MassGIS record
# of Nuveen BUYING the five-building Fort Point portfolio for $224M, so naming
# Nuveen as its seller was exactly backwards. Two rows can share a street
# address and be opposite ends of the same story, which is an argument for
# resolving on id once a row is known.
FIX = dict(
    row_id=630,
    match="374 Congress",
    seller="Nuveen (TIAA)",
    note=(" | SELLER RECOVERED ON AUDIT. Bisnow, 14 July 2026: \"The property was "
          "part of a five-building portfolio acquired by Nuveen in 2016 for "
          "$225M.\" The Real Reporter, 6 July 2026, names the exiting principal: "
          "\"TIAA's uneven exit from a 410,000-square-foot Seaport office "
          "portfolio finalized in homegrown Eastern Real Estate kicking off "
          "mid-year 2026 spending $28 million buying repurposed warehouse 374 "
          "Congress St.\" Nuveen is TIAA's real estate manager, so the two "
          "reports name the same seller at different levels. This was an "
          "extraction miss, not a source gap: the grantor is stated, just not in "
          "the sentence that carries the price.")
)

REASONS = {
    "Dorchester Avenue warehouse": (
        "SELLER NULL BECAUSE NONE EXISTS. J.T. Magen & Co. and Extell "
        "Development bid $75M at auction to RETAIN a portfolio they already "
        "owned, so there is no grantor distinct from the grantee. This is the "
        "one sellerless row where null is the correct answer rather than a gap."),
    "11 Beacon": (
        "SELLER NOT NAMED. The transaction is a recapitalisation and the exiting "
        "partner is not identified by either source. GreenOak Real Estate was "
        "Synergy's partner in the 2016 round and is the obvious candidate, which "
        "is precisely why it is not written in: 'obvious' is not 'stated', and a "
        "counterparty guessed from a ten-year-old deal is how a plausible wrong "
        "answer enters a database."),
    "4-6 and 28 Newbury": (
        "SELLER NOT NAMED. Bisnow reports that \"Newmark's Robert Griffin, "
        "Geoffrey Millerd and Paul Penman represented the seller and procured "
        "the buyer\" without naming the seller. ASG Equities appears in the same "
        "article as having done \"its third deal on the street in the last "
        "year\", but whether that makes ASG the seller here or a participant in "
        "the buying venture is not stated, and the article separately describes "
        "an ASG affiliate selling a DIFFERENT Newbury Street property to Ralph "
        "Lauren. Left null."),
    "343 Congress": (
        "SELLER NOT NAMED. Reported as a one-sentence aside to the 10-20 Channel "
        "Center story, with no grantor and no price."),
    "31 Buttonwood": (
        "SELLER NOT NAMED. Bisnow reports the buyer, the combined price and a "
        "$50M loan from Beacon Bank & Trust, but no grantor."),
    "294-302 Windsor": (
        "SELLER NOT NAMED. A brokerage announcement from the seller's own agent, "
        "Horvath & Tremblay, which names neither principal. Characteristic of "
        "how sub-$10M deals reach print at all."),
    "mixed-income apartments": (
        "SELLER NOT NAMED and not obtainable. The Real Reporter is paywalled "
        "past the lead paragraph, and the lead gives price, city and lender but "
        "neither principal nor address."),
}


def main(dry_run: bool):
    conn = engine.connect()
    if not dry_run:
        conn.execute(text(
            "update transactions set seller = :s, "
            "notes = coalesce(notes,'') || :n where id = :id "
            "and coalesce(seller,'') = ''"),
            {"s": FIX["seller"], "n": FIX["note"], "id": FIX["row_id"]})
    log.info("374 Congress Street  seller -> %s", FIX["seller"])

    for match, reason in REASONS.items():
        r = conn.execute(text(
            "select id, notes from transactions where address like :m"),
            {"m": f"%{match}%"}).first()
        if not r:
            log.warning("no row matching %r", match)
            continue
        if "SELLER NULL" in (r[1] or "") or "SELLER NOT NAMED" in (r[1] or ""):
            continue
        if not dry_run:
            conn.execute(text(
                "update transactions set notes = coalesce(notes,'') || :n "
                "where id = :id"), {"n": " | " + reason, "id": r[0]})
        log.info("  reason recorded: %s", match)

    if not dry_run:
        conn.commit()

    for label, cond in [("press + SEC", "source in ('press','sec_filing')"),
                        ("assessment spine",
                         "source in ('massgis_l3','cambridge_socrata')")]:
        tot = conn.execute(text(
            f"select count(*) from transactions where {cond}")).scalar()
        sel = conn.execute(text(
            f"select count(*) from transactions where {cond} "
            f"and coalesce(seller,'') <> ''")).scalar()
        log.info("%-18s %4d rows | seller %3d (%.0f%%)", label, tot, sel,
                 sel / tot * 100)
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
