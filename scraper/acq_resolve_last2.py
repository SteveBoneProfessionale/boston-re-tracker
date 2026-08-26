r"""Two more closed, one rejected on a test, and a correction I owe the record.

11 BEACON STREET, seller: BENTALLGREENOAK. I wrote a note last pass saying this
claim was "unsupported and not written", because a search summary asserted it
and the Bisnow article I fetched named no exiting partner. That was the wrong
conclusion from the right instinct: I checked one source and declared the claim
unsupported, when the supporting source was already sitting in my own cache.
The Real Reporter's lead paragraph, in data/trr_index.json since the archive
sweep, reads:

    "BOSTON—Facing an obdurate climate where prime locations and improving
    metrics still fail to inspire capital and office-averse lenders, global
    investment manager BENTALLGREENOAK has turned 9-11 Beacon St. over to joint
    venture partner SYNERGY INVESTMENTS for a sharply discounted $23 million"

That is a named publication naming the exiting partner, and it independently
confirms the price correction made last pass: $23 million is what Synergy paid
BentallGreenOak for its position, not a whole-asset valuation.

294-302 WINDSOR STREET, seller sponsor: TORRINGTON PROPERTIES. UEP JBBP LLC is
an address-form vehicle that no press report decodes. The City of Cambridge
Board of Zoning Appeal filing for this exact property does, on its face:

    "UEP JBBP LLC ... c/o TORRINGTON PROPERTIES INC., 60 K STREET, Boston, MA
    02127"

MUNICIPAL FILINGS ARE A ROUTE I HAD NOT USED, and they work precisely where
press and assessment rolls fail: a zoning or permit applicant must give a
care-of contact, and that contact is the sponsor. This is free, unblocked, and
property-specific.

31 BUTTONWOOD STREET / 14 WILLIS STREET: REJECTED, and the rejection is the
point. Boston's permit file shows PAWEL WOJCIK as applicant on both properties
-- the same two parcels later sold together as one $4.1M portfolio, which looks
like a common owner. It is not. Boston's permit data carries an `applicant`
field and no `owner` field, so it cannot distinguish the two, and counting his
other filings settles it: 199 permits across 131 DISTINCT ADDRESSES across the
city. That is a contractor. The same test on the other applicant at 14 Willis,
Adam Chudas, returns 200 permits across 106 addresses. Neither is written.

    python scraper/acq_resolve_last2.py --apply
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

TRR_LEAD = (
    'The Real Reporter, 9 March 2026, "Synergy Buys 11 Beacon Street; Cambridge '
    'Savings Bank Funds Trade": "BOSTON—Facing an obdurate climate where prime '
    'locations and improving metrics still fail to inspire capital and '
    'office-averse lenders, global investment manager BentallGreenOak has turned '
    '9-11 Beacon St. over to joint venture partner Synergy Investments for a '
    'sharply discounted $23 million".'
)


def main(dry_run: bool):
    conn = engine.connect()

    # ── 11 Beacon: seller is BentallGreenOak ────────────────────────
    if not dry_run:
        conn.execute(text("""
            update transactions
               set seller = 'BentallGreenOak',
                   seller_canonical = 'BentallGreenOak',
                   seller_confidence = 'web_corroborated',
                   seller_resolution_basis = 'web',
                   notes = coalesce(notes,'') || :n
             where address like '%11 Beacon%'"""), {
            "n": (" | SELLER IDENTIFIED: BENTALLGREENOAK, correcting an earlier "
                  "note on this row that called the claim unsupported. " + TRR_LEAD +
                  " I had rejected this because the Bisnow article names no "
                  "exiting partner and I treated a search summary as the only "
                  "other evidence. The supporting text was already in the cached "
                  "Real Reporter index from the archive sweep; I checked one "
                  "source and concluded the claim had none. It also confirms the "
                  "price correction: $23M is what Synergy paid BentallGreenOak "
                  "for its position, not a whole-asset valuation. GreenOak came "
                  "into the building in the 2016 recapitalisation at $63M, so "
                  "this is that partner exiting a decade later at a sharp "
                  "discount.")})
    log.info("11 Beacon Street   seller -> BentallGreenOak")

    # ── 294-302 Windsor: UEP JBBP LLC is Torrington Properties ──────
    if not dry_run:
        conn.execute(text("""
            update transactions
               set seller_canonical = 'Torrington Properties',
                   seller_confidence = 'registry_confirmed',
                   seller_resolution_basis = 'municipal_filing',
                   notes = coalesce(notes,'') || :n
             where address like '%294-302 Windsor%'"""), {
            "n": (" | SELLER SPONSOR RESOLVED FROM A MUNICIPAL FILING. The record "
                  "entity UEP JBBP LLC is an address-form vehicle that no press "
                  "report decodes. The City of Cambridge Board of Zoning Appeal "
                  "filing for 294-302 Windsor St names it on its face: \"UEP JBBP "
                  "LLC ... c/o TORRINGTON PROPERTIES INC., 60 K STREET, Boston, "
                  "MA 02127\". Municipal zoning and permit filings require a "
                  "care-of contact, and that contact is the sponsor — a free and "
                  "unblocked route that works exactly where press and assessment "
                  "rolls fail.")})
    log.info("294-302 Windsor    seller sponsor -> Torrington Properties")

    # ── 31 Buttonwood / 14 Willis: candidate rejected on a test ─────
    if not dry_run:
        conn.execute(text("""
            update transactions
               set notes = coalesce(notes,'') || :n
             where address like '%31 Buttonwood%'
               and coalesce(notes,'') not like '%PAWEL WOJCIK%'"""), {
            "n": (" | A CANDIDATE SPONSOR WAS FOUND AND REJECTED. Boston's "
                  "approved-permit file lists PAWEL WOJCIK as applicant on both "
                  "31 Buttonwood Street and 14 Willis Street — the same two "
                  "parcels sold together here as one $4.1M portfolio, which looks "
                  "like evidence of a common owner. Boston's permit data carries "
                  "an `applicant` field and no `owner` field, so it cannot "
                  "distinguish an owner from a contractor. Counting his other "
                  "filings settles it: 199 permits across 131 DISTINCT ADDRESSES "
                  "across the city. That is a contractor. The other applicant at "
                  "14 Willis, Adam Chudas, returns 200 permits across 106 "
                  "addresses — the same profile. Neither is written, and the "
                  "sponsor behind 31 BUTTONWOOD STREET LLC / 14 WILLIS STREET LLC "
                  "remains unestablished.")})
    log.info("31 Buttonwood      candidate rejected (contractor, 131 addresses)")

    if not dry_run:
        conn.commit()

    tot, vol = conn.execute(text(
        "select count(*), sum(coalesce(price,0)) from transactions")).first()
    log.info("\n%d transactions, $%.2fB", tot, vol / 1e9)
    for side in ("buyer", "seller"):
        ent = conn.execute(text(
            f"select count(*) from transactions "
            f"where coalesce({side},'') <> ''")).scalar()
        res, rv = conn.execute(text(
            f"select count(*), sum(coalesce(price,0)) from transactions "
            f"where coalesce({side}_canonical,'') <> ''")).first()
        log.info("%-7s entity %2d/%d (%3.0f%%) | sponsor %2d/%d (%3.0f%%), "
                 "$%.2fB (%.0f%%)", side, ent, tot, ent / tot * 100, res, tot,
                 res / tot * 100, (rv or 0) / 1e9, (rv or 0) / vol * 100)
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
