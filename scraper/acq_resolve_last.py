r"""The last gaps, worked one at a time rather than declared impossible.

Five of the eight remaining holes closed. What follows is what each needed,
because the methods were all different and none of them was "search harder".

294-302 WINDSOR STREET, seller. The ownership chain had never been run against
this row because it is press-sourced and carries no parcel id. Looking the
address up in the Cambridge assessment file directly gives the owner of record:
UEP JBBP LLC, last sale September 2015. The June 2026 sale postdates the FY2026
snapshot, so that owner is the grantor.

DORCHESTER AVENUE, seller. Recorded as "none exists" because the buyers bid to
retain their own portfolio. That was half right: no third party bought, but a
foreclosure has a LOSER, and that is the seller in every sense that matters
here. The Boston Globe: the auction "cut out" South Boston developer ANDREW J.
COLLINS, and the $75M bid is half the $150M owed by "a shell company connected
to Collins".

320 SUMMER STREET, buyer. HC 320 SUMMER ST. was undecoded and my best candidate,
Kendall Capital, did not fit the date. It is neither: Universal Hub reports that
"Du bought the building for $26.3 million in February, according to Suffolk
County Registry of Deeds records" -- FAN DU of Newton, who then won Zoning Board
approval to convert the old LogMeIn building to 145 apartments. Price, month and
registry source all match the row.

ARX, buyer. Arx Urban, a Boston mixed-income housing developer. The Real
Reporter abbreviates it to the stem.

11 BEACON STREET, price. This one is a correction, not a fill. I recorded price
as NULL and put $23M in implied_valuation, reasoning that "recapitalised for
$23M" reads as a whole-asset figure and that $157/SF supported it. Two sources
say otherwise: Bisnow states "The $23M deal was for 11 Beacon St... ACCORDING TO
PUBLIC RECORDS", and public records record consideration, not valuation; The
Real Reporter headlines it "Synergy BUYS 11 Beacon Street". The balance of
evidence says $23M is what Synergy paid to take out its partner. My $157/SF
reasoning was inference and the sources are not.

AND ONE THING I ALMOST GOT WRONG. A search summary asserted that "BentallGreenOak
turned the property over to Synergy Investments for $23 million", which would
have named the 11 Beacon seller. Fetching the Bisnow article shows it names NO
exiting partner at all -- it mentions GreenOak only in the context of the 2016
round. The claim was a paraphrase the source does not support. Not written.

    python scraper/acq_resolve_last.py --apply
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

FILLS = [
    dict(id=904, side="seller", entity="UEP JBBP LLC", sponsor=None,
         conf="registry_confirmed", basis="registry",
         note="SELLER ENTITY FILLED FROM THE OWNERSHIP CHAIN. The Cambridge "
              "assessment file gives the owner of record at 294-302 Windsor St "
              "as UEP JBBP LLC, last sale 22 September 2015 at $2,780,000. This "
              "row's sale is 12 June 2026, after the FY2026 snapshot, so the "
              "owner of record is the grantor. The SPONSOR behind UEP JBBP LLC "
              "is not established -- it is an address-form vehicle and the "
              "brokerage announcement named neither principal."),
    dict(id=901, side="seller", entity=None,
         sponsor="Andrew J. Collins (foreclosed borrower)",
         conf="web_corroborated", basis="web",
         note="SELLER IDENTIFIED, correcting an earlier note that said none "
              "exists. That was half right: no third party acquired the "
              "portfolio, because J.T. Magen and Extell bid to retain property "
              "they already had a mortgage over. But a foreclosure has a losing "
              "side and that is the seller in the sense this table cares about. "
              "The Boston Globe, 17 March 2026: the $75M auction \"cut out\" "
              "South Boston developer Andrew J. Collins, and the bid is half the "
              "$150M of debt Extell and Magen say they are owed by a shell "
              "company connected to Collins. Banker & Tradesman: \"Lender "
              "Submits $75M Bid for Dot Ave. Block.\" The specific borrower "
              "entity is not named in any source, so the principal is recorded "
              "rather than a vehicle."),
    dict(id=893, side="buyer", entity=None, sponsor="Fan Du",
         conf="web_corroborated", basis="web",
         note="BUYER RESOLVED, and it is neither of the candidates I had. The HC "
              "320 SUMMER ST. stem stayed undecoded and Kendall Capital, the "
              "obvious guess from Bisnow's \"picked up 320 Summer St. last "
              "year\", did not fit a February 2026 deed. Universal Hub: \"Du "
              "bought the building for $26.3 million in February, according to "
              "Suffolk County Registry of Deeds records\" -- Fan Du of Newton, "
              "who subsequently won Zoning Board approval to convert the former "
              "LogMeIn building into 145 apartments under Boston's "
              "office-to-apartments pilot. Price, month and registry source all "
              "match this row. This is why the guess was not written earlier."),
    dict(id=898, side="buyer", entity=None, sponsor="Arx Urban",
         conf="web_corroborated", basis="web",
         note="ARX is Arx Urban, a Boston developer of mixed-income housing, "
              "which The Real Reporter abbreviates to the stem. The SELLER and "
              "the ADDRESS remain unobtainable: TRR is paywalled past the lead "
              "and the lead gives price, city and lender only."),
]


def main(dry_run: bool):
    conn = engine.connect()
    for f in FILLS:
        sets, params = [], {"id": f["id"], "n": " | " + f["note"]}
        if f.get("entity"):
            sets.append(f"{f['side']} = :e")
            params["e"] = f["entity"]
        if f.get("sponsor"):
            sets.append(f"{f['side']}_canonical = :s")
            sets.append(f"{f['side']}_confidence = :c")
            sets.append(f"{f['side']}_resolution_basis = :b")
            params["s"] = f["sponsor"]
            params["c"] = f["conf"]
            params["b"] = f["basis"]
        elif f.get("entity"):
            sets.append(f"{f['side']}_confidence = :c")
            params["c"] = f["conf"]
        sets.append("notes = coalesce(notes,'') || :n")
        log.info("id=%-4s %-6s %s", f["id"], f["side"],
                 f.get("sponsor") or f.get("entity"))
        if not dry_run:
            conn.execute(text(
                f"update transactions set {', '.join(sets)} where id = :id"), params)

    # ── 11 Beacon: the $23M is a price, not a valuation ─────────────
    row = conn.execute(text(
        "select id, price, implied_valuation from transactions "
        "where address like '%11 Beacon%'")).first()
    if row and not dry_run:
        conn.execute(text("""
            update transactions
               set price = 23000000, implied_valuation = null,
                   price_caveat = :cav,
                   excise_implied_price = :ex,
                   notes = coalesce(notes,'') || :n
             where id = :id"""), {
            "id": row[0],
            "cav": "Consideration on a partner buy-out; no percentage disclosed.",
            "ex": round(23_000_000 / 1000.0 * 4.56, 2),
            "n": (" | PRICE CORRECTED FROM NULL TO $23,000,000. I had recorded "
                  "price as null with $23M in implied_valuation, reasoning that "
                  "\"recapitalised for $23M\" reads as a whole-asset figure and "
                  "that $157/SF over 146,000 SF supported it. Two sources say "
                  "otherwise. Bisnow: \"The $23M deal was for 11 Beacon St. in "
                  "Boston's Beacon Hill neighborhood, ACCORDING TO PUBLIC "
                  "RECORDS\" -- and public records record consideration, not "
                  "valuation. The Real Reporter headlines it \"Synergy BUYS 11 "
                  "Beacon Street; Cambridge Savings Bank Funds Trade\". The "
                  "balance of evidence is that $23M is what Synergy paid to take "
                  "out its partner. My per-square-foot reasoning was inference; "
                  "the sources are not. The SELLER remains unnamed: a search "
                  "summary asserted BentallGreenOak exited, but the Bisnow "
                  "article names no exiting partner and mentions GreenOak only "
                  "for the 2016 round, so that claim is unsupported and is not "
                  "written.")})
        log.info("11 Beacon: price null -> $23,000,000, implied_valuation cleared")

    if not dry_run:
        conn.commit()

    tot, vol = conn.execute(text(
        "select count(*), sum(coalesce(price,0)) from transactions")).first()
    log.info("\n%d transactions, $%.2fB", tot, vol / 1e9)
    for side in ("buyer", "seller"):
        ent = conn.execute(text(
            f"select count(*) from transactions where coalesce({side},'') <> ''")).scalar()
        res, rv = conn.execute(text(
            f"select count(*), sum(coalesce(price,0)) from transactions "
            f"where coalesce({side}_canonical,'') <> ''")).first()
        log.info("%-7s entity %2d/%d (%3.0f%%) | sponsor %2d/%d (%3.0f%%), "
                 "$%.2fB (%.0f%% of dollars)", side, ent, tot, ent / tot * 100,
                 res, tot, res / tot * 100, (rv or 0) / 1e9, (rv or 0) / vol * 100)
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
