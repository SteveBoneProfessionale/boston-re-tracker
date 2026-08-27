r"""Thirty-second pass. A cleared care-of value that should have been RESCUED.

1028-1030 MASSACHUSETTS AVENUE. The canonical audit cleared this row's buyer,
"Altus Ventas Unit 6795", classing it with the mail stops and calling it "a mail
stop" outright. That was half right and half wrong, and the wrong half matters.

    ALTUS is Altus Group, a property-tax and valuation adviser.
    UNIT 6795 is a mail box.
    VENTAS IS THE OWNER.

The care-of line read as an adviser collecting post ON BEHALF OF A NAMED
PRINCIPAL -- the same shape as "Cushman & Wakefield AAF Deka Immobilien", which
the same audit correctly RESCUED by dropping the agent and keeping the
principal. Two lines with identical structure, sorted into opposite bins,
because one had the agent's name first and legible and the other had it wrapped
around a mail-box number.

The row's own entity says it too, once you know to look: VTR LS 1030 MASS AVE.
VTR is Ventas's NYSE ticker and LS is its life-science arm.

CONFIRMED, NOT INFERRED. Bain Capital's own release, "Bain Capital Real Estate
Closes Sale of Class A Life Science Property in Boston", and Law360's real
estate column: Ventas bought 1030 Massachusetts Avenue -- 77,805 SF over four
storeys, LEED Gold lab and office in Harvard Square -- from BAIN CAPITAL REAL
ESTATE for $128 million, announced 5 April 2019. This row is $128,000,000 in
April 2019.

WHAT THIS SAYS ABOUT THE CLEANUP. Clearing 19 broker and mail-stop values was
right and the bands fell honestly. But "clear it" and "rescue the principal out
of it" are separated by whether a human recognises the firm inside the string,
and that is not a reliable test. Any care-of line containing a REIT ticker, a
fund name or a firm name should be read for a principal before it is discarded.

    python scraper/acq_press32.py --apply
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

VENTAS = (
    "VTR IS VENTAS, confirmed against a named property. Bain Capital's own "
    "release, \"Bain Capital Real Estate Closes Sale of Class A Life Science "
    "Property in Boston\", with Law360's real estate column: Ventas Inc bought "
    "1030 Massachusetts Avenue -- 77,805 SF over four storeys, a LEED Gold "
    "laboratory and office building in Harvard Square -- for $128 million, "
    "announced 5 April 2019. The record entity is VTR LS 1030 MASS AVE: VTR is "
    "Ventas's NYSE ticker and LS its life-science arm. AND THIS ROW'S BUYER WAS "
    "WRONGLY CLEARED EARLIER. The canonical audit removed \"Altus Ventas Unit "
    "6795\" as a mail stop. Altus Group is a property-tax adviser and Unit 6795 "
    "is a mail box, but VENTAS IS THE OWNER, sitting in the middle of the string "
    "-- the same agent-for-a-named-principal shape as \"Cushman & Wakefield AAF "
    "Deka Immobilien\", which the same audit correctly rescued. Two identically "
    "structured lines went into opposite bins because one had the principal's "
    "name in an obvious position and this one did not."
)

RESOLVE = [
    (1662, "buyer", "Ventas", VENTAS),
    (1662, "seller", "Bain Capital Real Estate",
     'Bain Capital\'s own release, "Bain Capital Real Estate Closes Sale of Class '
     'A Life Science Property in Boston", April 2019, names it as the seller of '
     '1030 Massachusetts Avenue to Ventas for $128 million. NEREJ separately '
     'records HFF arranging a $51 million recapitalisation financing on the '
     'building beforehand. The seller entity here is CAMBRIDGE 1030 MASS AVE LLC, '
     'an address vehicle that says nothing on its own -- an earlier pass searched '
     'this row and found nothing, because it searched 1028 and the reporting says '
     '1030.'),
]

NOTES = [
    (1372, "254 SUMMER STREET SELLER, $62,200,000, June 2017: NOT FOUND, AND THE "
           "DEAL IS NOW IDENTIFIED EVEN THOUGH THE SELLER IS NOT. BLDUP: \"Morgan "
           "Stanley acquires 250 Summer Street, 104,728-square-foot Fort Point "
           "office building, for $62.5 million\", July 2017. This row is 254 "
           "Summer Street at $62,200,000 in June 2017 -- the same brick-and-beam "
           "building over Fort Point Channel, addressed one door along in the "
           "parcel record, and the buyer side already reads Morgan Stanley Prime "
           "Property Fund. But no source found names the SELLER. Cambridge and "
           "Boston assessment records confirm CHANNEL HOLDINGS LLC as owner of "
           "both 254 and 256-260 Summer Street, matching the entity on this row, "
           "and nothing names the firm behind it. WHAT HAPPENED LATER: the "
           "building sold for $37.65 million and then, after loan trouble, for "
           "$15 million -- Connect CRE, \"Fort Point Offices Sell for $15M After "
           "Loan Trouble\". A fifth round trip, and the steepest in this table."),
    (1496, "131 DARTMOUTH STREET SELLER, $315,000,000, December 2015: NOT FOUND "
           "AFTER TWO ATTEMPTS. The buyer is solid and now doubly sourced: Boston "
           "Office Spaces, \"Dartmouth Street Office Building in Back Bay Nets "
           "$849 per Sq. Ft.\", records TA Associates Realty -- now TA Realty -- "
           "acquiring the twelve-storey, 371,000 SF building beside Back Bay "
           "Station, across Dartmouth Street from Copley Place, with a below-grade "
           "parking deck, at exactly this price. THE SELLER IS NAMED IN NO SOURCE "
           "FOUND, and the record entity ONE-31 DARTMOUTH STREET LLC is the "
           "address with a hyphen in it. Note also that this row is ADDRESSED "
           "48-20 Buckingham Street in the parcel data while the asset is 131 "
           "Dartmouth Street; that mismatch is in the assessment record."),
]


def main(dry_run: bool):
    conn = engine.connect()
    n = 0
    for rid, side, sponsor, why in RESOLVE:
        cur = conn.execute(text(
            f"select {side}, coalesce({side}_canonical,'') from transactions "
            f"where id = :id"), {"id": rid}).first()
        if not cur:
            log.warning("id %s missing", rid)
            continue
        if cur[1]:
            log.info("id=%-5s %-6s already %s, skipped", rid, side, cur[1])
            continue
        log.info("id=%-5s %-6s %-34s -> %s", rid, side, (cur[0] or "")[:34], sponsor)
        if not dry_run:
            basis = "prefix_confirmed" if "VTR IS VENTAS" in why else "web"
            conf = ("registry_confirmed" if basis == "prefix_confirmed"
                    else "web_corroborated")
            conn.execute(text(f"""
                update transactions
                   set {side}_canonical = :s, {side}_confidence = :c,
                       {side}_resolution_basis = :b,
                       notes = coalesce(notes,'') || :n
                 where id = :id"""), {
                "s": sponsor, "id": rid, "c": conf, "b": basis,
                "n": f" | {side.upper()} RESOLVED. " + why})
            n += 1

    for side in ("buyer", "seller"):
        for rid, ent in conn.execute(text(f"""
                select id, {side} from transactions
                 where coalesce(quarantined,0) = 0
                   and upper(coalesce({side},'')) like 'VTR %'
                   and coalesce({side}_canonical,'') = ''""")):
            log.info("id=%-5s %-6s %-34s -> Ventas [family sweep]", rid, side,
                     (ent or "")[:34])
            if not dry_run:
                conn.execute(text(f"""
                    update transactions
                       set {side}_canonical = 'Ventas',
                           {side}_confidence = 'registry_confirmed',
                           {side}_resolution_basis = 'prefix_confirmed',
                           notes = coalesce(notes,'') || :n
                     where id = :id"""),
                    {"id": rid, "n": f" | {side.upper()} RESOLVED. " + VENTAS})
                n += 1

    if not dry_run:
        for rid, note in NOTES:
            conn.execute(text(
                "update transactions set notes = coalesce(notes,'') || :n "
                "where id = :id"), {"n": " | " + note, "id": rid})
        conn.commit()

    log.info("\n%d sides written", n)
    tot = conn.execute(text(
        "select count(*) from transactions where coalesce(quarantined,0)=0")).scalar()
    for side in ("buyer", "seller"):
        v = conn.execute(text(
            f"select count(*) from transactions where coalesce(quarantined,0)=0 "
            f"and coalesce({side}_canonical,'') <> ''")).scalar()
        log.info("%s_canonical: %d of %d (%.0f%%)", side, v, tot, v / tot * 100)
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
