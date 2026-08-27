r"""Twenty-sixth pass. The Wells/Piedmont question, answered from the other leg.

AN EARLIER PASS REFUSED "WELLS OPERATING PARTNERSHIP LP" AND SAID WHY. The note
on 1416 Massachusetts Avenue read: the entity names a real firm, but Wells
Operating Partnership is the operating partnership of Wells Real Estate
Investment Trust, WHICH BECAME PIEDMONT OFFICE REALTY TRUST IN 2005 -- and the
row is dated December 2022, seventeen years later. Either the entity is a
survivor of that rename, in which case the sponsor is Piedmont and not Wells, or
it belongs to the separate Wells Real Estate Funds. It was left blank because
nothing found settled it.

IT IS SETTLED NOW, AND IT WAS THE FIRST READING. Connect CRE, "Piedmont Office
Realty Trust Sells Two Cambridge Assets for $160M Combined Proceeds", and
Piedmont's own Q4 2022 results: Piedmont sold ONE BRATTLE SQUARE and 1414
MASSACHUSETTS AVENUE in Cambridge in December 2022 for combined proceeds of
about $160 million, 94% leased, booking a $102.6 million gain, and used the
proceeds to clear its $600 million line of credit. Its CEO called it "a crucial
step in our asset recycling strategy".

BOTH LEGS ARE IN THIS TABLE AND NEITHER LOOKED LIKE THE OTHER.

    1416 Massachusetts Ave  $79,225,000   seller WELLS OPERATING PARTNERSHIP LP
    1 Brattle Square        $ 2,721,600   seller WELLS REIT ONE BRATTLE SQUARE

Two different Wells-named entities, two wildly different prices, fourteen days
apart, and only together do they read as one REIT's exit from a submarket it had
decided was non-strategic. This is the FelCor/RLJ pattern exactly: an entity
keeps its pre-merger name for years and the press is what settles who owns it.

AND THE BUYER OF THE BRATTLE SQUARE LEG DECODES ITS OWN ENTITY. It is BCSP 9 OBS
PROPERTY LLC -- Beacon Capital Strategic Partners fund 9, already confirmed, and
OBS is One Brattle Square. The asset name was sitting inside the entity the whole
time and only became legible once the deal it belonged to was identified.

    python scraper/acq_press26.py --apply
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

PIEDMONT = (
    "PIEDMONT OFFICE REALTY TRUST, and this reverses an earlier deliberate "
    "refusal on this table. Wells Operating Partnership LP is the operating "
    "partnership of Wells Real Estate Investment Trust, which RENAMED ITSELF "
    "PIEDMONT OFFICE REALTY TRUST IN 2005; an earlier pass left the sponsor blank "
    "because a Wells-named entity on a 2022 row could equally have belonged to "
    "the separate Wells Real Estate Funds, and nothing found settled it. Connect "
    "CRE, \"Piedmont Office Realty Trust Sells Two Cambridge Assets for $160M "
    "Combined Proceeds\", and Piedmont's own Q4 2022 results settle it: Piedmont "
    "sold ONE BRATTLE SQUARE and 1414 MASSACHUSETTS AVENUE in December 2022 for "
    "roughly $160 million combined, 94% leased, booking a $102.6 million gain on "
    "sale and using the proceeds to pay off its $600 million line of credit. Its "
    "CEO described it as \"a crucial step in our asset recycling strategy\" out of "
    "\"a non-strategic submarket\". The entity kept the pre-merger name for "
    "seventeen years, which is the same situation as FELCOR COPLEY PLAZA OWNER "
    "still carrying FelCor's name after RLJ acquired it."
)

RESOLVE = [
    (1577, "seller", "Piedmont Office Realty Trust", PIEDMONT +
     " THIS ROW IS THE MASSACHUSETTS AVENUE LEG at $79,225,000. Its sibling is 1 "
     "Brattle Square, fourteen days earlier in the same table, whose seller entity "
     "is WELLS REIT ONE BRATTLE SQUARE -- a second, differently-named Wells "
     "vehicle. The two legs look nothing alike and only read as one disposition "
     "once the reporting names them together."),
    (1580, "seller", "Piedmont Office Realty Trust", PIEDMONT +
     " THIS ROW IS THE ONE BRATTLE SQUARE LEG, and its seller entity WELLS REIT "
     "ONE BRATTLE SQUARE names both the vendor family and the asset. ITS BUYER "
     "ENTITY NOW DECODES TOO: BCSP 9 OBS PROPERTY LLC is Beacon Capital Strategic "
     "Partners fund 9 -- already confirmed as Beacon at the Canal Park complex -- "
     "and OBS is One Brattle Square. The asset name was inside the entity all "
     "along and only became legible once the deal was identified. PRICE: "
     "$2,721,600 on this row against roughly $160 million for the pair, so this "
     "row is one parcel of a larger conveyance, not the Brattle Square deal."),

    (1688, "buyer", "Lincoln Property Company / Stars Investments", None),
]

BUYER_WHY = (
    'NEREJ, "L&B Realty Advisors sells 625 Mass. Ave. for $75 million to Lincoln '
    'Property Company and Stars Investments", and BLDUP, "625 Mass Ave office and '
    'retail building in Cambridge\'s Central Square sells for $75 million": the '
    'buyer was a partnership of Lincoln Property Company and Chile-based Stars '
    'Investments, with CBRE/New England representing the seller and procuring the '
    'buyer, September 2017. Recorded as the venture, not one partner. THE SELLER '
    'SIDE ALREADY READ L&B REALTY ADVISORS, written from the entity L&B CIP 625 '
    'MASS AVE LLC naming its own owner -- entity and press agree independently. '
    'Lincoln Property Company now appears three times in this table: here, in the '
    'ASB venture at 327-333 Summer Street, and selling Two Financial Center with '
    'ASB to Nan Fung.'
)

NOTES = [
    (1296, "100-170 MEADOW ROAD, $64,000,000, July 2018: SELLER NOT WRITTEN, AND "
           "FLAGGED FOR REVIEW RATHER THAN QUARANTINED. Both entities name the same "
           "asset: the buyer is CPT BOSTON BUSINESS PARK LLC and the seller is 100 "
           "BBP LLC, where BBP is Boston Business Park. National Development's own "
           "case study records it acquiring a 450,000 SF industrial complex on 72+ "
           "acres in Readville in 2015, including a 430,000 SF manufacturing and "
           "distribution centre -- so National Development, which this row already "
           "carries as buyer, ALREADY OWNED THE PARK three years before this "
           "conveyance. That makes a 2018 transfer between two Boston Business "
           "Park vehicles either a partner buyout or an intra-sponsor "
           "restructuring. NOT QUARANTINED: the shared element is the ASSET NAME, "
           "which is the collision the Porter Square and American Twine cases "
           "proved innocent, and no source names a party to this conveyance. "
           "Flagged so a licensed feed can settle whether real capital moved."),
]


def main(dry_run: bool):
    conn = engine.connect()
    n = 0
    for rid, side, sponsor, why in RESOLVE:
        why = why or BUYER_WHY
        cur = conn.execute(text(
            f"select {side}, coalesce({side}_canonical,'') from transactions "
            f"where id = :id"), {"id": rid}).first()
        if not cur:
            log.warning("id %s missing", rid)
            continue
        if cur[1]:
            log.info("id=%-5s %-6s already %s, skipped", rid, side, cur[1])
            continue
        log.info("id=%-5s %-6s %-36s -> %s", rid, side, (cur[0] or "")[:36], sponsor)
        if not dry_run:
            conn.execute(text(f"""
                update transactions
                   set {side}_canonical = :s, {side}_confidence = 'web_corroborated',
                       {side}_resolution_basis = 'web',
                       notes = coalesce(notes,'') || :n
                 where id = :id"""), {
                "s": sponsor, "id": rid,
                "n": f" | {side.upper()} RESOLVED. " + why})
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
