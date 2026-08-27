r"""Eighteenth pass. Portfolio legs, self-identifying names, and one refusal.

CAMBRIDGE DISCOVERY PARK IS A $720M PORTFOLIO AND THIS ROW IS ONE BUILDING OF
IT. Bisnow, "Bulfinch, Harrison Street Sell Cambridge Discovery Park For $720M",
and Bulfinch's own release, "Bulfinch And Partners Announce Completion of
Recapitalization of Cambridge Discovery Park", December 2020: the sellers were
joint ventures among affiliates of BULFINCH, HARRISON STREET and NATIONAL REAL
ESTATE ADVISORS; Healthpeak paid $664M net of a 49% JV interest, against a $720M
gross. This row is 60 Acorn Park Drive at $165,000,000, one leg of that. That
same three-firm venture already appears in this table at 30 Hawkins Street.

THE ENVOY HOTEL CARRIES TWO PRICES AND THE ROW HAS THE SMALLER ONE. Hersha's own
release and Business Wire put the July 2016 acquisition at $112.5 million; this
row records $70,789,300 in the same month. Both figures are published. The row is
not adjusted -- that is the standing rule -- but the gap is written on it, as at
the Fairmont Copley Plaza ($163M/$170M), 125 Broadway ($592M/$603M) and 535
Boylston ($128M/$148M). Four instances now, which is a pattern rather than four
oddities: the registry records one number and the press another.

NORWICH PARTNERS IS CORROBORATED FROM TWO DIRECTIONS. It is named in the Envoy
reporting as the seller, and it separately appears in this table as the entity
NORWICH PARTNERS BOSTON LLC on 368 Congress Street. The press and the entity
agree without either being used to prove the other.

ONE REFUSAL THAT LOOKS LIKE A RESOLUTION. BLDUP reports 711 Atlantic Avenue
"acquired by Asset Preservation, Inc. for $68.5 million". Asset Preservation Inc
is a QUALIFIED INTERMEDIARY for 1031 exchanges -- it takes title temporarily so a
seller can defer tax, exactly as US Bank Trust National Association appears
elsewhere in this table holding title for bondholders. An intermediary is not a
buyer, and writing it would put a tax service at the top of a buyer ranking.

    python scraper/acq_press18.py --apply
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

RESOLVE = [
    (1647, "seller", "GCP Applied Technologies", "web",
     'GCP Applied Technologies\' own 8-K, "GCP Applied Technologies Announces $125 '
     'Million Sale of Cambridge, Massachusetts Headquarters": on 2 July 2020 GCP '
     'agreed to sell its corporate headquarters at 62 Whittemore Avenue for '
     '$125,000,000 in a SALE-LEASEBACK, with a provision letting GCP remain up to '
     '24 months, closing expected late July or early August 2020 -- this row is '
     'August 2020 at that price. CPE ("IQHQ Buys $125M Cambridge Life Sciences '
     'Campus") and NEREJ ("IQHQ, Inc. acquires 290,000 s/f Alewife Park") confirm '
     'the buyer, which this row already carried. The 26-acre campus is Alewife '
     'Park. Recorded as an arm\'s-length asset sale; the leaseback is a term of '
     'the deal, not an affiliation.'),

    (1638, "seller", "Harrison Street / The Bulfinch Cos. / National Real Estate Advisors",
     "web",
     'Bisnow, "Bulfinch, Harrison Street Sell Cambridge Discovery Park For $720M", '
     'and Bulfinch\'s own release announcing completion of the recapitalisation, '
     'December 2020: the sellers were joint ventures among affiliates of Bulfinch '
     'the developer, Chicago-based Harrison Street and Washington-based National '
     'Real Estate Advisors. Healthpeak acquired three life science facilities for '
     '$610M plus a 49% unconsolidated JV interest in a fourth for $54M, against a '
     '$720M gross figure. PORTFOLIO ALLOCATION: this row is 60 Acorn Park Drive at '
     '$165,000,000, one building of that campus, not a standalone trade. Recorded '
     'as the venture, not one partner. The same three firms already appear '
     'together in this table at 30 Hawkins Street.'),

    (1451, "buyer", "Hersha Hospitality Trust", "web",
     'Hersha\'s own Business Wire release, "Hersha Hospitality Trust Acquires The '
     'Envoy Hotel in Boston\'s Seaport", 28 July 2016, with CoStar, Hotel '
     'Management and REBusinessOnline reporting the same: Hersha bought the '
     '136-room boutique Envoy Hotel in the Seaport. The record entity is HHLP '
     'BOSTON SEAPORT, and HHLP is Hersha Hospitality Limited Partnership, the '
     'REIT\'s operating partnership -- confirmed here by the press naming the firm '
     'at this hotel in this month, not decoded from the initials. Press gives the '
     'address as 70 Sleeper Street and this row says 66; one parcel, one asset.'),
    (1451, "seller", "Norwich Partners", "web",
     'Same reporting names Norwich Partners as the seller of the Envoy. Norwich '
     'is corroborated independently within this table: the entity NORWICH PARTNERS '
     'BOSTON LLC is the seller of record at 368 Congress Street.'),

    (1282, "seller", "Norwich Partners", "self_identifying",
     'The record entity is NORWICH PARTNERS BOSTON LLC, which names its own owner. '
     'Norwich Partners is independently established in this table by press as the '
     'seller of the Envoy Hotel to Hersha in July 2016, so the name and the '
     'reporting agree.'),

    (1540, "buyer", "Target Corporation", "self_identifying",
     'The record entity is TARGET CORPORATION -- the retailer itself, buying its '
     'own store site at 1345 Boylston Street in the Fenway. No decoding step, so '
     'nothing to get wrong. NOTE that this row is one of those whose building_sf '
     'was the 1 SF placeholder and whose $/SF has been nulled.'),

    (1428, "buyer", "MassDevelopment", "self_identifying",
     'The record entity is MASSDEVELOPMENT/NECCO, naming the Massachusetts '
     'Development Finance Agency on its face. The site is 6 Necco Court in Fort '
     'Point, the former New England Confectionery Company works, which '
     'MassDevelopment jointly marketed with GE. A quasi-public agency taking title '
     'is a real conveyance and is recorded as one.'),

    (1444, "seller", "Brickman Associates", "self_identifying",
     'The record entity is BRICKMAN ONE BOWDOIN LLC, naming its own owner. '
     'Brickman Associates is independently established in this table three times '
     'over: at 237 Putnam Avenue, where REBusinessOnline names it at Blackstone '
     'Science Square, and on both sides of 535-545 Boylston Street, where Shimizu\'s '
     'release names it. The asset here is One Bowdoin Square at 15 New Chardon '
     'Street.'),
]

NOTES = [
    (1201, "711 ATLANTIC AVENUE BUYER: REFUSED, AND THE REASON MATTERS. BLDUP "
           "reports the building \"acquired by Asset Preservation, Inc. for $68.5 "
           "million\" in May 2019, matching this row. ASSET PRESERVATION INC IS A "
           "QUALIFIED INTERMEDIARY for section 1031 like-kind exchanges: it takes "
           "title temporarily so a seller can defer tax, then conveys on to the "
           "real buyer. It is not an owner, in the same way that US BANK TRUST "
           "NATIONAL ASSOCIATION elsewhere in this table is a securitisation "
           "trustee holding title for bondholders rather than a buyer. Writing it "
           "would put a tax service near the top of a buyer ranking. SEPARATE "
           "LEAD, NOT WRITTEN: Ashforth carries 711 Atlantic Avenue on its own "
           "portfolio page, with no acquisition date given."),
    (1430, "CAMBRIDGE STREET SELLER, $128,878,288, November 2016: NOT FOUND, AND "
           "THE NEAR MISS IS RECORDED SO IT IS NOT CHASED AGAIN. The buyer is "
           "Harvard University. The Harvard Crimson and Harvard's own accounts "
           "describe the university finalising in 2016 a land acquisition begun in "
           "2000 with CSX TRANSPORTATION, paying $147.4 million -- a different "
           "figure from this row's $128,878,288. Close enough to be tempting and "
           "far enough apart to be a different conveyance or a different "
           "allocation. CSX is therefore a lead, not the seller."),
    (1225, "18 TREMONT STREET SELLER, $102,750,000, March 2019: NOT FOUND. The "
           "buyer is confirmed -- BLDUP's \"18 Tremont Street Trades for $102.75 "
           "Million\" and CoStar and Connect CRE on the later resale all name "
           "Jamestown -- but no source found names the 2019 SELLER, and the record "
           "entity RECP V 18 TREMONT OWNER LLC is a fund series that is not "
           "decoded. Jamestown, Lincoln Property Company and Verdani Partners are "
           "named together in Fitwel's write-up of the building, but as the "
           "post-acquisition ownership and management team, not as the seller."),
    (1027, "131-147 SEAPORT BOULEVARD SELLER, $94,500,000, August 2021: NOT FOUND. "
           "The buyer side already reads WS Development, the Seaport master "
           "developer. The seller of record is 131 149 SEAPORT PRIMARY CONDOMINIUM "
           "TRUST, which is the condominium trust for the building rather than a "
           "sponsor -- a conveyance out of a condominium trust names the trust, "
           "not the party behind it. Searches return only residential resales at "
           "131 Seaport Boulevard, which is the Alyx at EchelonSeaport."),
]


def main(dry_run: bool):
    conn = engine.connect()
    n = 0
    for rid, side, sponsor, basis, why in RESOLVE:
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
            conf = ("web_corroborated" if basis == "web" else "registry_confirmed")
            conn.execute(text(f"""
                update transactions
                   set {side}_canonical = :s, {side}_confidence = :c,
                       {side}_resolution_basis = :b,
                       notes = coalesce(notes,'') || :n
                 where id = :id"""), {
                "s": sponsor, "id": rid, "c": conf, "b": basis,
                "n": f" | {side.upper()} RESOLVED. " + why})
            n += 1

    if not dry_run:
        for rid, note in NOTES:
            conn.execute(text(
                "update transactions set notes = coalesce(notes,'') || :n "
                "where id = :id"), {"n": " | " + note, "id": rid})
        conn.execute(text(
            "update transactions "
            "set price_caveat = coalesce(price_caveat || ' ', '') || :c "
            "where id = 1451"), {
            "c": ("Hersha's own release puts the Envoy Hotel acquisition at "
                  "$112.5M; this row records $70,789,300 in the same month. Both "
                  "figures are published and the row keeps the recorded one.")})
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
