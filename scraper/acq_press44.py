r"""Forty-fourth pass. The Davenport buyer, and a refusal that would have been
half wrong even if the firm had been right.

21 THORNDIKE STREET, after four attempts across this project. Boston Real Estate
Times: "OXFORD PROPERTY AND ALONY HETZ PROPERTIES ACQUIRE THE DAVENPORT
BUILDING". The chain around this row was already reconstructed -- DivcoWest paid
$79 million in 2013, spent $18 million on the lobby and common areas, and sold
to Jamestown for $136 million in December 2014, brokered by Cushman & Wakefield;
Jamestown is the seller on this row. Only the 2017 buyer was missing.

    The building is four storeys, 220,000 SF, built 1860 and converted to office
    in 1987, and trades as both 21 Thorndike Street and 25 First Street -- the
    HubSpot lease is executed against "25 First Street, The Davenport". Another
    parcel-versus-asset address split.

    Oxford Properties Group is independently in this table buying 745 Atlantic
    Avenue from Beacon Capital in 2015. Alony-Hetz Properties, the Israeli
    investor that partners with Oxford, is new.

AND A REFUSAL THAT WOULD HAVE BEEN HALF WRONG ANYWAY. 185 Franklin Street's
seller has now been searched five times and Bentall Kennedy has been recorded as
a LEAD throughout, on the strength of two facts: it succeeded Kennedy Associates,
which bought the tower from Verizon in 2008, and NEREJ records it selecting
Suffolk Construction to renovate 50 Post Office Square before the 2015 trade --
an owner's act. This pass adds a third: the 600,000 SF building was acquired in
October 2008 for $192 million BY BENTALL KENNEDY WITH PARTNER COMMONWEALTH
VENTURES.

    So had the lead been promoted at any point, this table would now say
    "Bentall Kennedy" where the answer is a VENTURE. That is the tenth time a
    partner would have been silently dropped, and it is the strongest argument
    yet for the standing rule: the cost of writing an unconfirmed sponsor is not
    only that it might be the wrong firm, but that it is likely to be an
    incomplete one.

    python scraper/acq_press44.py --apply
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
    (1701, "buyer", "Oxford Properties Group / Alony-Hetz Properties",
     'Boston Real Estate Times: "Oxford Property and Alony Hetz Properties Acquire '
     'The Davenport Building". This row is $202,500,000 in February 2017 with '
     'Jamestown already on the sell side, and the chain around it was already '
     'reconstructed from NEREJ and REBusinessOnline: DivcoWest bought the building '
     'for $79 million in 2013, spent $18 million on the lobby and common areas, '
     'and sold to Jamestown for $136 million in December 2014 through Cushman & '
     'Wakefield -- Jamestown\'s second Cambridge purchase after 245 First Street. '
     'Only the 2017 buyer was missing, across four earlier attempts. THE ASSET: '
     'four storeys, 220,000 SF, built 1860 and converted to office use in 1987; it '
     'trades as both 21 Thorndike Street and 25 FIRST STREET -- HubSpot\'s lease '
     'is executed against "25 First Street, The Davenport" -- which is the same '
     'parcel-versus-asset address split as 131 Dartmouth, 250/254 Summer and the '
     'Paddock Building. Recorded as the venture, not one partner. Oxford '
     'Properties Group is independently confirmed in this table buying 745 '
     'Atlantic Avenue from Beacon Capital in 2015; Alony-Hetz, the Israeli '
     'investor that partners with Oxford, is new to it. The entity DAVENPORT OWNER '
     '(DE) LLC stays undecoded -- Davenport names the A.H. Davenport furniture '
     'works that occupied the site, not a firm.'),
]

NOTES = [
    (1502, "185 FRANKLIN STREET SELLER: FIFTH SEARCH, STILL A LEAD -- AND THE "
           "LEAD IS NOW KNOWN TO BE INCOMPLETE, WHICH IS THE POINT. Bentall "
           "Kennedy has been recorded here as a lead throughout, on two facts: it "
           "is the successor to Kennedy Real Estate Counsel, which bought the "
           "tower from Verizon for $192M in September 2008, and NEREJ reports it "
           "selecting Suffolk Construction to renovate 50 Post Office Square "
           "before the December 2015 trade -- an owner's act, not an adviser's. "
           "THIS PASS ADDS A THIRD FACT THAT CHANGES THE SHAPE OF THE ANSWER: the "
           "600,000 SF building was acquired in October 2008 for $192 million by "
           "Bentall Kennedy WITH PARTNER COMMONWEALTH VENTURES. So if the lead had "
           "ever been promoted, this row would now read \"Bentall Kennedy\" where "
           "the truth is a venture -- the tenth partner this table would have "
           "silently dropped. The cost of writing an unconfirmed sponsor is not "
           "only that it may be the wrong firm; it is that it is likely to be an "
           "incomplete one. STILL MISSING: no source anywhere says Bentall Kennedy "
           "SOLD. Commonwealth Ventures is separately in this table developing the "
           "One Channel Center garage and selling 105 West First Street with Ares."),
    (1538, "50-60 STANIFORD STREET BUYER: THIRD SEARCH, NOT FOUND, AND THE ENTITY "
           "IS A FUND SERIES WITH NO PUBLIC MANAGER. RAR2 appears across multiple "
           "states as a numbered vehicle family -- RAR2 - Lake Ridge Owner LLC and "
           "RAR2 20080 West Dixie Highway LLC are both registered in Florida, for "
           "instance -- which confirms it is a fund series rather than a firm "
           "name, and confirms nothing about who manages it. The seller is solid: "
           "NEREJ has Cushman & Wakefield acting for EQUITY RESIDENTIAL in the "
           "$123.3 million sale of the ten-storey, 193,230 SF class A medical "
           "office building on the MGH campus, 100% let and anchored by MGH at 74%."),
    (1327, "40 COURT STREET: THIRD SEARCH, BRICKMAN STILL A LEAD. Carlyle's own "
           "release records THE CARLYLE GROUP selling 40 Court Street to BRICKMAN "
           "ASSOCIATES for $37 million in 2007, with CB Richard Ellis acting; "
           "Commercial Real Estate Direct reported it at about $40 million while "
           "pending, for a 115,069 SF building 84% let. That makes Brickman the "
           "likely owner going into this February 2018 row -- and it is the "
           "earlier-buyer inference, which this table refuses. Ashforth and Stars "
           "REI both also carry 40 Court Street on their own pages, so there are "
           "three candidates, and the Globe covered a later sale of the building "
           "at a sizeable loss in 2023. Brickman is heavily present in this table "
           "-- One Bowdoin Square, 237 Putnam Avenue, both sides of 535-545 "
           "Boylston -- which makes it more tempting and no better evidenced."),
    (1496, "131 DARTMOUTH STREET SELLER: THIRD SEARCH, NOT FOUND. The buyer is now "
           "triply sourced -- the ownership record, Boston Office Spaces' "
           "\"Dartmouth Street Office Building in Back Bay Nets $849 per Sq. Ft.\", "
           "and TA Realty's own tenancy listings for the eleven-to-twelve-storey, "
           "371,000 SF building beside Back Bay Station and across Dartmouth "
           "Street from Copley Place. NO SOURCE FOUND NAMES THE SELLER at any "
           "point, and the entity ONE-31 DARTMOUTH STREET LLC is the address with "
           "a hyphen in it."),
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
