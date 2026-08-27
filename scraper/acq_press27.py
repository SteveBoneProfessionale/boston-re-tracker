r"""Twenty-seventh pass. NC-P decodes itself, and four leads that stay leads.

625 MOUNT AUBURN STREET, and the entity turns out to be an acronym of the buying
venture. The Davis Companies' own release is titled "The Davis Cos. Harvests
Cambridge Office Building for $57.5M to NORTH COLONY, PARADIGM" -- a 137,400 SF
office building near Harvard Square, about 97% occupied with Charles River
Analytics and Mount Auburn Hospital as anchors, which Davis had bought in August
2017 for $45 million. The buyer entity on this row is NC-P MOUNT AUBURN LLC:
North Colony and Paradigm, initialled and hyphenated. Confirmed against the
property by the seller's own announcement, not read off the letters.

    Another venture that a single-partner record would have halved, and the
    eighth in this table. North Colony Asset Management was already present here
    -- it also bought 343 Congress from BGO in 2025 -- so only Paradigm was
    genuinely new.

FOUR LEADS RECORDED AND NOT WRITTEN. Every one of them would raise the number
and every one rests on the same defective step: knowing who owns a building now,
or who developed it, and treating that as knowledge of who transacted then.

    40 Court St        Carlyle sold it to BRICKMAN for $37M in 2007, so Brickman
                       is the likely 2018 seller -- but Ashforth and Stars REI
                       also carry the building, and this is the earlier-buyer
                       inference that produced the RREF error.
    70-76 Everett St   The Allston Yards partnership is documented in full --
                       New England Development with Stop & Shop, Bozzuto and
                       Southside Investment Partners -- and the buyer entity is
                       ALLSTON YARDS PARCEL B DEVELOPMENT. But which of those
                       four holds Parcel B is not stated anywhere found, and
                       naming all four would attribute a purchase to partners
                       who may not be in it.
    116 W First St     Tishman Speyer's own site carries "One Channel & Channel
                       Center Garage". No acquisition date.
    350 Main St        The seller entity FIREHOUSE INN LLC is confirmed as the
                       right ASSET -- the Kendall Hotel occupies a restored
                       19th-century Victorian firehouse at 350 Main Street -- but
                       an entity naming the building is not an entity naming a
                       sponsor.

    python scraper/acq_press27.py --apply
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
    (1656, "buyer", "North Colony Asset Management / Paradigm Properties",
     'The Davis Companies\' own release, "The Davis Cos. Harvests Cambridge Office '
     'Building for $57.5M to North Colony, Paradigm", October 2019 -- this row '
     'exactly. THE ENTITY IS AN ACRONYM OF THE VENTURE: NC-P MOUNT AUBURN LLC is '
     'North Colony and Paradigm, and the seller\'s own announcement names both, so '
     'the initials are confirmed rather than decoded. Recorded as the venture, not '
     'one partner -- the eighth venture in this table found to have been recorded '
     'as a single party. North Colony Asset Management was already present here, '
     'buying 343 Congress Street from BGO in 2025; Paradigm Properties is new.'),
    (1656, "seller", "The Davis Companies",
     'Same release, from the seller. Davis had bought the 137,400 SF building in '
     'August 2017 for $45 million and sold it at $57.5 million, roughly 97% let '
     'with Charles River Analytics and Mount Auburn Hospital as anchors. The '
     'record entity EOS AT 625 MOUNT AUBURN LLC names the building\'s brand, not '
     'the sponsor. The Davis Companies now appears seven times in this table, '
     'including inside the David Marcus Partners venture at 725 Concord Avenue.'),
]

NOTES = [
    (1327, "40 COURT STREET, $54,000,000, February 2018: A LEAD, NOT A "
           "RESOLUTION. Carlyle's own release records THE CARLYLE GROUP selling 40 "
           "Court Street to BRICKMAN ASSOCIATES for $37 million in 2007, with CBRE "
           "representing the seller, and Commercial Real Estate Direct reported the "
           "deal at about $40 million while it was pending. That makes Brickman the "
           "likely owner going into 2018 and therefore the likely seller here. "
           "LIKELY IS NOT RESOLVED: this is the same two-step inference -- earlier "
           "buyer plus no known intervening trade -- that produced the RREF/Rialto "
           "error, and both Ashforth and Stars REI also carry 40 Court Street on "
           "their own pages, which makes three candidates. Neither the buyer entity "
           "MDR BOSTON COURT LLC nor the seller entity FORTY COURT BOSTON is "
           "decoded. Brickman is separately confirmed in this table at One Bowdoin "
           "Square, 237 Putnam Avenue and both sides of 535-545 Boylston Street."),
    (1007, "70-76 EVERETT STREET, $51,500,000, November 2021: THE PROJECT IS "
           "IDENTIFIED, THE PARTY IS NOT. The buyer entity is ALLSTON YARDS PARCEL "
           "B DEVELOPMENT, and Allston Yards is documented in full: New England "
           "Development in partnership with STOP & SHOP, BOZZUTO and SOUTHSIDE "
           "INVESTMENT PARTNERS, a 1.2 million SF mixed-use redevelopment of an "
           "operating Stop & Shop and its car parks, with a 165-unit apartment "
           "building, a new Stop & Shop, 117,000 SF of retail and 350,000 SF of "
           "office and lab. Financing closed and construction began in NOVEMBER "
           "2021, matching this row's month. WHY IT IS STILL BLANK: nothing found "
           "states which of those four partners holds PARCEL B, and writing all "
           "four would attribute a purchase to partners who may have no interest "
           "in it -- the mirror of the half-recorded-venture error, made in the "
           "other direction. The seller of record is empty, so the conveyance may "
           "well be a land contribution into the venture rather than a purchase."),
    (1503, "116 WEST FIRST STREET, $52,000,000, December 2015: A LEAD. The asset "
           "is the One Channel Center Garage in South Boston, and TISHMAN SPEYER "
           "carries \"One Channel & Channel Center Garage\" on its own property "
           "page. Tishman Speyer is independently confirmed in this table at 105 "
           "West First Street and stands behind Breakthrough Properties at two "
           "more. But its page states NO ACQUISITION DATE, and current ownership "
           "does not establish who bought in 2015 -- the standard that kept "
           "Breakthrough a lead at 1 Canal Park until a source supplied the year. "
           "The seller of record is empty."),
    (1560, "350 MAIN STREET, $53,000,000, June 2024: THE ASSET IS CONFIRMED, THE "
           "SELLER IS NOT. The buyer side reads MIT. The seller entity is FIREHOUSE "
           "INN LLC, and that is the right building rather than a coincidence: the "
           "Kendall Hotel at 350 Main Street occupies a restored 19th-century "
           "Victorian FIREHOUSE, and is listed with Historic Hotels of America on "
           "that basis. But an entity that names the BUILDING is not an entity that "
           "names a SPONSOR -- the same distinction that keeps DAVENPORT OWNER at "
           "21 Thorndike and PUTNAM CIRCLE ASSOCIATES undecoded. No source found "
           "reports this conveyance; coverage of MIT and Kendall Square in this "
           "period is dominated by its $361.5M sale of 730-750 Main Street to "
           "BioMed Realty, which runs the other way."),
    (1282, "368 CONGRESS STREET BUYER, $64,000,000, August 2018: NOT FOUND. The "
           "seller is established as Norwich Partners, from the entity NORWICH "
           "PARTNERS BOSTON LLC and corroborated by press naming Norwich as the "
           "seller of the Envoy Hotel to Hersha. The asset is identified too: a "
           "six-storey, 102,000 SF office building converted to a 120-room extended "
           "stay Residence Inn, completed 2013, which Norwich developed after "
           "obtaining a use variance in 2011. But no coverage of the 2018 "
           "conveyance was found and the buyer entity 370 CONGRESS STREET LLC is "
           "the address next door."),
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
