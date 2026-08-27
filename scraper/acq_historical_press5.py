r"""Fifth press pass, and three rows where press and entity independently agree.

The useful pattern in this batch is CORROBORATION RATHER THAN FILL. Three rows
already carried a sponsor written from the entity naming its own owner, and the
press independently names the same firm:

    200 CambridgePark Dr   entity PPF OFF 200 CAMBRIDGE PARK DRIVE LLC had
                           already resolved to Morgan Stanley Prime Property
                           Fund. The Globe and GlobeSt say King Street sold to
                           "a fund advised by Morgan Stanley Real Estate
                           Investing". Same firm, two methods.
    380 Stuart St          entity JOHN HANCOCK MUTUAL LIFE had already resolved
                           to John Hancock. Skanska's release and the Globe both
                           name John Hancock Life Insurance as the seller.
    Everett St, Allston    entity ALLSTON LABWORKS DEVELOPER LLC had already
                           resolved to King Street Properties. King Street's own
                           site carries the Allston Labworks project at Western
                           Avenue and Everett Street.

That matters more than three more filled cells. The self-identifying and
prefix methods have never been tested against an independent source at scale,
and here they pass three for three.

160 FEDERAL STREET IS A CONDOMINIUM-FAULT ROW THAT PRESS CAN REPAIR. The row is
addressed "160 Federal ST G-93" and typed as a subterranean garage, because the
parcel record is a garage unit. The $190,000,000 conveyance is not a garage
unit: Bisnow's "160 Federal St Trades For $190M" is the 351,000 SF, 24-storey
Landmark Building. The address and property_type fields are left as the record
has them, because which unit the deed covered cannot be verified from here, but
the parties are now established from press rather than from the parcel's current
owner -- which is what the condominium sweep cleared on this row.

    python scraper/acq_historical_press5.py --apply
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
    (1398, "buyer", "LaSalle Investment Management",
     'Bisnow, "160 Federal St Trades For $190M", and Boston Office Spaces, "The '
     'Landmark Building at 160 Federal St. Sold to Beacon Capital": Chicago-based '
     'LaSalle Investment Management acquired the 351,000 SF, 24-storey Landmark '
     'Building for $190M, this row\'s exact price. This is LaSalle\'s second '
     'Boston purchase in the table, after 50 Post Office Square in December 2015. '
     'DATE CAVEAT: the coverage is dated November 2016 and this row records '
     '2017-04-12. A recording date trailing the announcement by months is normal '
     'and the price, building and parties all match, but the gap is recorded '
     'rather than hidden.', False),
    (1398, "seller", "Beacon Capital Partners",
     'Same reporting: LaSalle bought from Beacon Capital, which had acquired '
     'majority ownership of 160 Federal from Taurus Investment Holdings in April '
     '2015 for a reported $125M. One source renders the name "Beacon Capital '
     'Markets", which is an error for Beacon Capital Partners, the Boston firm. '
     'NOTE FOR LATER: Beacon\'s own 2015 purchase is described as MAJORITY '
     'ownership, i.e. a partial interest. It is not in this table, but if a '
     'licensed feed supplies it, it should be typed as one.', False),

    (1737, "seller", "King Street Properties",
     'The Boston Globe, "Cambridge lab sells for twice what owner invested in '
     'it", and GlobeSt, "King Street Properties\' Risk Pays Off Big": King Street '
     'sold 200 CambridgePark Drive for $165.5 million, this row\'s exact price. '
     'It had bought the property from Pfizer in mid-2014 for $54.5M and spent '
     'over $44M on renovations; the six-storey 221,845 SF building was 99% let to '
     'six tenants including Celgene and Amgen. Goulston & Storrs records advising '
     'on the reposition and sale. CORROBORATION: the buyer side already read '
     'Morgan Stanley Prime Property Fund, written from the entity PPF OFF 200 '
     'CAMBRIDGE PARK DRIVE LLC, and the press independently describes the buyer '
     'as a fund advised by Morgan Stanley Real Estate Investing.', False),

    (1086, "buyer", "Skanska USA Commercial Development",
     'Skanska\'s own release, "Skanska invests in land in Boston, Massachusetts '
     'for $177 million", with Bisnow, Connect CRE, REBusinessOnline and the Globe '
     'reporting the same December 2020 purchase of the Back Bay site for a '
     '27-storey Class A office tower. This is Skanska\'s third appearance in the '
     'table and its only purchase; the other two are its disposals of 101 Seaport '
     'and Two Drydock. CORROBORATION: the seller side already read John Hancock, '
     'written from the entity JOHN HANCOCK MUTUAL LIFE, and the Globe headline is '
     '"Major developer buys prime Back Bay office site from John Hancock". '
     'CAVEAT: this is effectively a LAND purchase -- Skanska bought a site '
     'carrying a nine-storey building it intends to replace -- so the recorded '
     'property type describes what stood there, not what was bought.', False),
]

NOTES = [
    (967, "EVERETT STREET SELLER, $181,601,288, March 2022. SEARCHED AND NOT "
          "FOUND. The buyer side already reads King Street Properties, and that "
          "is corroborated: the record entity is ALLSTON LABWORKS DEVELOPER LLC "
          "and King Street's own site carries the $915M Allston Labworks project "
          "at Western Avenue and Everett Street, on the former Stadium Auto Body "
          "site. But the seller of record, WESTERN AVENUE JOINT [VENTURE], is not "
          "named in any coverage found, which reports the project rather than the "
          "land assembly behind it."),
    (1437, "201-241 STUART STREET, $162,500,000, October 2016. SEARCHED AND NOT "
           "FOUND, AND THE OBVIOUS LEAD IS THE WRONG BUILDING. The seller of "
           "record is PARK SQ REVIVAL CORP, which points at the Park Square "
           "Building at 31 St James Avenue. This row is not that building: it is "
           "typed as a commercial parking garage, and the Park Square Building is "
           "540,000 SF of office and retail that went to foreclosure auction in "
           "March 2026 at $95M. A shared neighbourhood name is not a shared "
           "asset. Neither party written."),
]


def main(dry_run: bool):
    conn = engine.connect()
    n = 0
    for rid, side, sponsor, why, force in RESOLVE:
        cur = conn.execute(text(
            f"select {side}, coalesce({side}_canonical,'') from transactions "
            f"where id = :id"), {"id": rid}).first()
        if not cur:
            log.warning("id %s missing", rid)
            continue
        if cur[1] and not force:
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
                "n": f" | {side.upper()} RESOLVED FROM PRESS. " + why})
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
        v, d = conn.execute(text(
            f"select count(*), sum(coalesce(price,0)) from transactions "
            f"where coalesce(quarantined,0)=0 and coalesce({side}_canonical,'') <> ''"
        )).first()
        log.info("%s_canonical: %d of %d (%.0f%%), $%.2fB", side, v, tot,
                 v / tot * 100, (d or 0) / 1e9)
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
