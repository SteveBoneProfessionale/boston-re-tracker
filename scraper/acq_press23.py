r"""Twenty-third pass. Two prefixes confirmed on single rows, and a lender's exit.

DMP IS DAVID MARCUS PARTNERS, confirmed against the property. Cambridge Day, 12
December 2021: "Healthpeak spent $80 million for 725 Concord Ave. and 35, 49 and
59 Smith Place on November 3, all owned by DAVID MARCUS PARTNERS, A VENTURE OF
THE DAVIS COS. AND MARCUS PARTNERS". The seller entity on this row is DMP
BURLINGTON CONCORD LLC. The asset is an 85,000 SF, six-storey medical office
building let wholly to Mount Auburn Hospital on a fifteen-year lease signed in
January 2017, and to an affiliate of Beth Israel Lahey Health. It last traded in
February 2007 for $18.4 million.

    That is another venture that would have been recorded as one firm. The Davis
    Cos. already appears four times in this table on its own and twice more in
    other ventures; without the DMP decode, this row would have credited neither
    Davis nor Marcus.

CRE-MLL IS MLL CAPITAL, confirmed against the property. Bisnow's deal sheet of
12 November 2021 and Banker & Tradesman: MLL Capital acquired 33-41 Farnsworth
Street in December 2021 for $73.6 million for an office-to-lab conversion, with
close to $80 million of mortgage financing from Starwood Property Trust. The
building had briefly housed GE's headquarters when it moved to Boston in 2016.
The seller side already read TIAA (Nuveen), from the T-C Fort Point Creative
entity confirmed at 374 Congress Street.

AND THE END OF THAT STORY IS WORTH RECORDING, because it is a lab-conversion
loss in full. Starwood -- the LENDER -- later took the building for $57.2
million, assuming the remaining debt it had itself provided. A $73.6M purchase
plus a conversion, ending with the lender owning it at $57.2M.

    python scraper/acq_press23.py --apply
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
    (987, "buyer", "MLL Capital", "prefix_confirmed",
     'Bisnow\'s Boston deal sheet of 12 November 2021 and Banker & Tradesman, '
     '"Lender Buys Fort Point Lab Conversion for $57M", both name MLL CAPITAL as '
     'the acquirer of 33-41 Farnsworth Street in December 2021 for $73.6 million '
     '-- this row exactly -- for an office-to-lab conversion, with close to $80 '
     'million of mortgage financing from Starwood Property Trust. The building had '
     'briefly served as GE\'s headquarters when the company moved to Boston in '
     '2016. THIS CONFIRMS THE CRE-MLL PREFIX against the property: the entity is '
     'CRE-MLL FARNSWORTH PROPERTY OWNER LLC. HOW IT ENDED: Starwood, the lender, '
     'later took the building for $57.2 million, assuming the remaining debt it '
     'had provided -- a purchase at $73.6M plus a conversion, finishing with the '
     'lender owning it below cost. The seller side already read TIAA (Nuveen), '
     'from the T-C Fort Point Creative entity confirmed at 374 Congress Street.'),

    (1603, "seller", "David Marcus Partners (The Davis Cos. / Marcus Partners)",
     "prefix_confirmed",
     'Cambridge Day, 12 December 2021: "Healthpeak spent $80 million for 725 '
     'Concord Ave. and 35, 49 and 59 Smith Place on November 3, all owned by DAVID '
     'MARCUS PARTNERS, a venture of The Davis Cos. and Marcus Partners." This row '
     'is $80,000,000 in November 2021 and the seller entity is DMP BURLINGTON '
     'CONCORD LLC, so DMP is confirmed against the property rather than decoded. '
     'The asset is an 85,000 SF six-storey medical office building let wholly to '
     'Mount Auburn Hospital on a fifteen-year lease signed January 2017, and to an '
     'affiliate of Beth Israel Lahey Health; it last traded in February 2007 for '
     '$18.4 million. RECORDED AS THE VENTURE WITH BOTH PARENTS NAMED: without the '
     'decode this row would have credited neither The Davis Cos. nor Marcus '
     'Partners, and Davis already appears six times in this table. NOTE that the '
     '$80M covered 725 Concord AND three Smith Place addresses, so this is a '
     'portfolio price on one row.'),
]

NOTES = [
    (1394, "201 NEWBURY STREET, $75,000,000, April 2017: NOT FOUND. The asset is "
           "identified -- a multi-floor RETAIL CONDOMINIUM at the front of the "
           "former Prince School building on the corner of Exeter Street, one of "
           "Newbury Street's best retail blocks -- and the seller entity TWO 01 "
           "NEWBURY-PRINCE LLC matches that description exactly. But no coverage "
           "of the 2017 conveyance was found; searches return the smaller Newbury "
           "Street trades ($26.75M at 2 Newbury, $42M, $40M) that dominate "
           "reporting on the street. The buyer entity is TRPF 201 NEWBURY STREET, "
           "and TRPF also appears at 21-29 Harrison Avenue as TRPF 99/101 BOSTON "
           "OFFICE -- two rows on one undecoded prefix, worth confirming if a "
           "source ever names a firm at either address. NOTE: the buyer on this "
           "row previously read \"Newmark Christopher Ruggiero\", a broker plus an "
           "executive taken from the care-of line, and was cleared."),
    (1708, "1000 MASSACHUSETTS AVENUE BUYER, $69,500,000, December 2016: A LEAD, "
           "NOT A RESOLUTION. The seller is Cambridge College, which names itself "
           "on the record and was selling its own campus building. INTERCONTINENTAL "
           "REAL ESTATE CORPORATION carries \"1000 Mass Ave\" on its own property "
           "page, describing a 105,062 SF office building between Harvard and "
           "Kendall Square, and Intercontinental is independently confirmed in "
           "this table twice -- buying the Canal Park complex in 2016 and the Yard "
           "5 industrial portfolio in 2022. The buyer entity 1000 MASSACHUSETTS "
           "AVE MA LLC even resembles its [NUMBER] CANAL PARK MASSACHUSETTS LLC "
           "convention. But INTERCONTINENTAL'S PAGE GIVES NO ACQUISITION DATE, and "
           "current ownership does not establish who bought in 2016 -- the same "
           "reasoning that kept Breakthrough Properties a lead at 1 Canal Park "
           "until a source supplied the year."),
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
            conn.execute(text(f"""
                update transactions
                   set {side}_canonical = :s, {side}_confidence = 'registry_confirmed',
                       {side}_resolution_basis = :b,
                       notes = coalesce(notes,'') || :n
                 where id = :id"""), {
                "s": sponsor, "id": rid, "b": basis,
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
