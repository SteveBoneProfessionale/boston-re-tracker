r"""Thirtieth pass. CCF confirmed, and a refusal that was right to be made.

CCF IS CABOT, CABOT & FORBES. An earlier pass in this project listed it among
the names it would NOT write, in these words:

    CCF SMITH PLACE PROPERTY CO   CCF looks like Cabot, Cabot & Forbes. That is
                                  exactly the reasoning that produced the RREF /
                                  Rialto error, and it is not written.

The guess was correct. Cambridge Day, 1 November 2021: "Twelve acres owned by
CABOT, CABOT & FORBES in the Cambridge Highlands neighborhood sold for $120
million to a Denver firm called Healthpeak Properties", covering "eight
industrial parcels on MOONEY STREET AND SMITH PLACE along the MBTA commuter rail
and near Alewife Station". Both CCF rows in this table are on Mooney Street and
Smith Place, both sold to Healthpeak, both in that window.

    Being right by accident and being right on evidence are different states,
    and only the second one belongs in the table. The refusal cost two cells for
    a few weeks and it is what makes every other prefix in here trustworthy.

CAMBRIDGE DAY ALSO SIZES THE WHOLE CAMPAIGN, which this table had only inferred
from entity numbering: Healthpeak spent about $373 million on 18 parcels from
August 2021, buying "through three limited liability companies with variations
of the name LS ALEWIFE". That is the LS prefix confirmed a second time, from a
source that names the convention itself rather than one property.

155 NORTH BEACON STREET IS A FOURTH ROUND TRIP. BLDUP: IQHQ acquired the
3.2-acre site -- a two-storey, roughly 140,000 SF building housing the Sound
Museum, a rehearsal complex founded in the 1980s where more than 300 musicians
rented space -- from THE HAMILTON COMPANY for $50 million, then secured a $486.5
million Citizens Bank loan for a 409,395 SF life-science campus. It later sold
the site for $35.25 million.

    python scraper/acq_press30.py --apply
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

CCF = (
    "CCF IS CABOT, CABOT & FORBES, CONFIRMED AGAINST NAMED PROPERTIES, AND THIS "
    "REVERSES A DELIBERATE EARLIER REFUSAL. A previous pass listed CCF SMITH "
    "PLACE PROPERTY CO among the names it would not write, on the ground that "
    "\"CCF looks like Cabot, Cabot & Forbes, and that is exactly the reasoning "
    "that produced the RREF / Rialto error\". The guess was right, but it was "
    "right to refuse it until a source arrived. Cambridge Day, 1 November 2021: "
    "\"Twelve acres owned by CABOT, CABOT & FORBES in the Cambridge Highlands "
    "neighborhood sold for $120 million to a Denver firm called Healthpeak "
    "Properties\", comprising \"eight industrial parcels on MOONEY STREET AND "
    "SMITH PLACE along the MBTA commuter rail and near Alewife Station\". BLDUP "
    "covers the same deal as \"11-Acre Alewife Site Acquired for $120M\" and "
    "CoStar as \"Healthpeak Properties Rolls Up Multiple Properties and Sites in "
    "Alewife\". Both CCF rows in this table are on those two streets, both sold "
    "to Healthpeak, both inside that window."
)

RESOLVE = [
    (1594, "seller", "Cabot, Cabot & Forbes", "prefix_confirmed", CCF +
     " This row is 61-67 Smith Place at $72,000,000, matching Healthpeak's own "
     "disclosure of 67 Smith Place at $72 million closing January 2022."),
    (1611, "seller", "Cabot, Cabot & Forbes / ASVRF", "prefix_confirmed", CCF +
     " This row is 61 Mooney Street at $64,000,000, one of the Mooney Street "
     "parcels Healthpeak itemised at $123 million with 13 Mooney Street and 127 "
     "Smith Place. THE ENTITY NAMES TWO PARTIES -- CCF ASVRF 45-61 MOONEY LLC -- "
     "so both are carried through under the rule that a joint venture resolves to "
     "all partners. ASVRF IS NOT EXPANDED: it reads as a fund acronym and no "
     "source found says what it stands for, so it is kept exactly as the record "
     "renders it, in the same way P6 is kept alongside Saracen Properties at 6-10 "
     "Oliver Street and CPI alongside Brickman at 237 Putnam Avenue."),

    (1048, "seller", "The Hamilton Company", "web",
     'BLDUP, "IQHQ Acquires 3.2 Acre Brighton Property for $50M": IQHQ bought 155 '
     'North Beacon Street from THE HAMILTON COMPANY for $50 million -- this row '
     'exactly, June 2021. The site carried a two-storey, roughly 140,000 SF '
     'commercial building housing the Sound Museum, a rehearsal complex founded in '
     'the 1980s by Bill "Des" Desmond where more than 300 musicians rented space; '
     'WBUR covered their displacement as "Wiped out by biotech". IQHQ\'s approved '
     'scheme was a 409,395 SF life-science campus in three buildings, financed by '
     'a $486.5 million Citizens Bank loan. ROUND TRIP: IQHQ later sold the site '
     'for $35.25 million, roughly 70% of what it paid, without building it.'),
]

LS_NOTE = (
    " | THE LS CONVENTION IS NOW CONFIRMED FROM A SECOND, STRONGER DIRECTION. "
    "This table established LS <LOCATION> LLC as Healthpeak by matching two of "
    "the eight disclosed Alewife transactions on property, price and month. "
    "Cambridge Day states the convention itself: Healthpeak spent about $373 "
    "million on 18 parcels from August 2021, buying \"through three limited "
    "liability companies with variations of the name LS ALEWIFE\". A source "
    "naming the naming-convention is better evidence than a source naming one "
    "property, and it arrived after the decode rather than before it."
)


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

    # Sweep any remaining CCF entity, now that the prefix is confirmed.
    for side in ("buyer", "seller"):
        for rid, ent, price, addr in conn.execute(text(f"""
                select id, {side}, price, address from transactions
                 where coalesce(quarantined,0) = 0
                   and upper(coalesce({side},'')) like 'CCF %'
                   and coalesce({side}_canonical,'') = ''"""), ):
            log.info("id=%-5s %-6s %-34s -> Cabot, Cabot & Forbes [family sweep]",
                     rid, side, (ent or "")[:34])
            if not dry_run:
                conn.execute(text(f"""
                    update transactions
                       set {side}_canonical = 'Cabot, Cabot & Forbes',
                           {side}_confidence = 'registry_confirmed',
                           {side}_resolution_basis = 'prefix_confirmed',
                           notes = coalesce(notes,'') || :n
                     where id = :id"""),
                    {"id": rid, "n": f" | {side.upper()} RESOLVED. " + CCF})
                n += 1

    if not dry_run:
        r = conn.execute(text(
            "update transactions set notes = coalesce(notes,'') || :n "
            "where buyer_canonical = 'Healthpeak Properties' "
            "and coalesce(quarantined,0) = 0"), {"n": LS_NOTE})
        log.info("\nLS confirmation note added to %d Healthpeak rows", r.rowcount)
        conn.commit()

    log.info("%d sides written", n)
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
