r"""Forty-eighth pass. Landmarks, and BMR reversed by an entity that named a STREET.

315 KENDALL STREET, and the entity turns out to name the address in the
reporting rather than the address on the row. An earlier pass refused BMR-THIRD
LLC in these words: "BMR-<ASSET> LLC is BioMed Realty's standard convention ...
Every arrow points one way. But NO SOURCE FOUND NAMES BIOMED REALTY AT 315
KENDALL STREET, and 175 Federal Street was left as a lead on exactly this
reasoning."

Cambridge Day, 18 April 2023: "BioMed buys Eversource gas transfer station,
making whole the site of its 585 Kendall tower" -- 0.32 acres from NSTAR Gas, a
subsidiary of Eversource, for $49.5 million, closing 7 April 2023. This row is
$49,488,491 in April 2023.

    THIRD IS 330 THIRD STREET, the gas transfer station's own address. The
    entity was naming the asset all along, in the address the press uses, which
    is not the address the parcel record uses. That is the fifth row in this
    table where the parcel address and the asset address differ -- after 131
    Dartmouth, 250/254 Summer, the Paddock Building and 176 Federal -- and the
    first where the mismatch was what hid the decode.

    NOTE ON SIZE: this row records 541,705 SF, which is the parcel's area for
    the whole 585 Kendall site, not the 0.32-acre strip that changed hands.

65 LONG WHARF IS NOT THE MARRIOTT. REBusinessOnline: "ELV Associates Sells Long
Wharf in Boston to Capital Properties for $34M" -- the five-storey CUSTOM HOUSE
BLOCK and the adjoining four-storey GARDINER BUILDING, office and retail,
Cushman & Wakefield acting for the seller. The Boston Marriott Long Wharf is a
different building on the same wharf, and searching the wharf name returns it
first every time.

125 LINCOLN STREET: Oxford Properties, the real-estate arm of OMERS, bought the
converted parking garage in 2017 -- reported at $40 million against this row's
$39,500,000 -- and The Real Reporter's brief is headed "Intercontinental
Harvesting Lincoln Street Garage", which names the seller.

    python scraper/acq_press48.py --apply
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

BMR = (
    "BMR-<ASSET> LLC IS BIOMED REALTY, CONFIRMED AGAINST THE PROPERTY, AND THIS "
    "REVERSES A DELIBERATE EARLIER REFUSAL. That note said every arrow pointed at "
    "BioMed -- the convention, its confirmed presence at 321 Harrison Avenue, the "
    "BRE-BMR 215 FIRST STREET entity, six million SF in Greater Boston -- but that "
    "no source named BioMed Realty AT 315 KENDALL STREET. Cambridge Day, 18 April "
    "2023, supplies it under a different address: \"BioMed buys Eversource gas "
    "transfer station, making whole the site of its 585 Kendall tower\", 0.32 "
    "acres from NSTAR Gas for $49.5 million, closing 7 April 2023. THIRD IN THE "
    "ENTITY IS 330 THIRD STREET, the station's own address -- the entity named the "
    "asset all along, in the address the press uses rather than the one the parcel "
    "record uses. Fifth row in this table where parcel address and asset address "
    "differ, and the first where that mismatch is what hid the decode."
)

RESOLVE = [
    (1572, "buyer", "BioMed Realty (Blackstone)", "prefix_confirmed", BMR),
    (1572, "seller", "NSTAR Gas (Eversource)", "web",
     'Cambridge Day, 18 April 2023: BioMed Realty bought the gas transfer station '
     'from NSTAR GAS, a subsidiary of EVERSOURCE, for $49.5 million with a 7 April '
     '2023 closing -- this row\'s price and month. The purchase made whole the site '
     'of BioMed\'s 585 Kendall life-science tower. A utility selling an operational '
     'parcel to a developer assembling around it is an ordinary conveyance and is '
     'recorded as one.'),

    (1537, "buyer", "Capital Properties", "web",
     'REBusinessOnline, "ELV Associates Sells Long Wharf in Boston to Capital '
     'Properties for $34M", and CoStar, "Capital Properties Closes On Long Wharf '
     'Portfolio": a two-building office and retail property comprising the '
     'five-storey CUSTOM HOUSE BLOCK and the adjoining four-storey GARDINER '
     'BUILDING. This row is $34,000,000 in April 2015. NOT THE MARRIOTT: the Boston '
     'Marriott Long Wharf is a separate hotel on the same wharf and dominates every '
     'search of the name. Capital Properties was already a canonical in this table '
     'from separate reporting.'),
    (1537, "seller", "ELV Associates", "web",
     'Same reporting names ELV Associates as the seller, with Robert Griffin, '
     'Edward Maher, Matt Pullen, Geoffrey Millerd and Justin Smith of Cushman & '
     'Wakefield acting for it.'),

    (1389, "buyer", "Oxford Properties Group", "web",
     'Oxford Properties -- the real-estate arm of the Canadian pension fund OMERS '
     '-- acquired 125 Lincoln Street in 2017, reported at $40 million against this '
     'row\'s $39,500,000 in May 2017. The asset is a converted parking garage with '
     'office and retail in the Leather District, and the buyer entity is 125 '
     'LINCOLN STREET OWNER LLC. Oxford is independently confirmed in this table '
     'buying 745 Atlantic Avenue from Beacon Capital in 2015 and, with Alony-Hetz, '
     'the Davenport Building in 2017 -- three Boston purchases inside two years.'),
    (1389, "seller", "Intercontinental Real Estate Corp.", "web",
     'The Real Reporter\'s brief is headed "Intercontinental Harvesting Lincoln '
     'Street Garage" -- harvesting being the trade term for selling a stabilised '
     'asset -- which names the seller of the Lincoln Street garage that Oxford '
     'bought. Intercontinental Real Estate Corp is independently confirmed three '
     'times elsewhere in this table: buying the Canal Park complex in 2016, selling '
     'One Canal Park in 2021, and buying the Yard 5 industrial portfolio in 2022.'),

    (1414, "seller", "Rockwood Capital", "web",
     'CoStar, "Rockwood Capital Sells Courtyard Boston Logan Airport Hotel", and '
     'HFF\'s own release closing the sale of the 351-room Courtyard Boston Logan '
     'Airport at 225 William F. McClellan Highway in February 2017 -- this row\'s '
     'month -- marketing the property on behalf of AFFILIATES OF ROCKWOOD CAPITAL '
     'LLC. THE ENTITY IS PARTLY LEGIBLE AND ONLY PARTLY WRITTEN: the seller of '
     'record is OPROCK BOSTON FEE LLC, and Ocean Properties Hotels Resorts & '
     'Affiliates carries the Courtyard Boston Logan Airport in its portfolio while '
     'Oprock Boston TRS LLC is the operating entity -- so OPROCK reads as Ocean '
     'Properties plus Rockwood. That reading is NOT written into the sponsor, '
     'because the press names only Rockwood as seller and Ocean Properties could '
     'as easily be the operator continuing after the sale. The buyer entity '
     'SLUMBER TIME LLC is not decoded.'),
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
        log.info("id=%-5s %-6s %-34s -> %-36s [%s]", rid, side,
                 (cur[0] or "")[:34], sponsor, basis)
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

    # BMR is now confirmed; sweep the family.
    for side in ("buyer", "seller"):
        for rid, ent in conn.execute(text(f"""
                select id, {side} from transactions
                 where coalesce(quarantined,0) = 0
                   and upper(coalesce({side},'')) like 'BMR-%'
                   and coalesce({side}_canonical,'') = ''""")):
            log.info("id=%-5s %-6s %-34s -> BioMed Realty (Blackstone) [sweep]",
                     rid, side, (ent or "")[:34])
            if not dry_run:
                conn.execute(text(f"""
                    update transactions
                       set {side}_canonical = 'BioMed Realty (Blackstone)',
                           {side}_confidence = 'registry_confirmed',
                           {side}_resolution_basis = 'prefix_confirmed',
                           notes = coalesce(notes,'') || :n
                     where id = :id"""),
                    {"id": rid, "n": f" | {side.upper()} RESOLVED. " + BMR})
                n += 1

    if not dry_run:
        conn.execute(text(
            "update transactions "
            "set notes = coalesce(notes,'') || :n where id = 1572"), {
            "n": (" | SIZE CAVEAT. This row records 541,705 SF, which is the "
                  "parcel's area for the whole 585 Kendall site, not the 0.32-acre "
                  "gas transfer station strip that actually changed hands for "
                  "$49.5 million. Any $/SF derived from it describes nothing.")})
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
