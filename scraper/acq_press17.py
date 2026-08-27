r"""Seventeenth pass. Named buildings, and a confirmed entity applied twice.

HIVE PROPERTY OWNER LLC IS SYNERGY INVESTMENTS, confirmed against a named
property. BLDUP, writing up 55 Summer Street, states plainly that "Hive Property
Owner, LLC - a subsidiary of Synergy Investments - has sold 55 Summer Street"
for $106,646,350. That is a source naming the firm alongside the property, so
the other rows carrying the identical entity resolve too. Synergy now appears
seven times in this table, which is a portfolio rather than seven coincidences.

201-241 STUART STREET IS THE MOTOR MART GARAGE, and an earlier pass was right to
refuse the obvious lead. That note said the seller entity PARK SQ REVIVAL CORP
pointed at the Park Square Building at 31 St James Avenue, and that this row was
NOT that building because it is typed as a commercial garage. Confirmed:
CommercialCafe and BLDUP record CIM Group, with LAZ Parking Realty Investors,
closing a $162.5 million acquisition of the historic Motor Mart Garage at 201
Stuart Street in October 2016 -- this row's exact price and month. A nine-level
garage with about 50,000 SF of retail, not a 540,000 SF office building.

232 A STREET IS BREAKTHROUGH'S SECOND APPEARANCE IN A FORTNIGHT OF THIS WORK,
and it ties three rows together. The Globe, "Tishman Speyer buys another slice of
Gillette parking lots in Fort Point", 28 September 2021: Tishman and partners
closed an $80 million purchase of the 2.5-acre plot from Procter & Gamble. The
seller side already read Procter & Gamble (Gillette), written from the entity
naming its own owner. Related Beal bought the adjacent 244-284 A Street from the
same seller for $218 million in 2019. And Breakthrough -- the Tishman Speyer and
Bellco Capital venture -- is the same buyer resolved at 1 Canal Park in the
previous pass.

    python scraper/acq_press17.py --apply
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
    (1381, "buyer", "HYM Investment Group", "web",
     'BLDUP, "HYM closes on Suffolk Downs acquisition for $155 million", with '
     'CoStar ("HYM Secures $80 Million for Acquisition, Redevelopment of Suffolk '
     'Downs") and REBusinessOnline reporting the same: Boston-based HYM '
     'Investment Group bought the 161.2-acre Suffolk Downs racetrack in East '
     'Boston for $155 million in 2017, about $962,733 per acre, with Bank of the '
     'Ozarks providing $80 million of financing. The racetrack closed in summer '
     '2018. The seller side already read Sterling Suffolk Racecourse, the '
     'operating company, written from the entity naming its own owner. The record '
     'buyer entity is MCCLELLAN HIGHWAY, which is the ROAD the site fronts, so it '
     'is not decoded -- the sponsor comes from the press.'),

    (1022, "buyer", "Breakthrough Properties (Tishman Speyer / Bellco Capital)",
     "web",
     'The Boston Globe, "Tishman Speyer buys another slice of Gillette parking '
     'lots in Fort Point", 28 September 2021: Tishman and partners closed on the '
     '$80 million purchase of 232 A Street, a 2.5-acre plot that P&G put up for '
     'sale in 2021, with marketing brochures suggesting up to 500,000 SF of lab. '
     'Breakthrough Properties, the Tishman Speyer and Bellco Capital life-science '
     'venture, subsequently filed plans for the site including a waterfront park '
     'on the Fort Point Channel. Same buyer as 1 Canal Park. The seller side '
     'already read Procter & Gamble (Gillette), and Related Beal bought the '
     'adjacent 244-284 A Street from the same seller for $218M in 2019.'),

    (1437, "buyer", "CIM Group / LAZ Parking Realty Investors", "web",
     'CommercialCafe, "CIM Group\'s Boston CRE Shopping Spree Continues", and '
     'BLDUP: in October 2016 CIM Group, partnered with LAZ Parking Realty '
     'Investors, closed a $162.5 million acquisition of the historic MOTOR MART '
     'GARAGE at 201 Stuart Street -- this row\'s exact price and month. Nine '
     'levels of parking with about 50,000 SF of ground and lower-level retail. '
     'Recorded as the venture, not one partner. THIS CONFIRMS AN EARLIER REFUSAL '
     'ON THIS ROW: a previous pass declined to read the seller entity PARK SQ '
     'REVIVAL CORP as the Park Square Building at 31 St James Avenue, on the '
     'ground that this row is typed as a garage. It is the Motor Mart Garage. The '
     'seller stays blank because nothing found names the party behind PARK SQ '
     'REVIVAL CORP.'),

    (1436, "seller", "Boston University", "self_identifying",
     'The record entity is TRUSTEES OF BOSTON UNIVERSITY, which names its own '
     'owner -- the university\'s corporate form. No decoding step, so nothing to '
     'get wrong. Boston University is independently present in this table: its '
     'own account of selling nine Kenmore Square properties to Related Real Estate '
     'Fund II is what corrected the RREF/Rialto error, and the buyer on this row '
     'is Related Beal.'),

    (1384, "buyer", "Synergy Investments", "prefix_confirmed",
     'HIVE PROPERTY OWNER LLC IS SYNERGY INVESTMENTS, confirmed against a named '
     'property: BLDUP, writing up the $106,646,350 sale of 55 Summer Street, '
     'states that "Hive Property Owner, LLC - a subsidiary of Synergy Investments '
     '- has sold 55 Summer Street". That is the firm named alongside the property, '
     'so the identical entity on this row resolves to the same sponsor. The seller '
     'side already read DivcoWest.'),
    (1383, "buyer", "Synergy Investments", "prefix_confirmed",
     'Same confirmed entity: HIVE PROPERTY OWNER LLC is a Synergy Investments '
     'subsidiary per BLDUP\'s reporting on 55 Summer Street. Written even though '
     'this row is below the $50M working threshold, because applying a confirmed '
     'decode costs nothing.'),
]

NOTES = [
    (1496, "131 DARTMOUTH STREET SELLER, $315,000,000, December 2015: NOT FOUND. "
           "The buyer is solid -- the ownership record shows TA Associates Realty, "
           "now TA Realty, acquiring the 417,000 SF building at exactly this price "
           "-- but no source found names the SELLER, and the record entity ONE-31 "
           "DARTMOUTH STREET LLC is the address with a hyphen in it. Note that "
           "this row is ADDRESSED 48-20 Buckingham Street in the parcel data while "
           "the asset is 131 Dartmouth Street; that mismatch is in the assessment "
           "record, not an error here."),
    (1592, "80-90 FIRST STREET, $156,435,584, February 2022: NOT FOUND. The buyer "
           "entity is 20 CAMBRIDGE PLACE GROUND OWNER LLC and the word GROUND "
           "suggests a ground-lease interest rather than a fee conveyance, which "
           "would explain the silence -- ground leases are not reported as trades. "
           "80 First Street is part of the CambridgeSide complex in East "
           "Cambridge, which New England Development redeveloped as 20 "
           "CambridgeSide, and that name matches the entity. But no source found "
           "names a party to THIS conveyance at THIS price, and inferring the "
           "owner of a complex from its brand name is not resolution."),
    (1643, "100 BROADWAY BUYER, $98,991,934, October 2020: NOT FOUND. The seller "
           "is established -- the entity RLJ CAMBRIDGE HOTEL LLC names RLJ Lodging "
           "Trust, which is independently confirmed in this table at the Fairmont "
           "Copley Plaza. RLJ's investor-relations releases cover its 2020 "
           "dispositions but none found matches this Cambridge asset at this "
           "price, and searches keep returning Xenia's separate $107.5M sale of "
           "the Residence Inn at 120 Broadway the same month. ODYSSEY PROPCO LLC "
           "is not decoded."),
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
