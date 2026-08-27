r"""Twenty-fourth pass. A RECAPITALISATION recorded as a whole-building sale.

11 BEACON STREET IS NOT A SALE. Bisnow's later deal-sheet coverage sets out the
history: Synergy acquired the fourteen-storey, 152,060 SF 1922 building in 2013
for $35 million FROM DIVCOWEST, and in 2016 "the investment firm RECAPITALISED
the property and brought in GREENOAK REAL ESTATE as a partner in a $63M deal".

Synergy did not leave. GreenOak bought in. The $63,000,000 on this row is the
price of that recapitalisation, not of a building changing hands, and recorded
as an asset_sale it reads as the latter. It becomes partial_interest with
is_recapitalization set, which is what that column exists for.

    FIFTH MIS-TYPED PARTIAL INTEREST, after 101 Federal (50%), Congress Square
    (95%), the Taj Boston (95%) and 1 Hampshire Street (two floors). The
    registry cannot distinguish a recapitalisation from a sale, so every one of
    these is invisible until press says otherwise.

AND A SEVENTH PARTNER-CONTINUATION, the same firm as three of the others.
Synergy is on both sides here -- selling a stake and staying in. It is NOT
quarantined, for the reason established at 535-545 Boylston: GreenOak genuinely
entered and real capital genuinely moved. The pairing is corroborated elsewhere
in this table, where Synergy Investments and GreenOak together bought 2 Center
Plaza from Shorenstein for $365 million.

105 WEST FIRST STREET IS TISHMAN SPEYER'S THIRD APPEARANCE. BLDUP, "South Boston
Property Trades for $80 Million": Tishman Speyer bought the approved
seven-storey, 250,000 SF R&D site from ARES MANAGEMENT and CV PROPERTIES, who
had paid $25 million for it in 2017. Tishman also stands behind Breakthrough
Properties at 1 Canal Park and 232 A Street. Breakthrough later let the whole
building to CRISPR on a 263,500 SF lease, so the three rows are one strategy.

    python scraper/acq_press24.py --apply
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

RECAP_NOTE = (
    "TYPE CORRECTED TO PARTIAL INTEREST: THIS IS A RECAPITALISATION. Bisnow's "
    "coverage of the building's history states that Synergy acquired 11 Beacon "
    "Street in 2013 for $35 million from DivcoWest, and that in 2016 the firm "
    "\"recapitalised the property and brought in GreenOak Real Estate as a "
    "partner in a $63M deal\". Synergy did not sell and leave -- it sold a stake "
    "and stayed. The $63,000,000 recorded here is the recapitalisation price, not "
    "the price of a fourteen-storey, 152,060 SF building changing hands, and left "
    "as an asset_sale it reads as the latter. This is the FIFTH mis-typed partial "
    "interest found in this table, after 101 Federal Street at 50%, Congress "
    "Square at 95%, the Taj Boston at 95% and 1 Hampshire Street, which was two "
    "floors. The registry cannot distinguish a recapitalisation from a sale, so "
    "each one is invisible until the press says otherwise. NO PERCENTAGE IS "
    "WRITTEN because no source states one. SYNERGY IS ON BOTH SIDES and this is "
    "NOT quarantined, for the reason established at 535-545 Boylston Street: "
    "GreenOak genuinely entered and real capital genuinely moved. The pairing is "
    "corroborated independently in this table, where Synergy and GreenOak "
    "together bought 2 Center Plaza from Shorenstein for $365 million."
)

RESOLVE = [
    (1191, "buyer", "Tishman Speyer", "web",
     'BLDUP, "South Boston Property Trades for $80 Million": Tishman Speyer '
     'acquired 105 West First Street for $80 million -- an approved seven-storey, '
     '250,000 SF research and development building on a 42,219 SF site across from '
     'A Street Park, by the Channel Center development and Broadway station. This '
     'row is $80,000,000 in July 2019. Tishman Speyer stands behind Breakthrough '
     'Properties, which appears twice more in this table at 1 Canal Park and 232 A '
     'Street; Breakthrough later let this entire building to CRISPR on a 263,500 '
     'SF lease, so the three rows are one strategy rather than three deals.'),
    (1191, "seller", "Ares Management / CV Properties", "web",
     'Same reporting: Tishman acquired the site from ARES MANAGEMENT and CV '
     'PROPERTIES, which had bought it in 2017 for $25 million. CV Properties '
     'carries 105 W First Street on its own portfolio page. Recorded as the '
     'venture, not one partner. Ares Management already appears elsewhere in this '
     'table.'),

    (1452, "buyer", "Synergy Investments / GreenOak Real Estate", "web",
     'The post-recapitalisation ownership. See the type correction on this row: '
     'Synergy recapitalised 11 Beacon Street in 2016 and brought GreenOak Real '
     'Estate in as a partner in a $63M deal, so the resulting owner is the pair, '
     'not a new sole buyer. The record entity SHIGO 11 BEACON OWNER LLC is the '
     'recapitalised vehicle.'),
    (1452, "seller", "Synergy Investments", "web",
     'Synergy is the party that sold a stake and remained. It had bought the '
     'building from DivcoWest in 2013 for $35 million. SEVENTH '
     'PARTNER-CONTINUATION in this table and the third involving Synergy, after '
     '100 Franklin Street and 327-333 Summer Street.'),
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
                   set {side}_canonical = :s, {side}_confidence = 'web_corroborated',
                       {side}_resolution_basis = :b,
                       notes = coalesce(notes,'') || :n
                 where id = :id"""), {
                "s": sponsor, "id": rid, "b": basis,
                "n": f" | {side.upper()} RESOLVED. " + why})
            n += 1

    if not dry_run:
        conn.execute(text("""
            update transactions
               set transaction_type = 'partial_interest',
                   is_recapitalization = 1,
                   price_caveat = coalesce(price_caveat || ' ', '') || :c,
                   notes = coalesce(notes,'') || :n
             where id = 1452"""), {
            "c": ("Price is a RECAPITALISATION, not a whole-building sale: "
                  "Synergy retained an interest and GreenOak Real Estate bought "
                  "in. No percentage is stated by any source."),
            "n": " | " + RECAP_NOTE})
        log.info("\nid=1452 re-typed to partial_interest, is_recapitalization set")
        conn.commit()

    log.info("%d sides written", n)
    tot = conn.execute(text(
        "select count(*) from transactions where coalesce(quarantined,0)=0")).scalar()
    for side in ("buyer", "seller"):
        v = conn.execute(text(
            f"select count(*) from transactions where coalesce(quarantined,0)=0 "
            f"and coalesce({side}_canonical,'') <> ''")).scalar()
        log.info("%s_canonical: %d of %d (%.0f%%)", side, v, tot, v / tot * 100)
    for t, cnt, d in conn.execute(text(
            "select transaction_type, count(*), sum(coalesce(price,0)) "
            "from transactions where coalesce(quarantined,0)=0 "
            "group by 1 order by 2 desc")):
        log.info("  %-18s %4d  $%.2fB", t, cnt, (d or 0) / 1e9)
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
