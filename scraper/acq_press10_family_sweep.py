r"""Tenth pass. Sweep the two confirmed prefixes across every row that carries them.

Once a prefix is confirmed against a named property, the remaining rows on that
convention are free. This applies the two confirmed in the previous pass to
everything left carrying them, including rows below $25M, because a confirmed
decode costs nothing to apply and the instruction to stop grinding below $25M
was about SEARCH EFFORT, not about leaving free cells blank.

AND THE SWEEP ITSELF PRODUCED A THIRD CONFIRMATION. Healthpeak's disclosure
itemises "Mooney Street Parcels: 145,000 square feet of flex office and
industrial buildings on 11.9 acres for $123 million, including 13 & 40-61 Mooney
Street and 127 Smith Place". Three rows in this table carry the SAME entity, LS
ALEWIFE III LLC, in the SAME month, October 2021, at exactly those addresses:

    61 Mooney St     $64,000,000
    127 Smith Pl     $34,000,000
    13 Mooney St     $22,000,000
                    ------------
                     $120,000,000   against a disclosed $123 million

One entity, one month, the three addresses named in the disclosure, and the
prices summing to within 2.5% of the figure Healthpeak reported. That is the
same shape of cross-validation as the Canal Park portfolio arithmetic, arrived
at independently, and it is stronger evidence than any single article.

XHR AT 500-528 COMMONWEALTH is the fourth Hotel Commonwealth parcel row and it
is dated December 2019, eleven months before the disposition Xenia announced.
Written as Xenia because the prefix is confirmed and the entity is identical to
the 2020 row's, but the date gap is recorded, not smoothed over: Hotel
Commonwealth spans several Kenmore Square parcels and this table already carries
two of them at two different prices.

    python scraper/acq_press10_family_sweep.py --apply
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

MOONEY = (
    "THE MOONEY STREET ARITHMETIC. Healthpeak's own disclosure itemises \"Mooney "
    "Street Parcels: 145,000 square feet of flex office and industrial buildings "
    "on 11.9 acres for $123 million, including 13 & 40-61 Mooney Street and 127 "
    "Smith Place\". Three rows in this table carry the identical entity LS ALEWIFE "
    "III LLC in the identical month, October 2021, at exactly those addresses: 61 "
    "Mooney St $64,000,000, 127 Smith Pl $34,000,000 and 13 Mooney St "
    "$22,000,000, summing to $120,000,000 against the disclosed $123 million. One "
    "entity, one month, the named addresses, and prices reconciling to within "
    "2.5%. That is a cross-validation, not a citation."
)

FAMILIES = [
    ("LS ALEWIFE%", "Healthpeak Properties", "prefix_confirmed",
     "LS <LOCATION> LLC IS HEALTHPEAK PROPERTIES, confirmed against named "
     "properties: Healthpeak's Q3 2021 results and 8-K describe eight separate "
     "Alewife transactions totalling roughly $625 million across about 36 acres "
     "of West Cambridge, and two of them -- 10 Fawcett at $73M in October 2021 "
     "and 67 Smith Place at $72M in January 2022 -- match rows here on property, "
     "price and month at once. The roman numerals are that campaign's own "
     "numbering. " + MOONEY),
    ("XHR%", "Xenia Hotels & Resorts", "prefix_confirmed",
     "XHR IS XENIA HOTELS & RESORTS, confirmed against a named property: Xenia's "
     "own release, \"Xenia Hotels & Resorts Completes Dispositions Of Hotel "
     "Commonwealth And Renaissance Austin Hotel\", covers the 245-room Hotel "
     "Commonwealth in Kenmore Square, and the entity on that row is XHR BOSTON "
     "COMMONWEALTH LLC. DATE CAVEAT ON THIS ROW: it is dated December 2019, "
     "eleven months before the disposition Xenia announced in November 2020. "
     "Hotel Commonwealth spans several Kenmore Square parcels and this table "
     "already carries them at different prices, so this is most likely a separate "
     "parcel conveyance rather than the headline deal. The sponsor is not in "
     "doubt; which conveyance this row records is."),
]


def main(dry_run: bool):
    conn = engine.connect()
    n = 0
    for pat, sponsor, basis, why in FAMILIES:
        for side in ("buyer", "seller"):
            rows = conn.execute(text(f"""
                select id, {side}, price, address, substr(sale_date,1,7)
                  from transactions
                 where coalesce(quarantined,0) = 0
                   and upper(coalesce({side},'')) like :k
                   and coalesce({side}_canonical,'') = ''
                 order by price desc"""), {"k": pat}).fetchall()
            for rid, ent, price, addr, dt in rows:
                log.info("id=%-5s %-6s %s $%-13s %-34s -> %s", rid, side, dt,
                         f"{price:,}", (ent or "")[:34], sponsor)
                if not dry_run:
                    conn.execute(text(f"""
                        update transactions
                           set {side}_canonical = :s,
                               {side}_confidence = 'registry_confirmed',
                               {side}_resolution_basis = :b,
                               notes = coalesce(notes,'') || :n
                         where id = :id"""), {
                        "s": sponsor, "id": rid, "b": basis,
                        "n": f" | {side.upper()} RESOLVED. " + why})
                    n += 1
    if not dry_run:
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
    log.info("\nHealthpeak's Alewife position as this table now records it:")
    for r in conn.execute(text("""
            select count(*), sum(coalesce(price,0)) from transactions
             where coalesce(quarantined,0)=0
               and buyer_canonical = 'Healthpeak Properties'""")):
        log.info("  %d acquisitions, $%s", r[0], f"{int(r[1] or 0):,}")
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
