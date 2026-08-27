r"""Find and contain the condominium fault in the buyer derivation.

Buyers on spine rows come from the assessment roll's CURRENT owner of the
parcel, on the reasoning that a row records that parcel's most recent sale so
the current owner is its grantee. That reasoning fails on a condominiumised
parcel, where several units share one parcel id, each sells separately, and the
roll carries one owner for the lot.

It has now produced three wrong buyers found by hand -- 101 Seaport, 200 State
Street and 160 Federal -- so this measures the whole exposure rather than
waiting for a fourth.

THE SIGNATURE IS UNMISTAKABLE. Eight parcels carry more than one transaction,
and on all eight the IDENTICAL buyer string is stamped on every row:

    160 Federal ST G-93 / G-31 / G-30    all "160 FEDERAL OWNER LLC"
    200 State ST OFFICE / RETAIL         all "GAZIT HORIZONS (MARKETPLACE) LLC"
    1 WESTINGHOUSE PZ 7 / 5 / 3          all "PPF INDUSTRIAL 1 WESTINGHOUSE..."

Three garage units at 160 Federal did not all sell to the building owner; the
$222M office condominium at 200 State went to Carr Properties while Gazit bought
the retail.

SCOPE. 10 rows are not their parcel's latest sale and are wrong by construction.
But the fault is broader: where the addresses differ between rows on one parcel
they are DIFFERENT UNITS, so the current owner is unreliable on ALL of them,
including the latest. That is 20 rows and $573.9M of value.

WHAT THIS DOES. The record entity is KEPT -- it is a true fact about the parcel
and the key back to the deed -- but `buyer_confidence` is set to
parcel_derivation_unreliable and `buyer_canonical` is cleared, UNLESS the row's
sponsor was established independently by press. Ranking on a buyer that belongs
to a different condominium unit is the actual harm, and clearing the canonical
stops it while losing nothing recoverable.

    python scraper/acq_condo_parcel_fault.py --apply
"""

import argparse
import logging
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from db.database import engine

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

TRUSTED_BASIS = ("web", "self_identifying")


def main(dry_run: bool):
    conn = engine.connect()
    rows = conn.execute(text("""
        select id, parcel_id, address, sale_date, coalesce(price,0),
               coalesce(buyer,''), coalesce(buyer_canonical,''),
               coalesce(buyer_resolution_basis,'')
          from transactions
         where coalesce(quarantined,0) = 0
           and source in ('massgis_l3','cambridge_socrata')
           and coalesce(parcel_id,'') <> ''
         order by parcel_id, sale_date""")).fetchall()

    byp = defaultdict(list)
    for r in rows:
        byp[r[1]].append(r)
    multi = {k: v for k, v in byp.items() if len(v) > 1}

    affected, kept, vol = [], [], 0
    for pid, group in multi.items():
        distinct_addr = len({g[2].strip().upper() for g in group}) > 1
        latest = max(group, key=lambda g: str(g[3]))
        for g in group:
            # Wrong by construction if it is not the latest sale. Unreliable
            # regardless if the rows are different units of one parcel.
            if g[0] != latest[0] or distinct_addr:
                vol += g[4]
                if g[7] in TRUSTED_BASIS:
                    kept.append(g)
                else:
                    affected.append((g, distinct_addr, g[0] != latest[0]))

    log.info("%d parcels carry more than one transaction", len(multi))
    log.info("%d rows affected, $%s of value", len(affected) + len(kept),
             f"{vol:,}")
    log.info("  %d have a press-established sponsor and are left alone", len(kept))
    log.info("  %d have a parcel-derived buyer and are being contained",
             len(affected))
    for (g, diff_addr, not_latest), _ in zip(affected, range(12)):
        why = []
        if not_latest:
            why.append("not the parcel's latest sale")
        if diff_addr:
            why.append("parcel holds distinct units")
        log.info("   id=%-5s %s $%-12s %-30s %s", g[0], str(g[3])[:10],
                 f"{g[4]:,}", g[2][:30], "; ".join(why))

    if not dry_run:
        for g, diff_addr, not_latest in affected:
            reasons = []
            if not_latest:
                reasons.append("this row is NOT the parcel's most recent sale, "
                               "so the roll's current owner belongs to a later "
                               "conveyance")
            if diff_addr:
                reasons.append("the parcel carries transactions at DIFFERENT "
                               "unit addresses, so it is condominiumised and one "
                               "owner cannot be the grantee of every unit")
            conn.execute(text("""
                update transactions
                   set buyer_canonical = null,
                       buyer_confidence = 'parcel_derivation_unreliable',
                       buyer_resolution_basis = null,
                       notes = coalesce(notes,'') || :n
                 where id = :id"""), {
                "id": g[0],
                "n": (" | BUYER DERIVATION UNRELIABLE ON THIS ROW. The buyer on "
                      "spine rows is the assessment roll's CURRENT owner of the "
                      "parcel, which assumes one owner per parcel and one sale "
                      "per row. Here " + " and ".join(reasons) + ". The record "
                      "entity is kept, because it is a true fact about the "
                      "parcel and the key back to the deed, but the resolved "
                      "sponsor is cleared so nothing ranks on a buyer that may "
                      "belong to a different unit. Confirmed wrong by press on "
                      "three parcels already: 101 Seaport, 200 State Street and "
                      "160 Federal.")})
        conn.commit()

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
