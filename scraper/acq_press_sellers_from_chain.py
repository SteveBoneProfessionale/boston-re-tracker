r"""Fill the remaining press-row grantors from the ownership chain.

Seven press rows had no seller after the article audit, because the report named
only the buyer. Six of those name a Boston or Cambridge ADDRESS, and an address
is enough: look the parcel up, read the owner of record in the last annual
snapshot before the sale, and that is the grantor.

This is the same method used on the spine, applied in the other direction. On
spine rows the buyer is known and the transition is located by finding where the
buyer first appears. On these 2026 rows the sale is LATER than the newest
snapshot (FY2026 was published before most of them closed), so the newest
snapshot still shows the outgoing owner -- which is exactly the party wanted.

343 Congress Street is the one where the transition is actually visible: MEPT
SEAPORT 343 CONGRESS LLC holds it FY2022 through FY2025 and 343 CONGRESS PROPCO
LLC appears in FY2026. The seller is the former, and the chain shows the handover
rather than inferring it.

The Newbury pair carries a bonus. 4-6 Newbury is held by FOUR-6 NEWBURY JSRE TIC
LLC and 28 Newbury by TWENTY 8 NEWBURY JSRE TIC LLC -- two tenant-in-common
vehicles of one JSRE group. That is independent confirmation that a single
ownership group sold both buildings, which is why Bisnow reported them as one
$113.5M transaction, and it is the seller the article declined to name.

    python scraper/acq_press_sellers_from_chain.py --apply
"""
import argparse, logging, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from sqlalchemy import text
from db.database import engine

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

FILLS = [
    dict(match="343 Congress Street", seller="MEPT Seaport 343 Congress LLC",
         note=("Owner of record FY2022-FY2025 per Boston's annual assessment "
               "files; FY2026 shows 343 CONGRESS PROPCO LLC, so the handover to "
               "North Colony's vehicle is visible in the chain rather than "
               "inferred. MEPT is the Multi-Employer Property Trust.")),
    dict(match="4-6 and 28 Newbury",
         seller="Four-6 Newbury JSRE TIC LLC / Twenty 8 Newbury JSRE TIC LLC",
         note=("Owner of record FY2022 through FY2026 on both parcels. The two "
               "tenant-in-common vehicles share the JSRE name, which is "
               "independent confirmation that one ownership group held both "
               "buildings and is why the deal was reported as a single $113.5M "
               "transaction. Newmark's release said it 'represented the seller' "
               "without naming them.")),
    dict(match="31 Buttonwood Street",
         seller="31 Buttonwood Street LLC / 14 Willis Street LLC",
         note=("Owner of record FY2022 through FY2026 on both Dorchester "
               "parcels. Single-purpose entities, so this names the record "
               "grantor and not the principal behind it.")),
]


def main(dry_run: bool):
    conn = engine.connect()
    n = 0
    for f in FILLS:
        row = conn.execute(text(
            "select id from transactions where address like :m "
            "and source = 'press' and coalesce(seller,'') = ''"),
            {"m": f"%{f['match']}%"}).first()
        if not row:
            log.warning("no sellerless press row matching %r", f["match"])
            continue
        log.info("%-26s -> %s", f["match"][:26], f["seller"][:52])
        if not dry_run:
            conn.execute(text("""
                update transactions
                   set seller = :s, notes = coalesce(notes,'') || :n
                 where id = :id"""), {
                "s": f["seller"], "id": row[0],
                "n": (" | SELLER DERIVED FROM THE OWNERSHIP CHAIN, not from the "
                      "article, which named only the buyer. " + f["note"] +
                      " Record entity, not the sponsor: buyer_canonical and "
                      "seller_canonical stay null pending Secretary of the "
                      "Commonwealth resolution.")})
        n += 1
    if not dry_run:
        conn.commit()

    tot = conn.execute(text("select count(*) from transactions")).scalar()
    sel = conn.execute(text(
        "select count(*) from transactions where coalesce(seller,'') <> ''")).scalar()
    log.info("\n%d filled", n)
    for label, cond in (("press + SEC", "source in ('press','sec_filing')"),
                        ("assessment spine",
                         "source in ('massgis_l3','cambridge_socrata')")):
        t = conn.execute(text(
            f"select count(*) from transactions where {cond}")).scalar()
        s = conn.execute(text(
            f"select count(*) from transactions where {cond} "
            f"and coalesce(seller,'') <> ''")).scalar()
        log.info("%-18s %4d rows | seller %4d (%.0f%%)", label, t, s, s / t * 100)
    log.info("%-18s %4d rows | seller %4d (%.0f%%)", "OVERALL", tot, sel,
             sel / tot * 100)
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
