r"""Write sponsor resolutions onto the South Florida transaction rows.

    python -m scraper.acq_sofl_resolve --write

Touches SIX COLUMNS AND NO OTHERS -- buyer_canonical, buyer_confidence,
buyer_resolution_basis, buyer_resolution_url and the seller equivalents. Price,
areas, dates, classes and the verbatim deed names are left exactly as the
county recorded them, which is the brief's instruction and also the only way
the resolution work can be re-run or reversed without disturbing the record
underneath it.

THE DEED NAME IS NEVER OVERWRITTEN. `buyer` and `seller` keep the entity as
the instrument spells it, because that is the primary fact and the resolution
is an interpretation laid beside it. The display shows the firm first and the
entity beneath, so a reader always sees both.

Rows whose entity did not resolve are CLEARED rather than left alone, so that
a name withdrawn from acq_sofl_press.py disappears from the table instead of
surviving as a stale attribution nobody can trace.
"""

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from sqlalchemy import text                           # noqa: E402

from db.database import engine, init_db               # noqa: E402

SOURCES = "('miamidade_pa','pbcpao')"


def write(resolutions, dry_run=False, verbose=True):
    """Apply {entity -> (firm, confidence, basis, url) or None} to the rows."""
    init_db()
    updated = 0
    counts = {"buyer": 0, "seller": 0}

    with engine.begin() as conn:
        rows = conn.execute(text(
            f"select id, buyer, seller from transactions "
            f"where source in {SOURCES}")).mappings().all()
        for row in rows:
            payload = {"_id": row["id"]}
            for side in ("buyer", "seller"):
                hit = resolutions.get((row[side] or "").strip())
                if hit:
                    firm, conf, basis, url = hit
                    counts[side] += 1
                else:
                    firm = conf = basis = url = None
                payload[f"{side}_canonical"] = firm
                payload[f"{side}_confidence"] = conf
                payload[f"{side}_resolution_basis"] = basis
                payload[f"{side}_resolution_url"] = url
            sets = ", ".join(f"{k} = :{k}" for k in payload if k != "_id")
            if not dry_run:
                conn.execute(text(f"update transactions set {sets} "
                                  f"where id = :_id"), payload)
            updated += 1
        if dry_run:
            conn.rollback()

    if verbose:
        print(f"{'(dry run) ' if dry_run else ''}{updated} rows written; "
              f"{counts['buyer']} buyers and {counts['seller']} sellers "
              f"carry a resolved firm")
    return updated, counts
