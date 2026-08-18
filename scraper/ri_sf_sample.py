r"""
Tier-1 square footage over a stratified sample, and the audit of what is
already stored.

Two questions this answers before any bulk run:

  1  What does rereading EVERY planning document recover, per size tier? The
     developer work roughly doubled coverage that way, and the same is
     expected here -- but expected is not measured.
  2  How much of what is ALREADY in total_gsf is wrong? The first look found
     lot areas stored as building size, which is the error class that matters
     most because it reads as a reported figure.

    python scraper/ri_sf_sample.py --sample 30
"""

import sys
import json
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import get_session
from db.models import Project
from scraper.ri_sf_extract import (
    text_index, project_text, candidates_for, building_sf, RI,
)

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

OUT = Path(__file__).parent.parent / "data" / "ri_sf_sample.json"

TIERS = [
    ("A  100+ units",      lambda p: (p.residential_units or 0) >= 100),
    ("B  20-99 units",     lambda p: 20 <= (p.residential_units or 0) < 100),
    ("C  4-19 units",      lambda p: 4 <= (p.residential_units or 0) < 20),
    ("D  1-3 units",       lambda p: 1 <= (p.residential_units or 0) < 4),
    ("E  no units stated", lambda p: not p.residential_units),
]


def active(session):
    return [p for p in session.query(Project).filter(Project.city.in_(RI)).all()
            if not p.excluded]


def stratified(pool, n):
    """Even coverage across the size range, not just the big end."""
    per = max(1, n // len(TIERS))
    out = []
    for _, test in TIERS:
        band = [p for p in pool if test(p)]
        band.sort(key=lambda p: (p.residential_units or 0), reverse=True)
        step = max(1, len(band) // per)
        out.extend(band[::step][:per])
    return out[:n]


def main(n=30):
    idx = text_index()
    session = get_session()
    pool = active(session)

    # ---- audit what is already stored ---------------------------------
    stored = [p for p in pool if p.total_gsf is not None]
    audit = {"ok": [], "wrong": [], "unverifiable": []}
    for p in stored:
        txt, _ = project_text(p, idx)
        cands = candidates_for(txt)
        best, _b = building_sf(cands)
        match = next((c for c in cands if c["value"] == p.total_gsf), None)
        row = {"id": p.id, "city": p.city, "address": p.address,
               "stored": p.total_gsf, "reclassified_as": match["class"] if match else None,
               "building_says": best, "quote": (match or {}).get("quote", "")}
        if match is None:
            audit["unverifiable"].append(row)
        elif match["class"] == "BUILDING":
            audit["ok"].append(row)
        else:
            audit["wrong"].append(row)

    log.info("\nAUDIT OF STORED total_gsf  (%d active projects carry a value)", len(stored))
    log.info("  reclassifies as BUILDING (correct)      : %d", len(audit["ok"]))
    log.info("  reclassifies as LAND or other (WRONG)   : %d", len(audit["wrong"]))
    log.info("  figure not found in the documents       : %d", len(audit["unverifiable"]))
    log.info("  of the wrong ones, %d have a real building figure available in the same text",
             sum(1 for r in audit["wrong"] if r["building_says"]))

    # ---- tier-1 recovery on the sample --------------------------------
    missing = [p for p in pool if p.total_gsf is None]
    sample = stratified(missing, n)
    log.info("\nTIER 1 -- DOCUMENT REREAD, stratified sample of %d", len(sample))
    log.info("  %-20s %7s %9s %9s %9s", "TIER", "SAMPLED", "RECOVERED", "RATE", "LAND-ONLY")

    results, by_tier = [], {}
    for label, test in TIERS:
        band = [p for p in sample if test(p)]
        rec = land = 0
        for p in band:
            txt, nitems = project_text(p, idx)
            cands = candidates_for(txt)
            best, ev = building_sf(cands)
            if best:
                rec += 1
            elif any(c["class"] == "LAND" for c in cands):
                land += 1
            results.append({
                "id": p.id, "tier": label, "city": p.city, "address": p.address,
                "units": p.residential_units, "documents": nitems,
                "sf": best,
                "evidence": [e["quote"][:220] for e in ev][:2],
                "land_figures": sum(1 for c in cands if c["class"] == "LAND"),
            })
        by_tier[label] = {"sampled": len(band), "recovered": rec, "land_only": land}
        rate = f"{100*rec/len(band):.0f}%" if band else "—"
        log.info("  %-20s %7d %9d %9s %9d", label, len(band), rec, rate, land)

    tot = len(results)
    got = sum(1 for r in results if r["sf"])
    log.info("  %-20s %7d %9d %8.0f%%", "ALL", tot, got, 100 * got / tot if tot else 0)

    OUT.write_text(json.dumps({"audit": audit, "tier1": results, "by_tier": by_tier},
                              indent=1, ensure_ascii=False), encoding="utf-8")
    log.info("\nWrote %s", OUT)
    session.close()
    return results


if __name__ == "__main__":
    n = 30
    if "--sample" in sys.argv:
        n = int(sys.argv[sys.argv.index("--sample") + 1])
    main(n)
