r"""
Audit and correct total_gsf for Rhode Island, before adding any new values.

The square-foot field turned out to be the worst-quality column in the Rhode
Island set. Of 95 stored values, most are not building size at all -- they are
lot areas, zoning minimum lot sizes, sign faces, thresholds quoted from a
zoning table, and in two cases the lot area taken from a sentence that stated
the building area six words later:

    id=734  "The new lot will measure 53,582 SF with the building providing
             70,784 SF of area"          stored 53,582, should be 70,784
    id=705  "The subject lot ... measures approximately 11,033 SF, with the
             building proposed to provide ... 22,000 SF"
                                          stored 11,033, should be 22,000

This is the error class that matters most: a blank is visibly missing, while a
lot area sitting in a GSF column reads as a reported building size and will be
quoted as one.

Three outcomes, and the third is deliberately conservative:

  CORRECTED  the same text states a real building figure -> write it
  NULLED     the stored value is demonstrably land, a zoning standard, a
             threshold or a sign -> clear it and record why
  FLAGGED    the value appears nowhere in any document available here. It may
             be a good extraction from source this corpus does not hold, so it
             is NOT nulled -- it is flagged for review and left in place.

    python scraper/ri_sf_fix_stored.py --dry-run
"""

import re
import sys
import json
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import get_session
from db.models import Project, FlaggedExtraction
from scraper.ri_sf_extract import (
    text_index, project_text, candidates_for, building_sf, _full_text, RI,
)
from scraper.ri_ingest_llm import load_items, collapse
from scraper.ri_identity import normalize_address

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

OUT = Path(__file__).parent.parent / "data" / "ri_sf_audit.json"


def items_by_key():
    byk = {}
    for g in collapse(load_items()):
        muni = g["municipality"].lower()
        for it in g["items"]:
            byk.setdefault((muni, normalize_address(it.get("address") or "")), []).append(it)
    return byk


def verdict(p, idx, byk):
    """CORRECT / CORRECTED / NULL / FLAG for one stored value."""
    scoped, _ = project_text(p, idx)
    cands = candidates_for(scoped)
    match = next((c for c in cands if c["value"] == p.total_gsf), None)
    best, _ev = building_sf(cands)

    if match and match["class"] == "BUILDING":
        return "CORRECT", None, match["quote"]
    if match:
        return ("CORRECTED" if best else "NULL"), best, match["quote"]

    # Not in the scoped window -- look through the whole document before
    # concluding anything.
    for it in byk.get((p.city.lower(), normalize_address(p.address or "")), []):
        ft = _full_text(it.get("document") or "")
        if not ft:
            continue
        for v in (f"{p.total_gsf:,}", str(p.total_gsf)):
            i = ft.find(v)
            if i < 0:
                continue
            ctx = re.sub(r"\s+", " ", ft[max(0, i - 130):i + 80])
            cc = candidates_for(ctx)
            mm = next((c for c in cc if c["value"] == p.total_gsf), None)
            if mm and mm["class"] == "BUILDING":
                return "CORRECT", None, ctx
            if mm:
                return "NULL", None, ctx
            return "FLAG", None, ctx
    return "FLAG", None, ""


def main(dry_run=False):
    idx, byk = text_index(), items_by_key()
    session = get_session()
    pool = [p for p in session.query(Project).filter(Project.city.in_(RI)).all()
            if not p.excluded]
    stored = [p for p in pool if p.total_gsf is not None]

    report = {"correct": [], "corrected": [], "nulled": [], "flagged": []}
    for p in stored:
        v, better, quote = verdict(p, idx, byk)
        row = {"id": p.id, "city": p.city, "address": p.address,
               "stored": p.total_gsf, "quote": (quote or "")[:220]}
        if v == "CORRECT":
            report["correct"].append(row)
            continue
        if v == "CORRECTED":
            row["new"] = better
            report["corrected"].append(row)
            if not dry_run:
                p.notes = ((p.notes + " | ") if p.notes else "") + (
                    "total_gsf corrected from %s to %s: the stored figure was the LOT "
                    "area from a sentence that states the building area alongside it."
                    % (f"{p.total_gsf:,}", f"{better:,}"))
                p.total_gsf = better
            continue
        if v == "NULL":
            report["nulled"].append(row)
            if not dry_run:
                p.notes = ((p.notes + " | ") if p.notes else "") + (
                    "total_gsf cleared (was %s): the figure is land area, a zoning "
                    "standard, a threshold or a sign -- not building floor area."
                    % f"{p.total_gsf:,}")
                p.total_gsf = None
            continue
        report["flagged"].append(row)
        if not dry_run:
            session.add(FlaggedExtraction(
                project_id=p.id, field_name="total_gsf", status="open",
                current_value=str(p.total_gsf),
                user_note="Stored square footage does not appear in any planning "
                          "document held here. It may be a sound extraction from "
                          "source this corpus lacks, so it has been left in place "
                          "rather than cleared -- but it is unverified.",
            ))

    if dry_run:
        session.rollback()
    else:
        session.commit()
    session.close()

    OUT.write_text(json.dumps(report, indent=1, ensure_ascii=False), encoding="utf-8")
    log.info("Stored total_gsf values audited : %d", len(stored))
    log.info("  confirmed as building area    : %d", len(report["correct"]))
    log.info("  CORRECTED (lot -> building)   : %d", len(report["corrected"]))
    log.info("  NULLED (land/zoning/sign)     : %d", len(report["nulled"]))
    log.info("  FLAGGED, unverifiable, kept   : %d", len(report["flagged"]))
    for r in report["corrected"]:
        log.info("    id=%-4d %s -> %s", r["id"], f'{r["stored"]:,}', f'{r["new"]:,}')
    if dry_run:
        log.info("DRY RUN -- nothing written")


if __name__ == "__main__":
    main(dry_run="--dry-run" in sys.argv)
