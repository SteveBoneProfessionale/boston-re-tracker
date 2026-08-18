r"""
Ingest Rhode Island projects from semantically-segmented agenda items.

Why this exists
---------------
The regex pipeline (ri_ingest.py) fails at SEGMENTATION, not at field patterns.
Agenda items have no reliable delimiter, so blocks bounded by character windows
bleed into their neighbours. That single failure explains every thin field at
once: applicant captured for 20% of projects, units for 5-15%, parking for
0-12%. The names are all present in the text on disk.

The fix is to let a reader decide where each item begins and ends, given the
whole agenda, and return a structured list. This module consumes that list.

Input: data/ri_llm_items.json -- an array of item objects, each carrying its
source document, meeting date, reviewing body, and whatever fields the filing
actually stated. Absent fields are null and stay null; nothing is inferred.

The identity, dedup, stage-history and citation logic is unchanged from
ri_ingest.py -- only the segmentation source differs.

    python scraper/ri_ingest_llm.py --dry-run
    python scraper/ri_ingest_llm.py
"""

import sys
import json
import logging
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import init_db, get_session
from db.models import Project, ProjectStageEvent
from scraper.ri_identity import parcel_id, same_project, normalize_address
from scraper.ri_citations import record_extraction
from app.data import RI_STAGE_MAP

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

ITEMS = Path(__file__).parent.parent / "data" / "ri_llm_items.json"

# Stage ordering for "furthest stage heard". Non-advancing events (extensions,
# modifications, continuances, waivers) never participate.
_STAGE_ORDER = {s: i for i, s in enumerate([
    "Pre-application Conference", "Conceptual", "Master Plan",
    "Combined Master and Preliminary", "Preliminary Plan",
    "Development Plan Review", "Unified Development Review",
    "Special Use Permit", "Administrative Review", "Final Plan", "Rezoning",
])}


def load_items() -> list[dict]:
    if not ITEMS.exists():
        return []
    return json.loads(ITEMS.read_text(encoding="utf-8"))


def collapse(items: list[dict]) -> list[dict]:
    """Group item appearances into projects on parcel identity."""
    groups: list[dict] = []
    for it in items:
        pid = parcel_id(it.get("municipality", ""),
                        it.get("plat_lot_raw", ""),
                        it.get("address", ""))
        if not (pid.has_parcel or pid.address_key):
            continue
        placed = False
        for g in groups:
            plats = {p.plat for p in g["parcels"] if p.plat}
            if pid.plat and plats and pid.plat not in plats:
                continue                       # transitive over-merge guard
            for member in g["parcels"]:
                ok, _r, review = same_project(pid, member)
                if ok:
                    g["items"].append(it)
                    g["parcels"].append(pid)
                    g["needs_review"] = g["needs_review"] or review
                    placed = True
                    break
            if placed:
                break
        if not placed:
            groups.append({"items": [it], "parcels": [pid], "needs_review": False,
                           "municipality": it.get("municipality", "")})
    return groups


def _pick(items: list[dict], field: str):
    for it in items:
        v = it.get(field)
        if v not in (None, "", False, []):
            return v
    return None


def build(group: dict) -> dict:
    items = sorted(group["items"], key=lambda x: x.get("meeting_date") or "")
    pid = next((p for p in group["parcels"] if p.has_parcel), group["parcels"][0])

    heard = None
    for it in items:
        if not it.get("advances_stage", True):
            continue
        s = it.get("review_stage")
        if s and (heard is None or _STAGE_ORDER.get(s, -1) > _STAGE_ORDER.get(heard, -1)):
            heard = s

    return {
        "municipality": group["municipality"],
        "address": _pick(items, "address") or "",
        "assessor_plat": pid.plat,
        "assessor_lots": ",".join(sorted(pid.lots, key=int)) if pid.lots else None,
        "plat_lots_raw": (_pick(items, "plat_lot_raw") or "")[:200],
        "applicant_entity": _pick(items, "applicant_entity"),
        "case_number": _pick(items, "case_number"),
        "zoning_district_raw": _pick(items, "zoning_district"),
        "residential_units": _pick(items, "residential_units"),
        "parking_spaces": _pick(items, "parking_spaces"),
        "total_gsf": _pick(items, "square_feet"),
        "site_acreage": _pick(items, "acreage"),
        "num_stories": _pick(items, "stories"),
        "building_count": _pick(items, "building_count"),
        "adaptive_reuse": bool(_pick(items, "adaptive_reuse")),
        "review_scale": _pick(items, "classification"),
        "neighborhood": _pick(items, "neighborhood"),
        "description": _pick(items, "description"),
        "stage_heard": heard,
        "needs_review": group["needs_review"],
        "items": items,
    }


def run(dry_run: bool = False) -> dict:
    init_db()
    raw = load_items()
    if not raw:
        log.warning("No items in %s", ITEMS)
        return {}
    groups = collapse(raw)
    projects = [build(g) for g in groups]
    log.info("Items: %d  ->  projects: %d (%.1f%% collapse)",
             len(raw), len(projects), (1 - len(projects) / len(raw)) * 100)

    per = defaultdict(lambda: {"raw": 0, "proj": 0})
    for r in raw:
        per[r.get("municipality", "?")]["raw"] += 1
    for p in projects:
        per[p["municipality"]]["proj"] += 1
    for m in sorted(per):
        log.info("  %-12s items=%-5d projects=%-4d", m, per[m]["raw"], per[m]["proj"])

    if dry_run:
        return {"items": len(raw), "projects": len(projects), "dry_run": True}

    session = get_session()
    created = updated = events = cites = 0
    try:
        for p in projects:
            key = (f"manual:ri-{p['municipality'].lower()}-{p['assessor_plat'] or 'na'}-"
                   f"{(p['assessor_lots'] or normalize_address(p['address']) or 'x').replace(',', '_')[:40]}")
            proj = session.query(Project).filter_by(bpda_url=key).first()
            if proj is None:
                proj = Project(bpda_url=key, city=p["municipality"], requires_extraction=False)
                session.add(proj)
                created += 1
            else:
                updated += 1

            proj.name = p["address"] or p["plat_lots_raw"]
            for f in ("address", "assessor_plat", "assessor_lots", "plat_lots_raw",
                      "applicant_entity", "case_number", "zoning_district_raw",
                      "residential_units", "parking_spaces", "total_gsf",
                      "site_acreage", "num_stories", "building_count",
                      "adaptive_reuse", "review_scale", "description", "stage_heard"):
                val = p[f]
                # Never overwrite an existing value with a null on re-run.
                if val not in (None, "", False) or getattr(proj, f, None) in (None, ""):
                    setattr(proj, f, val)
            if p["neighborhood"]:
                proj.neighborhood = p["neighborhood"]
            proj.review_scale_raw = p["review_scale"]
            proj.dedupe_review = p["needs_review"]
            proj.status = p["stage_heard"] or ""
            session.flush()

            session.query(ProjectStageEvent).filter_by(project_id=proj.id).delete()
            for it in p["items"]:
                session.add(ProjectStageEvent(
                    project_id=proj.id,
                    meeting_date=it.get("meeting_date"),
                    reviewing_body=it.get("reviewing_body"),
                    entity_id=it.get("entity_id"),
                    review_stage_raw=it.get("review_stage_raw") or it.get("review_stage"),
                    stage=RI_STAGE_MAP.get(it.get("review_stage") or ""),
                    advances_stage=bool(it.get("advances_stage", True)),
                    vote_taken=it.get("vote_taken"),
                    outcome=it.get("outcome"),
                    source_url=it.get("source_url")))
                events += 1

            first = p["items"][0]
            cites += record_extraction(
                session, proj, p, source_url=first.get("source_url", ""),
                filing_name=first.get("reviewing_body", ""),
                filing_date=first.get("meeting_date", ""))
        session.commit()
    finally:
        session.close()

    log.info("Created %d, updated %d, events %d, citations %d",
             created, updated, events, cites)
    return {"items": len(raw), "projects": len(projects), "created": created,
            "updated": updated, "events": events, "citations": cites}


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    run(ap.parse_args().dry_run)
