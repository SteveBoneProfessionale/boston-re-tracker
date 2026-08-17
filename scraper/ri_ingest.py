r"""
Ingest Rhode Island projects from the harvested agenda corpus.

Pipeline: segment agenda text into items -> extract fields -> resolve parcel
identity -> collapse repeat appearances onto one project -> write Project rows,
stage history and per-field citations.

Nothing is inferred. A field absent from the filing stays null and gets no
citation. Construction status is never inferred from approval: Under
Construction and Complete are structurally unreachable for Rhode Island
(app/data.py MARKETS reachable_stages), so an approved project stays Approved.

Idempotent: re-running matches existing projects on parcel identity and updates
them rather than inserting duplicates.

    python scraper/ri_ingest.py --dry-run
    python scraper/ri_ingest.py
"""

import re
import sys
import json
import logging
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import init_db, get_session
from db.models import Project, ProjectStageEvent
from scraper.ri_extract import (
    extract_item, body_sections, body_at, _PVD_NBHD,
)
from scraper.ri_identity import parcel_id, same_project, normalize_address
from scraper.ri_citations import record_extraction
from scraper.ri_sources import BOARDS
from app.data import RI_STAGE_MAP, MARKETS

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

CORPUS = Path(__file__).parent.parent / "data" / "ri_agenda_corpus.json"
TEXT_DIR = Path(__file__).parent.parent / "data" / "ri_pdfs" / "text"
REPORT = Path(__file__).parent.parent / "data" / "ri_ingest_report.json"

# An agenda item begins at a parcel reference; the surrounding block carries
# the address, applicant and program.
_PARCEL_ANCHOR = re.compile(
    r"(?:TAP|A\.?P\.?|(?:tax\s*)?assessors?'?\s*plat|plat)\s*\.?\s*#?\s*\d+", re.I)
_ADDRESS = re.compile(
    r"\b(\d{1,5}[A-Za-z]?\s+[\w'\-]+(?:\s+[\w'\-]+){0,3}\s+"
    r"(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Lane|Ln|Boulevard|Blvd|Place|Pl|"
    r"Court|Ct|Terrace|Ter|Way|Highway|Hwy|Square|Sq))\b", re.I)

TIER1 = {b["entity_id"]: b for b in BOARDS if b["tier"] == 1}


def segment(text: str, municipality: str, default_body: str) -> list[dict]:
    """Split agenda text into items anchored on parcel references."""
    sections = body_sections(text)
    anchors = [m.start() for m in _PARCEL_ANCHOR.finditer(text)]
    items = []
    for i, pos in enumerate(anchors):
        # Bound the block by its NEIGHBOURS, not a fixed window. A fixed
        # +/-400 char window reaches into the adjacent agenda item on densely
        # packed agendas, and _pick() then takes that neighbour's first
        # non-null value -- which is how 94 Moshassuck acquired the unit count,
        # acreage and square footage belonging to 1 Moshassuck Street.
        prev_end = anchors[i - 1] + 60 if i > 0 else 0
        start = max(prev_end, pos - 400)
        nxt = anchors[i + 1] if i + 1 < len(anchors) else len(text)
        end = min(nxt, pos + 1100)
        if end <= start:
            end = min(len(text), pos + 400)
        block = text[start:end]
        am = _ADDRESS.search(text[pos:pos + 260]) or _ADDRESS.search(block)
        items.append({
            "block": block,
            # Stage, classification and the vote marker sit on the line ABOVE
            # the parcel line, so they need a tight window. Reading them from
            # the full block picks up the PREVIOUS item's header -- which is how
            # 180 Weeden, heard at Master Plan, came out as "Final Plan".
            "header": text[max(0, pos - 200): pos + 400],
            "parcel_text": re.sub(r"\s+", " ", text[pos:pos + 90]),
            "address": am.group(1).strip() if am else "",
            "reviewing_body": body_at(sections, pos, default_body),
        })
    return items


def build_records() -> list[dict]:
    corpus = json.loads(CORPUS.read_text(encoding="utf-8"))
    records = []
    for meeting in corpus.values():
        eid = meeting["entity_id"]
        if eid not in TIER1:                     # Tier 2/3 never create projects
            continue
        muni = meeting["municipality"]
        for doc in meeting["documents"]:
            p = TEXT_DIR / doc["text_file"]
            if not p.exists():
                continue
            text = p.read_text(encoding="utf-8", errors="replace")
            if len(text) < 200:
                continue
            for it in segment(text, muni, TIER1[eid]["name"]):
                fields = extract_item(it["block"])
                # ACCURACY GATE. Numeric program fields are only trusted when
                # the block unambiguously describes ONE item: a single address
                # and a single parcel reference. On densely packed agendas a
                # block can still touch its neighbour, and an inherited unit
                # count or acreage is far worse than a blank -- 94 Moshassuck
                # otherwise acquired 1 Moshassuck Street's 46 units.
                # Normalize before counting: one item routinely writes its own
                # address twice in different forms ("180 Weeden Street (AP 44
                # Lot 561)" then "addressed as 180 Weeden St"). Counting raw
                # strings treats that as two items and nulls good data.
                n_addr = len({normalize_address(m.group(1))
                              for m in _ADDRESS.finditer(it["block"])})
                n_parcel = len(_PARCEL_ANCHOR.findall(it["block"]))
                if n_addr > 1 or n_parcel > 1:
                    for k in ("residential_units", "parking_spaces", "total_gsf",
                              "site_acreage", "num_stories", "building_count",
                              "townhouses", "zoning_district_raw"):
                        fields[k] = None
                    fields["ambiguous_block"] = True
                else:
                    fields["ambiguous_block"] = False
                # Re-read stage/classification/vote from the tight header
                # window rather than the bleeding block.
                hdr = extract_item(it["header"])
                for k in ("review_stages", "review_stage_raw", "classification",
                          "vote_taken", "advances_stage"):
                    fields[k] = hdr[k]
                records.append({
                    "municipality": muni,
                    "entity_id": eid,
                    "reviewing_body": it["reviewing_body"],
                    "date": meeting["date"],
                    "kind": doc["kind"],
                    "source_url": doc["url"],
                    "parcel_text": it["parcel_text"],
                    "address": it["address"],
                    **fields,
                })
    return dedupe_appearances(records)


def dedupe_appearances(records: list[dict]) -> list[dict]:
    """One appearance per parcel per source document.

    Two sources of spurious repeats, both of which inflate stage history:

      * A parcel is named several times within one document -- in the item
        header, the description, and the minutes' account of the same item.
      * The Pawtucket/Central Falls Joint Commission (2513) and the Pawtucket
        City Planning Commission (2516) serve the SAME combined PDF, so every
        item in it is parsed once per board.

    Keying on the document URL plus the normalized parcel collapses both. The
    richest record wins, so nothing extracted is lost.
    """
    from scraper.ri_identity import parse_plat_lots

    best: dict[tuple, dict] = {}
    for r in records:
        plat, lots = parse_plat_lots(r["parcel_text"])
        # Key on the MEETING DATE, not the document URL. The two Pawtucket
        # boards file separate PDFs of the same combined agenda, so a URL key
        # leaves one appearance per board per document; a project heard once on
        # a date is one appearance.
        key = (r["municipality"], r["date"], plat,
               tuple(sorted(lots, key=lambda x: int(x))))
        filled = sum(1 for v in r.values() if v not in (None, "", False, []))
        if key not in best or filled > best[key][0]:
            best[key] = (filled, r)
    return [r for _, r in best.values()]


def collapse_records(records: list[dict]) -> list[dict]:
    """Group raw items into projects using parcel identity."""
    groups: list[dict] = []
    for r in records:
        pid = parcel_id(r["municipality"], r["parcel_text"], r["address"])
        if not (pid.has_parcel or pid.address_key):
            continue                              # nothing to identify it by
        placed = False
        for g in groups:
            plats = {p.plat for p in g["parcels"] if p.plat}
            if pid.plat and plats and pid.plat not in plats:
                continue                          # transitive over-merge guard
            for member in g["parcels"]:
                ok, _reason, review = same_project(pid, member)
                if ok:
                    g["records"].append(r)
                    g["parcels"].append(pid)
                    g["needs_review"] = g["needs_review"] or review
                    placed = True
                    break
            if placed:
                break
        if not placed:
            groups.append({"records": [r], "parcels": [pid],
                           "needs_review": False, "municipality": r["municipality"]})
    return groups


def _pick(records: list[dict], field: str):
    """First non-null value for a field across a project's appearances."""
    for r in records:
        v = r.get(field)
        if v not in (None, "", False):
            return v
    return None


def build_project(group: dict) -> dict:
    """Fold a group's appearances into one project record."""
    recs = sorted(group["records"], key=lambda r: r["date"])
    pid = next((p for p in group["parcels"] if p.has_parcel), group["parcels"][0])

    # Current stage: the furthest ADVANCING appearance. Extensions,
    # modifications and continuances are history, never stage moves.
    advancing = [r for r in recs if r.get("advances_stage") and r.get("review_stages")]
    order = {s: i for i, s in enumerate(
        ["Pre-application Conference", "Master Plan", "Combined Master and Preliminary",
         "Preliminary Plan", "Development Plan Review", "Unified Development Review",
         "Special Use Permit", "Final Plan", "Administrative Review", "Rezoning",
         "Conceptual"])}
    heard = None
    for r in advancing:
        for s in r["review_stages"]:
            if heard is None or order.get(s, 99) > order.get(heard, -1):
                heard = s

    address = _pick(recs, "address") or ""
    return {
        "municipality": group["municipality"],
        "address": address,
        "assessor_plat": pid.plat,
        "assessor_lots": ",".join(sorted(pid.lots, key=lambda x: int(x))) if pid.lots else None,
        "plat_lots_raw": recs[0]["parcel_text"][:200],
        "applicant_entity": _pick(recs, "applicant_entity"),
        "case_number": _pick(recs, "case_number"),
        "zoning_district_raw": _pick(recs, "zoning_district_raw"),
        "residential_units": _pick(recs, "residential_units"),
        "parking_spaces": _pick(recs, "parking_spaces"),
        "total_gsf": _pick(recs, "total_gsf"),
        "site_acreage": _pick(recs, "site_acreage"),
        "num_stories": _pick(recs, "num_stories"),
        "building_count": _pick(recs, "building_count"),
        "adaptive_reuse": bool(_pick(recs, "adaptive_reuse")),
        "review_scale": _pick(recs, "classification"),
        "neighborhood": _pick(recs, "neighborhood"),
        "description": _pick(recs, "description"),
        "stage_heard": heard,
        "stage_confirmed": None,      # set only from minutes with a recorded outcome
        "needs_review": group["needs_review"],
        "records": recs,
    }


def run(dry_run: bool = False) -> dict:
    init_db()
    raw = build_records()
    groups = collapse_records(raw)
    projects = [build_project(g) for g in groups]

    log.info("Raw agenda items: %d  ->  projects: %d (%.1f%% collapse)",
             len(raw), len(projects),
             (1 - len(projects) / len(raw)) * 100 if raw else 0)

    if dry_run:
        by_m = defaultdict(lambda: {"raw": 0, "proj": 0})
        for r in raw:
            by_m[r["municipality"]]["raw"] += 1
        for p in projects:
            by_m[p["municipality"]]["proj"] += 1
        for m in sorted(by_m):
            d = by_m[m]
            log.info("  %-12s raw=%-5d projects=%-4d", m, d["raw"], d["proj"])
        return {"raw": len(raw), "projects": len(projects), "dry_run": True}

    session = get_session()
    created = updated = events = citations = 0
    try:
        for p in projects:
            key = f"manual:ri-{p['municipality'].lower()}-{p['assessor_plat'] or 'na'}-" \
                  f"{(p['assessor_lots'] or normalize_address(p['address']) or 'x').replace(',', '_')[:40]}"
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
                      "adaptive_reuse", "review_scale", "neighborhood",
                      "description", "stage_heard", "stage_confirmed"):
                setattr(proj, f, p[f])
            proj.review_scale_raw = p["review_scale"]
            proj.dedupe_review = p["needs_review"]
            proj.status = p["stage_heard"] or ""
            session.flush()

            # Stage history: one row per appearance, replacing prior rows so a
            # re-run does not accumulate duplicates.
            session.query(ProjectStageEvent).filter_by(project_id=proj.id).delete()
            for r in p["records"]:
                session.add(ProjectStageEvent(
                    project_id=proj.id, meeting_date=r["date"],
                    reviewing_body=r["reviewing_body"], entity_id=r["entity_id"],
                    review_stage_raw=r.get("review_stage_raw"),
                    stage=RI_STAGE_MAP.get((r.get("review_stages") or [None])[0]),
                    advances_stage=bool(r.get("advances_stage")),
                    vote_taken=r.get("vote_taken"), outcome=r.get("outcome"),
                    source_url=r["source_url"]))
                events += 1

            first = p["records"][0]
            citations += record_extraction(
                session, proj, p, source_url=first["source_url"],
                filing_name=first["reviewing_body"], filing_date=first["date"])
        session.commit()
    finally:
        session.close()

    log.info("Created %d, updated %d, stage events %d, citations %d",
             created, updated, events, citations)
    return {"raw": len(raw), "projects": len(projects), "created": created,
            "updated": updated, "events": events, "citations": citations}


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    res = run(dry_run=ap.parse_args().dry_run)
    REPORT.write_text(json.dumps(res, indent=1), encoding="utf-8")
