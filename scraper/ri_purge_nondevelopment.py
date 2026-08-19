r"""
Record-by-record scope test over every Rhode Island project.

THE PROBLEM. The status filter exposes filing ACTIONS -- Lot Merger, Design
Waiver, Advisory Opinion, City Council Referral, Administrative Review -- and
most of those are paperwork, not buildings. But the label is not the test.
371 Pine Street (Crossroads, 35 units), BCM Realty's 26 units on Van Zandt
Avenue, the Waites Wharf hotel lots and the Treadway expansion all arrived as
SPECIAL USE PERMITS and are all real. A tire shop, a McDonald's, a JP Morgan
Chase branch and Moniker Brewing arrived the same way and are not.

So every record is judged on substance, read from the untruncated source text
plus the structured extraction, against three tests:

  1. BUILDING   does it create or materially change a building? New
                construction, addition, conversion, adaptive reuse,
                substantial demolition-and-rebuild.
  2. COMMERCIAL is it commercial as this repo defines it -- an income-
                producing use, or multifamily at four units and up? The
                definition lives in scraper/ri_commercial.py and is reused
                here rather than restated.
  3. PROGRAM    is there a programme? A unit count, a floor area, a storey
                count, or a named use. A filing with none of those describes
                a permission, not a project.

VERDICTS. All three pass -> KEEP. All three fail -> REMOVE. Anything else, or
any evidence that pulls both ways -> FLAG and leave it in. The instruction is
explicit and matches the whole history of this project: thirty borderline
records reviewed beats one real project lost.

REMOVE means QUARANTINE. Nothing is hard-deleted here. The reason and the
sentence that justified it are written to the record so the call is auditable
and reversible.

    python scraper/ri_purge_nondevelopment.py
    python scraper/ri_purge_nondevelopment.py --apply
"""

import re
import sys
import json
import logging
from pathlib import Path
from collections import Counter, defaultdict

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from db.database import get_session
from db.models import Project
from scraper.ri_sf_extract import text_index, project_text, RI
from scraper.ri_commercial import (
    MINOR_RELIEF, LOT_ONLY, CONSTRUCTION, SMALL_RESI, COMMERCIAL_USE,
    LAND_DEV_CASE, LAND_DEV_SUFFIX, UNIT_FLOOR,
)

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)
OUT = ROOT / "data" / "ri_purge_report.json"

# Test 1 -- a building is created or materially changed.
BUILDING = re.compile(
    r"\b(?:construct\w*|erect\w*|build(?:ing)?\s+a\b|new\s+(?:building|structure|"
    r"construction)|redevelop\w*|renovat\w*|convert\w*|conversion|adaptive\s+reuse|"
    r"addition\b|expand\w*|expansion|rehabilit\w*|substantial\s+alteration|"
    r"demolish\w*\s+and\s+(?:construct|build|replace|rebuild)|rebuild\w*)\b", re.I)

# Test 3 -- a programme, stated in any form.
PROGRAM_NUM = re.compile(
    r"\b\d[\d,]*\s*(?:dwelling\s+)?units?\b|\b\d[\d,]*\s*(?:square\s+f|sq\.?\s*f)|"
    r"\b\d+\s*(?:-|\s)?stor(?:y|ies|ey)\b|\b\d+\s*guest\s*rooms?\b", re.I)

# Paperwork with no building behind it. These fire only when nothing in tests
# 1-3 fires, so a special use permit WITH a building is never caught by them.
PAPER_ONLY = re.compile(
    r"\blot\s*line\b|\blot\s+merger\b|\bmerging\s+of\b|\bconsolidat\w+\s+(?:of\s+)?lots?\b|"
    r"\bdeed\s+(?:correction|restriction)\b|\badministrative\s+subdivision\b|"
    r"\badvisory\s+opinion\b|\breferral\s+to\s+(?:the\s+)?city\s+council\b|"
    r"\bdesign\s+waiver\b|\bwaiver\s+of\s+(?:the\s+)?(?:design|dimensional)\b|"
    r"\bextension\s+of\s+(?:time|approval)\b|\breinstatement\b|"
    r"\bchange\s+of\s+use\b(?![^.]{0,60}\b(?:construct|renovat|convert|addition)\b)|"
    r"\btenant\s+fit[- ]?out\b|\bsign(?:age)?\s+permit\b", re.I)

# Works that read as construction but change no building: adding a lane to a
# drive-through, restriping, re-facing a sign. McDonald's at 1481 Broad Street
# "expand an existing single lane Drive-Through Facility to 2 lanes" matched
# "expand" and so escaped the exclusion entirely.
NOT_A_BUILDING = re.compile(
    r"drive[- ]?thr(?:u|ough)[^.]{0,60}\blanes?\b|\blanes?\b[^.]{0,40}drive[- ]?thr|"
    r"re-?strip\w*|re-?fac\w*\s+(?:the\s+)?sign|parking\s+lot\s+(?:re)?(?:configur|surfac)\w*|"
    r"expand\w*\s+(?:the\s+)?(?:existing\s+)?(?:parking|patio|deck|outdoor\s+seating)", re.I)

# A use that is commercial in form but is a single tenant fitting out an
# existing box. Only ever decisive when no building and no programme is present.
SINGLE_TENANT = re.compile(
    r"\btire\s+(?:shop|center)\b|\bmcdonald|\bdunkin|\bstarbucks|\bchase\s+bank\b|"
    r"\bjp\s*morgan\b|\bbank\s+branch\b|\bdrive[- ]?thr(?:u|ough)\b|"
    r"\bbrewery\b|\bmicro-?brewery\b|\btaproom\b|\bnail\s+salon\b|\bbarber\b|"
    r"\bconvenience\s+store\b|\bcar\s+wash\b|\bgas\s+station\b|\bsmoke\s+shop\b|"
    r"\brestaurant\s+in\s+an?\s+existing\b", re.I)


def verdict(p, text, units, sf):
    """(verdict, reason, sentence)."""
    def sent(rx):
        m = rx.search(text)
        if not m:
            return ""
        i = max(0, m.start() - 90)
        return re.sub(r"\s+", " ", text[i:m.end() + 140]).strip()

    big = (units or 0) >= UNIT_FLOOR
    has_building = bool(BUILDING.search(text)) and not NOT_A_BUILDING.search(text)
    has_commercial = bool(COMMERCIAL_USE.search(text))
    has_program = bool(PROGRAM_NUM.search(text)) or big or bool(sf)
    land_dev = bool(LAND_DEV_CASE.search(text) or LAND_DEV_SUFFIX.search(p.case_number or "")
                    or p.review_scale in ("Major", "Minor"))

    # --- KEEP, on positive evidence, before any exclusion fires. A real
    # project routinely mentions a driveway or a parking garage, and testing
    # the exclusions first threw out a 326-unit building on the word "garage".
    if big and not SMALL_RESI.search(text):
        return "KEEP", "%d residential units, at or above the %d-unit floor" % (units, UNIT_FLOOR), sent(PROGRAM_NUM)
    if has_building and has_commercial and has_program:
        return "KEEP", "creates or changes a building, states a commercial use, states a programme", sent(BUILDING)
    if land_dev and has_building:
        return "KEEP", "land development / development plan review case with construction", sent(BUILDING)

    # --- A statutory land development classification is positive evidence and
    # outweighs any exclusion word. Colbea's Warwick scheme is a MAJOR land
    # development that happens to mention signage height; a design waiver "to
    # allow for residential use" is part of a residential project; a
    # "REQUEST FOR REINSTATEMENT - MINOR LAND DEVELOPMENT PROJECT" is a real
    # project being revived. All four were being removed on an incidental
    # word. Nothing carrying a Major or Minor scale, or an established
    # completion, may be removed by this pass.
    protected = (p.review_scale in ("Major", "Minor", "Administrative")
                 or bool(getattr(p, "completion_stage", None)))

    # --- REMOVE, only when all three tests fail.
    if not has_building and not has_program and not protected:
        if PAPER_ONLY.search(text):
            return "REMOVE", "a filing action with no building and no programme", sent(PAPER_ONLY)
        if SINGLE_TENANT.search(text):
            return "REMOVE", "a single tenant in an existing building, no construction", sent(SINGLE_TENANT)
        if MINOR_RELIEF.search(text):
            return "REMOVE", "minor zoning relief, no building and no programme", sent(MINOR_RELIEF)
        if SMALL_RESI.search(text) and not has_commercial:
            return "REMOVE", "single or two-family residential", sent(SMALL_RESI)
        if LOT_ONLY.search(text) and not CONSTRUCTION.search(text):
            return "REMOVE", "a parcel action with no construction proposed", sent(LOT_ONLY)

    # --- everything else stays, flagged.
    why = []
    if protected and not (has_building and has_program):
        why.append("carries a %s land development classification but the text is thin"
                   % (p.review_scale or "completed"))
    if not has_building:
        why.append("no construction language")
    if not has_commercial:
        why.append("no commercial use named")
    if not has_program:
        why.append("no unit count, floor area or storey count")
    if not why:
        return "KEEP", "passes the building, commercial and programme tests", sent(BUILDING)
    return "FLAG", "; ".join(why), re.sub(r"\s+", " ", text[:230]).strip()


def main(apply=False):
    idx = text_index()
    session = get_session()
    rows = [p for p in session.query(Project).filter(Project.city.in_(RI)).all()
            if not p.excluded]

    out = []
    for p in rows:
        text, _ = project_text(p, idx)
        text = text or (p.description or "")
        v, why, sentence = verdict(p, text, p.residential_units, p.total_gsf)
        out.append({"id": p.id, "city": p.city, "status": p.stage_heard or "",
                    "address": (p.address or p.name or "")[:52],
                    "units": p.residential_units, "sf": p.total_gsf,
                    "verdict": v, "reason": why, "sentence": sentence[:300]})

    tally = Counter(r["verdict"] for r in out)
    log.info("\nRHODE ISLAND SCOPE TEST -- %d active records\n", len(rows))
    log.info("  KEEP   %4d", tally.get("KEEP", 0))
    log.info("  FLAG   %4d   (left in, for review)", tally.get("FLAG", 0))
    log.info("  REMOVE %4d   (quarantine only -- nothing is deleted)", tally.get("REMOVE", 0))

    log.info("\nBY CITY")
    for c in sorted({r["city"] for r in out}):
        cc = Counter(r["verdict"] for r in out if r["city"] == c)
        log.info("  %-11s keep %3d  flag %3d  remove %3d", c,
                 cc.get("KEEP", 0), cc.get("FLAG", 0), cc.get("REMOVE", 0))

    log.info("\nREMOVALS BY REASON")
    for why, n in Counter(r["reason"] for r in out if r["verdict"] == "REMOVE").most_common():
        log.info("  %4d  %s", n, why)

    risky = [r for r in out if r["verdict"] == "REMOVE"
             and ((r["units"] or 0) >= 5 or (r["sf"] or 0) > 0)]
    log.info("\nREMOVALS STATING 5+ UNITS OR ANY FLOOR AREA -- check these: %d", len(risky))
    for r in risky:
        log.info("  id=%-4d %-11s units=%-5s sf=%-9s %-34s | %s", r["id"], r["city"],
                 r["units"], r["sf"], r["address"][:34], r["reason"][:46])
        log.info("        %s", r["sentence"][:150])

    OUT.write_text(json.dumps(out, indent=1, ensure_ascii=False), encoding="utf-8")

    if apply:
        n = 0
        for r in out:
            p = session.get(Project, r["id"])
            if r["verdict"] == "REMOVE":
                p.excluded = True
                p.excluded_reason = r["reason"]
                p.notes = ((p.notes + " | ") if p.notes else "") + (
                    "QUARANTINED by the scope test: %s. Justifying text: %r"
                    % (r["reason"], r["sentence"][:200]))
                n += 1
            elif r["verdict"] == "FLAG":
                p.is_flagged = True
                p.notes = ((p.notes + " | ") if p.notes else "") + (
                    "SCOPE FLAG (kept in): %s" % r["reason"])
        session.commit()
        log.info("\nAPPLIED -- %d quarantined, %d flagged and kept", n, tally.get("FLAG", 0))
    else:
        log.info("\nDRY RUN -- re-run with --apply")
    session.close()


if __name__ == "__main__":
    main(apply="--apply" in sys.argv)
