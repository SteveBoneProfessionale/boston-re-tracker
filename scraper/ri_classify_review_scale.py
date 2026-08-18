r"""
Classify the unclassified review_scale for Rhode Island.

RIGL 45-23 sorts land development into Major, Minor and Administrative, and
that tier is the best available proxy for "would a CRE professional discuss
this". Two thirds of the scoped set carried no tier at all, which flattened
every coverage number and hid how complete the Major set already is.

TIER ORDER, AND WHY IT IS NOT THE ORDER ORIGINALLY PROPOSED

  1  EXPLICIT AGENDA LANGUAGE -- the board's own words, "Major Land
     Development Project", "Minor Subdivision", "Administrative Review".
     Authoritative, and therefore first.

  2  CASE-NUMBER SUFFIX, but only the suffixes that survive validation.
     Checking each suffix against the explicit label in the same documents:

         MI  -> Minor          17 of 19   (89%)   USED
         MIL -> Minor                              USED
         MIS -> Minor subdivision                  USED
         A   -> Administrative                     USED
         MA  -> Major           8 of 13   (62%)   NOT USED

     MA was proposed as meaning Major. It does not reliably: five of thirteen
     MA filings are labelled Minor in their own agenda text (25-060MA is a
     four-storey wing, 21-031MA three self-storage buildings, both Minor).
     A 62% signal is a guess, so MA is left unclassified rather than applied.
     UDR is a process, not a scale, and is likewise not used.

  3  THE STATUTORY THRESHOLD, and only upward.
     RIGL 45-23-32 as amended effective 1 January 2024 makes a project MINOR
     if it involves any of:
         7,500 gsf or less of new commercial/manufacturing/industrial
         up to 50% expansion of existing floor area, or 10,000 sf
         mixed use up to 6 dwelling units plus 2,500 sf commercial
         multifamily or condominium of 9 units or less
         a change in use with no extensive construction
         adaptive reuse up to 25,000 sf commercial
         adaptive reuse resulting in fewer than 9 residential units
     Major is anything exceeding those.

     So the only figure that clears every minor pathway unambiguously is NEW
     construction above 25,000 gsf -- 25,000 being the largest minor ceiling
     (adaptive reuse). Expansions are excluded from this test entirely,
     because "up to 50% of existing floor area" has no absolute cap: a 50,000
     sf addition to a 100,000 sf building is still Minor.

     Nothing is inferred from unit count or acreage, and nothing is ever
     classified DOWNWARD to Minor from size, because a small building can
     still be Major on a different limb of the definition.

CAVEAT ON LOCAL VARIATION: a municipality may INCREASE but not decrease these
thresholds. Raising a threshold can only turn a Major into a Minor, so a
Major assigned here could be Minor under a local ordinance. It cannot go the
other way, which is why the size test only ever assigns Major.

Anything surviving all three stays unclassified. Report only without --apply.

    python scraper/ri_classify_review_scale.py
    python scraper/ri_classify_review_scale.py --apply
"""

import re
import sys
import json
import logging
from pathlib import Path
from collections import Counter

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import get_session
from db.models import Project
from scraper.ri_sf_extract import (
    text_index, project_text, candidates_for, building_sf, RI,
)

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

OUT = Path(__file__).parent.parent / "data" / "ri_review_scale_classified.json"

EXPLICIT = [
    ("Major", re.compile(r"\bmajor\s+land\s+development(?:\s+project)?\b", re.I)),
    ("Major", re.compile(r"\bmajor\s+subdivision\b", re.I)),
    ("Minor", re.compile(r"\bminor\s+land\s+development(?:\s+project)?\b", re.I)),
    ("Minor", re.compile(r"\bminor\s+subdivision\b", re.I)),
    ("Administrative", re.compile(r"\badministrative\s+(?:subdivision|review)\b", re.I)),
]

# Only the suffixes that survived validation against explicit agenda labels.
SUFFIX_SCALE = {"MI": "Minor", "MIL": "Minor", "MIS": "Minor", "A": "Administrative"}
SUFFIX_RE = re.compile(r"\b\d{2,4}\s*-\s*\d{1,4}\s*([A-Za-z]{1,4})\b")

# The largest ceiling on any minor pathway (adaptive reuse, commercial).
MINOR_CEILING_SF = 25_000

# An expansion has no absolute ceiling under the 50% limb, so size cannot
# decide it.
EXPANSION = re.compile(
    r"\b(?:addition|expan\w+|enlarge\w*|extension\s+of\s+the\s+building|"
    r"adaptive\s+reuse|convert\w*|renovat\w+|rehabilit\w+)\b", re.I)
NEW_BUILD = re.compile(
    r"\b(?:new\s+construction|construct\s+a\s+new|erect\s+a\s+new|"
    r"ground[- ]up|new\s+\w{0,12}\s*building)\b", re.I)


def classify(p, text):
    """(scale, tier, evidence) or (None, None, reason)."""
    # 1 -- the board's own label
    labels = {lab for lab, rx in EXPLICIT if rx.search(text)}
    if len(labels) == 1:
        lab = labels.pop()
        rx = next(r for l, r in EXPLICIT if l == lab and r.search(text))
        m = rx.search(text)
        i = max(0, m.start() - 90)
        return lab, "1_explicit_language", re.sub(r"\s+", " ", text[i:m.end() + 110]).strip()
    if len(labels) > 1:
        return None, None, "agenda text carries more than one scale label: %s" % sorted(labels)

    # 2 -- validated case-number suffix
    m = SUFFIX_RE.search(p.case_number or "")
    if m:
        suf = m.group(1).upper()
        if suf in SUFFIX_SCALE:
            return SUFFIX_SCALE[suf], "2_case_suffix", "case number %s, suffix %s" % (
                p.case_number, suf)
        if suf in ("MA", "UDR", "DPR"):
            pass  # deliberately not a scale signal

    # 3 -- statutory threshold, upward only, new construction only
    sf, _ev = building_sf(candidates_for(text))
    if sf and sf > MINOR_CEILING_SF:
        if EXPANSION.search(text) and not NEW_BUILD.search(text):
            return None, None, ("%s sf but the filing reads as an expansion or reuse, "
                                "where the 50%% limb has no ceiling" % f"{sf:,}")
        return "Major", "3_statutory_threshold", (
            "%s sf of new construction exceeds every minor ceiling (highest is "
            "25,000 sf for adaptive reuse) under RIGL 45-23-32 as amended 1 Jan 2024"
            % f"{sf:,}")

    if m and m.group(1).upper() in ("MA", "UDR", "DPR"):
        return None, None, ("case suffix %s is not a reliable scale signal "
                            "(MA is Major only 62%% of the time)" % m.group(1).upper())
    return None, None, "no explicit label, no validated suffix, no qualifying size"


# Bodies that do NOT administer RIGL 45-23 land development review. A filing
# heard only by one of these has no Major/Minor/Administrative scale to be
# missing -- it is a variance or a design review, a different statute.
NON_4523_BODY = re.compile(
    r"zoning\s+board|design\s+review|historic\s+district|redevelopment\s+agency",
    re.I)


def bodies_for(p, byk):
    from scraper.ri_identity import normalize_address
    return {i.get("reviewing_body") for i in
            byk.get((p.city.lower(), normalize_address(p.address or "")), [])
            if i.get("reviewing_body")}


def main(apply=False):
    idx = text_index()
    session = get_session()
    rows = [p for p in session.query(Project).filter(Project.city.in_(RI)).all()
            if not p.excluded]
    unc = [p for p in rows if p.review_scale not in ("Major", "Minor", "Administrative")]

    log.info("\nActive: %d   already classified: %d   unclassified: %d",
             len(rows), len(rows) - len(unc), len(unc))

    from scraper.ri_ingest_llm import load_items, collapse
    from scraper.ri_identity import normalize_address
    byk = {}
    for g in collapse(load_items()):
        muni = g["municipality"].lower()
        for it in g["items"]:
            byk.setdefault((muni, normalize_address(it.get("address") or "")), []).append(it)

    resolved, unresolved, na = [], [], []
    for p in unc:
        text, _ = project_text(p, idx)
        scale, tier, ev = classify(p, text)
        rec = {"id": p.id, "city": p.city, "address": p.address or "",
               "case_number": p.case_number, "scale": scale, "tier": tier,
               "evidence": (ev or "")[:260]}
        if scale:
            resolved.append(rec)
            continue
        # No scale found. Is one even applicable?
        b = bodies_for(p, byk)
        if b and all(NON_4523_BODY.search(x) for x in b):
            rec["evidence"] = ("not a RIGL 45-23 land development review: heard only by "
                               + ", ".join(sorted(b)))
            na.append(rec)
        else:
            unresolved.append(rec)

    log.info("\nRESOLVED BY TIER")
    by_tier = Counter(r["tier"] for r in resolved)
    for t in ("1_explicit_language", "2_case_suffix", "3_statutory_threshold"):
        n = by_tier.get(t, 0)
        log.info("   %-24s %4d   %s", t, n,
                 dict(Counter(r["scale"] for r in resolved if r["tier"] == t)))
    log.info("   %-24s %4d   heard only by a zoning board, design review or historic"
             " commission -- no 45-23 scale exists to be missing", "not applicable", len(na))
    log.info("   %-24s %4d   genuinely unknown", "still unclassified", len(unresolved))

    log.info("\nRESOLVED BY SCALE: %s", dict(Counter(r["scale"] for r in resolved)))
    log.info("BY CITY: %s", dict(Counter(r["city"] for r in resolved)))

    log.info("\nWHY THE REST COULD NOT BE CLASSIFIED")
    for why, n in Counter(
            r["evidence"][:60] for r in unresolved).most_common(8):
        log.info("   %4d  %s", n, why)

    log.info("\nTIER 3 ASSIGNMENTS (size-based Major), listed for checking:")
    for r in [x for x in resolved if x["tier"] == "3_statutory_threshold"]:
        log.info("   id=%-4d %-11s %-26s %s", r["id"], r["city"], r["address"][:26],
                 r["evidence"][:110])

    OUT.write_text(json.dumps({"resolved": resolved, "not_applicable": na,
                               "unresolved": unresolved},
                              indent=1, ensure_ascii=False), encoding="utf-8")

    if apply:
        for r in na:
            pr = session.get(Project, r["id"])
            pr.review_scale_basis = "not_applicable"
            pr.notes = ((pr.notes + " | ") if pr.notes else "") + r["evidence"][:150]
        for r in unresolved:
            pr = session.get(Project, r["id"])
            pr.review_scale_basis = "unknown"
        for p2 in rows:
            if p2.review_scale in ("Major", "Minor", "Administrative") and not p2.review_scale_basis:
                p2.review_scale_basis = "source"
        for r in resolved:
            p = session.get(Project, r["id"])
            p.review_scale = r["scale"]
            p.review_scale_basis = r["tier"].split("_", 1)[1]
            p.notes = ((p.notes + " | ") if p.notes else "") + (
                "review_scale %s set by %s: %s" % (r["scale"], r["tier"], r["evidence"][:150]))
        session.commit()
        log.info("\nAPPLIED: %d classified.", len(resolved))
    else:
        log.info("\nREPORT ONLY -- re-run with --apply to write.")
    session.close()


if __name__ == "__main__":
    main(apply="--apply" in sys.argv)
