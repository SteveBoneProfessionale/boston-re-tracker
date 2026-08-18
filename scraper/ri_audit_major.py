r"""
Audit the Major bucket, which the tracker now rests on.

43 of the 46 Major projects carry review_scale_basis="source" -- the scale
came from the original ingest and was never validated. The classifier only
ever ran on projects with NO scale, so anything the ingest had already
labelled was skipped. Four Providence "Major" projects turned out to be
zoning-board variances and a rezoning, which is the same failure as the MA
suffix: a plausible label taken on trust.

Every Major-by-source project is put through the same tests, in the same
order, used to classify the unlabelled ones:

  HOLDS          the documents call it a Major Land Development Project, or
                 a Major Subdivision, in the board's own words.
  CONTRADICTED   the documents call it Minor or Administrative, or a
                 validated case suffix (MI/MIL/MIS/A) says so. The ingest
                 label loses to the board's own words.
  NOT_APPLICABLE heard only by a zoning board, design review commission or
                 historic district commission. Those bodies do not
                 administer RIGL 45-23, so no Major/Minor scale exists to be
                 right or wrong -- a use variance is a different statute.
  UNSUPPORTED    no explicit label, no validated suffix, nothing either way.
                 NOT evidence the label is wrong, only that nothing here
                 confirms it. Left in place and flagged, never downgraded.

Nothing is written without --apply, and even then UNSUPPORTED keeps its
scale. Only CONTRADICTED and NOT_APPLICABLE change.

    python scraper/ri_audit_major.py
    python scraper/ri_audit_major.py --apply
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
from scraper.ri_sf_extract import text_index, project_text, RI
from scraper.ri_classify_review_scale import (
    EXPLICIT, SUFFIX_SCALE, SUFFIX_RE, NON_4523_BODY, bodies_for,
)

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

OUT = Path(__file__).parent.parent / "data" / "ri_major_audit.json"

# A filing whose own text describes it as one of these is not a 45-23 land
# development review at all, whatever the ingest called it.
VARIANCE_FILING = re.compile(
    r"application\s+for\s+(?:a\s+)?(?:use\s+and\s+)?(?:dimensional|use|special)\s+"
    r"(?:variance|use\s+permit)|"
    r"application\s+for\s+(?:use\s+and\s+)?dimens\s*ional\s+variance|"
    r"seeking\s+relief\s+from\s+section|"
    r"application\s+for\s+a?\s*special\s+use\s+permit", re.I)
REZONING_FILING = re.compile(
    r"\breferral\s+no\.?\s*\d+|requesting\s+a\s+rezoning|"
    r"rezoning\s+of\s+(?:the\s+)?(?:subject\s+)?(?:lot|parcel|propert)", re.I)
ORDINANCE_FILING = re.compile(
    r"amendment\s+of\s+the\s+zoning\s+ordinance|"
    r"amend(?:ment|ing)\s+(?:the\s+)?(?:zoning\s+)?ordinance|"
    r"\btext\s+amendment\b", re.I)

# A real 45-23 land development case carries a case number. Providence writes
# 22-022UDR, 23-029MA; the other cities write their own but all carry the
# year-number-suffix shape. Used ONLY to rescue a filing whose text reads as a
# variance or rezoning: 371 Pine really is a Major case that ALSO has a special
# use permit item at the same address, and would otherwise be lost.
LDP_CASE = re.compile(r"\b\d{2,4}\s*-\s*\d{1,4}\s*[A-Za-z]{1,4}\b")


def audit(p, text, bodies, shared=frozenset()):
    """(verdict, evidence).

    ORDER MATTERS, and it is not the order tried first. An agenda section
    heading ("MAJOR LAND DEVELOPMENT PROJECT PUBLIC HEARING") governs the
    section, not necessarily this item, and agenda footer boilerplate quotes
    other cases entirely. Two projects came back HOLDS on the same verbatim
    footer sentence about Major Subdivision 18-028MA, and two more on a
    heading that sat above a rezoning referral and a parking ordinance
    amendment. So what the filing IS now beats what heading it sat under.
    """
    has_case = bool(LDP_CASE.search(p.case_number or ""))

    # 1 -- filing type, unless a real land development case number rescues it
    if not has_case:
        for rx, what in ((VARIANCE_FILING, "a variance or special use permit"),
                         (REZONING_FILING, "a rezoning petition"),
                         (ORDINANCE_FILING, "a zoning ordinance amendment")):
            m = rx.search(text)
            if m:
                return "NOT_APPLICABLE", ("the filing is %s and carries no land "
                                          "development case number: %s" % (
                    what, re.sub(r"\s+", " ", text[m.start():m.end() + 70]).strip()))
        if bodies and all(NON_4523_BODY.search(b) for b in bodies):
            return "NOT_APPLICABLE", "heard only by " + ", ".join(sorted(bodies))

    labels = {lab for lab, rx in EXPLICIT if rx.search(text)}
    if labels == {"Major"}:
        rx = next(r for l, r in EXPLICIT if l == "Major" and r.search(text))
        m = rx.search(text)
        ev = re.sub(r"\s+", " ", text[max(0, m.start() - 70):m.end() + 90]).strip()
        # Boilerplate appearing verbatim under several projects proves nothing
        # about any one of them.
        if ev[:90] in shared:
            return "UNSUPPORTED", ("the only Major wording is text shared verbatim with "
                                   "other projects -- agenda boilerplate: %s" % ev[:140])
        return "HOLDS", ev
    if len(labels) > 1:
        return "UNSUPPORTED", "text carries more than one scale label: %s" % sorted(labels)
    if labels:
        lab = labels.pop()
        rx = next(r for l, r in EXPLICIT if l == lab and r.search(text))
        m = rx.search(text)
        return "CONTRADICTED:" + lab, re.sub(
            r"\s+", " ", text[max(0, m.start() - 70):m.end() + 90]).strip()

    # Validated case suffix disagreeing with Major
    m = SUFFIX_RE.search(p.case_number or "")
    if m and m.group(1).upper() in SUFFIX_SCALE:
        return "CONTRADICTED:" + SUFFIX_SCALE[m.group(1).upper()], (
            "case number %s, validated suffix %s" % (p.case_number, m.group(1).upper()))
    return "UNSUPPORTED", "no explicit label, no validated suffix, nothing either way"


def main(apply=False):
    idx = text_index()
    session = get_session()
    rows = [p for p in session.query(Project).filter(Project.city.in_(RI)).all()
            if not p.excluded]
    maj = [p for p in rows if p.review_scale == "Major"
           and p.review_scale_basis == "source"]

    from scraper.ri_ingest_llm import load_items, collapse
    from scraper.ri_identity import normalize_address
    byk = {}
    for g in collapse(load_items()):
        muni = g["municipality"].lower()
        for it in g["items"]:
            byk.setdefault((muni, normalize_address(it.get("address") or "")), []).append(it)

    texts = {p.id: project_text(p, idx)[0] for p in maj}

    # Pass one: which Major wording is shared between projects, i.e. boilerplate.
    seen = Counter()
    for p in maj:
        v, ev = audit(p, texts[p.id], bodies_for(p, byk))
        if v == "HOLDS":
            seen[ev[:90]] += 1
    shared = {k for k, n in seen.items() if n > 1}
    log.info("Boilerplate Major wording shared between projects: %d distinct string(s)",
             len(shared))

    out = []
    for p in maj:
        text = texts[p.id]
        v, ev = audit(p, text, bodies_for(p, byk), shared)
        out.append({"id": p.id, "city": p.city, "address": (p.address or "")[:40],
                    "case_number": p.case_number, "units": p.residential_units,
                    "gsf": p.total_gsf, "verdict": v, "evidence": ev[:300]})

    log.info("\nMAJOR PROJECTS WITH basis=source: %d\n", len(maj))
    tally = Counter(r["verdict"].split(":")[0] for r in out)
    for k in ("HOLDS", "CONTRADICTED", "NOT_APPLICABLE", "UNSUPPORTED"):
        log.info("  %-16s %4d   %3.0f%%", k, tally.get(k, 0),
                 100 * tally.get(k, 0) / len(maj) if maj else 0)
    log.info("\n  by city:")
    for c in sorted({r["city"] for r in out}):
        log.info("    %-12s %s", c, dict(Counter(
            r["verdict"].split(":")[0] for r in out if r["city"] == c)))

    for group in ("CONTRADICTED", "NOT_APPLICABLE"):
        g = [r for r in out if r["verdict"].startswith(group)]
        log.info("\n%s (%d) -- these lose their Major label:", group, len(g))
        for r in g:
            log.info("  id=%-4d %-11s %-28s -> %-10s %s", r["id"], r["city"],
                     r["address"][:28], r["verdict"].split(":")[-1][:10], r["evidence"][:100])

    uns = [r for r in out if r["verdict"] == "UNSUPPORTED"]
    log.info("\nUNSUPPORTED (%d) -- kept as Major, flagged, not downgraded:", len(uns))
    for r in uns:
        log.info("  id=%-4d %-11s %-28s case=%-10s units=%-5s gsf=%s", r["id"], r["city"],
                 r["address"][:28], r["case_number"] or "-", r["units"], r["gsf"])

    OUT.write_text(json.dumps(out, indent=1, ensure_ascii=False), encoding="utf-8")

    if apply:
        for r in out:
            p = session.get(Project, r["id"])
            if r["verdict"].startswith("CONTRADICTED"):
                new = r["verdict"].split(":")[1]
                p.review_scale = new
                p.review_scale_basis = "audit_corrected"
                p.notes = ((p.notes + " | ") if p.notes else "") + (
                    "review_scale corrected from Major to %s by audit: the ingest label "
                    "was not supported by the documents. %s" % (new, r["evidence"][:150]))
            elif r["verdict"] == "NOT_APPLICABLE":
                p.review_scale = None
                p.review_scale_basis = "not_applicable"
                p.notes = ((p.notes + " | ") if p.notes else "") + (
                    "review_scale Major removed by audit: not a RIGL 45-23 land "
                    "development review. %s" % r["evidence"][:150])
            elif r["verdict"] == "HOLDS":
                p.review_scale_basis = "explicit_language"
            else:
                p.review_scale_basis = "source_unverified"
        session.commit()
        log.info("\nAPPLIED.")
    else:
        log.info("\nREPORT ONLY -- re-run with --apply to write.")
    session.close()


if __name__ == "__main__":
    main(apply="--apply" in sys.argv)
