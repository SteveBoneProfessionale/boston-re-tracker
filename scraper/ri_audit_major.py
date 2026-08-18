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
from scraper.ri_sf_extract import text_index, project_text, _full_text, RI
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


def anchors_for(p):
    """Tokens that identify THIS project and no other on the same agenda.

    Cranston never states a street address for these schemes -- it identifies
    them by assessor plat and lot, and by project name. So the plat/lot string
    matters as much as the address does.
    """
    # IDENTITY fields only. developer and applicant_entity are themselves
    # extracted values and can be wrong: 532's developer was mis-extracted as
    # "Champlin Heights II, LLC" from a neighbouring item, and using it as an
    # anchor made the project match the very block it was wrongly copied from.
    out = set()
    for v in (p.plat_lots_raw, p.case_number):
        if v and len(str(v).strip()) > 3:
            out.add(re.sub(r"\s+", " ", str(v).strip()).lower())
    addr = re.sub(r"\s+", " ", (p.address or "").strip()).lower()
    if len(addr) > 5:
        out.add(addr)
        # "282 East Avenue" also appears as "282 East Ave."
        m = re.match(r"(\d+[\w-]*)\s+(.+?)\s+(street|st|avenue|ave|road|rd|drive|dr|"
                     r"boulevard|blvd|lane|ln|way|place|pl|court|ct)\b", addr)
        if m:
            out.add("%s %s" % (m.group(1), m.group(2)))
    if p.name and len(p.name) > 4:
        out.add(re.sub(r"\s+", " ", p.name.strip()).lower())
    return out


# Where one agenda item ends and the next begins. Three window heuristics were
# tried before this and each fixed one project by breaking another: a label
# belongs to an ITEM, and only segmentation says which item you are in.
#   Cranston  bullets a named project:  # "Champlin Heights" (vote taken)
#   Providence numbers it:              Case no. 25-075MA - 195 Nelson Street
#   Pawtucket  pipes its fields:        Master Plan Review | 282 East Avenue
ITEM_SPLIT = re.compile(
    r"(?=[▪●•■◦])"
    r"|(?=\bCase\s+(?:no|No|NO)\.?\s*\d)"
    r"|(?=\bReferral\s+(?:no|No|NO)\.?\s*\d)"
    r"|(?=\bAGENDA\s+ITEM\b)")


def item_blocks(text):
    return [b for b in ITEM_SPLIT.split(text) if b and b.strip()]


def full_text_for(p, idx):
    """Every planning document for this project, UNSCOPED.

    The scope window exists to stop one item's figures being read as another's,
    but it cuts the other way for labels: Cranston prints the label in the item
    heading and the plat two lines below, and 515's own "Champlin Heights Major
    Land Development" heading sat upstream of the window entirely. Segmentation
    plus an identity anchor does the job the window was doing, without the
    window's blind spot.
    """
    from scraper.ri_identity import normalize_address
    parts = []
    for it in idx.get((p.city.lower(), normalize_address(p.address or "")), []):
        if it.get("document"):
            ft = _full_text(it["document"])
            if ft:
                parts.append(ft)
    return "\n".join(dict.fromkeys(parts))


def blocks_for(text, anchors):
    """Every agenda item block that names this project.

    More than one can name it: project_text prepends the stored description,
    which forms its own block alongside the real agenda item. Checking only
    the first found the description and missed the item carrying the label.
    """
    return [b for b in item_blocks(text) if names_project(b, anchors)]


def major_evidences(text, window=260):
    """Every distinct Major-labelled passage in the text, in order.

    An agenda window holds several items. Taking only the first match ties a
    project to whichever item happened to appear first, which is how a real
    152-unit Major scheme came to rest on a bowling alley's label. The window
    runs well past the match because a Providence or Cranston item states its
    label first and its plat, lot or case number a couple of lines later.
    """
    out, seen = [], set()
    for lab, rx in EXPLICIT:
        if lab != "Major":
            continue
        for m in rx.finditer(text):
            ev = re.sub(r"\s+", " ",
                        text[max(0, m.start() - window):m.end() + window]).strip()
            if ev[:90] not in seen:
                seen.add(ev[:90])
                out.append(ev)
    return out


def names_project(ev, anchors):
    low = ev.lower()
    return any(a in low for a in anchors)


def best_evidence(block, anchors, max_gap=700):
    """The Major passage nearest an occurrence of this project's identifier.

    Segmentation alone is not enough in both directions. Cranston bullets its
    items, so a block is a clean item; Pawtucket marks nothing, so its whole
    agenda is one block and the first label in it wins regardless of whose it
    is. Proximity settles both: within a block, the label that belongs to this
    project is the one sitting next to the project's own plat, lot, case number
    or address. A label further than max_gap from any of them is another
    item's and is not used.
    """
    low = block.lower()
    apos = []
    for a in anchors:
        i = low.find(a)
        while i >= 0:
            apos.append(i)
            i = low.find(a, i + 1)
    if not apos:
        return None
    best, bestd = None, None
    for lab, rx in EXPLICIT:
        if lab != "Major":
            continue
        for m in rx.finditer(block):
            d = min(abs(m.start() - a) for a in apos)
            if bestd is None or d < bestd:
                bestd, best = d, m
    if best is None or bestd > max_gap:
        return None
    return re.sub(r"\s+", " ",
                  block[max(0, best.start() - 90):best.end() + 260]).strip()


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
        # EVERY Major match is considered, not just the first, and the one that
        # NAMES this project wins. Window boundaries proved the wrong tool:
        # Champlin Heights lost its own label to a neighbouring bowling alley,
        # while 282 East Avenue's own item text was dismissed as boilerplate
        # merely because a neighbour's window overlapped it.
        anchors = anchors_for(p)
        # The label must sit inside THIS project's own agenda item. Champlin
        # Heights sits directly above a "Legion Bowl" item that is itself a
        # Major Land Development, and every window-based attempt handed
        # Champlin the bowling alley's label -- or handed it to a third project
        # whose only connection was a mis-extracted developer name.
        # Full text, segmented, and within a block the label NEAREST this
        # project's own identifier. Ordering scoped-vs-full could not settle
        # it: scoped-first gave Champlin Heights the neighbouring bowling
        # alley's label, full-first gave 282 East Avenue a label belonging to
        # 258 Pine Street. Proximity to the project's own plat or address
        # settles both without preferring one city's agenda style.
        wide = blocks_for(full_text_for(p, IDX) or text, anchors)
        blks = blocks_for(text, anchors)
        for b in wide + blks:
            ev = best_evidence(b, anchors)
            if ev:
                return "HOLDS", ev
        if blks or wide:
            return "UNSUPPORTED", ("this project's own agenda item carries no Major "
                                   "label; the labels in the surrounding text belong "
                                   "to other items: %s"
                                   % re.sub(r"\s+", " ", (blks or wide)[0])[:150])
        evs = major_evidences(text)
        own = [e for e in evs if e[:90] not in shared]
        if own:
            return "HOLDS", own[0]
        return "UNSUPPORTED", ("no Major wording names this project; the label found "
                               "belongs to another item on the same agenda: %s"
                               % evs[0][:150])
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


IDX = {}


def main(apply=False):
    global IDX
    idx = IDX = text_index()
    session = get_session()
    rows = [p for p in session.query(Project).filter(Project.city.in_(RI)).all()
            if not p.excluded]
    # Re-runnable: the audit rewrites basis as it goes, so it must also select
    # the values it itself writes, or a second run sees an empty set.
    AUDITABLE = ("source", "source_unverified", "explicit_language")
    maj = [p for p in rows if p.review_scale == "Major"
           and p.review_scale_basis in AUDITABLE]

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
        for ev in major_evidences(texts[p.id]):
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
