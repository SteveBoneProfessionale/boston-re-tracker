r"""
Scope the Rhode Island set to actual commercial development.

THE TEST is what the project IS, not how it was filed: does the filing propose
CONSTRUCTION that CREATES commercial or multifamily space? Filing type is a
signal and nothing more -- a 40-unit building routinely reaches a board as a
special use permit, and excluding on the procedural label would throw away
real deals.

  STAYS   new construction, additions and conversions creating commercial or
          multifamily space -- multifamily residential, mixed-use, office, lab
          and research, industrial, retail, hotel, institutional. Adaptive
          reuse of a mill or any existing building into one of those stays.

  GOES    dimensional and use variances with no construction, change of use
          with no construction, lot line adjustments and deed corrections,
          minor subdivisions creating lots with no building programme, sign
          and fence permits, single- and two-family residential, tenant
          fit-outs, parking and site work, and anything describing an
          operational or paperwork change rather than a building.

AMBIGUOUS CASES ARE KEPT and flagged. Reviewing twenty borderline rows is
cheaper than losing one real project.

Classification reads the FULL document text from data/ri_pdfs/text/, not the
stored description -- those are truncated at 1,200 characters and 22% of items
hit that ceiling exactly, so the evidence that decides a case is often in the
part that was cut.

Report only. Nothing is written without --apply.

    python scraper/ri_scope_commercial.py            # report
    python scraper/ri_scope_commercial.py --apply    # quarantine
"""

import re
import sys
import json
import logging
from pathlib import Path
from collections import Counter, defaultdict

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import get_session
from db.models import Project
from scraper.ri_sf_extract import (
    text_index, project_text, candidates_for, building_sf, RI,
)

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

OUT = Path(__file__).parent.parent / "data" / "ri_scope_review.json"

# ---- POSITIVE: construction that creates space ------------------------
BUILD_VERB = re.compile(
    r"\b(?:construct(?:ion|ing|ed)?|erect(?:ing|ed)?|build(?:ing|s)?\s+a\b|"
    r"redevelop(?:ment|ing|ed)?|develop(?:ment)?\s+of|new\s+construction|"
    r"convert(?:ing|ed|sion)?|adaptive\s+reuse|rehabilit\w+|"
    r"(?<!in\s)\baddition\b|expand(?:ing|ed)?\s+the\s+(?:building|structure|existing)|"
    r"demolish\w*\s+.{0,60}?(?:and|to)\s+(?:construct|build|erect)|"
    r"ground[- ]up)\b", re.I)

# Programme nouns that make the created space commercial or multifamily.
PROGRAM = re.compile(
    r"\b(?:multi[- ]?family|multifamily|apartment|apartments|dwelling\s+units|"
    r"residential\s+units|mixed[- ]?use|commercial\s+(?:building|space|unit)|"
    r"retail|office|laborator\w+|\blab\b|research|industrial|warehouse|"
    r"self[- ]?storage|storage\s+facility|hotel|inn\b|restaurant|"
    r"institutional|school|church|clinic|health\s+cent|medical\s+office|"
    r"manufactur\w+|distribution|fitness|marina|car\s+wash|"
    r"gas\s+station|fuel(?:ing)?\s+station|drive[- ]?thru|dealership)\b", re.I)

# Unit counts are written every way a clerk can write them:
#   "42 units"   "a ten-unit residential complex"   "sixteen (16) multi-unit"
# Reading only bare digits sent real multifamily projects to the ambiguous
# pile, which is the opposite of the point.
_WORDS = ("three four five six seven eight nine ten eleven twelve thirteen "
          "fourteen fifteen sixteen seventeen eighteen nineteen twenty thirty "
          "forty fifty sixty seventy eighty ninety").split()
WORD_NUM = dict(zip(_WORDS, [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                             17, 18, 19, 20, 30, 40, 50, 60, 70, 80, 90]))

UNITS_PHRASE = re.compile(
    r"(?P<n>\d{1,4})\s*(?:\(\d+\)\s*)?(?:new\s+|additional\s+|total\s+)?"
    r"(?:residential\s+|dwelling\s+|apartment\s+|housing\s+|multi-?)?units?\b", re.I)

# "sixteen (16) multi-unit", "ten-unit". Lots are counted the same way in this
# corpus ("forty-four (44) lots") and are deliberately NOT matched here.
UNITS_WORD = re.compile(
    r"\b(?P<w>" + "|".join(_WORDS) + r")"
    r"(?:\s*\((?P<d>\d{1,4})\))?[\s-]+(?:multi-?)?units?\b", re.I)

UNITS_PAREN = re.compile(r"\((?P<n>\d{1,4})\)\s*(?:multi-?)?units?\b", re.I)

# ---- NEGATIVE: paperwork, operations, or out-of-scope programme -------
EXCLUSION_TESTS = [
    ("sign_or_fence", re.compile(
        r"\b(?:sign(?:age)?\s+(?:permit|variance|relief)|erect\s+a\s+sign|"
        r"freestanding\s+sign|wall\s+sign|message\s+board|banner|awning|"
        r"fence\s+height|(?:install|replace)\s+.{0,25}\bfence\b|"
        r"maximum\s+fence)\b", re.I)),
    ("lot_line_or_deed", re.compile(
        r"\b(?:lot\s*line\s+(?:adjust|relocat|revis|chang)|adjust\s+the\s+shared\s+lot\s+line|"
        r"boundary\s+line\s+(?:adjust|agreement)|illegally\s+merged|merged\s+by\s+deed|"
        r"unmerge|de-?merge|scrivener|confirmatory\s+deed|quiet\s+title|"
        r"administrative\s+subdivision\s+to\s+adjust|lot\s+merger)\b", re.I)),
    ("single_or_two_family", re.compile(
        r"\b(?:single[\s-]*family\s+(?:dwelling|home|house|residence|structure|lots?)|"
        r"one[\s-]*family\s+dwelling|two[\s-]*family\s+dwelling|duplex)\b", re.I)),
    ("minor_subdivision_no_program", re.compile(
        r"\b(?:subdivid\w+|subdivision\s+of)\b.{0,120}?\b(?:into|to\s+create)\b"
        r".{0,40}?\b(?:two|three|2|3|\d+)\b.{0,20}\blots?\b", re.I | re.S)),
    ("change_of_use", re.compile(
        r"\bchange\s+of\s+use\b|\bestablish\s+(?:the\s+)?(?:use\s+of\s+)?"
        r"(?:the\s+)?(?:property|premises|building)?\s*(?:as|to\s+be)\b", re.I)),
    ("parking_or_site_work", re.compile(
        r"\b(?:restrip\w+|re-?striping|parking\s+lot\s+(?:reconfigur|resurfac|expan)|"
        r"principal\s+use\s+parking|paving|repav\w+|landscape\s+buffer\s+only)\b", re.I)),
    ("procedural", re.compile(
        r"\b(?:extension\s+of\s+(?:the\s+)?(?:previously\s+granted\s+)?"
        r"(?:preliminary|master|final)?\s*(?:plan\s+)?approval|"
        r"one[- ]year\s+extension|two[- ]year\s+extension|"
        r"abandonment\s+of|request\s+to\s+transfer\s+ownership|"
        r"(?:zoning\s+)?ordinance\s+amendment|amendment\s+of\s+(?:the\s+)?ordinance|"
        r"amendment\s+of\s+section|"
        r"comprehensive\s+plan|tax\s+stabilization)\b", re.I)),
    ("tenant_fitout", re.compile(
        r"\b(?:tenant\s+(?:fit[- ]?out|improvement)|interior\s+(?:fit|renovation)\s+only|"
        r"storefront\s+(?:window|door)\s+replacement|replace\s+an?\s+existing\s+storefront)\b", re.I)),
    ("variance_no_construction", re.compile(
        r"\b(?:dimensional\s+variance|use\s+variance|special\s+use\s+permit|"
        r"zoning\s+relief|relief\s+from\s+section)\b", re.I)),
]

# EXPLICIT evidence that nothing is being built. The weak exclusions now
# require this rather than merely lacking a construction phrase.
#
# Why: 16 of the 59 variance-bucket exclusions had their text clipped by the
# 1,100-character scope window, so "no construction phrase found" could simply
# mean "the phrase was past the cut". Absence of evidence was doing work that
# only evidence should do. Anything failing this test becomes AMBIGUOUS and
# stays in.
NO_CONSTRUCTION = re.compile(
    r"\b(?:no\s+(?:new\s+)?construction|no\s+exterior\s+(?:work|changes?|alterations?)|"
    r"no\s+(?:physical\s+)?(?:changes?|alterations?)\s+to\s+the\s+"
    r"(?:existing\s+)?(?:structure|building|footprint|exterior)|"
    r"no\s+changes?\s+to\s+the\s+existing\s+footprint|"
    r"no\s+new\s+(?:structures?|buildings?)|no\s+expansion|"
    r"no\s+site\s+work|no\s+additions?\b|"
    r"without\s+any\s+construction|no\s+building\s+is\s+proposed|"
    r"existing\s+(?:structure|building)\s+(?:to|will)\s+remain\s+(?:unchanged|as\s+is)|"
    r"interior\s+(?:work|renovations?)\s+only|use\s+change\s+only)\b", re.I)

# A rezoning is a real pipeline signal but an EARLIER one than a planning
# filing: the programme is usually defined in a later application. Kept, and
# marked so it reads as the front of the pipeline rather than as a project
# with a defined programme.
REZONING = re.compile(
    r"\b(?:rezon(?:e|ing)|zone\s+change|change\s+of\s+zone|"
    r"zoning\s+map\s+(?:change|amendment)|comprehensive\s+plan\s+amendment|"
    r"requesting\s+(?:a\s+)?rezoning)\b", re.I)

# Reviewed by hand and plainly real; the classifier could not see enough of
# the text to reach them.
FORCE_KEEP = {
    601: "Carpionato Group / NorthPoint warehouse and distribution facility, 46.72 acres",
    671: "Strive Realty, 115 Harris Ave: rezoning M-1 to M-MU 90 for a hotel/residential conversion",
    674: "Moses Brown School campus master plan amendment",
    680: "550 Veazie Street: rezoning to allow expansion of a self-storage facility",
    506: "The Beatrice hotel: increasing the rooftop penthouse footprint",
}

# "Special use permit" and "dimensional variance" are the weakest signals of
# all -- most real projects carry one. They only exclude when NOTHING in the
# filing describes construction.
WEAK_ONLY = {"variance_no_construction", "change_of_use"}


def units_in(text):
    best = 0
    for m in UNITS_WORD.finditer(text):
        n = int(m.group("d")) if m.group("d") else WORD_NUM.get(m.group("w").lower(), 0)
        best = max(best, n)
    for m in UNITS_PAREN.finditer(text):
        best = max(best, int(m.group("n")))
    for m in UNITS_PHRASE.finditer(text):
        n = int(m.group("n"))
        if n <= 2000:
            # "units per acre" is a density, not a programme.
            after = text[m.end():m.end() + 18]
            if re.match(r"\s*(?:per|/)\s*acre", after, re.I):
                continue
            best = max(best, n)
    return best


def classify(p, text):
    """(verdict, reason, evidence sentence). verdict: KEEP | EXCLUDE | AMBIGUOUS."""
    def sentence(rx):
        m = rx.search(text)
        if not m:
            return ""
        i = max(0, m.start() - 110)
        return re.sub(r"\s+", " ", text[i:m.end() + 130]).strip()

    if p.id in FORCE_KEEP:
        return "KEEP", "reviewed by hand: " + FORCE_KEEP[p.id], ""

    stated_units = p.residential_units or 0
    text_units = units_in(text)
    units = max(stated_units, text_units)
    sf, _ev = building_sf(candidates_for(text))
    build = BUILD_VERB.search(text)
    prog = PROGRAM.search(text)

    hits = [name for name, rx in EXCLUSION_TESTS if rx.search(text)]
    strong_hits = [h for h in hits if h not in WEAK_ONLY]

    # --- positive evidence, strongest first ---------------------------
    if units >= 3:
        # Three or more dwelling units is multifamily. Single- and two-family
        # go, but only when nothing larger is proposed.
        if "single_or_two_family" in hits and units < 3:
            pass
        else:
            return "KEEP", f"multifamily programme: {units} units", sentence(UNITS_PHRASE)
    if sf and build:
        return "KEEP", f"construction with a stated building area of {sf:,} sf", sentence(BUILD_VERB)
    if build and prog:
        return "KEEP", "construction creating commercial or multifamily space", sentence(BUILD_VERB)

    # --- no positive evidence: is there a reason to exclude? ----------
    if strong_hits:
        name = strong_hits[0]
        rx = dict(EXCLUSION_TESTS)[name]
        return "EXCLUDE", name, sentence(rx)
    if hits:
        name = hits[0]
        rx = dict(EXCLUSION_TESTS)[name]
        # A variance or a change of use only excludes when the filing SAYS
        # nothing is being built. Absence of a construction phrase is not
        # evidence of absence when the text may have been clipped.
        if NO_CONSTRUCTION.search(text):
            return "EXCLUDE", name + "_explicitly_no_construction", sentence(NO_CONSTRUCTION)
        return "AMBIGUOUS", ("filed as a %s, but nothing in the text states that no "
                             "construction is proposed -- kept pending review" % name),             sentence(rx)

    if build or prog:
        return "AMBIGUOUS", ("construction or programme language present but no unit "
                             "count, building area, or clear pairing of the two"), \
               sentence(BUILD_VERB if build else PROGRAM)
    if not text.strip():
        return "AMBIGUOUS", "no document text available for this project", ""
    return "AMBIGUOUS", "no construction, programme or exclusion signal found", \
        re.sub(r"\s+", " ", text[:220]).strip()


FIELDS = ["address", "developer", "total_gsf", "residential_units", "asset_class",
          "stage_heard", "case_number", "zoning_district_raw", "site_acreage",
          "parking_spaces", "num_stories", "latitude", "applicant_entity"]


def main(apply=False):
    idx = text_index()
    session = get_session()
    pool = [p for p in session.query(Project).filter(Project.city.in_(RI)).all()
            if not p.excluded]

    rows = []
    for p in pool:
        text, ndocs = project_text(p, idx)
        v, reason, ev = classify(p, text)
        # A rezoning with no defined programme is the FRONT of the pipeline,
        # not a project with a programme. Marked so it reads as an earlier
        # signal rather than being mixed in with defined schemes.
        is_rez = bool(REZONING.search(text))
        has_program = bool(p.residential_units or p.total_gsf or units_in(text)
                           or building_sf(candidates_for(text))[0])
        entry = "rezoning" if (is_rez and not has_program) else "development"
        rows.append({"id": p.id, "city": p.city, "address": p.address or "",
                     "units": p.residential_units, "gsf": p.total_gsf,
                     "asset_class": p.asset_class, "docs": ndocs,
                     "entry_type": entry,
                     "verdict": v, "reason": reason, "evidence": ev[:400]})

    keep = [r for r in rows if r["verdict"] == "KEEP"]
    exc = [r for r in rows if r["verdict"] == "EXCLUDE"]
    amb = [r for r in rows if r["verdict"] == "AMBIGUOUS"]

    # 1 ── counts per city
    log.info("\n1  SCOPE RESULT  (%d active projects classified)", len(rows))
    log.info("   %-12s %8s %9s %10s %9s", "CITY", "KEEP", "AMBIG*", "EXCLUDE", "REMAIN")
    for c in RI:
        k = sum(1 for r in keep if r["city"] == c)
        a = sum(1 for r in amb if r["city"] == c)
        e = sum(1 for r in exc if r["city"] == c)
        log.info("   %-12s %8d %9d %10d %9d", c, k, a, e, k + a)
    log.info("   %-12s %8d %9d %10d %9d", "TOTAL", len(keep), len(amb), len(exc),
             len(keep) + len(amb))
    log.info("   * ambiguous rows are KEPT and flagged, never excluded")

    # 2 ── exclusions by reason
    log.info("\n2  EXCLUSIONS BY REASON")
    by_reason = Counter(r["reason"] for r in exc)
    for name, n in by_reason.most_common():
        log.info("   %-46s %4d", name, n)
    log.info("\n   by city and reason:")
    per = defaultdict(Counter)
    for r in exc:
        per[r["city"]][r["reason"]] += 1
    for c in RI:
        if per[c]:
            log.info("   %-11s %s", c, dict(per[c].most_common()))

    # 3 ── field coverage on the remaining set
    remaining_ids = {r["id"] for r in keep} | {r["id"] for r in amb}
    rem = [p for p in pool if p.id in remaining_ids]
    log.info("\n3  FIELD COVERAGE, REMAINING SET ONLY  (%d projects)", len(rem))
    log.info("   %-22s %10s %10s %9s", "FIELD", "NOW/401", "AFTER", "CHANGE")
    for f in FIELDS:
        a = sum(1 for p in pool if getattr(p, f, None) not in (None, "", False))
        b = sum(1 for p in rem if getattr(p, f, None) not in (None, "", False))
        pa, pb = 100 * a / len(pool), 100 * b / len(rem) if rem else 0
        log.info("   %-22s %9.0f%% %9.0f%% %+8.0f", f, pa, pb, pb - pa)

    # 4 ── safety check
    log.info("\n4  EXCLUSIONS STATING 4+ UNITS OR ANY SQUARE FOOTAGE  (safety check)")
    risky = [r for r in exc if (r["units"] or 0) >= 4 or r["gsf"]]
    if not risky:
        log.info("   none -- no excluded row states 4+ units or any building area")
    for r in risky:
        log.info("   id=%-4d %-11s %-28s u=%-5s sf=%-9s  %s",
                 r["id"], r["city"], r["address"][:28], r["units"],
                 f'{r["gsf"]:,}' if r["gsf"] else "—", r["reason"])
        log.info("        %s", r["evidence"][:190])

    # 4b ── pipeline entry type
    log.info("\n4b PIPELINE ENTRY TYPE, remaining set")
    ent = Counter(r["entry_type"] for r in keep + amb)
    log.info("   %-14s %4d   filings that state a programme", "development", ent["development"])
    log.info("   %-14s %4d   rezonings with no programme yet -- the front of the pipeline",
             "rezoning", ent["rezoning"])

    # 5 ── ambiguous, split by what kind of doubt it is
    build_no_figs = [r for r in amb if r["reason"].startswith("construction or programme")]
    weak_kept = [r for r in amb if r["reason"].startswith("filed as a")]
    other_amb = [r for r in amb if r not in build_no_figs and r not in weak_kept]
    log.info("\n5  AMBIGUOUS, KEPT IN AND FLAGGED  (%d)", len(amb))
    log.info("   %3d  describe construction but state no figures -- keeps in substance",
             len(build_no_figs))
    log.info("   %3d  filed as a variance or change of use, with no explicit statement",
             len(weak_kept))
    log.info("        that nothing is being built (the stronger test)")
    log.info("   %3d  no construction, programme or exclusion signal at all", len(other_amb))

    log.info("\n   CONSTRUCTION LANGUAGE, NO FIGURES -- keeps in substance (%d):",
             len(build_no_figs))
    for r in build_no_figs:
        log.info("     id=%-4d %-11s %-28s %-11s", r["id"], r["city"],
                 r["address"][:28], r["entry_type"])
        log.info("          %s", r["evidence"][:160])

    log.info("\n   NO SIGNAL EITHER WAY (%d):", len(other_amb))
    for r in other_amb:
        log.info("     id=%-4d %-11s %-28s %-11s", r["id"], r["city"],
                 r["address"][:28], r["entry_type"])

    OUT.write_text(json.dumps({"keep": keep, "ambiguous": amb, "exclude": exc},
                              indent=1, ensure_ascii=False), encoding="utf-8")
    log.info("\nWrote %s", OUT)

    if apply:
        for r in keep + amb:
            pr = session.get(Project, r["id"])
            pr.entry_type = r["entry_type"]
        for r in exc:
            p = session.get(Project, r["id"])
            p.excluded = True
            p.excluded_reason = "out of scope (%s): %s" % (r["reason"], r["evidence"][:180])
        session.commit()
        log.info("APPLIED: %d projects quarantined.", len(exc))
    else:
        log.info("REPORT ONLY -- nothing written. Re-run with --apply to quarantine.")
    session.close()


if __name__ == "__main__":
    main(apply="--apply" in sys.argv)
