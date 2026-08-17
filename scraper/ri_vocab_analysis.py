r"""
Measure review-stage vocabulary and field availability across harvested RI agendas.

Read-only analysis over data/ri_agenda_corpus.json. Produces three things needed
before ingestion can be designed:

  1. Review-stage phrase frequencies, for mapping onto the five pipeline stages.
  2. Square-footage availability per municipality -- how often a development item
     actually states GSF, versus stating only units and/or acreage.
  3. Case-number suffix taxonomy, derived empirically rather than guessed.

Counts agenda ITEMS, not documents. Items are numbered at line start on these
agendas ("2. Case no 26-047MIL - 203 Douglas Ave"), which is the segmentation
used here; text with no numbered items falls back to the whole document as one
item so nothing is silently dropped.

    python scraper/ri_vocab_analysis.py
"""

import re
import sys
import json
import logging
from pathlib import Path
from collections import Counter, defaultdict

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

CORPUS = Path(__file__).parent.parent / "data" / "ri_agenda_corpus.json"
TEXT_DIR = Path(__file__).parent.parent / "data" / "ri_pdfs" / "text"

ITEM_SPLIT = re.compile(r"^\s{0,6}(\d{1,2})[\.\)]\s+", re.M)

# Review-stage phrases. Ordered most specific first; an item is credited to
# every phrase it contains, since a single item can carry more than one
# (e.g. combined master and preliminary plan approval).
STAGE_PHRASES = {
    "pre-application":            r"pre[\s-]?application",
    "master plan":                r"\bmaster plan\b",
    "preliminary plan":           r"\bpreliminary plan\b",
    "final plan":                 r"\bfinal plan\b",
    "combined master+prelim":     r"master (?:and|&|/) preliminary|combined master",
    "development plan review":    r"development plan review",
    "administrative review":      r"administrative (?:review|subdivision|land development)",
    "unified development review": r"unified development review",
    "special use permit":         r"special[\s-]use permit",
    "dimensional variance":       r"dimensional (?:variance|adjustment)",
    "use variance":               r"use variance",
    "extension":                  r"\bextension\b|extend(?:ing)? .{0,24}approval",
    "modification":               r"\bmodification\b|\bamend(?:ment|ed)?\b",
    "rezoning":                   r"\brezon(?:e|ing)\b|zone change|zoning map amendment",
    "informational/no vote":      r"informational|no vote|discussion only",
    "for vote":                   r"[-–—]\s*for vote|for decision|for action",
    "continued":                  r"\bcontinued\b|\bcontinuance\b",
    "waiver":                     r"\bwaiver\b",
    "conceptual/concept":         r"\bconcept(?:ual)?\b",
    "certificate of completion":  r"certificate of (?:occupancy|completion)",
    "under construction":         r"under construction|construction (?:has )?(?:begun|commenced)|broke ground",
}

CLASSIFICATION = {
    "major land development":  r"major land development",
    "minor land development":  r"minor land development",
    "administrative (class)":  r"administrative land development|administrative subdivision",
    "major subdivision":       r"major subdivision",
    "minor subdivision":       r"minor subdivision",
}

# Field-availability probes
SF_PAT    = re.compile(r"([\d,]{3,})\s*(?:\+/-\s*)?(?:square\s*(?:feet|foot)|sq\.?\s*ft\.?|s\.?f\.?)\b", re.I)
UNIT_PAT  = re.compile(r"(\d{1,4})\s*(?:residential\s*|dwelling\s*|apartment\s*)?units?\b", re.I)
ACRE_PAT  = re.compile(r"([\d.]+)\s*(?:\+/-\s*)?acres?\b", re.I)
# Plat notation differs by municipality and there is no shared token:
#   Providence  "AP 68 Lot 846"
#   Pawtucket   "Tax Assessors Plat 44, Lot 561"
#   Newport     "TAP 34, Lot 13"          <- Tax Assessor's Plat, abbreviated
#   Cranston    "Plat 12, Lot 3"
#   Warwick     "Plat 288 Lot 485"
# Matching only AP/plat silently reported Newport at 0% plat coverage.
PLAT_PAT  = re.compile(
    r"\b(?:TAP|A\.?P\.?|(?:tax\s*)?assessor'?s?\s*plat|plat)\s*\.?\s*#?\s*(\d+)", re.I)
CASE_PAT  = re.compile(r"\b(\d{2})-(\d{2,4})\s*([A-Z]{2,4})\b")
# Providence appends the neighborhood after the plat/lot: "(AP 68 Lot 846, Smith Hill)"
PVD_NBHD  = re.compile(r"\(AP\s*\d+[^)]*?,\s*([A-Z][A-Za-z' \-]{2,32})\)")

# An item is treated as a development project if it describes construction or
# a land development/subdivision action -- filters out procedural business
# (minutes approval, roll call) and, for combined boards, code/tax appeals.
DEV_ITEM = re.compile(
    r"land development|subdivision|development plan review|construct|"
    r"rezon|dwelling|residential units?|mixed[\s-]use|site plan|"
    r"special[\s-]use permit|variance",
    re.I,
)


def load_docs():
    corpus = json.loads(CORPUS.read_text(encoding="utf-8"))
    for v in corpus.values():
        for d in v["documents"]:
            p = TEXT_DIR / d["text_file"]
            if not p.exists():
                continue
            yield v, d, p.read_text(encoding="utf-8", errors="replace")


def split_items(text: str) -> list[str]:
    parts = ITEM_SPLIT.split(text)
    if len(parts) < 3:
        return [text]
    out = []
    for i in range(1, len(parts) - 1, 2):
        out.append(parts[i + 1])
    return out or [text]


def main():
    stage_counts = Counter()
    stage_by_muni = defaultdict(Counter)
    classif = Counter()
    suffixes = Counter()
    nbhd_inline = defaultdict(Counter)

    field = defaultdict(lambda: Counter())
    doc_stats = Counter()

    for meta, doc, text in load_docs():
        muni = meta["municipality"]
        doc_stats[f"{muni}:docs"] += 1
        if len(text) < 200:
            doc_stats[f"{muni}:no_text_layer"] += 1
            continue
        if doc["kind"] != "agenda":
            continue
        doc_stats[f"{muni}:agendas_with_text"] += 1

        for item in split_items(text):
            if not DEV_ITEM.search(item):
                continue
            field[muni]["dev_items"] += 1
            stage_by_muni[muni]["_items"] += 1

            for label, pat in STAGE_PHRASES.items():
                if re.search(pat, item, re.I):
                    stage_counts[label] += 1
                    stage_by_muni[muni][label] += 1
            for label, pat in CLASSIFICATION.items():
                if re.search(pat, item, re.I):
                    classif[label] += 1

            if SF_PAT.search(item):
                field[muni]["has_sf"] += 1
            if UNIT_PAT.search(item):
                field[muni]["has_units"] += 1
            if ACRE_PAT.search(item):
                field[muni]["has_acres"] += 1
            if PLAT_PAT.search(item):
                field[muni]["has_plat"] += 1
            for m in CASE_PAT.finditer(item):
                suffixes[m.group(3)] += 1
            for m in PVD_NBHD.finditer(item):
                nbhd_inline[muni][m.group(1).strip()] += 1

    log.info("\n=== DOCUMENT / TEXT-LAYER COVERAGE ===")
    munis = sorted({k.split(":")[0] for k in doc_stats})
    for m in munis:
        d = doc_stats[f"{m}:docs"]
        n = doc_stats[f"{m}:no_text_layer"]
        a = doc_stats[f"{m}:agendas_with_text"]
        log.info("  %-12s docs=%-4d no_text_layer=%-4d (%3.0f%%)  agendas_with_text=%d",
                 m, d, n, 100 * n / d if d else 0, a)

    log.info("\n=== REVIEW-STAGE PHRASES (development items) ===")
    for label, n in stage_counts.most_common():
        log.info("  %-28s %4d", label, n)

    log.info("\n=== PROJECT CLASSIFICATION ===")
    for label, n in classif.most_common():
        log.info("  %-28s %4d", label, n)

    log.info("\n=== CASE-NUMBER SUFFIXES (empirical) ===")
    for s, n in suffixes.most_common():
        log.info("  %-8s %4d", s, n)

    log.info("\n=== FIELD AVAILABILITY (%% of development items) ===")
    log.info("  %-12s %6s %8s %8s %8s %8s", "MUNI", "items", "SF", "units", "acres", "plat")
    for m in sorted(field):
        f = field[m]
        t = f["dev_items"] or 1
        log.info("  %-12s %6d %7.0f%% %7.0f%% %7.0f%% %7.0f%%", m, f["dev_items"],
                 100 * f["has_sf"] / t, 100 * f["has_units"] / t,
                 100 * f["has_acres"] / t, 100 * f["has_plat"] / t)

    log.info("\n=== INLINE NEIGHBORHOOD MENTIONS (Providence-style) ===")
    for m, c in nbhd_inline.items():
        log.info("  %s: %d distinct, %d mentions", m, len(c), sum(c.values()))
        log.info("    %s", ", ".join(f"{k}({v})" for k, v in c.most_common(12)))

    log.info("\n=== PER-MUNICIPALITY STAGE MIX ===")
    for m in sorted(stage_by_muni):
        c = stage_by_muni[m]
        tot = c.pop("_items", 0)
        log.info("  %s (%d dev items): %s", m, tot,
                 ", ".join(f"{k}={v}" for k, v in c.most_common(10)))


if __name__ == "__main__":
    main()
