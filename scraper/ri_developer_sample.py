r"""
Resolve a sample of real applicant entities and print the evidence for each.

Audit tool, not an ingestion step. Nothing is written to the projects table --
this exists so the resolution rules can be reviewed against real applicants
before being applied across the corpus.

Applicant names are pulled from harvested agenda text. The wording differs by
municipality, so several patterns are matched:

    Providence   "Owner: PMP Group LLC" / "Applicant: ... LLC"
    Newport      "Application of Southeastern Holding, LLC, applicant and owner"
    Pawtucket    "Gary Dantzler, Applicant and Houses for the Community LLC, Owner"
    Cranston     "Applicant: ..." / "Owner: ..."

    python scraper/ri_developer_sample.py --limit 20
"""

import re
import sys
import json
import logging
from pathlib import Path
from collections import OrderedDict

import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))

from scraper.ri_corp_registry import (
    resolve, load_cache, save_cache, HEADERS, is_single_purpose_shell,
)

logging.basicConfig(level=logging.INFO, format="%(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

CORPUS = Path(__file__).parent.parent / "data" / "ri_agenda_corpus.json"
TEXT_DIR = Path(__file__).parent.parent / "data" / "ri_pdfs" / "text"
OUT = Path(__file__).parent.parent / "data" / "ri_developer_sample.json"

# An entity name: runs up to a legal suffix. Anchored on the suffix rather than
# on capitalisation, because OCR'd and PDF-extracted text is inconsistently cased.
_ENTITY = r"([A-Z][A-Za-z0-9&'\.\- ]{2,60}?,?\s+(?:LLC|L\.L\.C\.|Inc\.?|Corp\.?|Company|Trust|LP|L\.P\.))"

PATTERNS = [
    re.compile(r"(?:Applicant|Owner|Petitioner|Developer)\s*(?:and\s+Owner)?\s*[:\-]\s*" + _ENTITY),
    re.compile(r"Application\s+of\s+" + _ENTITY),
    re.compile(_ENTITY + r"\s*,?\s*(?:applicant|owner|petitioner)", re.I),
]

# Boilerplate that trips the suffix pattern but is not an applicant.
_PROJECT_ADDRESS = re.compile(
    r"\b(\d{1,5}[A-Za-z]?\s+[\w'\-]+(?:\s+[\w'\-]+){0,3}\s+"
    r"(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Lane|Ln|Boulevard|Blvd|Place|Pl|"
    r"Court|Ct|Terrace|Ter|Way|Highway|Hwy|Square|Sq))\b", re.I)

_NOISE = re.compile(
    r"\b(city of|town of|department|commission|board|authority|university|"
    r"college|school|church|housing authority|redevelopment agency)\b", re.I)


# Lead-ins and connector phrases that the suffix-anchored pattern sweeps up
# ahead of the real entity name. Without stripping these, "Application of
# Southeastern Holding, LLC" is looked up verbatim and correctly fails.
_LEAD_IN = re.compile(
    r"^.*?\b(?:application of|request of|petition of|applicant and|owner and|"
    r"applicant|owner|petitioner|developer|and)\s+", re.I)


def clean(name: str) -> str:
    n = re.sub(r"\s+", " ", name).strip(" ,.;:")
    # Repeatedly strip lead-ins: "Hasan Iqbal applicant and Fairlawn Properties
    # LLC" needs two passes to reach the entity.
    for _ in range(3):
        stripped = _LEAD_IN.sub("", n).strip(" ,.;:")
        if stripped == n or len(stripped) < 6:
            break
        n = stripped
    n = re.sub(r"^(?:the)\s+", "", n, flags=re.I)
    # A name that still carries a lowercase sentence fragment is not an entity.
    if re.search(r"\b(?:is|are|was|were|has|have|proposing|requesting|located)\b", n, re.I):
        return ""
    return n


def extract_applicants() -> "OrderedDict[str, dict]":
    corpus = json.loads(CORPUS.read_text(encoding="utf-8"))
    found: OrderedDict[str, dict] = OrderedDict()
    for meeting in sorted(corpus.values(), key=lambda m: m["date"], reverse=True):
        for doc in meeting["documents"]:
            p = TEXT_DIR / doc["text_file"]
            if not p.exists():
                continue
            text = p.read_text(encoding="utf-8", errors="replace")
            if len(text) < 200:
                continue
            for pat in PATTERNS:
                for m in pat.finditer(text):
                    name = clean(m.group(1))
                    if len(name) < 6 or _NOISE.search(name):
                        continue
                    key = name.upper()
                    if key in found:
                        found[key]["seen"] += 1
                        continue
                    # Capture the PROJECT address from the agenda. The entity's
                    # registry address is its corporate principal office, which
                    # for a shell is a back-office: 49 Newport Hotel LLC lists
                    # 1140 Reservoir Ave, while the project is 49 America's Cup
                    # Avenue. Researching the registry address searches the
                    # wrong parcel entirely.
                    window = text[max(0, m.start() - 260): m.end() + 260]
                    am = _PROJECT_ADDRESS.search(window)
                    found[key] = {
                        "name": name, "seen": 1,
                        "municipality": meeting["municipality"],
                        "board": meeting["board"], "date": meeting["date"],
                        "project_address": (am.group(1).strip() if am else ""),
                        "context": re.sub(r"\s+", " ", text[max(0, m.start() - 90): m.end() + 90]),
                    }
    return found


def main(limit: int):
    applicants = extract_applicants()
    log.info("Distinct applicant entities found in corpus: %d", len(applicants))

    # Sample deliberately mixes shells and named companies so the review sees
    # both the resolution path and the abstention path.
    shells = [a for a in applicants.values() if is_single_purpose_shell(a["name"])]
    others = [a for a in applicants.values() if not is_single_purpose_shell(a["name"])]
    n_shell = min(len(shells), max(limit // 2, limit - len(others)))
    sample = shells[:n_shell] + others[: limit - n_shell]
    log.info("Sampling %d (%d single-purpose shells, %d named entities)\n",
             len(sample), n_shell, len(sample) - n_shell)

    cache = load_cache()
    results = []
    try:
        with httpx.Client(headers=HEADERS, follow_redirects=True) as client:
            for i, a in enumerate(sample, 1):
                log.info("[%2d/%d] %s", i, len(sample), a["name"])
                rec = resolve(client, a["name"], cache)
                rec["source"] = {k: a[k] for k in ("municipality", "board", "date", "seen",
                                                   "context", "project_address")}
                results.append(rec)
                log.info("        -> %s", rec["developer"] or f"NULL — {rec['reason']}")
    finally:
        save_cache(cache)
        OUT.write_text(json.dumps(results, indent=1), encoding="utf-8")

    resolved = [r for r in results if r["developer"]]
    log.info("\n=== %d/%d resolved, %d left null ===",
             len(resolved), len(results), len(results) - len(resolved))
    log.info("Written to %s", OUT)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=20)
    main(ap.parse_args().limit)
