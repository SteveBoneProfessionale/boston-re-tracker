r"""Mine the filings already on disk before spending a single web search.

The top 100 Boston and Cambridge projects by square footage carry 198 null
fields between them -- architect, civil engineer, general contractor, unit
count. Most of these are Article 80 filings and the PDFs are already in
data/pdfs, named by project id.

Worth knowing what has already been tried: the earlier Boston batch extraction
targeted architect, civil_engineer and general_contractor on these same
documents, so a null in those three usually means the filing did not answer.
It never looked for UNIT COUNTS at all, which is where the documents still owe
us something.

Every hit stores the passage and the page, because a name pulled out of a
70,000-word PDF by a regex is worthless without the sentence it came from --
"Architect:" appears in title blocks, transmittal letters and consultant lists,
and the three do not always agree.

    python scraper/top100_doc_fill.py
"""

import json
import re
import sys
from pathlib import Path

import fitz  # pymupdf

WORKLIST = Path("data/top100_worklist.json")
PDF_DIR = Path("data/pdfs")
OUT = Path("data/top100_doc_findings.json")

# A firm name, not a sentence. Stops at the punctuation that ends a list entry
# so "Architect: Elkus Manfredi Architects Civil Engineer: Nitsch" cannot
# swallow the next label.
_NAME = r"([A-Z][^\n:;|]{2,60}?)(?=\s{2,}|\s*[\n;|]|\s*(?:Civil|Structural|MEP|Landscape|Traffic|Owner|Developer|Applicant|Attorney|General|Construction|Survey)\b|$)"

FIELD_PATTERNS = {
    "architect": [
        rf"(?:Architect\s+of\s+Record|Design\s+Architect|Project\s+Architect|Architect)\s*[:\-–]\s*{_NAME}",
    ],
    "civil_engineer": [
        rf"(?:Civil\s+Engineer(?:ing)?(?:\s+of\s+Record)?|Site\s+Civil\s+Engineer)\s*[:\-–]\s*{_NAME}",
    ],
    "general_contractor": [
        rf"(?:General\s+Contractor|Construction\s+Manager|CM\s+at\s+Risk|Contractor)\s*[:\-–]\s*{_NAME}",
    ],
}

# Unit counts. Ordered most specific first: on a mixed-use filing the
# residential count is stated separately from the total floor area, and a bare
# "250 units" in a parking or bicycle section is not a dwelling count.
UNIT_PATTERNS = [
    r"\b([\d,]{1,7})\s+(?:new\s+)?(?:residential|dwelling|rental|housing)\s+units\b",
    r"\b([\d,]{1,7})\s+(?:residential\s+)?(?:apartments|condominiums|condominium\s+units|homes)\b",
    r"\b(?:approximately\s+)?([\d,]{1,7})\s+units\s+of\s+(?:housing|residential)\b",
    r"\bunits?\s*[:\-–]\s*([\d,]{1,7})\b",
    r"\b([\d,]{1,7})\s+units\b",
]
# Contexts where a number followed by "units" is not housing.
_NOT_HOUSING = re.compile(
    r"parking|bicycle|bike|storage|hotel\s+(?:room|key)|condenser|mechanical|"
    r"air\s+handling|loading|square\s+f|sf\b|gsf", re.I)

_JUNK = re.compile(r"^(the|a|an|to be|tbd|n/?a|not\s|see\s|various|multiple)\b", re.I)


def _sentence(text: str, start: int, end: int, span: int = 340) -> str:
    lo = max(0, start - span // 2)
    hi = min(len(text), end + span)
    return re.sub(r"\s+", " ", text[lo:hi]).strip()


def _clean(name: str) -> str:
    n = re.sub(r"\s+", " ", name).strip(" .,:;-–—")
    return "" if _JUNK.match(n) or len(n) < 3 else n


def scan_pdf(path: Path, want: set) -> dict:
    out = {}
    try:
        doc = fitz.open(path)
    except Exception as exc:
        return {"_error": str(exc)}
    for pno in range(min(len(doc), 60)):        # team pages sit near the front
        try:
            text = doc[pno].get_text()
        except Exception:
            continue
        if not text.strip():
            continue
        for field in list(want):
            if field in out:
                continue
            for pat in FIELD_PATTERNS.get(field, []):
                m = re.search(pat, text, re.I)
                if m and _clean(m.group(1)):
                    out[field] = {"value": _clean(m.group(1)), "page": pno + 1,
                                  "passage": _sentence(text, m.start(), m.end())}
                    break
        if "residential_units" in want and "residential_units" not in out:
            for pat in UNIT_PATTERNS:
                for m in re.finditer(pat, text, re.I):
                    ctx = _sentence(text, m.start(), m.end(), 160)
                    if _NOT_HOUSING.search(ctx):
                        continue
                    try:
                        n = int(m.group(1).replace(",", ""))
                    except ValueError:
                        continue
                    if 3 <= n <= 5000:
                        out["residential_units"] = {
                            "value": n, "page": pno + 1,
                            "passage": _sentence(text, m.start(), m.end())}
                        break
                if "residential_units" in out:
                    break
    doc.close()
    return out


def main():
    work = json.loads(WORKLIST.read_text(encoding="utf-8"))
    blank = lambda v: str(v or "").strip() in ("", "not_yet_selected", "None")
    results, stats = {}, {"with_pdf": 0, "no_pdf": 0,
                          "architect": 0, "civil_engineer": 0,
                          "general_contractor": 0, "residential_units": 0}
    for i, row in enumerate(work, 1):
        pid = row["id"]
        want = set()
        if blank(row.get("architect")):
            want.add("architect")
        if blank(row.get("civil_engineer")):
            want.add("civil_engineer")
        if blank(row.get("contractor")):
            want.add("general_contractor")
        if row.get("residential_units") in (None, "", 0):
            want.add("residential_units")
        if not want:
            continue
        pdf = PDF_DIR / f"{pid}.pdf"
        if not pdf.exists():
            stats["no_pdf"] += 1
            continue
        stats["with_pdf"] += 1
        found = scan_pdf(pdf, want)
        found.pop("_error", None)
        if found:
            results[str(pid)] = {"name": row["name"], "city": row["city"],
                                 "gsf": row.get("total_gsf"), "found": found}
            for f in found:
                stats[f] = stats.get(f, 0) + 1
        if i % 20 == 0:
            print(f"  {i}/{len(work)} scanned")

    OUT.write_text(json.dumps(results, indent=1, ensure_ascii=False), encoding="utf-8")
    print(f"\nprojects with a null field and a PDF on disk: {stats['with_pdf']}"
          f"   without a PDF: {stats['no_pdf']}")
    print("fields the documents alone resolved:")
    for f in ("architect", "civil_engineer", "general_contractor", "residential_units"):
        print(f"   {f:<20} {stats.get(f, 0)}")
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
