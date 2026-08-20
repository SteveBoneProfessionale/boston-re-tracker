"""Locate the Development Team section in the Article 80 corpus.

The batch extraction truncates each PDF to a fixed page count. A 40-page cut
against a median of 45 pages drops the tail of more than half the corpus, so
before spending anything on the batch we measure where the team content
actually sits.
"""
import json
import re
import sys
from pathlib import Path

import fitz

PDF_DIR = Path("data/pdfs")

HEADER = re.compile(
    r"(development|project|design|consultant)\s+(team|directory)"
    r"|team\s+(directory|members)"
    r"|project\s+participants",
    re.I,
)
# A role label, colon-or-tab separated from a value, is what actually carries
# the answer. Header text alone is often a table-of-contents line.
ROLE = re.compile(
    r"^\s*(architect(\s+of\s+record)?|civil\s+engineer|general\s+contractor"
    r"|construction\s+manager|landscape\s+architect|structural\s+engineer"
    r"|surveyor|owner'?s?\s+representative)\s*[:\-\u2013]",
    re.I | re.M,
)
CIVIL = re.compile(r"civil\s+engineer", re.I)
GC = re.compile(r"general\s+contractor|construction\s+manager", re.I)
ARCH = re.compile(r"\barchitect\b", re.I)


def scan(path):
    try:
        doc = fitz.open(path)
    except Exception as exc:
        return {"error": str(exc)}
    out = {
        "pages": doc.page_count,
        "header_pages": [],
        "role_pages": [],
        "arch_pages": [],
        "civil_pages": [],
        "gc_pages": [],
        "text_pages": 0,
    }
    for i in range(doc.page_count):
        try:
            t = doc.load_page(i).get_text()
        except Exception:
            continue
        if not t.strip():
            continue
        out["text_pages"] += 1
        p = i + 1
        if HEADER.search(t):
            out["header_pages"].append(p)
        if ROLE.search(t):
            out["role_pages"].append(p)
        if ARCH.search(t):
            out["arch_pages"].append(p)
        if CIVIL.search(t):
            out["civil_pages"].append(p)
        if GC.search(t):
            out["gc_pages"].append(p)
    doc.close()
    return out


def main():
    results = {}
    files = sorted(PDF_DIR.glob("*.pdf"), key=lambda p: int(p.stem) if p.stem.isdigit() else 0)
    for n, f in enumerate(files, 1):
        results[f.stem] = scan(f)
        if n % 25 == 0:
            print(f"  {n}/{len(files)}", flush=True)
    Path("data/dev_team_pages.json").write_text(json.dumps(results, indent=1))
    print(f"wrote data/dev_team_pages.json ({len(results)} docs)")


if __name__ == "__main__":
    main()
