"""Batch-extract the project team from Rhode Island board documents.

These are zoning-board and city-plan-commission filings, staff reports and
minutes -- not drawings. They name who applied, who presented and who
prepared the plans, which is where a Rhode Island architect usually surfaces.
Civil engineers appear far less often, and general contractors almost never,
so the prompt is told to return null rather than reach.

The documents are matched to projects in code, not by the model: each request
asks the model to read the site address off the page, and the match back to a
project is made against the normalised address afterwards.
"""
import json
import re
import sys
from pathlib import Path

import fitz

sys.path.insert(0, str(Path(__file__).parent))

PLANSETS = Path("data/ri_plansets")
OUT = Path("data/ri_board_requests.jsonl")
IDMAP = Path("data/ri_board_idmap.json")
MODEL = "claude-haiku-4-5-20251001"
MAX_PAGES = 14
MAX_CHARS = 55000

ROLE = re.compile(
    r"architect|engineer|surveyor|contractor|designer|prepared by|"
    r"on behalf of|represent|presented by|appeared",
    re.I)

SYSTEM = """You read Rhode Island municipal land-use documents -- zoning board
and city plan commission applications, staff reports, minutes and decisions --
and report the project team exactly as the document states it.

Rules, all absolute:
- Take a firm ONLY where the document ties it to that role. "Plans prepared by
  Smith Architects" is an answer. "Attorney John Doe appeared on behalf of the
  applicant" is not an architect. A name in an attendance list is not an answer.
- An engineering firm is never the architect and an architecture firm is never
  the civil engineer, whichever is more prominent.
- A landscape architect is not the architect. A surveyor is not the civil
  engineer. A traffic engineer is not the civil engineer.
- If the document names a PERSON with no firm, report the person in
  `person` and leave `firm` null. Never guess the person's employer.
- A general contractor is almost never named in these documents. Return null
  unless the document actually labels one.
- Quote verbatim. Never paraphrase.

Return ONLY this JSON object:

{
  "site_address": "<the street address of the subject property as printed, or null>",
  "address_quote": "<verbatim line the address came from, or null>",
  "municipality": "<city or town as printed, or null>",
  "architect":          {"firm": <string or null>, "person": <string or null>, "role_label": <exact label text or null>, "quote": <verbatim or null>},
  "civil_engineer":     {"firm": <string or null>, "person": <string or null>, "role_label": <exact label text or null>, "quote": <verbatim or null>},
  "general_contractor": {"firm": <string or null>, "person": <string or null>, "role_label": <exact label text or null>, "quote": <verbatim or null>}
}"""


def select_pages(doc):
    """Front matter, which carries the caption, plus every page naming a role."""
    n = doc.page_count
    keep = list(range(min(3, n)))
    for i in range(n):
        if len(keep) >= MAX_PAGES:
            break
        if i in keep:
            continue
        try:
            t = doc.load_page(i).get_text()
        except Exception:
            continue
        if ROLE.search(t):
            keep.append(i)
    return sorted(set(keep))[:MAX_PAGES]


def text_of(pdf):
    try:
        d = fitz.open(pdf)
    except Exception:
        return ""
    parts = []
    for i in select_pages(d):
        try:
            t = d.load_page(i).get_text()
        except Exception:
            continue
        t = "\n".join(l.rstrip() for l in t.splitlines() if l.strip())
        if t.strip():
            parts.append(f"=== PAGE {i+1} ===\n{t}")
    d.close()
    return "\n\n".join(parts)[:MAX_CHARS]


def main():
    matches = json.loads(Path("data/ri_zbr_matches.json").read_text())
    log = {}
    p = Path("data/ri_fetch_log.json")
    if p.exists():
        log = json.loads(p.read_text())

    files = []
    for v in matches.values():
        for u in v["urls"]:
            e = log.get(u) or {}
            if e.get("file"):
                files.append(PLANSETS / e["file"])
    files = sorted({f for f in files if f.exists()})
    print(f"{len(files)} matched documents on disk")

    out, idmap, skipped, chars = [], {}, 0, 0
    for i, f in enumerate(files):
        t = text_of(f)
        if len(t.strip()) < 300:
            skipped += 1
            continue
        cid = f"rb{i:04d}"
        idmap[cid] = f.name
        chars += len(t)
        out.append({
            "custom_id": cid,
            "params": {"model": MODEL, "max_tokens": 1100, "system": SYSTEM,
                       "messages": [{"role": "user",
                                     "content": f"FILE: {f.name}\n\n{t}"}]},
        })
    with OUT.open("w", encoding="utf-8") as fh:
        for o in out:
            fh.write(json.dumps(o) + "\n")
    IDMAP.write_text(json.dumps(idmap, indent=1))

    tin = chars / 3.6 + len(SYSTEM) / 3.6 * len(out)
    tout = 320 * len(out)
    live = tin / 1e6 * 1.00 + tout / 1e6 * 5.00
    print(f"requests {len(out)}  skipped(no text) {skipped}")
    print(f"est input {tin/1e6:.2f}M  output {tout/1e6:.2f}M")
    print(f"COST live ${live:.2f}   batch ${live/2:.2f}")
    print("wrote", OUT)


if __name__ == "__main__":
    main()
