"""Batch-extract the design team from Rhode Island plan sets.

A drawing set's title block labels roles deterministically -- that is what a
stamped drawing is for -- so this is the best RI source. The files are not
pre-mapped to projects, so each request also asks the model to read the site
address off the drawing; the match back to a project is then done in code
against the normalised address, never by the model.
"""
import json
import sys
from pathlib import Path

import fitz

sys.path.insert(0, str(Path(__file__).parent))

PLANSETS = Path("data/ri_plansets")
OUT = Path("data/ri_planset_requests.jsonl")
MODEL = "claude-haiku-4-5-20251001"
MAX_PAGES = 12
MAX_CHARS = 60000

SYSTEM = """You read Rhode Island site plan sets, staff reports and planning
board materials, and report the project team exactly as the document labels it.

Rules, all absolute:
- Take a firm ONLY where the document labels it in that role. A title block
  that says "CIVIL ENGINEER: DiPrete Engineering" is an answer. A firm that
  merely appears on the sheet is not.
- An engineering firm is never the architect and an architecture firm is
  never the civil engineer, no matter which is more prominent.
- A landscape architect is not the architect. A surveyor is not the civil
  engineer. A traffic engineer is not the civil engineer.
- Never merge a person to a company unless the document states it.
- Quote verbatim. Never paraphrase.

Return ONLY this JSON object:

{
  "site_address": "<the street address of the site as printed, or null>",
  "address_quote": "<verbatim line the address came from, or null>",
  "municipality": "<city or town as printed, or null>",
  "architect":        {"firm": <string or null>, "role_label": <string or null>, "quote": <verbatim or null>},
  "civil_engineer":   {"firm": <string or null>, "role_label": <string or null>, "quote": <verbatim or null>},
  "general_contractor": {"firm": <string or null>, "role_label": <string or null>, "quote": <verbatim or null>},
  "landscape_architect": {"firm": <string or null>, "quote": <verbatim or null>},
  "surveyor":         {"firm": <string or null>, "quote": <verbatim or null>}
}"""


def text_of(pdf):
    """Title blocks live on every sheet; take the first pages plus a spread."""
    try:
        d = fitz.open(pdf)
    except Exception:
        return ""
    n = d.page_count
    idx = list(range(min(6, n)))
    if n > 6:
        step = max(1, n // 6)
        idx += list(range(6, n, step))[:MAX_PAGES - 6]
    parts = []
    for i in sorted(set(idx))[:MAX_PAGES]:
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
    files = sorted(PLANSETS.glob("*.pdf"))
    out, skipped = [], 0
    total = 0
    for f in files:
        t = text_of(f)
        if len(t.strip()) < 300:
            skipped += 1
            continue
        total += len(t)
        out.append({
            "custom_id": f"ri_{f.stem[:60]}",
            "params": {
                "model": MODEL, "max_tokens": 1200, "system": SYSTEM,
                "messages": [{"role": "user",
                              "content": f"FILE: {f.name}\n\n{t}"}],
            },
        })
    seen, uniq = set(), []
    for o in out:
        cid = o["custom_id"]
        i = 2
        while cid in seen:
            cid = f"{o['custom_id'][:58]}_{i}"
            i += 1
        seen.add(cid)
        o["custom_id"] = cid
        uniq.append(o)
    with OUT.open("w", encoding="utf-8") as fh:
        for o in uniq:
            fh.write(json.dumps(o) + "\n")
    tin = total / 3.6 + len(SYSTEM) / 3.6 * len(uniq)
    tout = 350 * len(uniq)
    live = tin / 1e6 * 1.00 + tout / 1e6 * 5.00
    print(f"requests {len(uniq)}  skipped(no text) {skipped}")
    print(f"est input {tin/1e6:.2f}M  output {tout/1e6:.2f}M")
    print(f"COST live ${live:.2f}  batch ${live/2:.2f}")
    print("wrote", OUT)


if __name__ == "__main__":
    main()
