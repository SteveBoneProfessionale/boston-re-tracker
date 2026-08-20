"""Build the Batch API payload for the Boston Article 80 corpus.

One request per project document. The pages are chosen by page_select, not
by a head-slice -- see that module for why.
"""
import json
import sqlite3
import sys
from pathlib import Path

import fitz

sys.path.insert(0, str(Path(__file__).parent))
from page_select import select

SCAN = Path("data/dev_team_pages.json")
PDF_DIR = Path("data/pdfs")
OUT = Path("data/boston_batch_requests.jsonl")
MODEL = "claude-haiku-4-5-20251001"

SYSTEM = """You extract project-team facts from Boston Article 80 development filings.

You are given selected pages of one filing. Report only what the document
states. The rules are absolute:

- Take a firm ONLY if the document labels it in that role. A firm appearing
  in a team list without a role label is NOT an answer.
- Never infer a role from what a firm is known for. If the document does not
  say "Civil Engineer", there is no civil engineer.
- Never merge a person to a company unless the document states the
  relationship in its own words.
- A general contractor named only prospectively ("the Proponent will select
  a General Contractor", "a GC will be retained") is NOT an answer. Report
  that as prospective_only.
- Quote the passage verbatim. Do not paraphrase. Give the page number as
  printed in the PAGE marker.

Return ONLY a JSON object, no prose, matching exactly:

{
  "address_passage": "<verbatim sentence establishing the project address or name, or null>",
  "address_page": <int or null>,
  "architect": {"firm": <string or null>, "role_label": <the exact label text or null>,
                "page": <int or null>, "passage": <verbatim sentence or null>,
                "is_person_name": <true if the value is an individual, not a firm>},
  "civil_engineer": {...same shape...},
  "general_contractor": {...same shape..., "prospective_only": <bool>},
  "prior_checks": {
     "architect": "confirmed" | "role_not_labelled" | "firm_absent" | "no_prior",
     "civil_engineer": "...",
     "general_contractor": "..."
  }
}"""


def doc_text(pdf, pages):
    d = fitz.open(pdf)
    parts = []
    for p in pages:
        if p < 1 or p > d.page_count:
            continue
        try:
            t = d.load_page(p - 1).get_text()
        except Exception:
            continue
        t = "\n".join(l.rstrip() for l in t.splitlines() if l.strip())
        if t.strip():
            parts.append(f"=== PAGE {p} ===\n{t}")
    d.close()
    return "\n\n".join(parts)


def main():
    scan = json.loads(SCAN.read_text())
    c = sqlite3.connect("data/boston_re.db")
    c.row_factory = sqlite3.Row
    rows = {str(r["id"]): r for r in c.execute(
        "select id,name,address,architect,civil_engineer,general_contractor,"
        "processed_filing_name,processed_filing_url,status "
        "from projects where coalesce(excluded,0)=0").fetchall()}

    out, skipped, total_chars = [], [], 0
    for pid, sc in scan.items():
        if "error" in sc or pid not in rows:
            skipped.append((pid, "no project row" if pid not in rows else "pdf error"))
            continue
        pdf = PDF_DIR / f"{pid}.pdf"
        if not pdf.exists():
            skipped.append((pid, "pdf missing"))
            continue
        pages = select(sc)
        text = doc_text(pdf, pages)
        if len(text.strip()) < 200:
            skipped.append((pid, "no extractable text"))
            continue
        r = rows[pid]
        prior = {
            "architect": r["architect"] or None,
            "civil_engineer": r["civil_engineer"] or None,
            "general_contractor": r["general_contractor"] or None,
        }
        user = (
            f"PROJECT (from the tracker, for identification only -- do not treat as fact "
            f"about the document):\n  name: {r['name']}\n  address: {r['address']}\n\n"
            f"VALUES ALREADY IN THE TRACKER, to check against this document:\n"
            f"  {json.dumps(prior)}\n"
            f"For each, answer in prior_checks: 'confirmed' only if this document names "
            f"that firm AND labels it in that role; 'role_not_labelled' if the firm "
            f"appears but the role is not stated; 'firm_absent' if the firm does not "
            f"appear; 'no_prior' if there was no prior value.\n\n"
            f"FILING: {r['processed_filing_name']}\n\n"
            f"--- SELECTED PAGES ---\n{text}"
        )
        total_chars += len(user)
        out.append({
            "custom_id": f"p{pid}",
            "params": {
                "model": MODEL,
                "max_tokens": 1500,
                "system": SYSTEM,
                "messages": [{"role": "user", "content": user}],
            },
        })

    with OUT.open("w", encoding="utf-8") as f:
        for o in out:
            f.write(json.dumps(o) + "\n")

    est_in = total_chars / 3.6            # chars per token, English + tables
    sys_tok = len(SYSTEM) / 3.6 * len(out)
    tin = est_in + sys_tok
    tout = 400 * len(out)
    # Haiku 4.5: $1.00/MTok in, $5.00/MTok out. Batch is half.
    live = tin / 1e6 * 1.00 + tout / 1e6 * 5.00
    print(f"requests: {len(out)}   skipped: {len(skipped)}")
    for s in skipped[:10]:
        print("   skip", s)
    print(f"payload chars: {total_chars:,}")
    print(f"est input tokens:  {tin/1e6:.2f}M")
    print(f"est output tokens: {tout/1e6:.2f}M")
    print(f"COST  live: ${live:.2f}    batch (50%): ${live/2:.2f}")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
