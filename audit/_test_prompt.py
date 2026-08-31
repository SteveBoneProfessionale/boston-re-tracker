"""Test the rewritten extraction prompt against the On the Dot memo.

Calls the same code path extract_projects.py uses -- same model, same PDF
handling, same system prompt -- but writes NOTHING to the database. A pass is
either the correct per-building figure or a null carrying scope="phase".

    python audit/_test_prompt.py
"""
import json
import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv()

from anthropic import Anthropic

from db.database import engine
from scraper.extract_projects import MODEL, SYSTEM_PROMPT, PDF_DIR, pdf_to_content

# id -> the figure BPDA publishes for that building, for scoring only.
EXPECT = {168: 487400, 170: 510900, 172: 388200}
PHASE = 1386500

client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
conn = engine.connect()

print(f"model: {MODEL}\n")
for pid in (168, 170, 172):
    r = conn.execute(text(
        "select name,address,neighborhood,processed_filing_type from projects "
        "where id=:i"), {"i": pid}).first()
    pdf = PDF_DIR / f"{pid}.pdf"
    content = pdf_to_content(pdf)
    if not content:
        print(f"id={pid}: could not read {pdf}")
        continue
    content.append({"type": "text", "text":
        f"Project: {r[0]}\nAddress: {r[1] or 'unknown'}\n"
        f"Neighborhood: {r[2] or 'unknown'}\nFiling type: {r[3]}\n\n"
        f"Extract the structured data from this filing."})
    resp = client.messages.create(
        model=MODEL, max_tokens=2000,
        system=[{"type": "text", "text": SYSTEM_PROMPT,
                 "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": content}])
    raw = resp.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1].lstrip("json").strip()
    try:
        d = json.loads(raw)
    except Exception:
        print(f"id={pid}: unparseable -> {raw[:200]}")
        continue

    g = d.get("total_gsf")
    scope = d.get("total_gsf_scope")
    larger = d.get("phase_or_master_gsf")
    if g == EXPECT[pid]:
        verdict = "PASS - exact per-building figure"
    elif g is None and scope in ("phase", "master_plan"):
        verdict = f"PASS - null with scope={scope}"
    elif g == PHASE:
        verdict = "FAIL - still returns the phase total as the building"
    elif g is None:
        verdict = f"PASS(weak) - null, scope={scope}"
    else:
        verdict = f"REVIEW - returned {g}"
    print(f"id={pid}  {r[0][:34]:<36}")
    print(f"   total_gsf={g}   scope={scope}   phase_or_master={larger}")
    print(f"   units={d.get('residential_units')}  arch={d.get('architect')}")
    print(f"   -> {verdict}\n")

conn.close()
