"""Batch-read the cached Rhode Island minutes passages for one project each.

The passages were already tied to a project by a street-number-plus-street
match, so the model is not asked to identify the address. It is asked to
confirm the passage really is about that address before reporting anything,
because a long meeting document can mention a number and a street that happen
to sit next to each other without being the same property.
"""
import json
from pathlib import Path

HITS = Path("data/ri_minutes_hits.json")
OUT = Path("data/ri_minutes_requests.jsonl")
MODEL = "claude-haiku-4-5-20251001"
MAX_CHARS = 22000

SYSTEM = """You read excerpts of Rhode Island planning and zoning board minutes
and staff reports, for one property at a time, and report the project team.

You are told which property the excerpts were matched to. First decide whether
the excerpts really concern that property. A meeting document covers many
cases; a number and a street can appear side by side for a different property,
or in an unrelated list. If the excerpts do not clearly concern the stated
property, set "about_this_property" to false and return nulls for everything
else.

Rules for the team, all absolute:
- Take a firm or person ONLY where the text ties them to that role. "Plans
  prepared by Smith Architects" is an answer, and so is "Ron Stevenson of South
  County Architecture presented the design". An attorney appearing on behalf of
  the applicant is not an architect. A name in an attendance list is not an
  answer.
- An engineering firm is never the architect and an architecture firm is never
  the civil engineer.
- A landscape architect is not the architect. A surveyor is not the civil
  engineer. A traffic engineer is not the civil engineer.
- If a person is named with their firm, give both. If only a person is named,
  give the person and leave firm null -- never guess their employer.
- General contractors are rarely named in these documents. Return null unless
  one is actually labelled.
- Quote verbatim. Never paraphrase.

Return ONLY this JSON object:

{
  "about_this_property": <true or false>,
  "architect":          {"firm": <string or null>, "person": <string or null>, "role_label": <exact wording or null>, "quote": <verbatim or null>},
  "civil_engineer":     {"firm": <string or null>, "person": <string or null>, "role_label": <exact wording or null>, "quote": <verbatim or null>},
  "general_contractor": {"firm": <string or null>, "person": <string or null>, "role_label": <exact wording or null>, "quote": <verbatim or null>}
}"""


def main():
    hits = json.loads(HITS.read_text())
    out, chars = [], 0
    for pid, v in sorted(hits.items(), key=lambda kv: int(kv[0])):
        body = []
        for h in v["passages"]:
            body.append(f"--- from {h['file']} ---\n{h['passage']}")
        text = "\n\n".join(body)[:MAX_CHARS]
        if len(text.strip()) < 200:
            continue
        chars += len(text)
        user = (f"PROPERTY: {v['address']}, {v['city']}, Rhode Island\n"
                f"FIELDS STILL NEEDED: {', '.join(v['gaps'])}\n\n"
                f"EXCERPTS MATCHED TO THIS PROPERTY:\n\n{text}")
        out.append({
            "custom_id": f"rm{int(pid):05d}",
            "params": {"model": MODEL, "max_tokens": 1000, "system": SYSTEM,
                       "messages": [{"role": "user", "content": user}]},
        })
    with OUT.open("w", encoding="utf-8") as f:
        for o in out:
            f.write(json.dumps(o) + "\n")
    tin = chars / 3.6 + len(SYSTEM) / 3.6 * len(out)
    tout = 300 * len(out)
    live = tin / 1e6 * 1.00 + tout / 1e6 * 5.00
    print(f"requests {len(out)}   payload {chars:,} chars")
    print(f"est input {tin/1e6:.2f}M  output {tout/1e6:.2f}M")
    print(f"COST live ${live:.2f}   batch ${live/2:.2f}")
    print("wrote", OUT)


if __name__ == "__main__":
    main()
