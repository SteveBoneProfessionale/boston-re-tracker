"""Second-opinion pass over the Rhode Island document values.

Reading minutes at scale produces a specific kind of false positive: an
expert witness called by the applicant's attorney, a surveyor, or a bare
"Kurt Stenberg, PE" gets recorded as the project's civil engineer. The
extraction prompt forbids all three, but a single pass does not catch itself.

This asks a fresh call, one value at a time, a narrower question: does this
exact quote assign this exact party this exact role on this project? It sees
only the value, the field and the passage -- no address, no project context,
nothing to rationalise from.
"""
import json
import sqlite3
from pathlib import Path

OUT = Path("data/ri_verify_requests.jsonl")
MODEL = "claude-haiku-4-5-20251001"

SYSTEM = """You are checking one recorded fact against the passage it was taken
from. Be strict. The default answer is no.

Say the passage SUPPORTS the record only if the passage itself states that
this party holds this role on this project. Apply these rules:

- "Architect Jane Roe presented the plan" supports architect = Jane Roe.
- "civil engineer Philip Henry of Civil Design Group" supports
  civil_engineer = Civil Design Group.
- A SURVEYOR is not a civil engineer. "Surveyor(s): Acme Engineering" does
  NOT support civil_engineer = Acme Engineering.
- An EXPERT WITNESS is not the project's engineer or architect. "Attorney
  Smith presented John Doe, Professional Engineer, as an expert witness" does
  NOT support civil_engineer = John Doe.
- A bare name with credentials and no role assignment -- "Kurt Stenberg, PE"
  -- does NOT support anything, unless the surrounding words assign the role.
- A landscape architect is not the architect. A traffic engineer is not the
  civil engineer. A structural engineer is not the civil engineer.
- A firm's capability list or tagline is not a role assignment.
- An attorney, applicant, developer or owner is none of these roles.

Return ONLY this JSON object:

{
  "supports": <true or false>,
  "why": "<one short sentence>",
  "better_value": "<if the passage assigns the role to a DIFFERENT party than the recorded one, name it; otherwise null>"
}"""


def main():
    c = sqlite3.connect("data/boston_re.db")
    c.row_factory = sqlite3.Row
    rows = c.execute("""select fp.id, fp.project_id, fp.field, fp.value,
                               fp.firm_sentence, fp.source_type
                        from field_provenance fp
                        join projects p on p.id = fp.project_id
                        where fp.superseded = 0 and fp.outcome = 'resolved'
                          and coalesce(fp.retracted,0) = 0
                          and fp.source_type in ('minutes','board_document',
                                                 'planset','scanned_board_document')
                          and p.city in ('Providence','Cranston','Warwick',
                                         'Pawtucket','Newport')
                     """).fetchall()
    out = []
    for r in rows:
        q = (r["firm_sentence"] or "").strip()
        if not q:
            continue
        user = (f"RECORDED FIELD: {r['field']}\n"
                f"RECORDED VALUE: {r['value']}\n\n"
                f"PASSAGE IT WAS TAKEN FROM:\n\"\"\"\n{q[:3000]}\n\"\"\"")
        out.append({
            "custom_id": f"v{r['id']:06d}",
            "params": {"model": MODEL, "max_tokens": 350, "system": SYSTEM,
                       "messages": [{"role": "user", "content": user}]},
        })
    with OUT.open("w", encoding="utf-8") as f:
        for o in out:
            f.write(json.dumps(o) + "\n")
    print(f"{len(rows)} live RI document values, {len(out)} with a quote to check")
    print("wrote", OUT)


if __name__ == "__main__":
    main()
