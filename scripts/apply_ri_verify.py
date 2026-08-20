"""Retract Rhode Island document values whose own quote does not support them.

The second-opinion pass looked at each value with only its field and its
passage -- no address, no project, nothing to rationalise from -- and was told
the default answer is no. Anything it refuses is retracted, which takes it out
of the live set permanently and lets the next-best evidence stand instead.
"""
import json
import re
import sqlite3
from collections import Counter
from pathlib import Path


def parse(t):
    if not t:
        return None
    t = t.strip()
    if t.startswith("```"):
        t = re.sub(r"^```(json)?\s*", "", t)
        t = re.sub(r"\s*```$", "", t)
    try:
        return json.loads(t)
    except Exception:
        m = re.search(r"\{.*\}", t, re.S)
        if m:
            try:
                return json.loads(m.group(0))
            except Exception:
                return None
    return None


def main():
    results = json.loads(Path("data/ri_verify_results.json").read_text())
    c = sqlite3.connect("data/boston_re.db")
    c.row_factory = sqlite3.Row
    stats = Counter()
    rejected = []
    for r in results:
        rid = int(r["custom_id"][1:])
        d = parse(r.get("text"))
        if not d:
            stats["unparseable"] += 1
            continue
        row = c.execute("select fp.*, p.address, p.city from field_provenance fp "
                        "join projects p on p.id=fp.project_id where fp.id=?",
                        (rid,)).fetchone()
        if row is None:
            continue
        if d.get("supports"):
            stats["upheld"] += 1
            continue
        stats["rejected"] += 1
        why = (d.get("why") or "").strip()
        better = (d.get("better_value") or "").strip()
        note = f"second-opinion check: {why}"
        if better and better.lower() not in ("null", "none"):
            note += f" | passage points to: {better}"
        c.execute("update field_provenance set retracted=1, "
                  "reason=coalesce(reason,'') || ? where id=?",
                  (" || " + note, rid))
        rejected.append((row, note))
    c.commit()
    for k, v in sorted(stats.items()):
        print(f"  {k:14} {v}")
    print()
    for row, note in rejected:
        print(f"  {row['project_id']:4} {str(row['city'])[:10]:10} "
              f"{str(row['address'])[:22]:22} {row['field'][:14]:14} "
              f"{str(row['value'])[:26]:26} [{row['source_type']}]")
        print(f"       {note[:150]}")


if __name__ == "__main__":
    main()
