"""Append searched-but-unresolved Step 4 outcomes from a compact JSON list.

Input is a list of [project_id, field, reason]. A null with a reason recorded
is the correct output for a field that was searched and did not resolve.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scraper"))
from provenance import connect, construction_started, record


def main(path):
    items = json.loads(Path(path).read_text(encoding="utf-8"))
    c = connect()
    rows = {r["id"]: r for r in c.execute(
        "select id,status,completion_stage from projects")}
    made = skipped = 0
    for pid, field, reason in items:
        live = c.execute("select * from field_provenance where project_id=? and field=? "
                         "and superseded=0", (pid, field)).fetchone()
        if live is not None and live["outcome"] == "resolved":
            skipped += 1
            continue
        r = rows.get(pid)
        outcome = "null"
        if field == "general_contractor" and r is not None and not construction_started(r):
            outcome = "not_yet_selected"
        record(c, pid, field, value=None, outcome=outcome, source_type="web",
               resolution_step=4, reason=reason)
        made += 1
    c.commit()
    print(f"recorded {made} nulls ({skipped} already resolved)")


if __name__ == "__main__":
    main(sys.argv[1])
