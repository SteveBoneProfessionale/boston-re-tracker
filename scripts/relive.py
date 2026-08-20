"""Recompute which provenance row is live for each (project, field).

Insertion order should not decide the answer. The waterfall does: an answer
beats no answer, an earlier step beats a later one, and on a tie the stronger
tier wins. Running this is idempotent and repairs any ordering accident.
"""
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scraper"))
from provenance import TIER_RANK

BASELINE = "data/_baseline_prerun.db"


def rank(r):
    return (
        0 if r["outcome"] == "resolved" else 1,       # an answer first
        -TIER_RANK.get(r["tier"], 0),                 # then the strongest evidence
        r["resolution_step"] or 9,                    # then the earliest step
        -r["id"],                                     # newest of equals
    )


def drop_spurious_audits(c):
    """An audit is only meaningful against the value that existed before this run."""
    b = sqlite3.connect(BASELINE)
    b.row_factory = sqlite3.Row
    base = {(r["id"], f): (r[f] or "").strip()
            for r in b.execute("select id,architect,civil_engineer,general_contractor "
                               "from projects")
            for f in ("architect", "civil_engineer", "general_contractor")}
    bad = []
    for r in c.execute("select id,project_id,field,prior_value from prior_value_audit"):
        want = base.get((r["project_id"], r["field"]), "")
        if (r["prior_value"] or "").strip() != want:
            bad.append(r["id"])
    c.executemany("delete from prior_value_audit where id=?", [(i,) for i in bad])
    print(f"removed {len(bad)} audit rows whose 'prior' was written during this run")


def main():
    c = sqlite3.connect("data/boston_re.db")
    c.row_factory = sqlite3.Row
    drop_spurious_audits(c)

    pairs = c.execute("select distinct project_id, field from field_provenance").fetchall()
    changed = 0
    for p in pairs:
        rows = c.execute("select * from field_provenance where project_id=? and field=?",
                         (p["project_id"], p["field"])).fetchall()
        win = sorted(rows, key=rank)[0]
        for r in rows:
            want = 0 if r["id"] == win["id"] else 1
            if r["superseded"] != want:
                c.execute("update field_provenance set superseded=? where id=?",
                          (want, r["id"]))
                changed += 1
        val = win["value"] if win["outcome"] == "resolved" else (
            "not_yet_selected" if win["outcome"] == "not_yet_selected" else None)
        c.execute(f"update projects set {p['field']}=? where id=?",
                  (val, p["project_id"]))
    c.commit()
    print(f"{len(pairs)} (project, field) pairs relived; {changed} superseded flags changed")
    for r in c.execute("""select field, outcome, count(*) from field_provenance
                          where superseded=0 group by 1,2 order by 1,3 desc"""):
        print("  ", tuple(r))


if __name__ == "__main__":
    main()
