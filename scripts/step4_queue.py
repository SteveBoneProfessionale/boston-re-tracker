"""Group the remaining work by project so one search can serve several fields."""
import json
import sqlite3
from collections import Counter
from pathlib import Path

FIELDS = ("architect", "civil_engineer", "general_contractor")
RI = {"Providence", "Cranston", "Warwick", "Pawtucket", "Newport"}


def started(r):
    return (r["completion_stage"] in ("Under Construction", "Complete")
            or r["status"] in ("Under Construction", "Complete",
                               "Building Permit Granted"))


def main():
    c = sqlite3.connect("data/boston_re.db")
    c.row_factory = sqlite3.Row
    rows = c.execute("select id,name,address,city,status,completion_stage,developer,"
                     "residential_units,total_gsf,neighborhood from projects "
                     "where coalesce(excluded,0)=0").fetchall()
    live = {(r["project_id"], r["field"]): r for r in
            c.execute("select * from field_provenance where superseded=0")}

    queue = []
    for r in rows:
        city = r["city"] or "(null)"
        need, weak = [], []
        for f in FIELDS:
            l = live.get((r["id"], f))
            if l is None:
                need.append(f)
            elif l["outcome"] == "resolved" and l["tier"] == "unverified_prior":
                weak.append(f)
            elif l["outcome"] == "null":
                need.append(f)
        if not need and not weak:
            continue
        if city in ("Boston", "Cambridge") and any(f != "general_contractor"
                                                   for f in need):
            pri = 1
        elif "general_contractor" in need and started(r):
            pri = 2
        elif city in ("Boston", "Cambridge") and weak:
            pri = 3
        elif city in RI:
            pri = 5
        else:
            pri = 4
        queue.append({
            "project_id": r["id"], "priority": pri, "city": city,
            "name": r["name"], "address": r["address"], "developer": r["developer"],
            "neighborhood": r["neighborhood"], "status": r["status"],
            "started": started(r), "gsf": r["total_gsf"], "units": r["residential_units"],
            "need": need, "corroborate": weak,
        })
    queue.sort(key=lambda q: (q["priority"], -(q["gsf"] or 0), -(q["units"] or 0)))
    Path("data/step4_queue.json").write_text(json.dumps(queue, indent=1))

    print(f"projects needing at least one search: {len(queue)}")
    byp = Counter(q["priority"] for q in queue)
    lbl = {1: "Boston/Cambridge arch or civil missing", 2: "GC, construction started",
           3: "Boston/Cambridge corroborate unverified", 4: "other", 5: "Rhode Island"}
    for p in sorted(byp):
        print(f"  priority {p} ({lbl[p]}): {byp[p]} projects")
    print()
    fields = Counter(f for q in queue for f in q["need"])
    print("field gaps across the queue:", dict(fields))


if __name__ == "__main__":
    main()
