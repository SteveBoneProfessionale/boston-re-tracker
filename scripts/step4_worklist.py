"""Build and prioritise the Step 4 web-search work list.

Priority follows the instruction: Boston and Cambridge architects and civil
engineers first, then general contractors on projects already under
construction, then everything else. Rhode Island is capped per field because
its trade-press coverage is thin and searches there mostly fail to resolve.
"""
import json
import sqlite3
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
    rows = c.execute(
        "select id,name,address,city,status,completion_stage,developer,"
        "residential_units,total_gsf from projects where coalesce(excluded,0)=0"
    ).fetchall()
    live = {}
    for r in c.execute("select project_id,field,outcome,tier from field_provenance "
                       "where superseded=0"):
        live[(r["project_id"], r["field"])] = r

    work = []
    for r in rows:
        city = r["city"] or "(null)"
        for f in FIELDS:
            l = live.get((r["id"], f))
            if l is not None and l["outcome"] == "resolved" and l["tier"] != "unverified_prior":
                continue
            if f == "general_contractor" and not started(r) and \
                    l is not None and l["outcome"] == "not_yet_selected":
                continue          # correctly parked; not a search target
            absent = (l is None) or (l["tier"] == "unverified_prior")
            unverified = (l is not None and l["tier"] == "unverified_prior")
            if city in ("Boston", "Cambridge") and f != "general_contractor"                     and not unverified:
                pri = 1
            elif f == "general_contractor" and started(r):
                pri = 2
            elif city in ("Boston", "Cambridge") and unverified:
                pri = 3
            elif city in RI:
                pri = 5
            else:
                pri = 4
            work.append({
                "project_id": r["id"], "field": f, "priority": pri,
                "city": city, "name": r["name"], "address": r["address"],
                "developer": r["developer"], "status": r["status"],
                "completion_stage": r["completion_stage"],
                "started": started(r),
                "units": r["residential_units"], "gsf": r["total_gsf"],
                "current": l["outcome"] if l is not None else "absent",
                "current_tier": l["tier"] if l is not None else None,
            })

    # Size first within a priority band: a 400-unit tower is far more likely to
    # have been written about than a three-decker.
    work.sort(key=lambda w: (w["priority"], -(w["gsf"] or 0), -(w["units"] or 0)))
    Path("data/step4_worklist.json").write_text(json.dumps(work, indent=1))

    print(f"work list: {len(work)} (project, field) pairs\n")
    from collections import Counter
    byp = Counter(w["priority"] for w in work)
    for p in sorted(byp):
        lbl = {1: "Boston/Cambridge arch+civil, absent",
               2: "GC, construction started",
               3: "Boston/Cambridge arch+civil, corroborate unverified",
               4: "other", 5: "Rhode Island"}[p]
        print(f"  priority {p} ({lbl}): {byp[p]}")
    print()
    bycf = Counter((w["city"], w["field"]) for w in work)
    for (city, f), n in sorted(bycf.items()):
        print(f"    {city:12} {f:20} {n}")


if __name__ == "__main__":
    main()
