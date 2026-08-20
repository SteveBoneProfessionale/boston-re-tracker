"""Boston GC source: Jobs Policy Compliance Reports.

2M worker-period rows collapse to one row per (project, address, GC). The
dataset emits the literal string "No General Contractor" for projects with
none on file; that is a null, not a firm.
"""
import json
import sys
from pathlib import Path

import requests

SQL = "https://data.boston.gov/api/3/action/datastore_search_sql"
RESOURCE = "5ab4b4de-c970-4619-ab55-ce4338535b24"

QUERY = f'''
SELECT "compliance_project_name", "project_address", "neighborhood",
       "developer", "general_contractor_name", "agency",
       MIN("period_ending") AS first_period,
       MAX("period_ending") AS last_period,
       COUNT(*) AS rows
FROM "{RESOURCE}"
GROUP BY 1,2,3,4,5,6
'''


def main():
    r = requests.get(SQL, params={"sql": QUERY}, timeout=180)
    r.raise_for_status()
    recs = r.json()["result"]["records"]
    print(f"{len(recs)} distinct (project, address, GC) tuples")

    named = [x for x in recs if (x.get("general_contractor_name") or "").strip()
             and x["general_contractor_name"].strip().lower() != "no general contractor"]
    nogc = [x for x in recs if (x.get("general_contractor_name") or "").strip().lower()
            == "no general contractor"]
    print(f"  with a named GC: {len(named)}")
    print(f'  literal "No General Contractor": {len(nogc)}  -> treated as null')
    projects = {(x.get("compliance_project_name"), x.get("project_address")) for x in recs}
    print(f"  distinct compliance projects: {len(projects)}")
    firms = {x["general_contractor_name"].strip() for x in named}
    print(f"  distinct GC firms: {len(firms)}")

    Path("data/boston_gc_compliance.json").write_text(json.dumps(recs, indent=1))
    print("wrote data/boston_gc_compliance.json")


if __name__ == "__main__":
    main()
