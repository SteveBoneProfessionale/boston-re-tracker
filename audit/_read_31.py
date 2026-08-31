"""Read the full permit record behind each low-ratio co_issued row.

The ratio only says which rows to read. The call is made on what the permit
DESCRIBES -- its worktype, description and comments -- against what the project
is. 33-61 Temple Street is the control: a change-of-occupancy CO valued at
$74,145,000 is a real completion, so the code is not the test either.

Read-only.
"""
import json
import pathlib
import re
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

import requests
from sqlalchemy import text

from db.database import engine

R = "6ddcd912-32a0-43df-9908-63574f8c7e77"


def q(sql):
    try:
        r = requests.get("https://data.boston.gov/api/3/action/datastore_search_sql",
                         params={"sql": sql}, timeout=90)
        return r.json().get("result", {}).get("records", [])
    except Exception:
        return []


def main():
    co = json.loads((pathlib.Path(__file__).parent / "_co_type.json")
                    .read_text(encoding="utf-8"))
    rows = []
    for pid, v in co.items():
        g = int(v["gsf"] or 0)
        val = re.sub(r"[^0-9.]", "", str(v["valuation"]) or "")
        val = float(val) if val else 0.0
        if g > 0 and val / g < 20:
            rows.append((val / g, val, g, int(pid), v))
    rows.sort()
    print(f"{len(rows)} rows below $20/SF\n", flush=True)

    c = engine.connect()
    out = {}
    for ratio, val, g, pid, v in rows:
        pr = c.execute(text(
            "select name,address,status,completion_stage,description "
            "from projects where id=:i"), {"i": pid}).first()
        rec = q(f'SELECT "permitnumber","address","worktype","permittypedescr",'
                f'"description","comments","issued_date","declared_valuation",'
                f'"occupancytype","sq_feet" FROM "{R}" '
                f'WHERE "permitnumber" = \'{v["permit"]}\'')
        time.sleep(0.3)
        rec = rec[0] if rec else {}
        out[pid] = {
            "name": pr[0], "proj_addr": pr[1], "status": pr[2],
            "stage": pr[3], "gsf": g, "ratio": round(ratio, 2), "valuation": val,
            "permit": v["permit"], "permit_addr": rec.get("address"),
            "worktype": rec.get("worktype"), "desc": rec.get("description"),
            "comments": rec.get("comments"), "occupancytype": rec.get("occupancytype"),
            "sq_feet": rec.get("sq_feet"), "issued": str(rec.get("issued_date"))[:10],
            "proj_desc": (pr[4] or "")[:300],
        }
        print(f"id={pid:<5}{str(pr[0])[:28]:<30}{g:>10,}  ${val:>12,.0f}  "
              f"{ratio:>6.2f}  {str(rec.get('worktype'))[:9]:<11}"
              f"{str(rec.get('description'))[:30]:<32}sq_feet={rec.get('sq_feet')}",
              flush=True)
        cm = (rec.get("comments") or "").strip()
        if cm:
            print(f"        comments: {cm[:170]}", flush=True)
    (pathlib.Path(__file__).parent / "_read31.json").write_text(
        json.dumps(out, indent=1), encoding="utf-8")
    c.close()


if __name__ == "__main__":
    main()
