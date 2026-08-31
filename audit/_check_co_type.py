"""Is each `co_issued` completion actually a CO for NEW CONSTRUCTION?

The address-matching problem turned out to be the smaller one. Reading the
permit records behind the range matches, every description was "Change
Occupancy", "Renovations - Interior NSC" or "Interior/Exterior Work" -- COs
issued for altering a building that already stood. A CO of that kind is not
evidence that a proposed new development was built; the Motor Mart Garage has
had a certificate of occupancy since the 1920s.

This reads the permit behind every co_issued row and classifies it.

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
NEW = re.compile(r"new construction|erect|new building|new bldg", re.I)
ALTER = re.compile(r"change occupancy|renovat|interior|exterior work|alteration|"
                   r"repair|fit-?out|remodel", re.I)


def q(sql):
    try:
        r = requests.get("https://data.boston.gov/api/3/action/datastore_search_sql",
                         params={"sql": sql}, timeout=90)
        return r.json().get("result", {}).get("records", [])
    except Exception:
        return []


def main():
    c = engine.connect()
    rows = list(c.execute(text(
        "select id,name,address,total_gsf,completion_evidence,completion_date,status "
        "from projects where completion_basis='co_issued' order by coalesce(total_gsf,0) desc")))
    print(f"{len(rows)} co_issued rows\n", flush=True)
    out = {}
    for pid, name, addr, gsf, ev, cdate, st in rows:
        m = re.search(r"\b([A-Z]{1,4}\d+)\s*\(", str(ev or ""))
        pno = m.group(1) if m else None
        rec = None
        if pno:
            got = q(f'SELECT "permitnumber","address","worktype","permittypedescr",'
                    f'"description","comments","issued_date","declared_valuation" '
                    f'FROM "{R}" WHERE "permitnumber" = \'{pno}\'')
            time.sleep(0.3)
            rec = got[0] if got else None
        desc = str((rec or {}).get("description") or "")
        wt = str((rec or {}).get("worktype") or "")
        val = str((rec or {}).get("declared_valuation") or "")
        if not rec:
            k = "permit not found"
        elif NEW.search(desc) or wt.upper() in ("ERECT", "NEWCON", "ADDITION"):
            k = "NEW CONSTRUCTION"
        elif ALTER.search(desc):
            k = "alteration / change of occupancy"
        else:
            k = f"unclear ({desc[:32]})"
        out[pid] = {"name": name, "addr": addr, "gsf": gsf, "permit": pno,
                    "permit_addr": (rec or {}).get("address"), "worktype": wt,
                    "desc": desc, "valuation": val, "verdict": k, "co_date": cdate,
                    "status": st}
        print(f"  id={pid:<5}{str(name)[:30]:<32}{int(gsf or 0):>10,}  "
              f"{wt[:10]:<12}{k[:34]:<36}{desc[:34]}", flush=True)
    (pathlib.Path(__file__).parent / "_co_type.json").write_text(
        json.dumps(out, indent=1), encoding="utf-8")
    import collections
    print()
    for k, n in collections.Counter(v["verdict"].split(" (")[0]
                                    for v in out.values()).most_common():
        print(f"  {k:<40}{n}")
    c.close()


if __name__ == "__main__":
    main()
