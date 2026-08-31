"""Settle the 13 range-match failures by DISTANCE, not by arithmetic on street numbers.

Street-number reasoning has now produced two wrong answers in this audit: it read
a cross-street CO as a completion, and it read a project's own address range
("34-44 Lochdale Road") as a mismatch against a permit inside that range. The
permits dataset carries coordinates, and so do the projects, so the question
"is this permit on this site" has a direct answer.

Read-only.
"""
import json
import math
import pathlib
import re
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

import requests
from sqlalchemy import text

from db.database import engine

R = "6ddcd912-32a0-43df-9908-63574f8c7e77"
IDS = [67, 133, 140, 224, 247, 254, 261, 273, 370]   # the nine parity-crossed, as a CONTROL


def q(sql):
    try:
        r = requests.get("https://data.boston.gov/api/3/action/datastore_search_sql",
                         params={"sql": sql}, timeout=90)
        return r.json().get("result", {}).get("records", [])
    except Exception:
        return []


def metres(a, b, c, d):
    if None in (a, b, c, d):
        return None
    p = math.pi / 180
    x = (0.5 - math.cos((c - a) * p) / 2
         + math.cos(a * p) * math.cos(c * p) * (1 - math.cos((d - b) * p)) / 2)
    return 12742000 * math.asin(math.sqrt(x))


def rng(s):
    m = re.match(r"^\s*(\d+)[A-Za-z]?(?:\s*-\s*(\d+))?", str(s or ""))
    if not m:
        return None, None
    lo = int(m.group(1))
    return lo, int(m.group(2)) if m.group(2) else lo


def main():
    c = engine.connect()
    out = {}
    for pid in IDS:
        r = c.execute(text(
            "select id,name,address,latitude,longitude,total_gsf,completion_evidence,"
            "completion_date from projects where id=:i"), {"i": pid}).first()
        ev = str(r[6] or "")
        pm = re.search(r"\b([A-Z]{1,4}\d+)\s*\(", ev)
        permit_no = pm.group(1) if pm else None
        am = re.search(r"\bat ([^.]{3,34}?)\. Matched", ev)
        permit_addr = am.group(1).strip() if am else None
        rec = None
        if permit_no:
            got = q(f'SELECT "permitnumber","address","y_latitude","x_longitude",'
                    f'"description","permittypedescr","issued_date","parcel_id" '
                    f'FROM "{R}" WHERE "permitnumber" = \'{permit_no}\'')
            time.sleep(0.35)
            rec = got[0] if got else None
        d = None
        if rec:
            try:
                d = metres(float(r[3]), float(r[4]),
                           float(rec["y_latitude"]), float(rec["x_longitude"]))
            except (TypeError, ValueError, KeyError):
                d = None
        plo, phi = rng(r[2])
        klo, khi = rng(permit_addr)
        overlap = (plo is not None and klo is not None
                   and not (phi < klo or khi < plo))
        out[pid] = {"name": r[1], "addr": r[2], "gsf": r[5], "permit": permit_no,
                    "permit_addr": permit_addr, "metres": d,
                    "ranges_overlap": overlap,
                    "desc": (rec or {}).get("description"),
                    "parcel": (rec or {}).get("parcel_id"),
                    "co_date": r[7]}
        dm = f"{d:,.0f} m" if d is not None else "no coords"
        print(f"id={pid:<5}{str(r[1])[:28]:<30}{int(r[5] or 0):>10,}  "
              f"proj {str(r[2])[:22]:<24}permit {str(permit_addr)[:20]:<22}"
              f"{dm:>10}  ranges_overlap={overlap}")
    (pathlib.Path(__file__).parent / "_parity_control.json").write_text(
        json.dumps(out, indent=1), encoding="utf-8")
    c.close()


if __name__ == "__main__":
    main()
