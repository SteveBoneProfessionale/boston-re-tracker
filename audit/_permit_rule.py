"""Classify every permit_active row from its own address's permit history.

Address matching is done on the EXACT street-number + street-body pair and the
matched address strings are recorded, because loose matching is what produced a
wrong answer the first time: a Certificate of Occupancy at 6 Stack ST was very
nearly read as evidence that 10 Stack ST had finished. They are different
buildings on the same street.

Read-only. Writes nothing to the database.
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
# Enumerated from the dataset, NOT guessed. The first attempt used "ELECTRIC",
# which is how an 8-character console column had truncated "ELECTRICAL", and
# the rule silently found no trade permits at 10 Stack Street as a result.
TRADE = {"ELECTRICAL", "PLUMBING", "GAS", "FA", "LVOLT", "SPRINK",
         "INTREN", "INTEXT", "SRVCHG", "TMPSER"}
# Work that happens on a site WITHOUT a building going up. Present at 10 Stack
# Street -- special events, then a demolition -- which is not construction.
SITE_ONLY = {"SPCEVE", "RAZE", "OTHER", "SIGNES", "GEN", "FENCE", "SITE"}


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
        "select id,name,address,total_gsf,status,completion_stage "
        "from projects where completion_basis='permit_active' "
        "order by coalesce(total_gsf,0) desc")))
    out = {}
    for pid, name, addr, gsf, st, cs in rows:
        m = re.match(r"^\s*(\d+)\s*[-–]?\s*\d*\s*([A-Za-z].*?)\s*$", (addr or "").strip())
        if not m:
            out[pid] = {"name": name, "verdict": "unparseable", "addr": addr}
            continue
        num = m.group(1)
        body = re.sub(r"\s+(St|Street|Ave|Avenue|Rd|Road|Blvd|Way|Pl|Place|Sq|Square|"
                      r"Ter|Dr|Drive|Hwy|Highway)\.?$", "", m.group(2), flags=re.I).strip()
        safe = body.replace("'", "''")
        recs = q(f'SELECT "permitnumber","address","worktype","permittypedescr",'
                 f'"status","issued_date","declared_valuation" FROM "{R}" '
                 f'WHERE "address" ILIKE \'{num} {safe}%\' ORDER BY "issued_date"')
        time.sleep(0.4)
        exact = [x for x in recs
                 if re.match(rf"^{num}\s+{re.escape(body)}\b", str(x["address"] or ""), re.I)]
        co = [x for x in exact if "occupancy" in str(x["permittypedescr"]).lower()]
        trade = [x for x in exact if str(x["worktype"]).upper() in TRADE]
        erect = [x for x in exact if str(x["worktype"]).upper() == "ERECT"]
        if co:
            v = "Complete"
            why = (f"Certificate of Occupancy {co[-1]['permitnumber']} issued "
                   f"{str(co[-1]['issued_date'])[:10]}")
        elif trade:
            v = "Under Construction"
            why = (f"{len(trade)} trade permit(s) after the erect permit: "
                   + ", ".join(sorted({str(x['worktype']) for x in trade})))
        elif erect:
            v = "Permitted - Not Started"
            why = "erect permit only; no trade permit and no CO at this address"
        elif exact:
            v = "no erect permit"
            why = f"{len(exact)} permits but none is Erect/New Construction"
        else:
            v = "no permit found"
            why = "no permit at this exact street number"
        out[pid] = {"name": name, "addr": addr, "gsf": gsf, "status": st, "stage": cs,
                    "verdict": v, "why": why, "n_exact": len(exact),
                    "addresses": sorted({str(x["address"]) for x in exact}),
                    "permits": [{"n": x["permitnumber"], "w": x["worktype"],
                                 "t": x["permittypedescr"], "s": x["status"],
                                 "d": str(x["issued_date"])[:10]} for x in exact]}
        print(f"id={pid:<5}{str(name)[:30]:<32}{int(gsf or 0):>9,}  "
              f"was={str(st)[:22]:<24}-> {v:<24} ({why[:60]})")
    (pathlib.Path(__file__).parent / "_permit_rule.json").write_text(
        json.dumps(out, indent=1), encoding="utf-8")
    c.close()


if __name__ == "__main__":
    main()
