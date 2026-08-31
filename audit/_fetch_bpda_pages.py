"""Fetch the BPDA project page for every Boston row in the audit scope.

Read-only against the database and against bostonplans.org. Caches the parsed
Gross Floor Area and Land Sq. Feet to audit/_bpda_pages_20260831.json so every
proposed change in structural_corrections.csv rests on a page retrieved in this
session rather than on a value the scraper stored earlier.
"""
import json
import pathlib
import re
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

import requests
from bs4 import BeautifulSoup
from sqlalchemy import text

from db.database import engine

ROOT = pathlib.Path(__file__).parent
OUT = ROOT / "_bpda_pages_20260831.json"


def num(s):
    d = re.sub(r"[^0-9]", "", s or "")
    return int(d) if d else None


def main():
    c = engine.connect()
    rows = list(c.execute(text(
        "select id,name,bpda_url,bpda_gsf,total_gsf from projects "
        "where coalesce(total_gsf,0)>=250000 and city='Boston' "
        "and coalesce(bpda_url,'') like '%bostonplans%' order by total_gsf desc")))
    c.close()
    print(f"fetching {len(rows)} pages", flush=True)
    out, fail = {}, []
    for i, (pid, name, url, bg, tg) in enumerate(rows, 1):
        try:
            r = requests.get(url, timeout=30,
                             headers={"User-Agent": "Mozilla/5.0 (pipeline audit)"})
            if r.status_code != 200:
                fail.append([pid, name, f"HTTP {r.status_code}"])
            else:
                s = BeautifulSoup(r.text, "html.parser")
                d = {}
                for ct in s.find_all("div", class_="detailsContainer"):
                    h = ct.find("div", class_="bpdaPrjHeader")
                    v = ct.find("div", class_="bpdaPrjDetails")
                    if h and v:
                        d[h.get_text(strip=True).lower()] = v.get_text(strip=True)
                out[pid] = {
                    "name": name, "url": url,
                    "gfa": num(next((v for k, v in d.items() if "floor area" in k), None)),
                    "land": num(next((v for k, v in d.items() if "land sq" in k), None)),
                    "neighborhood": next((v for k, v in d.items() if "neighborhood" in k), None),
                    "address": next((v for k, v in d.items() if "address" in k), None),
                    "db_bpda_gsf": bg, "db_total_gsf": tg,
                }
        except Exception as e:                                   # noqa: BLE001
            fail.append([pid, name, type(e).__name__])
        time.sleep(0.3)
        if i % 20 == 0:
            print(f"  {i}/{len(rows)}", flush=True)

    OUT.write_text(json.dumps({"fetched_utc": "2026-08-31", "fetched": out,
                               "failed": fail}, indent=1), encoding="utf-8")
    print(f"\nOK {len(out)} / {len(rows)}   failed {len(fail)}")
    print("  GFA present            :", sum(1 for v in out.values() if v["gfa"]))
    print("  Land Sq. Feet present  :", sum(1 for v in out.values() if v["land"]))
    print("  live GFA == bpda_gsf   :",
          sum(1 for v in out.values() if v["gfa"] == v["db_bpda_gsf"]), "(scraper fidelity)")
    print("  live GFA == total_gsf  :",
          sum(1 for v in out.values() if v["gfa"] == v["db_total_gsf"]), "(column the app uses)")
    for f in fail[:12]:
        print("   FAIL", f)


if __name__ == "__main__":
    main()
