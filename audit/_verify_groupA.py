"""Read the BPDA page for all 18 Group A rows, live.

The verification report corroborated most Group A square footages against trade
press and flagged that the BPDA Gross Floor Area field itself had not been read.
It has: the figure already stored on these rows came from that field, and this
re-reads every page to confirm none has moved.

Read-only.
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

IDS = [406, 910, 405, 403, 909, 400, 904, 402, 399, 908,
       907, 397, 906, 407, 903, 396, 398, 905]


def num(s):
    d = re.sub(r"[^0-9]", "", s or "")
    return int(d) if d else None


def main():
    c = engine.connect()
    out = {}
    print(f"{'id':<6}{'project':<34}{'stored':>10}{'live GFA':>11}{'land SF':>10}  match")
    print("-" * 92)
    for pid in IDS:
        r = c.execute(text(
            "select name,bpda_url,bpda_gsf,total_gsf,status from projects where id=:i"),
            {"i": pid}).first()
        try:
            g = requests.get(r[1], timeout=40,
                             headers={"User-Agent": "Mozilla/5.0 (pipeline audit)"})
            s = BeautifulSoup(g.text, "html.parser")
            det = {}
            for ct in s.find_all("div", class_="detailsContainer"):
                h = ct.find("div", class_="bpdaPrjHeader")
                v = ct.find("div", class_="bpdaPrjDetails")
                if h and v:
                    det[h.get_text(strip=True).lower()] = v.get_text(strip=True)
            gfa = num(det.get("gross floor area"))
            land = num(det.get("land sq. feet") or det.get("land sq feet"))
            ph = s.find("ul", class_="projectPhaseList")
            phase = None
            if ph:
                a = ph.find("li", class_="active")
                phase = a.get_text(strip=True) if a else None
            d = s.find("div", style=re.compile(r"font-size"))
            desc = d.get_text(" ", strip=True) if d else ""
            status = f"HTTP {g.status_code}"
        except Exception as e:                                  # noqa: BLE001
            gfa = land = phase = None
            desc = ""
            status = type(e).__name__
        stored = r[2] or r[3]
        ok = ("MATCH" if gfa and stored and gfa == stored
              else ("no page GFA" if not gfa else f"DIFFERS by {gfa - (stored or 0):+,}"))
        out[pid] = {"name": r[0], "url": r[1], "stored": stored, "live_gfa": gfa,
                    "land": land, "phase": phase, "db_status": r[4],
                    "desc": desc[:600], "http": status}
        print(f"{pid:<6}{str(r[0])[:32]:<34}{(stored or 0):>10,}{(gfa or 0):>11,}"
              f"{(land or 0):>10,}  {ok}")
        time.sleep(0.4)
    (pathlib.Path(__file__).parent / "_groupA_live.json").write_text(
        json.dumps(out, indent=1), encoding="utf-8")
    c.close()


if __name__ == "__main__":
    main()
