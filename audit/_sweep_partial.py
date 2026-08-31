"""How many rows across the whole table need bpda_gsf_is_partial?

Austin Street's BPDA page publishes 126,000 in its Gross Floor Area field and
790,000 in its own description. The field can describe one parcel of a larger
site without saying so, and nothing on the page marks the difference. The only
detector available is the page's own prose.

For every Boston row carrying a bpda_gsf, this fetches the page, pulls every
square-footage figure out of the description, and flags the row when the
description states a figure MATERIALLY LARGER than the field -- which is the
Austin Street signature.

Read-only. Writes a JSON cache; changes nothing.
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
OUT = ROOT / "_partial_sweep.json"


def figs(txt):
    out = []
    for m in re.finditer(r"([\d][\d,]{4,})\s*(?:\+/-\s*)?"
                         r"(?:sf|s\.f\.|square feet|square foot|gsf)", txt, re.I):
        try:
            v = int(m.group(1).replace(",", ""))
        except ValueError:
            continue
        if v >= 10000:
            lo = max(0, m.start() - 110)
            out.append((v, re.sub(r"\s+", " ", txt[lo:m.end() + 50]).strip()))
    return out


def main():
    c = engine.connect()
    rows = list(c.execute(text(
        "select id,name,bpda_url,bpda_gsf,total_gsf from projects "
        "where bpda_gsf is not null and city='Boston' "
        "and coalesce(bpda_url,'') like '%bostonplans%' "
        "order by bpda_gsf desc")))
    print(f"sweeping {len(rows)} Boston rows with a bpda_gsf", flush=True)
    out, flagged = {}, 0
    for i, (pid, name, url, bg, tg) in enumerate(rows, 1):
        try:
            g = requests.get(url, timeout=30,
                             headers={"User-Agent": "Mozilla/5.0 (pipeline audit)"})
            if g.status_code != 200:
                out[pid] = {"name": name, "error": f"HTTP {g.status_code}"}
                continue
            s = BeautifulSoup(g.text, "html.parser")
            d = s.find("div", style=re.compile(r"font-size"))
            desc = d.get_text(" ", strip=True) if d else ""
        except Exception as e:                              # noqa: BLE001
            out[pid] = {"name": name, "error": type(e).__name__}
            continue
        f = figs(desc)
        # Material = at least 25% larger than the field and at least 50,000 SF
        # more, which keeps ordinary rounding and component figures out.
        bigger = [(v, ctx) for v, ctx in f if v >= bg * 1.25 and v - bg >= 50000]
        if bigger:
            flagged += 1
            top = max(bigger)[0]
            print(f"  [{i}/{len(rows)}] PARTIAL? id={pid:<5}{str(name)[:30]:<32}"
                  f"field={bg:>9,}  description states {top:>9,}", flush=True)
        out[pid] = {"name": name, "url": url, "field": bg, "stored": tg,
                    "desc_figs": f[:6], "bigger": bigger[:3]}
        time.sleep(0.35)
    OUT.write_text(json.dumps({"flagged": flagged, "checked": len(rows),
                               "rows": out}, indent=1), encoding="utf-8")
    print(f"\nchecked {len(rows)}, candidates for bpda_gsf_is_partial: {flagged}")
    c.close()


if __name__ == "__main__":
    main()
