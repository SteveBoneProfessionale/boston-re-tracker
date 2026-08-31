"""How often does a BPDA page's description disagree with its own structured fields?

Three cases have now turned up by accident: Austin Street Lots (field 126,000,
description 790,000), 60 Kilmarnock (field 429,700, description 420,800) and
1725 Hyde Park Avenue (land field 238,068, description 119,034). This measures
the whole population rather than waiting for a fourth to appear.

Compares BOTH structured fields against every square-footage figure the
description states, and classifies what the description figure appears to be so
the count is not inflated by land areas quoted next to a GFA field.

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
# Words near a figure that say it is LAND rather than floor area.
LAND_CUE = re.compile(r"site|parcel|lot|acre|land", re.I)
GFA_CUE = re.compile(r"gross floor|gross square|gsf|floor area|building|comprising|"
                     r"totaling|totalling|consist", re.I)


def num(s):
    d = re.sub(r"[^0-9]", "", s or "")
    return int(d) if d else None


def figures(txt):
    out = []
    for m in re.finditer(r"([\d][\d,]{4,})\s*(?:\+/-\s*)?"
                         r"(?:sf|s\.f\.|square feet|square foot|gsf)", txt, re.I):
        try:
            v = int(m.group(1).replace(",", ""))
        except ValueError:
            continue
        if v < 5000:
            continue
        lo = max(0, m.start() - 120)
        ctx = re.sub(r"\s+", " ", txt[lo:m.end() + 60]).strip()
        kind = ("land" if LAND_CUE.search(ctx) and not GFA_CUE.search(ctx)
                else ("gfa" if GFA_CUE.search(ctx) else "unclear"))
        out.append({"v": v, "kind": kind, "ctx": ctx[:200]})
    return out


def main():
    c = engine.connect()
    rows = list(c.execute(text(
        "select id,name,bpda_url,bpda_gsf,land_sq_ft,total_gsf from projects "
        "where coalesce(bpda_url,'') like '%bostonplans%' order by id")))
    print(f"sweeping {len(rows)} BPDA pages", flush=True)
    out, n_gfa, n_land, fail = {}, 0, 0, 0
    for i, (pid, name, url, bg, land_db, tg) in enumerate(rows, 1):
        try:
            g = requests.get(url, timeout=30,
                             headers={"User-Agent": "Mozilla/5.0 (pipeline audit)"})
            if g.status_code != 200:
                fail += 1
                continue
            s = BeautifulSoup(g.text, "html.parser")
            det = {}
            for ct in s.find_all("div", class_="detailsContainer"):
                h = ct.find("div", class_="bpdaPrjHeader")
                v = ct.find("div", class_="bpdaPrjDetails")
                if h and v:
                    det[h.get_text(strip=True).lower()] = v.get_text(strip=True)
            gfa_f = num(det.get("gross floor area"))
            land_f = num(det.get("land sq. feet") or det.get("land sq feet"))
            d = s.find("div", style=re.compile(r"font-size"))
            desc = d.get_text(" ", strip=True) if d else ""
        except Exception:                                        # noqa: BLE001
            fail += 1
            continue
        figs = figures(desc)
        # A disagreement is a description figure of the SAME KIND as a populated
        # structured field, differing by more than 2% and more than 2,000 SF.
        def off(field, kinds):
            if not field:
                return None
            cands = [f for f in figs if f["kind"] in kinds]
            if not cands:
                return None
            if any(abs(f["v"] - field) <= max(2000, 0.02 * field) for f in cands):
                return None
            best = max(cands, key=lambda f: abs(f["v"] - field))
            return best
        bad_gfa = off(gfa_f, ("gfa",))
        bad_land = off(land_f, ("land",))
        if bad_gfa:
            n_gfa += 1
        if bad_land:
            n_land += 1
        out[pid] = {"name": name, "url": url, "gfa_field": gfa_f,
                    "land_field": land_f, "figs": figs[:8],
                    "bad_gfa": bad_gfa, "bad_land": bad_land}
        time.sleep(0.3)
        if i % 50 == 0:
            print(f"  {i}/{len(rows)}  gfa_disagree={n_gfa} land_disagree={n_land}",
                  flush=True)
    (ROOT / "_desc_vs_field.json").write_text(
        json.dumps({"checked": len(out), "failed": fail, "gfa_disagree": n_gfa,
                    "land_disagree": n_land, "rows": out}, indent=1), encoding="utf-8")
    print(f"\nchecked {len(out)}, failed {fail}")
    print(f"  GFA field contradicted by description : {n_gfa}")
    print(f"  Land field contradicted by description: {n_land}")
    c.close()


if __name__ == "__main__":
    main()
