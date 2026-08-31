"""Re-check every `probable` GSF row against the BPDA page DESCRIPTION, not just
the Gross Floor Area field.

Austin Street Lots is why this exists: its GFA field reads 126,000 while its own
description says the project is "four new mixed-use buildings collectively
containing up to 790,000 sf". On a multi-parcel site the structured field can
describe ONE parcel while the row is the whole redevelopment.

Read-only. Writes a JSON cache; changes nothing.
"""
import csv
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


def sf_figures(txt):
    """Every square-footage figure stated in a block of prose, with its context."""
    out = []
    for m in re.finditer(
            r"([\d][\d,]{4,})\s*(?:\+/-\s*)?(?:sf|s\.f\.|square feet|square foot|gsf)",
            txt, re.I):
        try:
            v = int(m.group(1).replace(",", ""))
        except ValueError:
            continue
        if v < 10000:                       # ignore retail/amenity fragments
            continue
        lo = max(0, m.start() - 110)
        out.append((v, re.sub(r"\s+", " ", txt[lo:m.end() + 60]).strip()))
    return out


def main():
    rows = [r for r in csv.DictReader((ROOT / "structural_corrections.csv")
                                      .open(encoding="utf-8"))
            if r["confidence"] == "probable" and r["field"] == "total_gsf"]
    print(f"{len(rows)} probable GSF rows to re-check", flush=True)
    c = engine.connect()
    out = {}
    for i, r in enumerate(rows, 1):
        pid = int(r["project_id"])
        db = c.execute(text("select name,total_gsf,bpda_gsf from projects where id=:i"),
                       {"i": pid}).first()
        url = r["source_url"]
        try:
            g = requests.get(url, timeout=40,
                             headers={"User-Agent": "Mozilla/5.0 (pipeline audit)"})
            s = BeautifulSoup(g.text, "html.parser")
            det = {}
            for ct in s.find_all("div", class_="detailsContainer"):
                h = ct.find("div", class_="bpdaPrjHeader")
                v = ct.find("div", class_="bpdaPrjDetails")
                if h and v:
                    det[h.get_text(strip=True).lower()] = v.get_text(strip=True)
            gfa = det.get("gross floor area", "")
            gfa_n = int(re.sub(r"[^0-9]", "", gfa)) if gfa else None
            d = s.find("div", style=re.compile(r"font-size"))
            desc = d.get_text(" ", strip=True) if d else ""
        except Exception as e:                              # noqa: BLE001
            out[pid] = {"error": type(e).__name__}
            continue
        figs = sf_figures(desc)
        vals = [v for v, _ in figs]
        # Does the description corroborate the field, the old stored value, or
        # neither? A 2% tolerance absorbs "approximately".
        def near(a, b):
            return a and b and abs(a - b) <= max(2000, 0.02 * b)
        stored = int(r["current_value"])
        agrees_field = any(near(v, gfa_n) for v in vals)
        agrees_stored = any(near(v, stored) for v in vals)
        if not figs:
            verdict = "no figure in description"
        elif agrees_field and not agrees_stored:
            verdict = "description AGREES with the GFA field"
        elif agrees_stored and not agrees_field:
            verdict = "DESCRIPTION AGREES WITH THE OLD STORED VALUE"
        elif agrees_field and agrees_stored:
            verdict = "description mentions both"
        else:
            verdict = "description figures match NEITHER"
        out[pid] = {"name": db[0], "url": url, "gfa_field": gfa_n,
                    "stored_old": stored, "desc_figs": figs[:6],
                    "verdict": verdict, "desc": desc[:700]}
        print(f"  [{i:>2}/{len(rows)}] id={pid:<5}{str(db[0])[:32]:<34}"
              f"field={gfa_n or 0:>9,} old={stored:>9,}  {verdict}", flush=True)
        time.sleep(0.4)
    (ROOT / "_desc_check.json").write_text(json.dumps(out, indent=1), encoding="utf-8")
    c.close()
    print("\nwritten to audit/_desc_check.json")


if __name__ == "__main__":
    main()
