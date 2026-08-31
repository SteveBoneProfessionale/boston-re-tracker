import json, pathlib, re, sys, time
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))
import requests
from bs4 import BeautifulSoup
from sqlalchemy import text
from db.database import engine
P = json.loads((pathlib.Path(__file__).parent/"_bpda_pages_20260831.json").read_text(encoding="utf-8"))["fetched"]
F = {int(k): v for k, v in P.items()}
under = sorted([(k, v) for k, v in F.items() if v["gfa"] and v["db_total_gsf"] < v["gfa"]],
               key=lambda kv: -(kv[1]["gfa"]/kv[1]["db_total_gsf"]))
c = engine.connect()
out = {}
for pid, v in under:
    r = c.execute(text("select name,address,land_sq_ft,processed_filing_name,status from projects where id=:i"), {"i": pid}).first()
    g = requests.get(v["url"], timeout=40, headers={"User-Agent": "Mozilla/5.0 (audit)"}); time.sleep(0.4)
    s = BeautifulSoup(g.text, "html.parser")
    d = s.find("div", style=re.compile(r"font-size"))
    desc = d.get_text(" ", strip=True) if d else ""
    out[pid] = {"name": r[0], "stored": v["db_total_gsf"], "page": v["gfa"],
                "land": v["land"], "filing": r[3], "desc": desc[:600], "url": v["url"]}
    print(f"id={pid:<5}{str(r[0])[:36]:<38}{v['db_total_gsf']:>10,} -> {v['gfa']:>10,}  {v['gfa']/v['db_total_gsf']:.2f}x  filing={str(r[3])[:24]}")
    if desc:
        hits = re.findall(r"[^.]{0,110}\b\d{2,3},\d{3}\s*(?:sf|square feet|gsf)[^.]{0,60}", desc, re.I)
        for h in hits[:2]:
            print(f"      \"...{h.strip()[:170]}\"")
    print()
(pathlib.Path(__file__).parent/"_under14.json").write_text(json.dumps(out, indent=1), encoding="utf-8")
c.close()
