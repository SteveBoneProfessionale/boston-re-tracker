"""Match the un-fetched Rhode Island zoning-board PDFs to projects by filename.

Providence names its zoning-board uploads after the address, so a filename is
a usable index. Match is on street number plus street name, both normalised,
so "1-Fields-Point" and "1 Fields Point Drive" meet.
"""
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from urllib.parse import unquote

sys.path.insert(0, str(Path(__file__).parent.parent / "scraper"))
from addr_norm import norm_address, street_name, street_numbers

import sqlite3

RI = ("Providence", "Cranston", "Warwick", "Pawtucket", "Newport")


def filename_tokens(url):
    base = unquote(url.rsplit("/", 1)[-1])
    base = re.sub(r"\.pdf$", "", base, flags=re.I)
    return re.sub(r"[._\-]+", " ", base)


def main():
    c = sqlite3.connect("data/boston_re.db")
    c.row_factory = sqlite3.Row
    live = {(r["project_id"], r["field"]): r for r in
            c.execute("select * from field_provenance where superseded=0")}
    projects = c.execute(
        f"select id,name,address,city,alt_addresses from projects "
        f"where coalesce(excluded,0)=0 and city in {RI}").fetchall()

    def gaps(pid):
        out = []
        for f in ("architect", "civil_engineer", "general_contractor"):
            l = live.get((pid, f))
            if l is None or l["outcome"] == "null" or l["tier"] == "unverified_prior":
                out.append(f)
        return out

    want = {}
    for p in projects:
        g = gaps(p["id"])
        if not g:
            continue
        addrs = [p["address"] or ""]
        if p["alt_addresses"]:
            addrs += [a.strip() for a in str(p["alt_addresses"]).split("|") if a.strip()]
        keys = set()
        for a in addrs:
            sn = street_name(a)
            for n in street_numbers(a):
                if sn:
                    keys.add((n, sn))
        if keys:
            want[p["id"]] = {"project": p, "keys": keys, "gaps": g}
    print(f"{len(want)} RI projects with a gap and a usable address")

    urls = []
    for f in ("data/ri_plansets/zbr_urls.txt", "data/ri_plansets/cpc_urls.txt"):
        urls += [u.strip() for u in Path(f).read_text().splitlines() if u.strip()]
    have = {p.name for p in Path("data/ri_plansets").glob("*.pdf")}
    print(f"{len(urls)} URLs known, {len(have)} already downloaded")

    hits = defaultdict(list)
    for u in urls:
        toks = filename_tokens(u)
        tl = toks.lower()
        nums = set(int(x) for x in re.findall(r"\b(\d{1,4})\b", toks))
        for pid, w in want.items():
            for (n, sn) in w["keys"]:
                if n in nums and sn and sn.split()[0] in tl:
                    hits[pid].append(u)
                    break
    print(f"{len(hits)} projects matched at least one document by filename")
    tot = sorted({u for v in hits.values() for u in v})
    new = [u for u in tot if unquote(u.rsplit('/',1)[-1]) not in have]
    print(f"{len(tot)} distinct documents matched; {len(new)} not yet downloaded")
    Path("data/ri_zbr_matches.json").write_text(json.dumps(
        {str(k): {"address": want[k]["project"]["address"],
                  "city": want[k]["project"]["city"],
                  "gaps": want[k]["gaps"], "urls": sorted(set(v))}
         for k, v in hits.items()}, indent=1))
    print("wrote data/ri_zbr_matches.json")
    for pid, v in list(hits.items())[:12]:
        p = want[pid]["project"]
        print(f"  {pid:4} {str(p['address'])[:30]:30} {len(set(v))} docs  gaps={','.join(want[pid]['gaps'])}")


if __name__ == "__main__":
    main()
