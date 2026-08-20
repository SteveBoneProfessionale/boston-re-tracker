"""Index the cached Rhode Island minutes text against projects that still have gaps.

19 MB of board minutes and staff reports were extracted to text in an earlier
pass and only ever mined with a keyword rule for architects. This finds, for
each project that still needs a field, the passages in that corpus that name
its address, so they can be read properly.

Matching is on street number plus street name, then the passage is windowed
around the hit so the extraction sees the surrounding discussion rather than
the whole meeting.
"""
import json
import re
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scraper"))
from addr_norm import address_keys

TEXT = Path("data/ri_pdfs/text")
RI = ("Providence", "Cranston", "Warwick", "Pawtucket", "Newport")
WINDOW = 2600          # characters either side of a hit
MAX_PASSAGES = 4       # per project, best-scoring first

ROLE = re.compile(r"architect|engineer|surveyor|contractor|prepared by|"
                  r"on behalf of|represent|design", re.I)


def main():
    c = sqlite3.connect("data/boston_re.db")
    c.row_factory = sqlite3.Row
    live = {(r["project_id"], r["field"]): r for r in
            c.execute("select * from field_provenance where superseded=0")}
    projects = c.execute(
        f"select id,name,address,city,alt_addresses from projects "
        f"where coalesce(excluded,0)=0 and city in {RI}").fetchall()

    want = {}
    for p in projects:
        gaps = []
        for f in ("architect", "civil_engineer", "general_contractor"):
            l = live.get((p["id"], f))
            if l is None or l["outcome"] == "null" or l["tier"] == "unverified_prior":
                gaps.append(f)
        if not gaps:
            continue
        addrs = [p["address"] or ""]
        if p["alt_addresses"]:
            addrs += [a.strip() for a in str(p["alt_addresses"]).split("|") if a.strip()]
        keys = set()
        for a in addrs:
            keys |= address_keys(a)
        if keys:
            want[p["id"]] = {"p": p, "keys": keys, "gaps": gaps}
    print(f"{len(want)} RI projects with a gap and a usable address")

    # One pass over the corpus; for each file, test every project key.
    pats = {}
    for pid, w in want.items():
        alts = []
        for (n, sn) in w["keys"]:
            first = sn.split()[0] if sn else ""
            if not first or len(first) < 3:
                continue
            alts.append(rf"\b{n}\b[^\n]{{0,40}}?\b{re.escape(first)}")
        if alts:
            pats[pid] = re.compile("|".join(alts), re.I)

    hits = defaultdict(list)
    files = sorted(TEXT.glob("*.txt"))
    for i, f in enumerate(files, 1):
        try:
            t = f.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        if not ROLE.search(t):
            continue
        for pid, pat in pats.items():
            m = pat.search(t)
            if not m:
                continue
            s = max(0, m.start() - WINDOW)
            seg = t[s:m.end() + WINDOW]
            score = len(ROLE.findall(seg))
            hits[pid].append({"file": f.name, "score": score,
                              "passage": " ".join(seg.split())})
        if i % 500 == 0:
            print(f"  scanned {i}/{len(files)}", flush=True)

    out = {}
    for pid, hs in hits.items():
        hs.sort(key=lambda h: -h["score"])
        out[str(pid)] = {
            "address": want[pid]["p"]["address"],
            "city": want[pid]["p"]["city"],
            "gaps": want[pid]["gaps"],
            "passages": hs[:MAX_PASSAGES],
        }
    Path("data/ri_minutes_hits.json").write_text(json.dumps(out, indent=1))
    print(f"{len(out)} projects have at least one role-bearing passage")
    print(f"total passages kept: {sum(len(v['passages']) for v in out.values())}")


if __name__ == "__main__":
    main()
