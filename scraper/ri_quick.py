r"""
Shorthand for recording step-2 outcomes during the research run.

Writing the JSON out by hand for every project was the slowest part of the
loop. This takes a compact line per project and fills in the developer name
and document citation from the step-1 candidate file:

    python scraper/ri_quick.py "553|confirmed|Dridrigal Properties LLC and Empire Builders|note|src=valleybreeze.com:https://..."
    python scraper/ri_quick.py --stdin < lines.txt

Fields: id|outcome|developer|note[|src=domain:url ...]
  developer "-"  means take the top step-1 candidate
  outcome blank   needs no developer
"""

import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from scraper.ri_record import add

CAND = Path(__file__).parent.parent / "data" / "ri_developer_candidates.json"


def parse(line):
    parts = [p.strip() for p in line.split("|")]
    if len(parts) < 3:
        raise SystemExit("need id|outcome|developer[|note][|src=...]  got: %r" % line)
    pid, outcome, dev = int(parts[0]), parts[1], parts[2]
    note = parts[3] if len(parts) > 3 else ""
    srcs = []
    for p in parts[4:]:
        if p.startswith("src="):
            body = p[4:]
            dom, _, url = body.partition(":")
            srcs.append({"type": "article", "domain": dom, "url": url})

    cands = {r["id"]: r for r in json.loads(CAND.read_text(encoding="utf-8"))}
    top = (cands.get(pid, {}).get("candidates") or [None])[0]
    if dev == "-":
        dev = top["name"] if top else None
    if top:
        srcs.insert(0, {"type": "document",
                        "detail": "planning filing, %s" % top["found_as"],
                        "quote": top.get("quote", "")[:240]})
    rec = {"id": pid, "outcome": outcome, "note": note, "sources": srcs}
    if outcome != "blank":
        rec["developer"] = dev
    return rec


def main():
    lines = (sys.stdin.read().splitlines() if "--stdin" in sys.argv
             else [a for a in sys.argv[1:] if not a.startswith("--")])
    recs = [parse(l) for l in lines if l.strip()]
    add(recs)
    for r in recs:
        print("  %-5s %-14s %s" % (r["id"], r["outcome"], r.get("developer") or ""))


if __name__ == "__main__":
    main()
