r"""
Append one or more step-2 results to data/ri_developer_verification.json.

Kept deliberately small: the research loop calls it with a JSON blob so every
result is written to disk the moment it is decided, and the run is resumable
if it stops part way.

    python scraper/ri_record.py '[{"id":605,"outcome":"confirmed",...}]'
    python scraper/ri_record.py --file batch.json
    python scraper/ri_record.py --status
"""

import sys
import json
from pathlib import Path

STORE = Path(__file__).parent.parent / "data" / "ri_developer_verification.json"
OUTCOMES = ("confirmed", "document_only", "conflicted", "blank")


def load():
    if STORE.exists():
        return json.loads(STORE.read_text(encoding="utf-8"))
    return {}


def add(records):
    store = load()
    for r in records:
        o = r.get("outcome")
        if o not in OUTCOMES:
            raise SystemExit("bad outcome %r on id=%s" % (o, r.get("id")))
        if o in ("confirmed", "document_only") and not r.get("developer"):
            raise SystemExit("outcome %s needs a developer (id=%s)" % (o, r.get("id")))
        if o == "conflicted" and not (r.get("developer") and r.get("web_developer")):
            raise SystemExit("conflicted needs both names (id=%s)" % r.get("id"))
        store[str(r["id"])] = {
            "outcome": o,
            "developer": r.get("developer"),
            "web_developer": r.get("web_developer"),
            "sources": r.get("sources", []),
            "note": r.get("note"),
            # Auto-derived names get recomputed when the parser changes;
            # hand-researched ones must not be clobbered by that pass.
            "auto": bool(r.get("auto")),
        }
    STORE.write_text(json.dumps(store, indent=1, ensure_ascii=False), encoding="utf-8")
    return store


def status():
    store = load()
    from collections import Counter
    c = Counter(v["outcome"] for v in store.values())
    total = 307
    print("recorded %d / %d" % (len(store), total))
    for o in OUTCOMES:
        print("  %-14s %4d" % (o, c.get(o, 0)))
    print("  %-14s %4d" % ("remaining", total - len(store)))


if __name__ == "__main__":
    if "--status" in sys.argv:
        status()
    elif "--file" in sys.argv:
        p = sys.argv[sys.argv.index("--file") + 1]
        add(json.loads(Path(p).read_text(encoding="utf-8")))
        status()
    else:
        add(json.loads(sys.argv[1]))
        status()
