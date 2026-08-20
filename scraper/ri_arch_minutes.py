r"""
Look for the architect in meeting MINUTES, not in staff reports.

A staff report lists the documents in an application; if the applicant filed
only civil plans, no architect appears anywhere in it. The minutes are a
different record: they name who stood up and spoke, and in Rhode Island the
architect very often presents the building in person --

    "Ron Stevenson of South County Architecture and Design Inc. presented..."

so the appearance line carries a name the filing itself never had.

Every board's minutes are already cached by ri_harvest_agendas.py under
data/ri_pdfs/text/. This searches that cache for an address, then prints the
lines around the hit that mention an architect. It prints; it never writes.

    python scraper/ri_arch_minutes.py "180 Weeden"
    python scraper/ri_arch_minutes.py --all      # every address in the work list
"""

import re
import sys
import json
import argparse
from pathlib import Path

ROOT = Path(__file__).parent.parent
CORPUS = ROOT / "data" / "ri_agenda_corpus.json"
TEXT = ROOT / "data" / "ri_pdfs" / "text"

# The words an appearance line uses. "architect" alone is too narrow: minutes
# frequently write "AIA" or name the firm without the noun.
ARCH = re.compile(r"architect|\bA\.?I\.?A\b|architecture", re.I)


def docs():
    corpus = json.loads(CORPUS.read_text(encoding="utf-8"))
    for key, mtg in corpus.items():
        for d in mtg.get("documents", []):
            tf = d.get("text_file")
            if not tf:
                continue
            p = TEXT / tf
            if p.exists():
                yield mtg, d, p


def search(needles, window=600):
    pats = [re.compile(n, re.I) for n in needles]
    hits = 0
    for mtg, d, path in docs():
        try:
            t = path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        flat = " ".join(t.split())
        for pat in pats:
            for m in pat.finditer(flat):
                seg = flat[max(0, m.start() - window):m.end() + window]
                if not ARCH.search(seg):
                    continue
                hits += 1
                print("\n== %s | %s | %s | %s" % (
                    mtg["municipality"], mtg["board"], mtg["date"], d.get("kind")))
                print("   %s" % d.get("url", "")[:120])
                for s in re.split(r"(?<=[.;])\s+", seg):
                    if ARCH.search(s):
                        print("   ... %s" % s.strip()[:300].encode("ascii", "replace").decode())
                break
    if not hits:
        print("(no minutes or agenda text mentions an architect near %s)" % needles)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("needle", nargs="*")
    a = ap.parse_args()
    search(a.needle or [r"\barchitect"])
