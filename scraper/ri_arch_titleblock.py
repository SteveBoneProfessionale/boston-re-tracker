r"""
Second sweep of the Providence plan sets, looking at the title block.

ri_planset_architects.py takes a firm only when its NAME says architecture --
"Kite Architects", "ZDS Architecture". That is the right rule for prose, but it
walks past the strongest evidence in a drawing set. 870 Westminster's DPR
sheet never uses the word architect once; it identifies its author as
"mcgeorgeai.com" over a street address, because that is what a title block is.

So this reads the same cached documents again and reports two things per file:
every line labelled architect, and every web or e-mail domain. A domain in a
title block is a firm; which firm, and whether it is the architect rather than
the civil engineer, is left to be judged -- this prints, it never writes.

    python scraper/ri_arch_titleblock.py            # cached files
    python scraper/ri_arch_titleblock.py --url URL  # one document
"""

import re
import sys
import argparse
from pathlib import Path
from collections import Counter

import pymupdf

ROOT = Path(__file__).parent.parent
CACHE = ROOT / "data" / "ri_plansets"

DOMAIN = re.compile(r"\b([a-z0-9][a-z0-9.\-]{2,40}\.(?:com|net|org))\b", re.I)
LABEL = re.compile(r"(architect[s]?)\s*[:\-]\s*([A-Za-z0-9&'\.\+\- ]{3,45})", re.I)
# Domains that are never the design team: the city, the state, a mapping or
# software vendor stamped into the plotter output, a news site.
NOISE = re.compile(
    r"providenceri|cranstonri|warwickri|pawtucketri|newportri|\.ri\.gov|ri\.gov|"
    r"autodesk|adobe|bluebeam|arcgis|esri|google|microsoft|usgs|noaa|fema|"
    r"nfpa|ashrae|icc-?es|astm|energystar|masshousing|rihousing|"
    r"gmail|yahoo|hotmail|outlook|aol", re.I)


def scan(path_or_doc, name, max_pages=8):
    doc = path_or_doc
    hits, labels = Counter(), []
    for i in range(min(max_pages, doc.page_count)):
        text = doc[i].get_text()
        for m in DOMAIN.finditer(text):
            d = m.group(1).lower().lstrip(".")
            if not NOISE.search(d):
                hits[d] += 1
        for m in LABEL.finditer(" ".join(text.split())):
            labels.append(" ".join(m.group(0).split())[:70])
    if hits or labels:
        print("== %s" % name)
        for d, n in hits.most_common(6):
            print("   domain  %-40s x%d" % (d, n))
        for l in dict.fromkeys(labels):
            print("   label   %s" % l.encode("ascii", "replace").decode())


def main(url=None):
    if url:
        import io, urllib.request
        raw = urllib.request.urlopen(
            urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"}),
            timeout=90).read()
        scan(pymupdf.open(stream=io.BytesIO(raw), filetype="pdf"), url)
        return
    for f in sorted(CACHE.glob("*.pdf")):
        try:
            scan(pymupdf.open(f), f.name)
        except Exception as e:
            print("== %s  UNREADABLE %s" % (f.name, e))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url")
    a = ap.parse_args()
    main(a.url)
