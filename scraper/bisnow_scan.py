r"""Scan non-deal-sheet articles for acquisition language.

The deal sheets are the dense source, but standalone stories carry deals too --
One Marina Park Drive got its own article before it appeared in a deal sheet.
This surfaces candidates for reading; it does not extract transactions, because
deciding what is a sale, what city it is in and whose price it is needs
judgement that a regex does not have.
"""
import re, sys
from pathlib import Path
from pathlib import Path
import sys as _s; _s.path.insert(0, str(Path(__file__).parent.parent)); from scraper.bisnow_sales import body

BUY = re.compile(r"\b(acquired|sold|bought|purchase[ds]?|trades? for|paid|"
                 r"took possession|deed record)\b", re.I)
MONEY = re.compile(r"\$\d[\d.,]*\s*(?:million|billion|M\b|B\b)")
PLACE = re.compile(r"\b(Boston|Cambridge|Seaport|Back Bay|Fenway|Kendall|"
                   r"Downtown Crossing|Financial District|South Boston|"
                   r"East Boston|Dorchester|Charlestown|Allston|Brighton|"
                   r"Roxbury|Jamaica Plain|South End|Beacon Hill|Fort Point)\b")

d = Path("data/bisnow")
for p in sorted(d.glob("*.txt")):
    if "deal-sheet" in p.name:
        continue
    t = body(p)
    if not t:
        t = p.read_text(encoding="utf-8")
    if BUY.search(t) and MONEY.search(t) and PLACE.search(t):
        first = [l for l in t.splitlines() if len(l) > 60][:3]
        print("=" * 76)
        print(p.name[:10], p.name[11:72])
        for l in first:
            print("   ", l[:250])
