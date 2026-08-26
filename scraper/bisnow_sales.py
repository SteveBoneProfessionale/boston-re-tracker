r"""Pull the transaction prose out of each cached Bisnow deal sheet.

TWO SECTIONS, NOT ONE. A deal sheet opens with a LEAD STORY -- the headline
deal, given three or four paragraphs -- and only then runs the sectioned
SALES / LEASES / FINANCING / CONSTRUCTION AND DEVELOPMENT lists. An extractor
anchored on the "SALES" header alone silently drops the lead, which is by
construction the LARGEST deal in the issue. The 27 April issue is the proof:
its SALES list has the $36M Brighton Avenue portfolio, while the $113.5M
Newbury Street pair that gives the issue its headline sits above the header.

So the body runs from the second occurrence of the headline (the first is the
<title>, above the site navigation) to the first non-sales section marker.

Leases and financings are excluded deliberately. A lease is not an acquisition
and a refinancing is not a change of ownership -- Winthrop Center's $856M is
debt, and treating it as a transaction would have been a large false positive.
"""
import re, sys
from pathlib import Path

STOP = ("LEASES", "FINANCING", "CONSTRUCTION AND DEVELOPMENT", "PEOPLE",
        "EXECUTIVE MOVES", "OTHER", "DEVELOPMENT", "LEASING")


def body(p: Path) -> str:
    lines = [l.rstrip() for l in p.read_text(encoding="utf-8").splitlines()]
    title = lines[3].strip() if len(lines) > 3 else ""
    starts = [n for n, l in enumerate(lines) if l.strip() == title and title]
    start = starts[1] + 1 if len(starts) > 1 else 0
    out = []
    for l in lines[start:]:
        s = l.strip()
        if s in STOP:
            break
        if s and s not in ("Google Maps", "***"):
            out.append(s)
        elif s == "***":
            out.append("***")
    return "\n".join(out)


if __name__ == "__main__":
    d = Path("data/bisnow")
    only = sys.argv[1] if len(sys.argv) > 1 else ""
    for p in sorted(d.glob("*.txt")):
        if only and only not in p.name:
            continue
        s = body(p)
        if not s:
            continue
        print("=" * 78)
        print(p.name[:10], p.name[11:70])
        print(s)
        print()
