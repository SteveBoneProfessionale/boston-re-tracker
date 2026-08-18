r"""
Find records that are not commercial development, across the whole Rhode
Island set -- not only the three noticed by hand.

Three known examples set the shape:

  Newport, 16 Congdon Ave   lots merged by deed in 1993, being unmerged. A
                            3-bed cottage. A title correction, not a project.
  Cranston, 40 Cindy Lane   two family lots into three so a daughter can build
                            between two existing houses. Stated on the record
                            as estate planning, not development.
  Newport, 1 York Street    shifting a shared lot line so a building footprint
                            sits inside its own lot.

These are paper actions on residential parcels. They inflate the pipeline
count and drag every per-project average down.

REPORTS ONLY. Nothing is deleted or flagged here -- the list is for review.

    python scraper/ri_noncommercial_scan.py
"""

import re
import sys
import json
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import get_session
from db.models import Project

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

OUT = Path(__file__).parent.parent / "data" / "ri_noncommercial_review.json"
RI = ("Providence", "Warwick", "Cranston", "Pawtucket", "Newport")

# Ordered: the first category that matches is the one reported.
CATEGORIES = [
    ("deed_correction", re.compile(
        r"illegally\s+merged|merged\s+by\s+deed|unmerge|de-?merge|"
        r"correct(?:ing|ion)?\s+(?:of\s+)?(?:a\s+)?(?:scrivener|deed|title|record)|"
        r"scrivener'?s?\s+error|confirmatory\s+deed|quiet\s+title", re.I)),

    ("lot_line_adjustment", re.compile(
        r"lot\s*line\s+(?:adjust|relocat|revis|chang|modif)|adjust\s+the\s+shared\s+lot\s+line|"
        r"boundary\s+line\s+(?:adjust|agreement)|administrative\s+subdivision\s+to\s+adjust|"
        # "reconfigure the two EXISTING lots into three" -- an adjective between
        # the article and the noun defeated a fixed (two|three|existing) slot.
        r"reconfigur\w*\s+the\s+[\w\s]{0,24}?lots|"
        r"(?:divid\w+|subdivi\w+|split\w*)\s+(?:the\s+|an?\s+)?(?:existing\s+)?"
        r"[\w\s,]{0,30}?lots?\s+into\s+(?:two|three|2|3)\b", re.I)),

    ("single_family", re.compile(
        r"single[\s-]*family\s+(?:dwelling|home|house|residence|structure)|"
        r"one[\s-]*family\s+dwelling|existing\s+single[\s-]*family|"
        r"construct\s+(?:a\s+)?(?:new\s+)?single[\s-]*family", re.I)),

    ("residential_accessory", re.compile(
        r"\b(?:fence|shed|swimming\s+pool|above[\s-]?ground\s+pool|deck|"
        r"detached\s+garage|carport|dormer|farmer'?s\s+porch|"
        r"accessory\s+family\s+dwelling)\b", re.I)),

    ("signage_only", re.compile(
        r"^(?:.{0,80})?(?:sign(?:age)?\s+(?:permit|variance|relief|only)|"
        r"erect\s+a\s+sign|freestanding\s+sign|wall\s+sign|"
        r"replace\s+(?:an?\s+)?existing\s+sign)", re.I)),

    ("street_abandonment", re.compile(
        r"abandonment\s+of\s+(?:a\s+portion\s+of\s+)?\w+\s+(?:street|avenue|road|way)|"
        r"paper\s+street|discontinu\w+\s+(?:a\s+)?(?:portion\s+of\s+)?\w*\s*street", re.I)),

    ("estate_planning", re.compile(
        r"estate\s+planning|family\s+(?:transfer|conveyance)", re.I)),
]

# Evidence that it IS development after all, which outranks a keyword match.
DEVELOPMENT = re.compile(
    r"\b(?:[4-9]|[1-9]\d+)\s+(?:residential\s+)?units\b|"
    r"mixed[\s-]use|apartment|multi[\s-]?family|hotel|retail|office|laborator|lab\b|"
    r"warehouse|industrial|self[\s-]?storage|restaurant|medical|institutional|"
    r"adaptive\s+reuse|major\s+land\s+development", re.I)


def main():
    session = get_session()
    rows = session.query(Project).filter(Project.city.in_(RI)).all()
    found = []
    for p in rows:
        text = " ".join(x for x in (p.description, p.notes) if x)
        if not text:
            continue
        units = p.residential_units or 0
        for name, rx in CATEGORIES:
            m = rx.search(text)
            if not m:
                continue
            # A big program outranks the keyword: a 200-unit building that
            # also adjusts a lot line is still a development.
            dev = DEVELOPMENT.search(text)
            if units >= 4 or (p.total_gsf or 0) >= 20000:
                break
            found.append({
                "id": p.id, "city": p.city, "address": p.address or "",
                "category": name, "matched": m.group(0)[:60],
                "units": p.residential_units, "gsf": p.total_gsf,
                "asset_class": p.asset_class,
                "has_development_language": bool(dev),
                "description": (p.description or "")[:200],
            })
            break

    OUT.write_text(json.dumps(found, indent=1, ensure_ascii=False), encoding="utf-8")

    from collections import Counter
    c = Counter(f["category"] for f in found)
    log.info("RI projects scanned : %d", len(rows))
    log.info("Non-commercial candidates: %d", len(found))
    for k, n in c.most_common():
        log.info("    %-24s %3d", k, n)
    amb = [f for f in found if f["has_development_language"]]
    log.info("  of those, carrying some development language (review first): %d", len(amb))
    log.info("Wrote %s", OUT)


if __name__ == "__main__":
    main()
