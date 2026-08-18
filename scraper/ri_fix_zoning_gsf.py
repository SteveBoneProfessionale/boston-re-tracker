r"""
Null total_gsf where the figure was lifted from the zoning district code
rather than from a stated building size.

Cranston writes its single-family zones as the minimum lot size in square
feet -- A-80 is 80,000 sq ft, A-20 is 20,000, A-12 is 12,000, and the raw
zoning string spells it out:

    "A-12 - Single Family Residential (12,000 sq. ft.)"

The extractor took that 12,000 as the building's gross square footage. A
minimum lot size is a zoning standard, not a program, exactly as a density
statement ("10.89 units per acre") is not a unit count.

The test is deliberately narrow: total_gsf must equal a number appearing in
the project's own zoning code, either directly or scaled by a thousand. That
cannot fire on a real building size unless it coincides exactly with its own
zone's minimum, and every match is printed for inspection before it is
written.

    python scraper/ri_fix_zoning_gsf.py --dry-run
"""

import re
import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import get_session
from db.models import Project

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

RI = ("Providence", "Warwick", "Cranston", "Pawtucket", "Newport")


def suspects(session):
    rows = (session.query(Project)
            .filter(Project.city.in_(RI))
            .filter(Project.total_gsf.isnot(None))
            .all())
    out = []
    for p in rows:
        zone = " ".join(x for x in (p.zoning_district_raw, p.zoning_components) if x)
        nums = {int(n) for n in re.findall(r"\d+", zone)}
        for n in nums:
            if not n:
                continue
            if p.total_gsf == n or p.total_gsf == n * 1000:
                out.append((p, n, zone))
                break
    return out


def main(dry_run=False):
    session = get_session()
    try:
        found = suspects(session)
        log.info("total_gsf matching a number in its own zoning code: %d", len(found))
        for p, n, zone in found:
            log.info("  id=%-4d %-9s %-26s gsf=%-7s units=%-4s zone=%s",
                     p.id, p.city, (p.address or "")[:26], p.total_gsf,
                     p.residential_units, zone[:50])
            p.total_gsf = None
            note = "total_gsf cleared: value was the zoning minimum lot size, not a building size"
            p.notes = ((p.notes + " | ") if p.notes else "") + note

        if dry_run:
            session.rollback()
            log.info("DRY RUN -- rolled back")
        else:
            session.commit()
            log.info("Cleared %d", len(found))
    finally:
        session.close()


if __name__ == "__main__":
    main(dry_run="--dry-run" in sys.argv)
