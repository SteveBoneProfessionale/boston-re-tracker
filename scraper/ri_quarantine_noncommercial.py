r"""
Quarantine the non-commercial Rhode Island records.

These are paper actions and minor relief on residential or institutional
property -- deed corrections, lot line adjustments, single-family items, fence
variances, mural installations, a street abandonment. They are not pipeline.
They inflate the project count and drag every per-project average down.

QUARANTINE, NOT DELETION. `excluded` drops a row out of every count and chart
via load_projects(); the row itself stays in the table with the reason on it,
so the decision is auditable and reversible.

Reviewed by hand before quarantining, per the four fence matches and the two
public-art items:

  794  31 Candace St      EXCLUDE  fence height variance at a library, R-4 zone
  807  370 Cranston St    EXCLUDE  8ft fence to enclose a courtyard. The
                                   building is being renovated by DownCity
                                   Design, but THIS filing is a fence.
  860  150 Bridgham St    EXCLUDE  homeowner's fence, R-3 and historic overlay
  764  335 Westminster    EXCLUDE  painting a mural on a masonry wall
  776  91 Clemence St     EXCLUDE  installing a mural
  795  963 North Main St  KEPT IN  special use permit ADDING 400 sf of GFA to
                                   an existing car wash in a C-2 district. That
                                   is a real, if small, commercial expansion --
                                   not a fence, despite matching the scan.

    python scraper/ri_quarantine_noncommercial.py --dry-run
"""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import get_session
from db.models import Project

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

EXCLUDE = {
    512: "estate-planning lot split: two family lots into three so a daughter can build between two existing houses",
    523: "lot subdivision: one 8,000 sf lot into two 4,000 sf lots",
    531: "lot subdivision of a parcel with an existing single-family dwelling",
    533: "lot subdivision: one lot into two, residential",
    560: "single-family residence, Newport R-10 zone",
    561: "subdivision of an existing lot into two, residential",
    562: "single-family residence, Newport R-10A zone",
    565: "residential conveyance/application between individual homeowners on Harrison Ave",
    566: "lot line adjustment so a building footprint sits within its own lot",
    568: "deed correction: lots illegally merged by deed in 1993, being unmerged. A 3-bed cottage.",
    608: "abandonment of a portion of Steele Street: a paper street action, not development",
    687: "subdivision of a house-of-worship lot into two",
    689: "subdivision of a 12,070 sf R-3 lot with a three-family dwelling into two",
    692: "subdivision of a 17,117 sf R-1 lot with a three-family dwelling into three",
    764: "public art: painting a permanent mural on an exterior masonry wall",
    776: "public art: installing a permanent mural on a building elevation",
    794: "fence height variance on a front lot line, R-4 residential zone",
    807: "dimensional variance for an 8ft fence enclosing an internal courtyard",
    847: "single-family dwelling, R-1 residential and historic district",
    860: "homeowner's fence height variance, R-3 residential and historic overlay",
    # Found by the pipeline report: not a filing at all. The "address" is the
    # name of Cranston's comprehensive plan and the text is a speaker's views
    # on density at a plan hearing.
    534: "not a project: discussion of Cranston's 2030 comprehensive plan",
}

# Reviewed and deliberately NOT excluded.
KEPT = {
    795: "special use permit adding 400 sf of GFA to an existing car wash in a "
         "C-2 district -- a genuine commercial expansion",
}


def main(dry_run=False):
    session = get_session()
    try:
        n = 0
        for pid, why in EXCLUDE.items():
            p = session.get(Project, pid)
            if p is None:
                log.warning("  id=%s missing (merged away?)", pid)
                continue
            p.excluded = True
            p.excluded_reason = why
            n += 1
            log.info("  id=%-4d %-26s %s", pid, (p.address or "")[:26], why[:58])
        for pid, why in KEPT.items():
            p = session.get(Project, pid)
            if p is not None:
                p.excluded = False
                p.excluded_reason = None
                log.info("  id=%-4d KEPT IN -- %s", pid, why[:58])
        if dry_run:
            session.rollback()
            log.info("DRY RUN -- nothing written")
            return
        session.commit()
        log.info("Quarantined %d; 1 reviewed and kept in.", n)
    finally:
        session.close()


if __name__ == "__main__":
    main(dry_run="--dry-run" in sys.argv)
