r"""
Merge the nine duplicate Rhode Island rows found during developer research.

Each pair is one project that survived collapse twice, mostly because OCR'd
minutes gave a different plat/lot or a mangled street number than the clean
agenda did:

  665 <- 503   116 Waterman St, Case 22-033MA
  746 <- 606   217 Angell St, Referral 3473 (OCR read plat/lot 13-320 where the
               clean agenda gave 13-52_53_55, and the text is visibly garbled:
               "Referraf No. 3473")
  742 <- 668   473 Washington St, Case 18-027MI
  739 <- 737   50 Ashburton St (one row carried the referral number inside the
               address field: "3479 - 50 Ashburton Street")
  723 <- 725   100 Niantic Ave (the absorbed row read "00 Niantic Ave")

Survivors were chosen on which row carries the better data, not the lower id:
the deeper stage history, the cleaner address, and figures that match verified
coverage. Merging moves history rather than dropping it, so the loser's
appearances end up on the survivor either way.

    python scraper/ri_merge_duplicates.py --dry-run
"""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import get_session
from db.models import Project
from app.tabs.review import merge_projects

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

MERGES = [
    (665, 503, "116 Waterman St, Case 22-033MA. Survivor has the deeper stage history."),
    (746, 606, "217 Angell St, Referral 3473. Absorbed row came from OCR'd minutes "
               "reading plat/lot 13-320 against the agenda's 13-52_53_55; its "
               "gsf of 1,000 is not trustworthy."),
    (742, 668, "473 Washington St, Case 18-027MI. Survivor's 21 units match the "
               "unit count in Greater City Providence's coverage; the absorbed "
               "row's 27 is the other iteration of the same scheme."),
    (739, 737, "50 Ashburton St. Survivor has the clean address; the absorbed row "
               "carried the referral number inside the address field."),
    (723, 725, "100 Niantic Ave. Absorbed row's address was truncated to "
               "'00 Niantic Ave'."),
]


def main(dry_run=False):
    session = get_session()
    try:
        for keep_id, absorb_id, why in MERGES:
            keep, absorb = session.get(Project, keep_id), session.get(Project, absorb_id)
            if keep is None or absorb is None:
                log.info("  %s <- %s already merged or missing, skipping", keep_id, absorb_id)
                continue
            log.info("  keep %-4d (%s)  <-  absorb %-4d (%s)",
                     keep_id, (keep.address or "")[:26], absorb_id, (absorb.address or "")[:26])
            if dry_run:
                continue
            res = merge_projects(session, keep_id, absorb_id, "duplicate row: " + why)
            if not res.get("ok"):
                log.error("    FAILED: %s", res.get("error"))
            else:
                log.info("    moved %s", res["moved"])
        if dry_run:
            log.info("DRY RUN -- nothing written")
    finally:
        session.close()


if __name__ == "__main__":
    main(dry_run="--dry-run" in sys.argv)
