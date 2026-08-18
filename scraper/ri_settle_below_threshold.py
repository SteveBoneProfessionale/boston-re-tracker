r"""
Settle the below-threshold projects from step 1 alone.

Step 2 verification is spent only where a project is 20+ units, 20,000+ SF, or
states no size at all -- that last case because a blank size does not mean a
small project, and some of the largest deals carry no figures on the agenda.

Everything below that settles on the planning document by itself: named ->
document_only, unnamed -> blank. These are NOT searched. Development coverage
does not exist for a six-unit filing and searching for it buys nothing.

    python scraper/ri_settle_below_threshold.py --dry-run
"""

import sys
import json
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import get_session
from db.models import Project
from scraper.ri_record import add, load

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

CAND = Path(__file__).parent.parent / "data" / "ri_developer_candidates.json"
RI = ("Providence", "Warwick", "Cranston", "Pawtucket", "Newport")


def above_threshold(p):
    if (p.residential_units or 0) >= 20:
        return True
    if (p.total_gsf or 0) >= 20000:
        return True
    return p.residential_units is None and p.total_gsf is None


def main(dry_run=False):
    cands = {r["id"]: r for r in json.loads(CAND.read_text(encoding="utf-8"))}
    already = load()
    session = get_session()
    rows = (session.query(Project)
            .filter(Project.city.in_(RI))
            .filter((Project.developer.is_(None)) | (Project.developer == ""))
            .all())
    session.close()

    recs, named, blank = [], 0, 0
    for p in rows:
        if above_threshold(p) or str(p.id) in already:
            continue
        cs = cands.get(p.id, {}).get("candidates") or []
        top = cs[0] if cs else None
        if top:
            named += 1
            recs.append({
                "id": p.id, "outcome": "document_only", "developer": top["name"],
                "auto": True,
                "note": ("Below the step-2 threshold (%s units / %s sf), settled on the "
                         "planning document alone and deliberately not searched. Found in "
                         "the filing as '%s'.%s"
                         % (p.residential_units, p.total_gsf, top["found_as"],
                            " Applicant entity appears to be a shell named for the address."
                            if top.get("is_shell") else "")),
                "sources": [{"type": "document",
                             "detail": "planning filing, %s" % top["found_as"],
                             "quote": top.get("quote", "")[:300]}],
            })
        else:
            blank += 1
            recs.append({
                "id": p.id, "outcome": "blank",
                "note": "Below the step-2 threshold and no company name anywhere in the "
                        "planning documents. Not searched.",
                "sources": [],
            })

    log.info("Below threshold, unsettled: %d", len(recs))
    log.info("  document_only : %d", named)
    log.info("  blank         : %d", blank)
    if dry_run:
        log.info("DRY RUN -- nothing written")
        return
    add(recs)
    log.info("Recorded.")


if __name__ == "__main__":
    main(dry_run="--dry-run" in sys.argv)
