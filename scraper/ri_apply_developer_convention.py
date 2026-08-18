r"""
Apply the standing convention for what "developer" means, to the conflicted
Rhode Island records.

    Developer means the party EXECUTING the development. A public agency, a
    redevelopment authority or a passive landowner is not the developer even
    when it is the named applicant -- it goes in owner_or_agency instead of
    being discarded.

That resolves three of the five conflicts and leaves two genuinely open:

  717  50 Sims Ave        developer Scout, agency Providence Redevelopment
                          Agency. The PRA owns and petitions; Scout signed the
                          master developer agreement.
  721  St Joseph Hospital developer Paolino Properties. Urban Land Development
                          is dropped rather than carried as a second name --
                          no source names it at all.
  788  78 Fountain St     left BLANK. An individual's filing inside a building
                          the Nordblom/Cornish JV developed is a separate small
                          item; attributing the JV to it would be wrong.
  774  55 Pine St         stays conflicted. Granoff and Lundgren both claim it,
                          which means a joint venture or a change of hands.
  849  321 South Main St  stays conflicted. White Columns Properties vs 321 SMS
                          Partners, with no source reconciling them.

Also nulls id=719. Seth Yurdin is a sitting city councilman and a land use
attorney with an office next door to the filing; attorneys appear on filings
routinely and are not developers.

    python scraper/ri_apply_developer_convention.py --dry-run
"""

import sys
import json
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import get_session
from db.models import Project

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

STORE = Path(__file__).parent.parent / "data" / "ri_developer_verification.json"

DECISIONS = {
    717: {
        "developer": "Scout",
        "owner_or_agency": "Providence Redevelopment Agency",
        "outcome": "confirmed",
        "note": ("Resolved under the standing convention: the developer is the party "
                 "executing the work. Scout, a Philadelphia firm, signed the master "
                 "developer agreement with the PRA to renovate and lease the 110,000 sf "
                 "former manufacturing building at 50 Sims Ave. The PRA owns the site "
                 "and petitioned the rezoning, so it is recorded as owner_or_agency."),
    },
    721: {
        "developer": "Paolino Properties",
        "owner_or_agency": None,
        "outcome": "confirmed",
        "note": ("Resolved under the standing convention. Paolino Properties held and "
                 "marketed the former St Joseph Hospital and is the party of record in "
                 "the coverage. Urban Land Development LLC is DROPPED rather than kept "
                 "as a second name: no source names it at all, only the filing. Knight "
                 "& Swan LLC bought the site later and is the party to watch if this "
                 "row is revisited."),
    },
    788: {
        "developer": None,
        "owner_or_agency": None,
        "outcome": "blank",
        "note": ("Left BLANK deliberately. The filing is an individual's item at an "
                 "address inside the building developed by the 78 Fountain JV "
                 "(Nordblom Development Company and Cornish Associates). Attributing "
                 "the JV to this filing would be wrong -- a small separate item is not "
                 "the $55m redevelopment it sits inside."),
    },
}

NULL_OUT = {
    719: ("Seth Yurdin is a sitting Providence city councilman (Ward 1, Majority "
          "Leader) and a land use attorney whose office is at 148 Governor Street, "
          "next door to the filing at 146. Attorneys appear on filings routinely and "
          "are not developers. Nulled."),
}


def main(dry_run=False):
    store = json.loads(STORE.read_text(encoding="utf-8"))
    session = get_session()
    try:
        for pid, d in DECISIONS.items():
            p = session.get(Project, pid)
            if p is None:
                log.warning("no project %s", pid)
                continue
            p.developer = d["developer"]
            p.developer_canonical = d["developer"]
            p.owner_or_agency = d["owner_or_agency"]
            p.developer_resolution_method = (None if d["outcome"] == "blank"
                                             else d["outcome"])
            p.is_flagged = False
            rec = store.get(str(pid), {})
            rec["outcome"] = d["outcome"]
            rec["developer"] = d["developer"]
            rec["owner_or_agency"] = d["owner_or_agency"]
            rec["web_developer"] = None
            rec["note"] = d["note"]
            rec["auto"] = False
            store[str(pid)] = rec
            log.info("  id=%-4d developer=%-28s agency=%s", pid,
                     str(d["developer"])[:28], d["owner_or_agency"])

        for pid, why in NULL_OUT.items():
            p = session.get(Project, pid)
            if p is None:
                continue
            p.developer = None
            p.developer_canonical = None
            p.developer_resolution_method = None
            rec = store.get(str(pid), {})
            rec["outcome"] = "blank"
            rec["developer"] = None
            rec["note"] = why
            rec["auto"] = False
            store[str(pid)] = rec
            log.info("  id=%-4d NULLED -- %s", pid, why[:60])

        if dry_run:
            session.rollback()
            log.info("DRY RUN -- nothing written")
            return
        session.commit()
        STORE.write_text(json.dumps(store, indent=1, ensure_ascii=False), encoding="utf-8")
        log.info("Applied. 774 and 849 deliberately left conflicted.")
    finally:
        session.close()


if __name__ == "__main__":
    main(dry_run="--dry-run" in sys.argv)
