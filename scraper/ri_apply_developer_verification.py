r"""
STEP 2 writer for the two-step Rhode Island developer method.

Reads data/ri_developer_verification.json -- one record per project, each
already carrying the outcome the research landed in -- and writes it to the
database. The three outcomes are stored, not collapsed, so it stays visible at
a glance which developers outside coverage confirms and which rest on the
planning file alone:

  confirmed      an article names the same company as the planning document
  document_only  the planning document names it and no article was found
                 either way. The planning file is a primary source, so this is
                 a real answer, just weaker.
  conflicted     an article names a DIFFERENT company. Both are stored with
                 their sources and the row is flagged. Nothing is picked here.

A project with no name from either path is left blank. Blank is an answer.

    python scraper/ri_apply_developer_verification.py --dry-run
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
OUTCOMES = ("confirmed", "document_only", "conflicted", "blank")


def load():
    if not STORE.exists():
        return {}
    return json.loads(STORE.read_text(encoding="utf-8"))


def save(store):
    STORE.write_text(json.dumps(store, indent=1, ensure_ascii=False), encoding="utf-8")


def record(store, pid, outcome, name=None, sources=None, web_name=None, note=None):
    """Add one project's result. Called by the research loop."""
    if outcome not in OUTCOMES:
        raise ValueError("bad outcome: %r" % outcome)
    store[str(pid)] = {
        "outcome": outcome,
        "developer": name,
        "web_developer": web_name,
        "sources": sources or [],
        "note": note,
    }
    return store


def main(dry_run=False):
    store = load()
    if not store:
        log.warning("Nothing in %s", STORE)
        return

    session = get_session()
    counts = dict.fromkeys(OUTCOMES, 0)
    wrote = 0
    try:
        for pid, r in store.items():
            p = session.query(Project).get(int(pid))
            if p is None:
                log.warning("no project %s", pid)
                continue
            counts[r["outcome"]] = counts.get(r["outcome"], 0) + 1
            if r["outcome"] == "blank" or not r.get("developer"):
                continue

            # The agenda is primary and is never overwritten by this pass.
            if p.developer:
                continue

            p.developer = r["developer"]
            p.developer_resolution_method = r["outcome"]
            p.developer_sources = json.dumps(r.get("sources") or [], ensure_ascii=False)
            if r["outcome"] == "conflicted":
                p.is_flagged = True
            wrote += 1

        if dry_run:
            session.rollback()
            log.info("DRY RUN -- rolled back")
        else:
            session.commit()
    finally:
        session.close()

    log.info("Records: %d", len(store))
    for o in OUTCOMES:
        log.info("  %-14s %4d", o, counts.get(o, 0))
    log.info("Rows written: %d", wrote)


if __name__ == "__main__":
    main(dry_run="--dry-run" in sys.argv)
