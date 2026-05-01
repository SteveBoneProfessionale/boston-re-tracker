"""
Backfill developer_canonical for all existing projects.
Safe to re-run — skips projects already resolved.
"""

import sys
import logging
from pathlib import Path

import anthropic

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import init_db, get_session
from db.models import Project
from scraper.normalize_developer import normalize

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def run():
    init_db()
    session = get_session()
    client = anthropic.Anthropic()

    try:
        projects = (
            session.query(Project)
            .filter(Project.developer.isnot(None), Project.developer != "")
            .all()
        )
        log.info("Projects with developer data: %d", len(projects))

        updated = 0
        for proj in projects:
            raw = (proj.developer or "").strip()
            if not raw:
                continue

            canonical = normalize(raw, session=session, client=client)

            if proj.developer_canonical != canonical:
                proj.developer_canonical = canonical
                updated += 1
                if canonical != raw:
                    log.info("  %-55s  →  %s", raw[:55], canonical)

        session.commit()
        log.info("Done. Updated %d projects.", updated)

        # Summary
        from collections import Counter
        all_canonical = [
            p.developer_canonical for p in session.query(Project).all()
            if p.developer_canonical
        ]
        counts = Counter(all_canonical).most_common(20)
        log.info("\nTop developers by project count:")
        for name, count in counts:
            log.info("  %3d  %s", count, name)

    finally:
        session.close()


if __name__ == "__main__":
    run()
