"""
Promote raw developer names to canonical where the raw name IS a valid company name.

When Haiku returns the same name (can't identify parent), the canonical stays null.
But many raw names like "Alpine Property Group" are real companies — use them directly.

Strips legal suffixes (LLC, Inc., etc.) before storing as canonical.
"""

import re
import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import init_db, get_session
from db.models import Project
from scraper.normalize_developer import is_real_company, _SUFFIX_RE

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

_BAD_TARGETS = {"", "Unknown", "UNKNOWN", "Unknown - review needed"}


def clean_name(raw: str) -> str:
    """Strip legal suffixes and normalize whitespace."""
    n = raw.strip()
    n = re.sub(r'\s+and\s+(its\s+)?affiliates?\.?$', '', n, flags=re.I).strip()
    n = _SUFFIX_RE.sub('', n).strip()
    n = _SUFFIX_RE.sub('', n).strip()  # second pass
    n = re.sub(r',\s*$', '', n).strip()
    return n if n else raw.strip()


def run():
    init_db()
    session = get_session()

    try:
        targets = session.query(Project).filter(
            (Project.developer_canonical == None) |
            (Project.developer_canonical == '') |
            (Project.developer_canonical.in_(_BAD_TARGETS))
        ).all()

        log.info("Projects with null/bad canonical: %d", len(targets))

        promoted = 0
        skipped_no_raw = 0
        skipped_not_real = 0

        for p in targets:
            raw = (p.developer or "").strip()
            if not raw:
                skipped_no_raw += 1
                continue

            if not is_real_company(raw):
                skipped_not_real += 1
                continue

            canonical = clean_name(raw)
            if not canonical or not is_real_company(canonical):
                skipped_not_real += 1
                continue

            p.developer_canonical = canonical
            promoted += 1
            log.info("  [%d] %-50s  ->  %s", p.id, raw[:50], canonical)

        session.commit()
        log.info(
            "\n=== Promote complete ===\n"
            "  Promoted:          %d\n"
            "  Skipped (no raw):  %d\n"
            "  Skipped (not real): %d\n",
            promoted, skipped_no_raw, skipped_not_real,
        )

        total = session.query(Project).count()
        with_canonical = len([
            p for p in session.query(Project).all()
            if p.developer_canonical and is_real_company(p.developer_canonical)
        ])
        log.info("Canonical developer coverage: %d / %d (%.0f%%)",
                 with_canonical, total, 100 * with_canonical / total)

    finally:
        session.close()


if __name__ == "__main__":
    run()
