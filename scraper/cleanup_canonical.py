"""
Post-promotion cleanup — safe version:
  1. Re-apply rules so rule-matched projects keep their proper canonical
  2. Fix ALL CAPS canonical names → Title Case
  3. Restore Realty Trust names that were incorrectly stripped

Never overwrites a rule-matched canonical with a raw name.
"""

import re
import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import init_db, get_session
from db.models import Project
from scraper.normalize_developer import is_real_company, _rule_match, _SUFFIX_RE

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

_BAD_TARGETS = {"", "Unknown", "UNKNOWN", "Unknown - review needed"}

# Trust-type suffixes we should NOT strip from canonical names
_TRUST_RE = re.compile(r'\bTrust\b', re.IGNORECASE)


def to_title(name: str) -> str:
    """Convert ALL CAPS names to Title Case. Mixed-case names are unchanged."""
    stripped = name.strip()
    if stripped == stripped.upper() and len(stripped) > 3:
        return stripped.title()
    return stripped


def run():
    init_db()
    session = get_session()
    try:
        all_projects = session.query(Project).filter(
            Project.developer.isnot(None),
            Project.developer != '',
        ).all()

        updated = 0

        for p in all_projects:
            raw = (p.developer or "").strip()
            current = (p.developer_canonical or "").strip()
            new_canonical = None

            # Step 1: re-apply rules — always wins if there's a match
            rule = _rule_match(raw)
            if rule and is_real_company(rule):
                new_canonical = rule

            # Step 2: if no rule, and current canonical looks like a promoted raw name —
            # just apply case normalization and Trust restoration
            if new_canonical is None and current and current not in _BAD_TARGETS:
                candidate = to_title(current)

                # Restore Realty Trust if raw has it but canonical lost it
                if (_TRUST_RE.search(raw) and not _TRUST_RE.search(candidate)):
                    # Find how raw ends with Trust and restore
                    m = re.search(r'(\w.*\bTrust)\b', raw, re.IGNORECASE)
                    if m:
                        candidate = to_title(m.group(1).strip())

                if is_real_company(candidate):
                    new_canonical = candidate

            if new_canonical and new_canonical != p.developer_canonical:
                log.info("  [%4d] %-50s  was: %-40s  -> %s",
                         p.id, raw[:50], (p.developer_canonical or "")[:40], new_canonical)
                p.developer_canonical = new_canonical
                updated += 1

        session.commit()
        log.info("Updated %d canonical names", updated)

        total = session.query(Project).count()
        with_canonical = len([
            p for p in session.query(Project).all()
            if p.developer_canonical and is_real_company(p.developer_canonical)
        ])
        log.info("Coverage: %d / %d (%.0f%%)", with_canonical, total,
                 100 * with_canonical / total)

    finally:
        session.close()


if __name__ == "__main__":
    run()
