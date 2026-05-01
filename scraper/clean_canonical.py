"""
Comprehensive cleanup of developer_canonical values in the database.

Steps:
  1. Delete AI-failure and address-LLC entries from developer_cache
  2. Set developer_canonical = NULL on projects that have bad canonical values
     (NULL means: unknown/unresolved — shown as raw LLC in detail card, excluded from dropdown)
  3. Re-run rules against all projects with NULL canonical (may now resolve with new rules)
  4. Deduplicate canonical name variants
     e.g. "Cedarwood Development LLC" → "Cedarwood Development"
  5. Report final dropdown-eligible company list
"""

import re
import sys
import logging
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import init_db, get_session
from db.models import Project, DeveloperCache
from scraper.normalize_developer import (
    is_real_company, suffix_stripped, _rule_match, RULES
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def run():
    init_db()
    s = get_session()

    try:
        # ── Step 1: Wipe bad cache entries ────────────────────────────────
        all_cache = s.query(DeveloperCache).all()
        wiped = 0
        for c in all_cache:
            if not is_real_company(c.canonical_name):
                s.delete(c)
                wiped += 1
        s.commit()
        log.info("Wiped %d bad cache entries", wiped)

        # ── Step 2: Null out bad developer_canonical on projects ──────────
        nulled = 0
        for p in s.query(Project).filter(Project.developer_canonical.isnot(None)).all():
            if not is_real_company(p.developer_canonical):
                p.developer_canonical = None
                nulled += 1
        s.commit()
        log.info("Nulled %d projects with bad canonical values", nulled)

        # ── Step 3: Re-run rules for ALL projects with a developer ────────
        # Catches: (a) newly-null projects, (b) projects where canonical == raw
        # (no previous rule match), now covered by newly-added rules.
        resolved = 0
        for p in s.query(Project).filter(
            Project.developer.isnot(None),
            Project.developer != "",
        ).all():
            raw = (p.developer or "").strip()
            if not raw or raw == "Unknown - review needed":
                continue
            rule_canonical = _rule_match(raw)
            if rule_canonical and is_real_company(rule_canonical):
                if p.developer_canonical != rule_canonical:
                    p.developer_canonical = rule_canonical
                    resolved += 1
        s.commit()
        log.info("Re-resolved/updated %d projects via rules", resolved)

        # ── Step 4: Deduplicate canonical variants ────────────────────────
        # Group all canonical values by their suffix-stripped key
        all_projects = s.query(Project).filter(
            Project.developer_canonical.isnot(None),
            Project.developer_canonical != "",
        ).all()

        # Build groups: stripped_key -> set of canonical forms seen
        groups: dict[str, set] = defaultdict(set)
        for p in all_projects:
            key = suffix_stripped(p.developer_canonical)
            groups[key].add(p.developer_canonical)

        # For each group with >1 variant, pick the canonical form
        # Preference: shortest version without LLC/Inc/Trust suffix
        replacements: dict[str, str] = {}
        for key, variants in groups.items():
            if len(variants) <= 1:
                continue
            # Pick the form that is shortest after stripping suffix
            def score(v):
                stripped = re.sub(
                    r',?\s*(LLC|L\.L\.C\.|Inc\.?|Corp\.?|Ltd\.?|LLP|Trust)$',
                    '', v.strip(), flags=re.I
                ).strip()
                return (len(stripped), v)
            best = min(variants, key=score)
            for variant in variants:
                if variant != best:
                    replacements[variant] = best
                    log.info("  Dedup: '%s'  ->  '%s'", variant, best)

        deduped = 0
        for p in all_projects:
            if p.developer_canonical in replacements:
                p.developer_canonical = replacements[p.developer_canonical]
                deduped += 1
        s.commit()
        log.info("Deduped %d project canonical values", deduped)

        # Update cache entries for deduped names
        for old, new in replacements.items():
            existing = s.query(DeveloperCache).filter_by(canonical_name=old).first()
            if existing:
                existing.canonical_name = new
        s.commit()

        # ── Step 5: Report final company list ─────────────────────────────
        from collections import Counter
        all_canonical = [
            p.developer_canonical for p in s.query(Project).all()
            if p.developer_canonical and is_real_company(p.developer_canonical)
        ]
        counts = Counter(all_canonical).most_common()
        real = sorted(set(all_canonical))

        log.info("\n=== Dropdown-eligible companies (%d unique) ===", len(real))
        for name, count in sorted(counts, key=lambda x: (-x[1], x[0])):
            log.info("  %3d  %s", count, name)

        # Summary counts
        total = s.query(Project).count()
        has_real = s.query(Project).filter(
            Project.developer_canonical.isnot(None)
        ).count()
        log.info(
            "\nProjects with dropdown-eligible canonical name: %d / %d",
            len(all_canonical), total
        )

    finally:
        s.close()


if __name__ == "__main__":
    run()
