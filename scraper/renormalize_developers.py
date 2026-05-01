"""
Full developer re-normalization pass.

For every raw developer name that still looks like an LLC / shell company:
  1. Clear its cache entry so we can re-resolve with the better prompt
  2. Apply updated rules (including parenthetical extraction)
  3. Send remaining names to Claude Haiku with the specific Boston RE prompt
  4. UNKNOWN responses -> set canonical to None (excluded from dropdown)
  5. Update all project records

Safe to re-run — only re-processes names that look unresolved.
"""

import re
import sys
import time
import logging
from pathlib import Path

import anthropic

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import init_db, get_session
from db.models import Project, DeveloperCache
from scraper.normalize_developer import (
    _rule_match, is_real_company, suffix_stripped,
    _store_cache, _ADDRESS_LLC,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# The exact prompt the user specified
HAIKU_PROMPT = """\
This is the legal entity name from a Boston real estate development filing. \
What is the well-known parent development company behind this entity? \
Common Boston developers include HYM Investment Group, Marcus Partners, \
The Davis Companies, WS Development, The Fallon Company, Related Beal, \
National Development, Samuels and Associates, BioMed Realty, Oxford Properties, \
CIM Group, The Abbey Group, Skanska, Cabot Cabot and Forbes, Accordia Development, \
HRP, Carr Properties, Lendlease, Greystar, and others. \
Return only the canonical company name. \
If it is a joint venture between two real companies, return both separated by a slash. \
If you genuinely cannot identify it, return UNKNOWN."""

# Patterns that indicate a name still needs resolution
_LLC_RE = re.compile(
    r'\b(LLC|L\.L\.C\.|Propco|Owner LLC|Holdings LLC|Acquisitions? LLC'
    r'|Associates LLC|Partners LLC|Ventures? LLC|Realty LLC|Trust)\b',
    re.I,
)

_BAD_PHRASES = (
    "i don't", "i cannot", "i can't", "without access",
    "cannot identify", "unable to", "not able to",
)


def _needs_resolution(raw: str, canonical: str | None) -> bool:
    """True if this developer name should be sent to Haiku."""
    if not raw or raw.strip() == "Unknown - review needed":
        return False
    # If canonical is a real resolved company, skip
    if canonical and is_real_company(canonical) and canonical != raw:
        return False
    # If raw starts with digit (address LLC), Haiku can't help — but user wants us to try
    # Only skip if canonical is already good
    return True


def _haiku_lookup(raw_name: str, client: anthropic.Anthropic) -> str | None:
    """Call Haiku and return canonical name, or None for UNKNOWN/failures."""
    content = f"Legal entity name: {raw_name}\n\n{HAIKU_PROMPT}"
    for attempt in range(3):
        try:
            resp = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=80,
                messages=[{"role": "user", "content": content}],
            )
            result = resp.content[0].text.strip().strip('"').strip("'")

            # Reject failures / disclaimers
            if any(p in result.lower() for p in _BAD_PHRASES):
                return None
            if len(result) > 100:
                return None
            if result.upper() == "UNKNOWN" or result.lower() == "unknown":
                return None
            return result

        except anthropic.RateLimitError:
            wait = 20 * (attempt + 1)
            log.warning("  Rate limit — sleeping %ds", wait)
            time.sleep(wait)
        except Exception as exc:
            log.warning("  API error (attempt %d): %s", attempt + 1, exc)
            time.sleep(5)
    return None


def run():
    init_db()
    session = get_session()
    client = anthropic.Anthropic()

    try:
        # Collect all unique raw names that need work
        all_projects = session.query(Project).filter(
            Project.developer.isnot(None),
            Project.developer != "",
        ).all()

        # Group projects by raw developer name
        by_raw: dict[str, list[Project]] = {}
        for p in all_projects:
            raw = (p.developer or "").strip()
            by_raw.setdefault(raw, []).append(p)

        # Decide which raw names to process
        to_process = {
            raw for raw, projs in by_raw.items()
            if _needs_resolution(raw, projs[0].developer_canonical)
        }
        log.info("Unique developer names to process: %d", len(to_process))

        # Step 1: Clear stale cache entries for names we're re-processing
        cleared = 0
        for raw in to_process:
            cached = session.query(DeveloperCache).filter_by(raw_name=raw).first()
            if cached:
                session.delete(cached)
                cleared += 1
        session.commit()
        log.info("Cleared %d stale cache entries", cleared)

        # Step 2: Resolve each name
        resolved_rule = 0
        resolved_haiku = 0
        unknown_count = 0
        results: dict[str, str | None] = {}

        sorted_names = sorted(to_process)
        for i, raw in enumerate(sorted_names, 1):
            # Try rules first (includes parenthetical extraction)
            canonical = _rule_match(raw)
            if canonical and is_real_company(canonical):
                results[raw] = canonical
                _store_cache(session, raw, canonical, "rules")
                resolved_rule += 1
                log.info("[%d/%d] RULE  %-55s -> %s", i, len(sorted_names), raw[:55], canonical)
                continue

            # Haiku
            log.info("[%d/%d]        %-55s ...", i, len(sorted_names), raw[:55])
            canonical = _haiku_lookup(raw, client)

            if canonical and is_real_company(canonical):
                results[raw] = canonical
                _store_cache(session, raw, canonical, "ai")
                log.info("  -> %s", canonical)
                resolved_haiku += 1
            else:
                results[raw] = None
                log.info("  -> UNKNOWN")
                unknown_count += 1
                _store_cache(session, raw, raw, "ai")  # cache as-is to avoid re-hitting

            time.sleep(0.4)

        # Step 3: Update all projects
        updated = 0
        for raw, canonical in results.items():
            for proj in by_raw.get(raw, []):
                new_val = canonical if (canonical and is_real_company(canonical)) else None
                if proj.developer_canonical != new_val:
                    proj.developer_canonical = new_val
                    updated += 1
        session.commit()

        log.info(
            "\n=== Re-normalization complete ===\n"
            "  Resolved via rules: %d\n"
            "  Resolved via Haiku: %d\n"
            "  UNKNOWN:            %d\n"
            "  Projects updated:   %d\n",
            resolved_rule, resolved_haiku, unknown_count, updated,
        )

        # Show final dropdown list
        from collections import Counter
        all_canonical = [
            p.developer_canonical for p in session.query(Project).all()
            if p.developer_canonical and is_real_company(p.developer_canonical)
        ]
        counts = Counter(all_canonical).most_common(30)
        log.info("Top developers in dropdown:")
        for name, count in counts:
            log.info("  %3d  %s", count, name)

    finally:
        session.close()


if __name__ == "__main__":
    run()
