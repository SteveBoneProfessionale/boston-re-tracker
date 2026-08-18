r"""
Apply developer resolution to ingested Rhode Island projects.

Runs the registry tier over every project's applicant entity and writes the
resolved sponsor, the method, and the evidence. Tier 2 (web corroboration) is
applied separately from its own reviewed cache, so nothing inferred is written
without having been looked at.

Writes:
  developer                    resolved sponsor, or left null
  developer_canonical          same, for the charts
  developer_resolution_method  registry_confirmed | web_corroborated |
                               web_low_confidence | null
  developer_sources            JSON list of evidence

A project whose applicant cannot be resolved keeps a null developer. That is
the correct outcome, not a gap to be filled with the applicant's own shell name
-- writing "180 Weeden St LLC" into the developer column is exactly the
contamination this pipeline exists to prevent.
"""

import sys
import json
import logging
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import init_db, get_session
from db.models import Project
from scraper.ri_corp_registry import (
    resolve, load_cache, save_cache, HEADERS, is_single_purpose_shell, display_name,
)
from scraper.ri_shell import shell_verdict, RULE_NOTE

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

WEB_CACHE = Path(__file__).parent.parent / "data" / "ri_developer_web.json"
# Reviewed tier-2 results. Keyed by applicant name, upper-cased. Populated from
# the audited sample rather than written blind by the resolver.
REVIEWED = Path(__file__).parent.parent / "data" / "ri_developer_reviewed.json"


def load_reviewed() -> dict:
    if REVIEWED.exists():
        try:
            return {k.upper(): v for k, v in
                    json.loads(REVIEWED.read_text(encoding="utf-8")).items()}
        except json.JSONDecodeError:
            log.warning("reviewed developer file unreadable — ignoring")
    return {}


def run(dry_run: bool = False) -> dict:
    init_db()
    reviewed = load_reviewed()
    cache = load_cache()
    session = get_session()
    stats = {"registry_confirmed": 0, "registry_self": 0, "web_corroborated": 0,
             "web_low_confidence": 0, "shell_to_web": 0, "null": 0, "no_applicant": 0}
    shell_queue: list[tuple] = []
    try:
        projects = (session.query(Project)
                    .filter(Project.bpda_url.like("manual:ri-%")).all())
        log.info("Rhode Island projects: %d", len(projects))

        with httpx.Client(headers=HEADERS, follow_redirects=True) as client:
            for i, p in enumerate(projects, 1):
                applicant = (p.applicant_entity or "").strip()
                if not applicant:
                    stats["no_applicant"] += 1
                    continue

                # Reviewed web result wins: it was looked at by a human.
                rv = reviewed.get(applicant.upper())
                if rv and rv.get("developer"):
                    if not dry_run:
                        p.developer = rv["developer"]
                        p.developer_canonical = rv["developer"]
                        p.developer_resolution_method = rv.get(
                            "method", "web_corroborated")
                        p.developer_sources = json.dumps(rv.get("sources", []))
                    stats[rv.get("method", "web_corroborated")] += 1
                    continue

                # A single-purpose entity never populates the developer column
                # from the registry, however the registry describes it. It
                # routes to the web tier instead: registry first, web second,
                # null third, confidence rules unchanged.
                is_shell, shell_rule = shell_verdict(applicant, p.address)
                if is_shell:
                    if not dry_run and p.developer:
                        p.developer = None
                        p.developer_canonical = None
                        p.developer_resolution_method = None
                        p.developer_sources = None
                    stats["shell_to_web"] += 1
                    shell_queue.append((p.city, applicant, p.address, shell_rule))
                    continue

                rec = resolve(client, applicant, cache)
                dev = rec.get("developer")
                if dev:
                    # Only an address-cluster finding is "registry_confirmed".
                    # The self path merely confirms the applicant exists and is
                    # not shell-shaped; labelling it confirmed would claim
                    # cluster evidence that was never gathered.
                    method = ("registry_self" if rec.get("confidence") == "self"
                              else "registry_confirmed")
                    if not dry_run:
                        p.developer = dev
                        p.developer_canonical = dev
                        p.developer_resolution_method = method
                        ev = rec.get("evidence", {}).get("entity", {})
                        p.developer_sources = json.dumps([{
                            "publisher": "RI Corporate Database",
                            "url": ("https://business.sos.ri.gov/CorpWeb/CorpSearch/"
                                    f"CorpSummary.aspx?FEIN={ev.get('id','')}&SEARCH_TYPE=1"),
                            "address_sentence": ev.get("address", ""),
                            "developer_sentence": rec.get("reason", ""),
                        }])
                    stats[method] += 1
                else:
                    # A null verdict must CLEAR any previously written name.
                    # Leaving the old value in place would silently preserve a
                    # resolution this run has just decided is unsupported.
                    if not dry_run and p.developer:
                        p.developer = None
                        p.developer_canonical = None
                        p.developer_resolution_method = None
                        p.developer_sources = None
                    stats["null"] += 1

                if i % 25 == 0:
                    if not dry_run:
                        session.commit()
                    save_cache(cache)
                    log.info("  %d/%d  %s", i, len(projects),
                             {k: v for k, v in stats.items() if v})
        if not dry_run:
            session.commit()
    finally:
        save_cache(cache)
        session.close()

    log.info("\n=== Developer resolution applied ===")
    total = sum(stats.values())
    for k, v in stats.items():
        log.info("  %-20s %4d  (%.0f%%)", k, v, 100 * v / total if total else 0)
    return stats


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    run(ap.parse_args().dry_run)
