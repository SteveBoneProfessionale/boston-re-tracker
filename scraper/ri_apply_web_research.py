r"""
Apply web-researched values to Rhode Island projects.

Rules enforced in code, not just in the data file:

  * FILL NULLS ONLY. A field the agenda already states is never overwritten.
    The filing is the primary source; the web tier exists for what the filing
    does not say. A conflict is NOT resolved here -- it is reported.
  * Developers need two independent registrable domains, each with a sentence
    establishing THIS address, or they are marked web_low_confidence.
  * Every value written carries a source URL, and the record is marked so the
    value renders distinctly from an agenda-extracted one.

Conflicts (source disagrees with the filing) are collected and returned for
the cleanup report rather than written.

ORDER MATTERS: run this AFTER scraper/ri_apply_developers.py. The registry
pass clears a developer whose applicant it cannot resolve, so running it second
would wipe every web-researched name written here.

    python scraper/ri_apply_web_research.py --dry-run
"""

import sys
import json
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import init_db, get_session
from db.models import Project
from scraper.ri_developer_web import registrable_domain

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

RESEARCH = Path(__file__).parent.parent / "data" / "ri_web_research.json"

# Fields the web tier may fill. Everything else is agenda-only.
FILLABLE = {"residential_units", "total_gsf", "num_stories", "parking_spaces",
            "site_acreage", "asset_class", "zoning_district_raw"}


def load() -> dict:
    if not RESEARCH.exists():
        return {}
    return json.loads(RESEARCH.read_text(encoding="utf-8"))


def independent(sources: list) -> bool:
    """Two or more distinct registrable domains."""
    doms = {registrable_domain(s.get("url", "")) for s in sources or []}
    doms.discard(None)
    doms.discard("")
    return len(doms) >= 2


def run(dry_run: bool = False) -> dict:
    init_db()
    data = load()
    session = get_session()
    stats = {"dev_set": 0, "dev_downgraded": 0, "fields_set": 0, "applicant_set": 0,
             "conflicts": 0, "not_found": 0}
    conflicts = []
    try:
        by_key = {}
        for p in session.query(Project).filter(
                Project.bpda_url.like("manual:ri-%")).all():
            by_key[f"{p.city}|{(p.address or '').strip()}"] = p

        for key, rec in data.items():
            if key.startswith("_"):
                continue
            p = by_key.get(key)
            if p is None:
                stats["not_found"] += 1
                log.warning("  no project matches %s", key)
                continue

            # ── developer ──
            dev = rec.get("developer")
            if dev:
                method = rec.get("method", "web_corroborated")
                if method == "web_corroborated" and not independent(rec.get("sources")):
                    method = "web_low_confidence"
                    stats["dev_downgraded"] += 1
                    log.warning("  %s: downgraded to web_low_confidence "
                                "(fewer than 2 independent domains)", key)
                if not dry_run:
                    p.developer = dev
                    p.developer_canonical = dev
                    p.developer_resolution_method = method
                    p.developer_sources = json.dumps(rec.get("sources", []))
                stats["dev_set"] += 1

            # ── applicant recovered from the web ──
            # Only when the agenda named nobody. A web-sourced applicant is
            # still a null-fill, and it is marked so it never reads as though
            # the filing stated it.
            appl = rec.get("applicant_entity")
            if appl and not (p.applicant_entity or "").strip():
                if not dry_run:
                    p.applicant_entity = appl
                    p.applicant_source = "web"
                stats["applicant_set"] += 1

            # ── other fields, nulls only ──
            for f, v in (rec.get("fields") or {}).items():
                if f not in FILLABLE:
                    log.warning("  %s: field %s is agenda-only, skipped", key, f)
                    continue
                cur = getattr(p, f, None)
                if cur not in (None, "", False):
                    if str(cur) != str(v):
                        conflicts.append({"project": key, "field": f,
                                          "stored": cur, "source_says": v,
                                          "url": (rec.get("sources") or [{}])[0].get("url", "")})
                        stats["conflicts"] += 1
                    continue                      # never overwrite the filing
                if not dry_run:
                    setattr(p, f, v)
                stats["fields_set"] += 1

        if not dry_run:
            session.commit()
    finally:
        session.close()

    log.info("\n=== Web research applied ===")
    for k, v in stats.items():
        log.info("  %-16s %3d", k, v)
    if conflicts:
        log.info("\n  CONFLICTS (not written, for the cleanup report):")
        for c in conflicts:
            log.info("    %s %s: stored=%s source=%s", c["project"], c["field"],
                     c["stored"], c["source_says"])
    return {"stats": stats, "conflicts": conflicts}


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    run(ap.parse_args().dry_run)
