r"""
The Rhode Island reporting block from the original plan.

Everything here reports the ACTIVE pipeline -- quarantined rows are excluded,
the same way every chart sees them -- and says so, with the excluded count
alongside rather than hidden.

Sections:
  1  projects per municipality, with the raw agenda item count behind them
  2  pipeline SF and residential units per municipality
  3  stage distribution
  4  unresolved developers
  5  failed geocodes
  6  a 20-row deduplication sample
  7  every field under 70% coverage

    python scraper/ri_pipeline_report.py
"""

import sys
import json
import logging
from pathlib import Path
from collections import Counter, defaultdict

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import get_session
from db.models import Project, ProjectStageEvent
from scraper.ri_ingest_llm import load_items

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

RI = ("Providence", "Warwick", "Cranston", "Pawtucket", "Newport")
OUT = Path(__file__).parent.parent / "data" / "ri_pipeline_report.json"

# Fields worth measuring. Identity and bookkeeping columns are left out --
# reporting 100% coverage on `id` tells nobody anything.
FIELDS = [
    "address", "description", "latitude", "applicant_entity", "developer",
    "asset_class", "stage_heard", "stage_confirmed", "case_number",
    "zoning_district_raw", "residential_units", "total_gsf", "commercial_gsf",
    "site_acreage", "parking_spaces", "num_stories", "building_height_ft",
    "building_count", "assessor_plat", "assessor_lots", "review_scale",
    "adaptive_reuse", "expected_delivery", "architect", "neighborhood",
    "owner_or_agency",
]


def _fmt(n):
    return "—" if n in (None, 0) else f"{int(n):,}"


def main():
    session = get_session()
    active = (session.query(Project).filter(Project.city.in_(RI))
              .filter((Project.excluded.is_(None)) | (Project.excluded == False))  # noqa: E712
              .all())
    excluded = (session.query(Project).filter(Project.city.in_(RI))
                .filter(Project.excluded == True).all())  # noqa: E712

    raw_items = Counter(i.get("municipality") for i in load_items()
                        if i.get("municipality") in RI)
    events = Counter()
    for pid, in session.query(ProjectStageEvent.project_id).all():
        events[pid] += 1

    report = {}

    # 1 ── projects per municipality, against the raw corpus behind them
    log.info("\n1  PROJECTS PER MUNICIPALITY")
    log.info("   %-12s %8s %8s %10s %10s", "CITY", "ACTIVE", "EXCL", "RAW ITEMS", "COLLAPSE")
    per_city = Counter(p.city for p in active)
    per_city_ex = Counter(p.city for p in excluded)
    rows = []
    for c in RI:
        raw = raw_items.get(c, 0)
        act = per_city.get(c, 0)
        collapse = f"{(1 - act / raw) * 100:.0f}%" if raw else "—"
        log.info("   %-12s %8d %8d %10d %10s", c, act, per_city_ex.get(c, 0), raw, collapse)
        rows.append({"city": c, "active": act, "excluded": per_city_ex.get(c, 0),
                     "raw_items": raw, "collapse": collapse})
    log.info("   %-12s %8d %8d %10d", "TOTAL", len(active), len(excluded), sum(raw_items.values()))
    report["per_municipality"] = rows

    # 2 ── pipeline size
    log.info("\n2  PIPELINE SF AND RESIDENTIAL UNITS PER MUNICIPALITY")
    log.info("   %-12s %12s %10s %10s %10s", "CITY", "TOTAL SF", "W/ SF", "UNITS", "W/ UNITS")
    size = []
    for c in RI:
        ps = [p for p in active if p.city == c]
        sf = sum(p.total_gsf or 0 for p in ps)
        un = sum(p.residential_units or 0 for p in ps)
        nsf = sum(1 for p in ps if p.total_gsf)
        nun = sum(1 for p in ps if p.residential_units)
        log.info("   %-12s %12s %10d %10s %10d", c, _fmt(sf), nsf, _fmt(un), nun)
        size.append({"city": c, "total_sf": sf, "with_sf": nsf,
                     "units": un, "with_units": nun})
    log.info("   %-12s %12s %10d %10s %10d", "TOTAL",
             _fmt(sum(s["total_sf"] for s in size)), sum(s["with_sf"] for s in size),
             _fmt(sum(s["units"] for s in size)), sum(s["with_units"] for s in size))
    log.info("   NOTE: only %d of %d active projects state any SF at all.",
             sum(s["with_sf"] for s in size), len(active))
    report["pipeline_size"] = size

    # 3 ── stage distribution
    log.info("\n3  STAGE DISTRIBUTION (stage_heard; stage_confirmed is 0% -- minutes "
             "never report groundbreaking or occupancy)")
    st = Counter(p.stage_heard or "(none)" for p in active)
    for k, n in st.most_common():
        log.info("   %-34s %4d  %5.1f%%", k, n, 100 * n / len(active))
    report["stages"] = dict(st)

    # 4 ── unresolved developers
    log.info("\n4  UNRESOLVED DEVELOPERS")
    unres = [p for p in active if not (p.developer or "").strip()]
    log.info("   %d of %d active projects have no developer (%.0f%% resolved)",
             len(unres), len(active), 100 * (1 - len(unres) / len(active)))
    by_city = Counter(p.city for p in unres)
    for c in RI:
        tot = per_city.get(c, 0)
        log.info("   %-12s %4d unresolved of %4d", c, by_city.get(c, 0), tot)
    conf = Counter(p.developer_resolution_method or "(none)"
                   for p in active if (p.developer or "").strip())
    log.info("   resolved by: %s", dict(conf.most_common()))
    report["unresolved_developers"] = {"count": len(unres), "by_city": dict(by_city),
                                       "methods": dict(conf)}

    # 5 ── failed geocodes
    log.info("\n5  FAILED GEOCODES")
    nogeo = [p for p in active if p.latitude is None or p.longitude is None]
    log.info("   %d of %d active projects have no coordinates (%.0f%% geocoded)",
             len(nogeo), len(active), 100 * (1 - len(nogeo) / len(active)))
    for p in nogeo:
        log.info("     id=%-4d %-11s %s", p.id, p.city, (p.address or "(blank address)")[:52])
    approx = sum(1 for p in active if p.coords_approximate)
    log.info("   %d more are geocoded only approximately.", approx)
    report["failed_geocodes"] = [{"id": p.id, "city": p.city, "address": p.address}
                                 for p in nogeo]

    # 6 ── dedup sample
    log.info("\n6  DEDUPLICATION SAMPLE (20 rows, most-collapsed first)")
    log.info("   %-5s %-11s %-30s %6s  %s", "ID", "CITY", "ADDRESS", "ITEMS", "PLAT/LOTS")
    ranked = sorted(active, key=lambda p: events.get(p.id, 0), reverse=True)[:20]
    sample = []
    for p in ranked:
        log.info("   %-5d %-11s %-30s %6d  %s", p.id, p.city or "",
                 (p.address or "(blank)")[:30], events.get(p.id, 0),
                 f"{p.assessor_plat or '—'}/{(p.assessor_lots or '—')[:18]}")
        sample.append({"id": p.id, "city": p.city, "address": p.address,
                       "appearances": events.get(p.id, 0),
                       "plat": p.assessor_plat, "lots": p.assessor_lots})
    report["dedupe_sample"] = sample

    # 7 ── fields under 70%
    log.info("\n7  FIELDS UNDER 70%% COVERAGE (of %d active projects)", len(active))
    cov = []
    for f in FIELDS:
        n = sum(1 for p in active
                if getattr(p, f, None) not in (None, "", False))
        cov.append((f, n, 100 * n / len(active)))
    under = [c for c in sorted(cov, key=lambda x: x[2]) if c[2] < 70]
    for f, n, pct in under:
        log.info("   %-22s %4d  %5.1f%%", f, n, pct)
    log.info("   (%d of %d measured fields are at or above 70%%)",
             len(cov) - len(under), len(cov))
    report["field_coverage"] = [{"field": f, "n": n, "pct": round(pct, 1)} for f, n, pct in cov]

    session.close()
    OUT.write_text(json.dumps(report, indent=1, ensure_ascii=False), encoding="utf-8")
    log.info("\nWrote %s", OUT)


if __name__ == "__main__":
    main()
