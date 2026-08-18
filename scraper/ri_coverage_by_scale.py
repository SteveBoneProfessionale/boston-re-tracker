r"""
Field coverage on the scoped Rhode Island set, broken out by review scale.

The question this answers is not "how complete is the tracker" but "is it
complete on the projects a CRE professional would actually discuss". RIGL
45-23 sorts filings into Major, Minor and Administrative land development, and
those tiers are a reasonable proxy for that: a Major Land Development Project
is a real scheme, an Administrative one rarely is.

Coverage is reported per tier so the two questions stay separate. A tracker
that is 90% complete on Major and 20% complete on Administrative is a good
tracker; the blended average would hide that.

    python scraper/ri_coverage_by_scale.py
"""

import sys
import json
import logging
from pathlib import Path
from collections import Counter

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import get_session
from db.models import Project

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

RI = ("Providence", "Warwick", "Cranston", "Pawtucket", "Newport")
OUT = Path(__file__).parent.parent / "data" / "ri_coverage_by_scale.json"

FIELDS = [
    ("developer", "developer"),
    ("residential_units", "units"),
    ("total_gsf", "square footage"),
    ("asset_class", "asset class"),
    ("stage_heard", "stage"),
    ("latitude", "geocode"),
    ("applicant_entity", "applicant"),
    ("case_number", "case number"),
    ("zoning_district_raw", "zoning"),
    ("site_acreage", "site acreage"),
    ("parking_spaces", "parking"),
    ("num_stories", "storeys"),
]

# Five buckets, not four. "Not applicable" is a different fact from
# "unknown": a zoning-board variance has no RIGL 45-23 scale to be
# missing, and lumping it in with genuine gaps made the tracker look far
# less complete than it is.
TIERS = ["Major", "Minor", "Administrative", "n/a (not 45-23)", "unknown"]


def main():
    session = get_session()
    rows = [p for p in session.query(Project).filter(Project.city.in_(RI)).all()
            if not p.excluded]

    def tier(p):
        if p.review_scale in TIERS[:3]:
            return p.review_scale
        return ("n/a (not 45-23)" if p.review_scale_basis == "not_applicable"
                else "unknown")

    buckets = {t: [p for p in rows if tier(p) == t] for t in TIERS}

    log.info("\nSCOPED RHODE ISLAND SET: %d projects", len(rows))
    log.info("  %-16s %6s   %s", "REVIEW SCALE", "N", "share")
    for t in TIERS:
        n = len(buckets[t])
        log.info("  %-16s %6d   %4.0f%%", t, n, 100 * n / len(rows) if rows else 0)

    ent = Counter(p.entry_type or "(unset)" for p in rows)
    log.info("\n  entry type: %s", dict(ent))

    log.info("\nFIELD COVERAGE BY REVIEW SCALE")
    log.info("  %-18s %8s %8s %8s %8s %8s %8s",
             "FIELD", "Major", "Minor", "Admin", "n/a", "unknown", "ALL")
    report = {}
    for col, label in FIELDS:
        line = []
        for t in TIERS:
            b = buckets[t]
            n = sum(1 for p in b if getattr(p, col, None) not in (None, "", False))
            line.append(100 * n / len(b) if b else 0)
        allpct = 100 * sum(1 for p in rows
                           if getattr(p, col, None) not in (None, "", False)) / len(rows)
        log.info("  %-18s %7.0f%% %7.0f%% %7.0f%% %7.0f%% %7.0f%% %7.0f%%",
                 label, line[0], line[1], line[2], line[3], line[4], allpct)
        report[col] = {"by_tier": dict(zip(TIERS, [round(x) for x in line])),
                       "all": round(allpct)}

    # The headline: how complete is the tracker on the projects that matter.
    major = buckets["Major"]
    if major:
        log.info("\n  MAJOR LAND DEVELOPMENT ONLY (%d projects) -- the schemes a CRE "
                 "professional would discuss:", len(major))
        for col, label in FIELDS[:5]:
            n = sum(1 for p in major if getattr(p, col, None) not in (None, "", False))
            log.info("    %-18s %3d/%d  %3.0f%%", label, n, len(major), 100 * n / len(major))

    OUT.write_text(json.dumps(report, indent=1), encoding="utf-8")
    log.info("\nWrote %s", OUT)
    session.close()


if __name__ == "__main__":
    main()
