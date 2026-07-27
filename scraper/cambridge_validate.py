"""
Validation harness for the Cambridge Development Log ingestion.

Hard assertions against the live-verified "2026 Q1" current edition
(data.cambridgema.gov dataset wjwg-93qh, checked 2026-07-23). Fails loudly
(non-zero exit + diff) rather than warning quietly -- a wrong GFA on a
Kendall Square building is worse than no Cambridge tab at all.

Note: the total-GFA target here is 16,941,843, which is the live-verified
sum of `total_gfa` across all 67 current-edition rows, and also exactly
matches summing the per-status subtotals below. This does NOT match a
"12,641,651" figure floated during planning, which never reconciled with
either the source data or its own per-status breakdown -- see
cambridge_devlog.py's module docstring for the full discrepancy note.

Run after scraper/cambridge_devlog.py:
    python scraper/cambridge_validate.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import get_session
from db.models import Project, CambridgeSpecialPermit, CambridgeBuildingPermit

EXPECTED_COUNT = 67

EXPECTED_STATUS_COUNTS = {
    "Pre-Permitting": 11,
    "Permitting": 3,
    "Approved PUD/Master Plan Development Remaining": 3,
    "Design Review": 1,
    "Zoning Permit Granted or As of Right": 19,
    "Building Permit Granted": 29,
    "Complete": 1,
}

EXPECTED_TOTAL_GFA = 16_941_843
EXPECTED_RESIDENTIAL_UNITS = 7_212
EXPECTED_PARKING_SPACES = 11_465
EXPECTED_HOTEL_ROOMS = 235

EXPECTED_STATUS_GFA = {
    "Pre-Permitting": 4_985_847,
    "Permitting": 43_433,
    "Approved PUD/Master Plan Development Remaining": 4_409_524,
    "Design Review": 13_200,
    "Zoning Permit Granted or As of Right": 2_676_427,
    "Building Permit Granted": 4_618_412,
    "Complete": 195_000,
}

# (cambridge_project_id, expected fields to spot-check)
EXPECTED_RECORDS = [
    {
        "name": "Kendall Common - Building C3",
        "developer": "MITIMCO",
        "address": "75 Broadway",
        "total_gsf": 474_866,
        "special_permit_base": "PB368",
        "building_permit": "1152040",
        "status": "Building Permit Granted",
        "zoning_raw": "PUD-7",
        "neighborhood": "East Cambridge",
    },
    {
        "name": "745 Concord Avenue",
        "developer": "Boylston Properties",
        "residential_units": 236,
        "total_gsf": 230_525,
        "special_permit_base": "PB407",
        "status": "Zoning Permit Granted or As of Right",
        "zoning_raw": "O-1",
        "neighborhood": "Cambridge Highlands",
    },
    {
        "name": "350 Massachusetts Ave",
        "developer": "BioMed Realty",
        "asset_class": "Lab/R&D",
        "total_gsf": 112_600,
        "special_permit_base": "PB409",
        "zoning_raw": "CRDD",
        "neighborhood": "Cambridgeport",
    },
    {
        "name": "Cambridge Point/Healthpeak PUD",
        "developer": "Healthpeak",
        "project_type": "Master Plan",
        "total_gsf": 4_825_140,
        "special_permit_base": "PB410",
        "address": "",
        "coords_approximate": True,
        "status": "Pre-Permitting",
    },
]


def fail(msg: str):
    print(f"FAIL: {msg}")
    global _failed
    _failed = True


_failed = False


def main():
    session = get_session()
    try:
        # Scope to rows this pipeline actually ingested (cambridge_project_id set),
        # not just city == "Cambridge" -- a pre-existing manual entry can carry
        # that city tag too (see the dedupe pass in cambridge_devlog.py) and would
        # otherwise silently pollute these counts.
        projects = session.query(Project).filter(Project.cambridge_project_id.isnot(None)).all()

        print(f"Cambridge projects in DB: {len(projects)}")
        if len(projects) != EXPECTED_COUNT:
            fail(f"expected {EXPECTED_COUNT} projects, found {len(projects)}")

        # Status counts
        from collections import Counter
        status_counts = Counter(p.status for p in projects)
        for status, expected in EXPECTED_STATUS_COUNTS.items():
            actual = status_counts.get(status, 0)
            if actual != expected:
                fail(f"status {status!r}: expected {expected}, got {actual}")
        extra = set(status_counts) - set(EXPECTED_STATUS_COUNTS)
        if extra:
            fail(f"unexpected status values present: {extra}")

        # Aggregate sums
        def total(attr):
            return sum(getattr(p, attr) or 0 for p in projects)

        checks = [
            ("total_gsf sum", total("total_gsf"), EXPECTED_TOTAL_GFA),
            ("residential_units sum", total("residential_units"), EXPECTED_RESIDENTIAL_UNITS),
            ("parking_spaces sum", total("parking_spaces"), EXPECTED_PARKING_SPACES),
            ("hotel_rooms sum", total("hotel_rooms"), EXPECTED_HOTEL_ROOMS),
        ]
        for label, actual, expected in checks:
            print(f"{label}: {actual:,} (expected {expected:,})")
            if actual != expected:
                fail(f"{label}: expected {expected:,}, got {actual:,}")

        # Per-status GFA subtotals
        gfa_by_status = {}
        for p in projects:
            gfa_by_status[p.status] = gfa_by_status.get(p.status, 0) + (p.total_gsf or 0)
        for status, expected in EXPECTED_STATUS_GFA.items():
            actual = gfa_by_status.get(status, 0)
            if actual != expected:
                fail(f"per-status GFA {status!r}: expected {expected:,}, got {actual:,}")

        # Named spot-checks
        by_name = {p.name: p for p in projects}
        for rec in EXPECTED_RECORDS:
            name = rec["name"]
            p = by_name.get(name)
            if p is None:
                fail(f"spot-check record missing: {name!r}")
                continue
            for field, expected in rec.items():
                if field == "name":
                    continue
                if field == "special_permit_base":
                    sp = session.query(CambridgeSpecialPermit).filter_by(project_id=p.id).first()
                    actual = sp.base_permit if sp else None
                elif field == "building_permit":
                    bp = session.query(CambridgeBuildingPermit).filter_by(project_id=p.id).first()
                    actual = bp.permit_number if bp else None
                else:
                    actual = getattr(p, field, None)
                if actual != expected:
                    fail(f"{name!r}.{field}: expected {expected!r}, got {actual!r}")

        if _failed:
            print("\nVALIDATION FAILED -- see FAIL lines above.")
            sys.exit(1)
        else:
            print("\nAll validation checks passed.")
    finally:
        session.close()


if __name__ == "__main__":
    main()
