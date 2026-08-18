r"""
Strip the Warwick records whose address field holds OCR prose.

    885   "57 units. The main point"
    892   "2021 Page"     (from "Regular hearing of November 9, 2021 Page 2")
    896   "2019 Page"

A record whose address is a sentence fragment was segmented wrongly, and every
other figure on it came out of the same broken segmentation. The 181 units on
892 and the 57 on 885 are no more trustworthy than the phantom 89 units were.
So every extracted field is nulled and the row is reduced to its address, its
description (the source text) and its source-document key, then flagged for
re-extraction.

The rows are NOT excluded here. They are real filings that were parsed badly,
not non-commercial items, and they should come back into the counts once the
segmentation is fixed. They will show as projects with no data until then --
visible, which is the point.

    python scraper/ri_strip_corrupt_records.py --dry-run
"""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import get_session
from db.models import Project, FlaggedExtraction

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

CORRUPT = {
    885: "address is the prose fragment '57 units. The main point'",
    892: "address is '2021 Page', cut from 'Regular hearing of November 9, 2021 Page 2'",
    896: "address is '2019 Page', the same OCR'd-minutes failure",
    # Found by the pipeline report. Same principle: the address is a sentence
    # fragment, so nothing extracted alongside it can be trusted.
    695: "address is the prose fragment '20 feet of a main street', cut from an ordinance sentence",
    900: "address is '10616 Ward', a docket or ward number rather than a street address",
}

# Everything derived from the mis-segmented text. Address, description,
# bpda_url, city and first_seen_date are deliberately preserved.
STRIP = (
    "residential_units", "total_gsf", "commercial_gsf", "bpda_gsf", "parking_spaces",
    "site_acreage", "lot_area", "building_count", "num_stories", "building_height_ft",
    "affordable_units", "hotel_rooms", "far", "far_scope",
    "asset_class", "asset_class_raw", "project_type", "permit_type",
    "zoning_raw", "zoning_components", "zoning_district_raw",
    "stage_heard", "stage_confirmed", "status", "project_status_filing",
    "review_scale", "review_scale_raw", "project_scale",
    "applicant_entity", "applicant_source", "case_number",
    "developer", "developer_canonical", "owner_or_agency",
    "developer_resolution_method", "developer_sources",
    "assessor_plat", "assessor_lots", "plat_lots_raw",
    "adaptive_reuse", "architect", "civil_engineer", "equity_partner",
    "expected_delivery", "neighborhood", "neighborhood_raw",
    # A geocode derived from a prose address points nowhere real.
    "latitude", "longitude",
)


def main(dry_run=False):
    session = get_session()
    try:
        for pid, why in CORRUPT.items():
            p = session.get(Project, pid)
            if p is None:
                log.warning("  id=%s missing", pid)
                continue
            cleared = []
            for f in STRIP:
                if getattr(p, f, None) not in (None, ""):
                    cleared.append(f)
                    setattr(p, f, None)
            p.coords_approximate = None
            p.notes = ((p.notes + " | ") if p.notes else "") + \
                "ALL EXTRACTED FIELDS NULLED: " + why + \
                ". Reduced to address plus source document pending re-extraction."
            session.add(FlaggedExtraction(
                project_id=p.id, field_name="__corrupt_segmentation__", status="open",
                current_value=p.address,
                user_note="Address field is OCR prose; every figure came from the same "
                          "broken segmentation. Needs re-extraction from the Warwick "
                          "minutes before any number on this row is used.",
            ))
            log.info("  id=%-4d %-28s cleared %d field(s)", pid, (p.address or "")[:28], len(cleared))
            log.info("        %s", ", ".join(cleared[:12]) + (" ..." if len(cleared) > 12 else ""))
        if dry_run:
            session.rollback()
            log.info("DRY RUN -- nothing written")
            return
        session.commit()
        log.info("Stripped %d records and flagged them for re-extraction.", len(CORRUPT))
    finally:
        session.close()


if __name__ == "__main__":
    main(dry_run="--dry-run" in sys.argv)
