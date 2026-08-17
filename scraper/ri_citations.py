r"""
Per-field source citations for extracted project data.

The ExtractionSource model has existed since the original schema but nothing
ever wrote to it -- the table held 0 rows, so the "existing Boston convention"
of per-field citations was schema-only. This implements the write path.

Every extracted field records where it came from: the filing, its URL, and the
page when determinable. A field absent from the filing is left null and gets no
citation, so "no citation" and "no value" stay distinguishable from "value with
unknown provenance".

Written so a Boston backfill can reuse it unchanged -- nothing here is Rhode
Island specific except the caller.
"""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.models import ExtractionSource

log = logging.getLogger(__name__)

# Fields worth citing. Narrative and identity fields are excluded: the
# description IS the source text, and parcel identity is cited via the filing
# that established it rather than per-component.
CITABLE_FIELDS = (
    "developer", "applicant_entity", "asset_class", "total_gsf",
    "residential_units", "commercial_gsf", "parking_spaces", "site_acreage",
    "building_count", "num_stories", "building_height_ft",
    "zoning_district_raw", "review_scale", "adaptive_reuse", "case_number",
)


def record_field(session, project_id: int, field_name: str, value,
                 *, source_url: str, filing_name: str = "",
                 filing_date: str = "", page_number: int | None = None,
                 replace: bool = True) -> ExtractionSource | None:
    """Cite one field. Returns None when there is nothing to cite.

    A null value is not cited: an absent field must stay visibly absent rather
    than acquiring a citation that implies the filing stated it.
    """
    if value is None or value == "":
        return None
    if replace:
        (session.query(ExtractionSource)
         .filter_by(project_id=project_id, field_name=field_name)
         .delete(synchronize_session=False))
    src = ExtractionSource(
        project_id=project_id,
        field_name=field_name,
        field_value=str(value)[:500],
        filing_name=filing_name or None,
        filing_date=filing_date or None,
        pdf_url=source_url or None,
        page_number=page_number,
    )
    session.add(src)
    return src


def record_extraction(session, project, extracted: dict, *, source_url: str,
                      filing_name: str = "", filing_date: str = "",
                      pages: dict | None = None) -> int:
    """Cite every citable field present in one extraction.

    `pages` optionally maps field name -> page number, for extractors that can
    determine it. Returns the number of citations written.
    """
    pages = pages or {}
    written = 0
    for field in CITABLE_FIELDS:
        if field not in extracted:
            continue
        rec = record_field(
            session, project.id, field, extracted.get(field),
            source_url=source_url, filing_name=filing_name,
            filing_date=filing_date, page_number=pages.get(field),
        )
        if rec is not None:
            written += 1
    return written


def citations_for(session, project_id: int) -> dict:
    """field name -> citation, for display."""
    out = {}
    for s in (session.query(ExtractionSource)
              .filter_by(project_id=project_id).all()):
        out[s.field_name] = {
            "value": s.field_value, "url": s.pdf_url,
            "filing": s.filing_name, "date": s.filing_date,
            "page": s.page_number,
        }
    return out


def coverage(session) -> dict:
    """How many projects carry citations, and for how many fields.

    Reports the gap rather than assuming coverage -- the Boston rows have none
    until that backfill runs.
    """
    from db.models import Project
    from collections import Counter

    per_project = Counter()
    for (pid,) in session.query(ExtractionSource.project_id).all():
        per_project[pid] += 1
    total = session.query(Project).count()
    return {
        "projects_total": total,
        "projects_with_citations": len(per_project),
        "citations_total": sum(per_project.values()),
        "mean_fields_cited": (round(sum(per_project.values()) / len(per_project), 1)
                              if per_project else 0),
    }
