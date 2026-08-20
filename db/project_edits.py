"""Record-level edits: merge, split, set a developer by hand, quarantine.

These were the operations behind the REVIEW tab. The tab is gone, but the
operations are not tied to it -- they take a session and ids and commit, and
scraper/ri_merge_duplicates.py drives merge_projects from the command line.
They live here, beside the models they mutate, rather than under app/ where
importing them dragged in Streamlit.

Merging moves stage history and filings onto the surviving record rather than
discarding them, and records what happened so the decision stays auditable.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from db.models import (
    Project, ProjectStageEvent, ProjectFiling, NewsItem, FlaggedExtraction,
)
from scraper.ri_citations import record_field


def merge_projects(session, keep_id: int, absorb_id: int, note: str) -> dict:
    """Fold one project into another.

    History is moved, not dropped: a merge is a statement that two records were
    always one project, so every appearance belongs on the survivor.
    """
    keep = session.get(Project, keep_id)
    absorb = session.get(Project, absorb_id)
    if not keep or not absorb or keep.id == absorb.id:
        return {"ok": False, "error": "pick two different existing projects"}

    moved = {"stage_events": 0, "filings": 0, "news": 0}
    # Reassign through the RELATIONSHIP, not the foreign-key column. Both
    # collections cascade delete-orphan, so setting ev.project_id while the ORM
    # still holds the row in absorb.stage_events means session.delete(absorb)
    # cascades and destroys the history instead of moving it.
    for ev in list(absorb.stage_events):
        absorb.stage_events.remove(ev)
        keep.stage_events.append(ev)
        moved["stage_events"] += 1
    for f in list(absorb.filings):
        # The filing table is unique on (project_id, url); skip a duplicate
        # rather than failing the whole merge on it.
        exists = session.query(ProjectFiling).filter_by(
            project_id=keep.id, url=f.url).first()
        absorb.filings.remove(f)
        if exists:
            session.delete(f)
        else:
            keep.filings.append(f)
            moved["filings"] += 1
    for n in session.query(NewsItem).filter_by(linked_project_id=absorb.id).all():
        n.linked_project_id = keep.id
        moved["news"] += 1

    # Fill blanks on the survivor from the absorbed record; never overwrite.
    for field in ("address", "neighborhood", "developer", "developer_canonical",
                  "asset_class", "total_gsf", "residential_units", "commercial_gsf",
                  "parking_spaces", "site_acreage", "zoning_district_raw",
                  "assessor_plat", "assessor_lots", "plat_lots_raw",
                  "applicant_entity", "case_number", "latitude", "longitude",
                  # Added with the developer-provenance and quarantine columns:
                  # without these a merge silently dropped the survivor's
                  # resolution method and its sources.
                  "owner_or_agency", "developer_resolution_method",
                  "developer_sources", "stage_heard", "stage_confirmed",
                  "description", "notes", "building_count", "adaptive_reuse"):
        if getattr(keep, field, None) in (None, "") and getattr(absorb, field, None) not in (None, ""):
            setattr(keep, field, getattr(absorb, field))

    session.add(FlaggedExtraction(
        project_id=keep.id, field_name="__merge__", status="resolved",
        current_value=f"absorbed project {absorb.id} ({absorb.name or absorb.address})",
        user_note=note or "manual merge",
    ))
    keep.dedupe_review = False
    session.delete(absorb)
    session.commit()
    return {"ok": True, "moved": moved, "kept": keep.id, "absorbed": absorb_id}


def split_project(session, project_id: int, event_ids: list[int], note: str) -> dict:
    """Move selected stage events onto a new project record.

    Used when matching over-merged -- two parcels chained through a shared
    address, for instance. The new record starts from the moved events; parcel
    and address are left blank for the reviewer to fill, rather than guessed at.
    """
    src = session.get(Project, project_id)
    if not src:
        return {"ok": False, "error": "project not found"}
    if not event_ids:
        return {"ok": False, "error": "select at least one appearance to split out"}
    remaining = [e for e in src.stage_events if e.id not in set(event_ids)]
    if not remaining:
        return {"ok": False, "error": "cannot split out every appearance — nothing would remain"}

    new = Project(
        bpda_url=f"manual:split-{project_id}-{min(event_ids)}",
        name=f"{src.name or src.address or 'Project'} (split)",
        city=src.city,
        status=src.status,
        requires_extraction=True,
        dedupe_review=True,          # a split record always needs a human pass
    )
    session.add(new)
    session.flush()

    for ev in session.query(ProjectStageEvent).filter(
            ProjectStageEvent.id.in_(event_ids)).all():
        ev.project_id = new.id

    session.add(FlaggedExtraction(
        project_id=new.id, field_name="__split__", status="open",
        current_value=f"split from project {project_id}",
        user_note=note or "manual split — parcel and address need confirming",
    ))
    src.dedupe_review = False
    session.commit()
    return {"ok": True, "new_project": new.id, "moved_events": len(event_ids)}


def set_developer_by_hand(session, project_id: int, developer: str,
                          owner_or_agency: str, note: str) -> dict:
    """Set a developer by hand and mark it so nothing overwrites it.

    human_set is the one resolution method the automated passes must never
    touch. ri_rederive_settled.py recomputes document_only names after every
    step-1 parser change; without this marker a hand-corrected name would be
    silently reverted the next time the parser moved.
    """
    p = session.get(Project, project_id)
    if p is None:
        return {"ok": False, "error": "no such project"}
    name = (developer or "").strip()
    agency = (owner_or_agency or "").strip()
    if not name and not agency:
        return {"ok": False, "error": "enter a developer or an owner/agency"}

    before = p.developer or "—"
    p.developer = name or None
    p.developer_canonical = name or None
    p.owner_or_agency = agency or None
    p.developer_resolution_method = "human_set" if name else p.developer_resolution_method
    # A hand-set name supersedes the conflict that prompted it.
    p.is_flagged = False

    record_field(session, p.id, "developer",
                 f"{name}" + (f"  [owner/agency: {agency}]" if agency else ""),
                 source_url="", filing_name="SET BY HAND" + (f" — {note}" if note else ""),
                 filing_date="")
    session.add(FlaggedExtraction(
        project_id=p.id, field_name="__developer__", status="resolved",
        current_value=f"{before}  ->  {name or '(cleared)'}"
                      + (f"  [owner/agency: {agency}]" if agency else ""),
        user_note=note or "set by hand",
    ))
    session.commit()
    return {"ok": True, "developer": name, "owner_or_agency": agency}


def set_excluded(session, project_id: int, excluded: bool, reason: str) -> dict:
    """Quarantine a row, or bring one back. Never deletes."""
    p = session.get(Project, project_id)
    if p is None:
        return {"ok": False, "error": "no such project"}
    p.excluded = bool(excluded)
    p.excluded_reason = (reason or "").strip() or None if excluded else None
    session.add(FlaggedExtraction(
        project_id=p.id, field_name="__excluded__", status="resolved",
        current_value=("excluded: " + (reason or "")) if excluded else "restored to the pipeline",
        user_note=reason or "",
    ))
    session.commit()
    return {"ok": True}
