"""Manual review: deduplication merge/split and flagged-field triage.

Automated matching gets project identity mostly right and will sometimes get it
wrong, so this exists to fix it by hand. Two failure directions, both covered:

  MERGE  two records that are actually one project (matching missed the link --
         common in Warwick, where address carries identity)
  SPLIT  one record that is actually two (matching over-merged, e.g. two
         parcels chained through a shared address string)

Merging moves stage history and filings onto the surviving record rather than
discarding them, and records what happened so the decision stays auditable.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pandas as pd
import streamlit as st

from db.database import get_session
from db.models import Project, ProjectStageEvent, ProjectFiling, NewsItem, FlaggedExtraction
from app.data import load_projects

_BORDER = "#1E2530"
_ORANGE = "#F5821E"
_MUTED  = "#8A9BB0"
_MONO   = "'JetBrains Mono', 'IBM Plex Mono', monospace"


def _section(label: str):
    st.markdown(
        f'<p style="font-family:{_MONO};font-size:9px;font-weight:700;'
        f'letter-spacing:0.18em;color:{_MUTED};text-transform:uppercase;'
        f'margin:16px 0 8px 0">{label}</p>',
        unsafe_allow_html=True,
    )


def _label(p: Project) -> str:
    bits = [p.name or p.address or f"project {p.id}"]
    if p.assessor_plat:
        bits.append(f"Plat {p.assessor_plat} Lot {p.assessor_lots or '?'}")
    if p.city:
        bits.append(p.city)
    return f"[{p.id}] " + " · ".join(bits)


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
                  "applicant_entity", "case_number", "latitude", "longitude"):
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


def render(df: pd.DataFrame):
    st.markdown(
        f'<div style="font-family:{_MONO};font-size:11px;color:{_MUTED};'
        f'padding:8px 0 4px">Fix project identity by hand where automated '
        f'matching missed a link or merged two parcels that are not one project.'
        f'</div>', unsafe_allow_html=True)

    session = get_session()
    try:
        flagged = session.query(Project).filter(
            Project.dedupe_review == True).all()  # noqa: E712

        _section(f"REVIEW QUEUE  ({len(flagged)})")
        if flagged:
            st.dataframe(pd.DataFrame([{
                "ID": p.id, "PROJECT": p.name or p.address or "—",
                "CITY": p.city or "—",
                "PLAT/LOT": f"{p.assessor_plat or '?'} / {p.assessor_lots or '?'}",
                "APPEARANCES": len(p.stage_events),
            } for p in flagged]), use_container_width=True, hide_index=True, height=220)
        else:
            st.caption("Nothing flagged for identity review.")

        projects = session.query(Project).order_by(Project.city, Project.name).all()
        opts = {_label(p): p.id for p in projects}

        # ── Merge ────────────────────────────────────────────────────
        _section("MERGE — two records that are one project")
        m1, m2 = st.columns(2)
        keep_l = m1.selectbox("KEEP", ["—"] + list(opts), key="rv_keep")
        absorb_l = m2.selectbox("ABSORB INTO KEEP", ["—"] + list(opts), key="rv_absorb")
        note_m = st.text_input("NOTE", key="rv_merge_note",
                               placeholder="why these are the same project")
        if keep_l != "—" and absorb_l != "—":
            k, a = session.get(Project, opts[keep_l]), session.get(Project, opts[absorb_l])
            st.caption(
                f"Keeping **{k.name or k.address}** ({len(k.stage_events)} appearances) · "
                f"absorbing **{a.name or a.address}** ({len(a.stage_events)} appearances). "
                f"History and filings move to the survivor; blank fields are filled, "
                f"never overwritten.")
        if st.button("MERGE", disabled=(keep_l == "—" or absorb_l == "—"), key="rv_do_merge"):
            res = merge_projects(session, opts[keep_l], opts[absorb_l], note_m)
            if res["ok"]:
                load_projects.clear()
                st.success(f"Merged. Moved {res['moved']['stage_events']} appearance(s), "
                           f"{res['moved']['filings']} filing(s), {res['moved']['news']} article(s).")
            else:
                st.error(res["error"])

        # ── Split ────────────────────────────────────────────────────
        _section("SPLIT — one record that is two projects")
        split_l = st.selectbox("PROJECT TO SPLIT", ["—"] + list(opts), key="rv_split")
        if split_l != "—":
            p = session.get(Project, opts[split_l])
            events = list(p.stage_events)
            if len(events) < 2:
                st.caption("This project has fewer than two appearances — nothing to split.")
            else:
                chosen = st.multiselect(
                    "APPEARANCES TO MOVE TO A NEW PROJECT",
                    options=[e.id for e in events],
                    format_func=lambda eid: next(
                        f"{e.meeting_date or '?'} · {e.reviewing_body or '?'} · "
                        f"{e.review_stage_raw or '?'}" for e in events if e.id == eid),
                    key="rv_split_events")
                note_s = st.text_input("NOTE", key="rv_split_note",
                                       placeholder="why these are a separate project")
                if st.button("SPLIT", disabled=not chosen, key="rv_do_split"):
                    res = split_project(session, p.id, chosen, note_s)
                    if res["ok"]:
                        load_projects.clear()
                        st.success(f"Split into new project {res['new_project']} with "
                                   f"{res['moved_events']} appearance(s). It is flagged "
                                   f"for review — parcel and address need confirming.")
                    else:
                        st.error(res["error"])

        # ── Audit trail ──────────────────────────────────────────────
        _section("RECENT IDENTITY CHANGES")
        hist = (session.query(FlaggedExtraction)
                .filter(FlaggedExtraction.field_name.in_(["__merge__", "__split__"]))
                .order_by(FlaggedExtraction.flagged_at.desc()).limit(25).all())
        if hist:
            st.dataframe(pd.DataFrame([{
                "WHEN": f.flagged_at.strftime("%Y-%m-%d %H:%M") if f.flagged_at else "—",
                "ACTION": f.field_name.strip("_").upper(),
                "PROJECT": f.project_id,
                "DETAIL": f.current_value or "—",
                "NOTE": f.user_note or "—",
            } for f in hist]), use_container_width=True, hide_index=True, height=240)
        else:
            st.caption("No manual merges or splits yet.")
    finally:
        session.close()
