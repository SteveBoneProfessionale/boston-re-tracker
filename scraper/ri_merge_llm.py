r"""
Merge the LLM extraction into the 40 Major projects, and sweep for withdrawals.

MATCHING. An extracted item is tied to a project only by an IDENTITY token --
assessor plat/lot, case number, or street address. Never by developer or
applicant name: 532's developer was itself mis-extracted from a neighbouring
item, and matching on it would have re-attached the project to the very item
the wrong name came from.

WRITING. Existing values are not overwritten. A field is filled only where the
project is blank, except:

  * is_withdrawn / outcome, which is the point of the sweep -- a project shown
    as live when it is dead is worse than a missing field, so a withdrawal is
    written even over an existing stage.
  * building_sf, which is never written here at all. Floor area from an agenda
    has been wrong often enough (lot areas, ordinance thresholds, comparables
    in other states) that it stays a manual decision.

Every written value records the meeting, document and verbatim evidence.

    python scraper/ri_merge_llm.py --dry-run
    python scraper/ri_merge_llm.py --apply
"""

import re
import sys
import json
import logging
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from db.database import get_session
from db.models import Project
from scraper.ri_sf_extract import RI

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

JSONL = ROOT / "data" / "ri_llm_extract.jsonl"
OUT = ROOT / "data" / "ri_llm_merge_report.json"

WITHDRAWN_OUTCOMES = {"withdrawn", "denied"}


def norm(s):
    return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()


def keys_for_project(p):
    """Identity tokens only. Never developer or applicant."""
    ks = set()
    if p.plat_lots_raw:
        ks.add(("plat", norm(p.plat_lots_raw)))
    if p.case_number and len(p.case_number.strip("- ")) > 3:
        ks.add(("case", norm(p.case_number)))
    a = norm(p.address)
    if len(a) > 5:
        ks.add(("addr", a))
    return ks


def keys_for_item(it):
    ks = set()
    if it.get("plat_lot"):
        ks.add(("plat", norm(it["plat_lot"])))
    if it.get("case_number"):
        ks.add(("case", norm(it["case_number"])))
    a = norm(it.get("address"))
    if len(a) > 5:
        ks.add(("addr", a))
    return ks


def match(pk, ik):
    """A plat or case match is exact. An address match allows containment,
    since '2281 and 2279 West Shore Rd' and '2281 West Shore Road' are the
    same site written two ways."""
    for kind, v in pk:
        for kind2, v2 in ik:
            if kind != kind2:
                continue
            if kind in ("plat", "case"):
                if v == v2:
                    return True
            elif v == v2 or (len(v) > 8 and (v in v2 or v2 in v)):
                return True
    return False


# The extraction prompt uses its own lowercase enum for asset class. That is
# NOT the tracker's vocabulary, and writing it straight through broke the
# canonical set -- 48 Rhode Island records ended up holding "multifamily" and
# "retail" alongside the proper "Residential" and "Retail". Every market must
# write to ASSET_CLASSES and nothing else, so the fold happens here.
# self-storage has no canonical entry and folds to Industrial, which is where
# the sector normally sits; the source wording survives in asset_class_raw.
ASSET_FOLD = {
    "multifamily": "Residential", "residential": "Residential",
    "mixed-use": "Mixed-Use", "mixed use": "Mixed-Use",
    "office": "Office", "lab": "Lab/Research", "lab/research": "Lab/Research",
    "retail": "Retail", "hotel": "Hotel", "industrial": "Industrial",
    "self-storage": "Industrial", "institutional": "Institutional",
    "parking": "Parking", "other": "Other",
}


# LLM field -> Project column. building_sf deliberately absent.
FIELDS = [
    ("residential_units", "residential_units"),
    ("affordable_units", "affordable_units"),
    ("stories", "num_stories"),
    ("parking_spaces", "parking_spaces"),
    ("asset_class", "asset_class"),
    ("architect", "architect"),
    ("engineer", "civil_engineer"),
    ("owner_or_agency", "owner_or_agency"),
    ("site_acres", "site_acreage"),
    ("lot_sf", "lot_area"),
    ("project_name", "name"),
    ("developer", "developer"),
]


def main(apply=False):
    if not JSONL.exists():
        log.error("no extraction output yet")
        return
    recs = []
    for line in JSONL.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            recs.append(json.loads(line))
        except Exception:                                       # noqa: BLE001
            pass

    session = get_session()
    everything = "--all" in sys.argv
    maj = [p for p in session.query(Project).filter(Project.city.in_(RI)).all()
           if not p.excluded and (everything or p.review_scale == "Major")]

    hits = defaultdict(list)
    for r in recs:
        for it in r.get("items", []):
            ik = keys_for_item(it)
            if not ik:
                continue
            for p in maj:
                if p.city != r.get("municipality"):
                    continue
                if match(keys_for_project(p), ik):
                    hits[p.id].append((r, it))

    report = {"filled": [], "withdrawn": [], "unmatched": []}
    for p in maj:
        got = hits.get(p.id, [])
        if not got:
            report["unmatched"].append({"id": p.id, "address": p.address, "name": p.name})
            continue

        # Withdrawal sweep -- newest document wins.
        got_sorted = sorted(got, key=lambda x: x[0].get("date") or "")
        for r, it in got_sorted:
            if it.get("is_withdrawn") or (it.get("outcome") or "").lower() in WITHDRAWN_OUTCOMES:
                ev = (it.get("evidence") or {}).get("is_withdrawn") or it.get("outcome")
                w = {"id": p.id, "address": p.address or p.name,
                     "outcome": it.get("outcome"), "date": r.get("date"),
                     "doc": r.get("text_file"), "evidence": str(ev)[:200]}
                report["withdrawn"].append(w)
                if apply:
                    p.project_status_filing = (it.get("outcome") or "withdrawn").title()
                    p.notes = ((p.notes + " | ") if p.notes else "") + (
                        "STATUS %s per %s meeting %s (%s): %s" % (
                            w["outcome"], p.city, r.get("date"), r.get("text_file"),
                            str(ev)[:160]))

        for src, col in FIELDS:
            if getattr(p, col, None) not in (None, "", 0):
                # FILL-BLANKS-ONLY is why a stale figure could never be
                # corrected: 311 Knight Street kept its December 2019 count of
                # 34 through a 2021 major change and a 2022 final plan that
                # both said 41. Revisions are the normal life of a project, so
                # a later filing has to be able to win. That is handled in
                # scraper/ri_supersede_units.py rather than here, because it
                # needs the corroboration rules this loop has no access to --
                # two documents, and no collapse to under a quarter.
                continue
            vals = [(r, it) for r, it in got_sorted if it.get(src) not in (None, "", [])]
            if not vals:
                continue
            distinct = {str(it.get(src)).strip().lower() for _, it in vals}
            if len(distinct) > 1:
                # Sources disagree -> blank and flag, per standing rule.
                report["filled"].append({"id": p.id, "field": col, "value": None,
                                         "note": "sources disagree: %s" % sorted(distinct)})
                continue
            r, it = vals[-1]
            v = it.get(src)
            ev = (it.get("evidence") or {}).get(src)
            report["filled"].append({
                "id": p.id, "address": p.address or p.name, "field": col, "value": v,
                "date": r.get("date"), "doc": r.get("text_file"),
                "evidence": str(ev)[:180] if ev else ""})
            if col == "asset_class":
                folded = ASSET_FOLD.get(str(v).strip().lower())
                if not folded:
                    continue        # unknown wording -> leave blank, never invent
                v = folded
            if apply:
                setattr(p, col, v)
                p.notes = ((p.notes + " | ") if p.notes else "") + (
                    "%s=%s from %s %s meeting, doc %s%s" % (
                        col, v, p.city, r.get("date"), r.get("text_file"),
                        (": " + str(ev)[:140]) if ev else ""))

    if apply:
        session.commit()
    OUT.write_text(json.dumps(report, indent=1, ensure_ascii=False), encoding="utf-8")

    log.info("Major projects: %d   matched: %d   unmatched: %d",
             len(maj), len(maj) - len(report["unmatched"]), len(report["unmatched"]))
    log.info("fields filled: %d   withdrawal signals: %d",
             len([f for f in report["filled"] if f.get("value") is not None]),
             len(report["withdrawn"]))
    for w in report["withdrawn"]:
        log.info("  WITHDRAWN/DENIED id=%-4d %-28s %-10s %s", w["id"],
                 str(w["address"])[:28], w["outcome"], w["evidence"][:80])
    log.info("%s", "APPLIED" if apply else "DRY RUN -- use --apply")
    session.close()


if __name__ == "__main__":
    main(apply="--apply" in sys.argv)
