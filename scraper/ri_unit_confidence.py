r"""
How much should each Rhode Island unit count be trusted?

THE QUESTION THIS ANSWERS. 1077 Westminster carried 34 units when the final
plan said 41. The operator's follow-up was the right one: if that was wrong,
how do I know the others are right? A blanket assurance is worthless. This
grades every stored figure against the documents behind it.

FOUR GRADES:

  CORROBORATED   the stored figure is the latest stated figure and appears in
                 two or more documents. Nothing else in the record contradicts
                 it. This is as good as agenda evidence gets.
  SINGLE SOURCE  the stored figure appears in exactly one document. Probably
                 right, unverifiable here. A transcription error or a
                 mis-attributed neighbouring item would look identical.
  CONTRADICTED   a LATER document states a different figure that the
                 supersede rules refused -- a collapse to under a quarter, or
                 a single-document claim. The stored figure is kept, but two
                 numbers are on the record and only one can be true.
  UNSOURCED      no document in the corpus states this figure at all. It came
                 from the original ingest and cannot be traced. These are the
                 ones to distrust first.

The grade is written to the record so it can be filtered and rendered, in the
same way developer confidence is. A number a reader cannot weigh is worse
than a number with a caveat attached.

    python scraper/ri_unit_confidence.py
    python scraper/ri_unit_confidence.py --apply
"""

import re
import sys
import json
import logging
from pathlib import Path
from collections import defaultdict, Counter

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from db.database import get_session
from db.models import Project
from scraper.ri_sf_extract import RI
from scraper.ri_merge_llm import keys_for_project, keys_for_item, match

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)
JSONL = ROOT / "data" / "ri_llm_extract.jsonl"
OUT = ROOT / "data" / "ri_unit_confidence.json"


def main(apply=False):
    recs = []
    for line in JSONL.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            recs.append(json.loads(line))
        except Exception:                                       # noqa: BLE001
            pass

    session = get_session()
    rows = [p for p in session.query(Project).filter(Project.city.in_(RI)).all()
            if not p.excluded and p.residential_units]

    stated = defaultdict(list)
    for r in recs:
        for it in r.get("items", []):
            u = it.get("residential_units")
            if not u:
                continue
            ik = keys_for_item(it)
            if not ik:
                continue
            for p in rows:
                if p.city != r.get("municipality"):
                    continue
                if not match(keys_for_project(p), ik):
                    continue
                pc = re.sub(r"[^a-z0-9]", "", (p.case_number or "").lower())
                ic = re.sub(r"[^a-z0-9]", "", (it.get("case_number") or "").lower())
                if pc and ic and not (pc.startswith(ic[:6]) or ic.startswith(pc[:6])):
                    continue
                stated[p.id].append({"units": int(u), "date": r.get("date") or "",
                                     "doc": r.get("text_file")})

    graded = []
    for p in rows:
        seen = stated.get(p.id, [])
        n_docs = len({s["doc"] for s in seen if s["units"] == p.residential_units})
        others = sorted({s["units"] for s in seen} - {p.residential_units})
        later_diff = [s for s in seen
                      if s["units"] != p.residential_units
                      and s["date"] >= max([x["date"] for x in seen
                                            if x["units"] == p.residential_units] or [""])]
        if not seen or n_docs == 0:
            grade, why = "unsourced", "no document in the corpus states this figure"
        elif later_diff:
            grade, why = "contradicted", ("a later document states %s"
                                          % ", ".join(str(s["units"]) for s in later_diff[:3]))
        elif n_docs >= 2:
            grade, why = "corroborated", "stated in %d documents, latest figure" % n_docs
        else:
            grade, why = "single_source", "stated in one document only"
        graded.append({"id": p.id, "city": p.city,
                       "address": (p.address or p.name or "")[:44],
                       "units": p.residential_units, "grade": grade, "why": why,
                       "docs": n_docs, "other_figures": others})

    t = Counter(g["grade"] for g in graded)
    n = len(graded)
    log.info("\nRHODE ISLAND UNIT COUNTS: %d projects state one\n", n)
    for k in ("corroborated", "single_source", "contradicted", "unsourced"):
        c = t.get(k, 0)
        log.info("  %-14s %4d  %3.0f%%   %s", k, c, 100 * c / n if n else 0,
                 {"corroborated": "two or more documents agree, latest figure",
                  "single_source": "one document only -- probably right, unverifiable",
                  "contradicted": "a later document disagrees; both numbers on record",
                  "unsourced": "no document states it -- distrust these first"}[k])

    units_by = defaultdict(int)
    for g in graded:
        units_by[g["grade"]] += g["units"]
    log.info("\n  units carried by grade: %s",
             {k: f"{v:,}" for k, v in sorted(units_by.items(), key=lambda kv: -kv[1])})

    for grade in ("unsourced", "contradicted"):
        g = [x for x in graded if x["grade"] == grade]
        if not g:
            continue
        log.info("\n%s (%d), largest first:", grade.upper(), len(g))
        for x in sorted(g, key=lambda z: -z["units"])[:18]:
            log.info("  id=%-4d %-11s %-36s %5s units  %s", x["id"], x["city"],
                     x["address"][:36], x["units"], x["why"][:52])

    OUT.write_text(json.dumps(graded, indent=1, ensure_ascii=False), encoding="utf-8")

    if apply:
        for g in graded:
            p = session.get(Project, g["id"])
            p.units_confidence = g["grade"]
            p.notes = ((p.notes + " | ") if p.notes else "") + (
                "units_confidence=%s: %s%s" % (
                    g["grade"], g["why"],
                    (". Other figures on record: %s" % g["other_figures"])
                    if g["other_figures"] else ""))
        session.commit()
        log.info("\nAPPLIED to %d projects", len(graded))
    else:
        log.info("\nDRY RUN -- re-run with --apply")
    session.close()


if __name__ == "__main__":
    main(apply="--apply" in sys.argv)
