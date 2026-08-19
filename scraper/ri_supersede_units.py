r"""
Let a later filing correct an earlier one.

THE DEFECT. scraper/ri_merge_llm.py fills a field only when it is BLANK:

    if getattr(p, col, None) not in (None, "", 0):
        continue

So the first figure a project ever states is the figure it keeps forever. 311
Knight Street / 1077 Westminster Street was proposed at 34 units in December
2019, came back for a major change at 34 in July 2021, and was approved at
FINAL PLAN REVIEW in October 2022 as "a 41 unit, five story mixed use
development". The tracker still said 34, because 34 got there first.

A revision is the normal life of a development. The rule already agreed for
plan sets applies here too: take the figure from the most recent document,
and record the earlier one rather than discarding it.

WHAT COUNTS AS LATER. The meeting date of the document the figure came from,
not the order rows happen to sit in. Only a document that actually states a
figure can supersede -- an extension request that mentions no units does not
blank out a count.

GUARD. A change is only applied when the later document names this project's
own case number or address in the same breath as the figure. Providence
agendas reuse a street name across unrelated cases: the September 2024 agenda
carries "Case no. 19-015MA - 311 Knight Street, Applicant: SWAP ... (AP 85
Lot 555, Elmhurst)", which is a different case, a different applicant and a
different plat from case 19-051 at AP 32. Matching on the address alone would
have let that overwrite the real project.

    python scraper/ri_supersede_units.py
    python scraper/ri_supersede_units.py --apply
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
from db.models import Project, FlaggedExtraction
from scraper.ri_sf_extract import RI
from scraper.ri_merge_llm import keys_for_project, keys_for_item, match

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)
JSONL = ROOT / "data" / "ri_llm_extract.jsonl"
OUT = ROOT / "data" / "ri_supersede_units.json"

# A filing stage that settles the programme. A figure from one of these beats
# a figure from an earlier informational hearing even before dates are read.
AUTHORITATIVE = re.compile(r"final\s+plan|preliminary\s+plan|major\s+change", re.I)


def main(apply=False):
    recs = []
    for line in JSONL.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            recs.append(json.loads(line))
        except Exception:                                       # noqa: BLE001
            pass

    session = get_session()
    rows = [p for p in session.query(Project).filter(Project.city.in_(RI)).all()
            if not p.excluded]

    # Every dated unit figure, per project.
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
                # The guard: the item must carry this project's own case
                # number when the project has one, so a reused street name in
                # an unrelated case cannot overwrite it.
                pc = re.sub(r"[^a-z0-9]", "", (p.case_number or "").lower())
                ic = re.sub(r"[^a-z0-9]", "", (it.get("case_number") or "").lower())
                if pc and ic and not (pc.startswith(ic[:6]) or ic.startswith(pc[:6])):
                    continue
                stated[p.id].append({
                    "units": int(u), "date": r.get("date") or "",
                    "doc": r.get("text_file"), "stage": it.get("filing_type") or "",
                    "quote": ((it.get("evidence") or {}).get("residential_units") or "")[:160],
                })

    changes, refused = [], []
    for p in rows:
        seen = stated.get(p.id, [])
        if not seen:
            continue
        seen.sort(key=lambda x: (x["date"], 1 if AUTHORITATIVE.search(x["stage"]) else 0))
        latest = seen[-1]
        # CORROBORATION, because "latest wins" alone is not safe. An agenda
        # lists several items and the extraction sometimes attaches a small
        # neighbouring figure to this project, which shows up as a collapse:
        # 580 South Water 69 -> 5, 153-165 Gano 132 -> 4, 16 Waites Wharf
        # 100 -> 7. The operator has separately confirmed 580 South Water is a
        # 69-unit building, so applying that would have destroyed a known-good
        # figure. Two rules keep it honest:
        #   the new figure must appear in at least two documents, and
        #   a fall to under a quarter of the old count is refused outright.
        n_docs = len({s2["doc"] for s2 in seen if s2["units"] == latest["units"]})
        collapse = latest["units"] < p.residential_units * 0.25 if p.residential_units else False
        if (p.residential_units and latest["units"] != p.residential_units
                and n_docs >= 2 and not collapse):
            changes.append({
                "id": p.id, "city": p.city,
                "address": (p.address or p.name or "")[:44],
                "was": p.residential_units, "now": latest["units"],
                "date": latest["date"], "doc": latest["doc"],
                "stage": latest["stage"], "quote": latest["quote"],
                "history": [(s["date"], s["units"], s["stage"][:22]) for s in seen],
                "corroborating_docs": n_docs,
            })
        elif p.residential_units and latest["units"] != p.residential_units:
            refused.append({"id": p.id, "city": p.city,
                            "address": (p.address or p.name or "")[:44],
                            "was": p.residential_units, "proposed": latest["units"],
                            "docs": n_docs, "collapse": collapse})

    log.info("\nProjects whose LATEST filing states a different unit count: %d", len(changes))
    for c in sorted(changes, key=lambda x: -abs(x["now"] - x["was"])):
        log.info("  id=%-4d %-11s %-36s %s -> %s  (%s, %s)", c["id"], c["city"],
                 c["address"][:36], c["was"], c["now"], c["date"], c["stage"][:26])
        log.info("        history: %s", " | ".join("%s=%s" % (d[:7], u) for d, u, _ in c["history"]))

    log.info("\nREFUSED -- a later figure that is not trustworthy: %d", len(refused))
    for r in refused:
        why = "a collapse to under a quarter" if r["collapse"] else "stated in only one document"
        log.info("  id=%-4d %-11s %-36s keeps %s, refused %s (%s)", r["id"], r["city"],
                 r["address"][:36], r["was"], r["proposed"], why)

    OUT.write_text(json.dumps({"applied": changes, "refused": refused}, indent=1,
                              ensure_ascii=False), encoding="utf-8")

    if apply:
        for c in changes:
            p = session.get(Project, c["id"])
            p.residential_units = c["now"]
            p.notes = ((p.notes + " | ") if p.notes else "") + (
                "residential_units %s -> %s: superseded by the later filing of %s (%s, doc %s). "
                "Earlier figures kept here rather than discarded: %s. Verbatim: %r"
                % (c["was"], c["now"], c["date"], c["stage"], c["doc"],
                   ", ".join("%s=%s" % (d[:10], u) for d, u, _ in c["history"]), c["quote"]))
            session.add(FlaggedExtraction(
                project_id=p.id, field_name="residential_units", status="open",
                current_value=str(c["now"]),
                user_note="Unit count revised from %s to %s by the %s filing on %s. Earlier "
                          "figures: %s." % (c["was"], c["now"], c["stage"] or "later", c["date"],
                                            ", ".join("%s=%s" % (d[:10], u) for d, u, _ in c["history"]))))
        session.commit()
        log.info("\nAPPLIED %d", len(changes))
    else:
        log.info("\nDRY RUN -- re-run with --apply")
    session.close()


if __name__ == "__main__":
    main(apply="--apply" in sys.argv)
