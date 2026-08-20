"""Turn the Article 80 batch results into provenance rows.

Steps 1 and 2 of the waterfall land together here: the same read of the
document both audits the value already in the tracker and supplies a new one.
A prior value survives only on an explicit "confirmed"; anything else is
recorded as a failed verification and the field falls through to Step 3.
"""
import json
import re
import sqlite3
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from provenance import audit_prior, connect, looks_like_person, record

RESULTS = Path("data/boston_batch_results.json")
FIELDS = ("architect", "civil_engineer", "general_contractor")


def parse(txt):
    if not txt:
        return None
    t = txt.strip()
    if t.startswith("```"):
        t = re.sub(r"^```(json)?\s*", "", t)
        t = re.sub(r"\s*```$", "", t)
    try:
        return json.loads(t)
    except Exception:
        m = re.search(r"\{.*\}", t, re.S)
        if m:
            try:
                return json.loads(m.group(0))
            except Exception:
                return None
    return None


def main():
    rows = json.loads(RESULTS.read_text())
    c = connect()
    projects = {r["id"]: r for r in c.execute(
        "select id,name,address,architect,civil_engineer,general_contractor,"
        "status,completion_stage,processed_filing_name,processed_filing_url "
        "from projects").fetchall()}

    stats = Counter()
    for r in rows:
        pid = int(r["custom_id"][1:])
        p = projects.get(pid)
        if p is None:
            stats["no_project"] += 1
            continue
        d = parse(r.get("text"))
        if d is None:
            stats["unparseable"] += 1
            continue
        stats["parsed"] += 1
        url = p["processed_filing_url"]
        name = p["processed_filing_name"]
        addr_q = d.get("address_passage")

        # ---- Step 1: audit what was already there ----
        checks = d.get("prior_checks") or {}
        for f in FIELDS:
            prior = p[f]
            if not prior or str(prior).strip() in ("", "not_yet_selected"):
                continue
            v = str(checks.get(f) or "no_prior")
            if v not in ("confirmed", "role_not_labelled", "firm_absent", "no_prior"):
                v = "firm_absent"
            got = d.get(f) or {}
            audit_prior(c, pid, f, prior, v,
                        page_ref=str(got.get("page") or ""),
                        firm_sentence=got.get("passage"),
                        note=f"checked against {name}")
            stats[f"prior_{f}_{v}"] += 1

        # ---- Step 2: what the document itself labels ----
        for f in FIELDS:
            got = d.get(f) or {}
            firm = (got.get("firm") or "").strip() or None
            label = (got.get("role_label") or "").strip() or None
            passage = got.get("passage")
            page = got.get("page")
            prospective = bool(got.get("prospective_only"))

            if f == "general_contractor" and prospective:
                started = (p["completion_stage"] in ("Under Construction", "Complete")
                           or p["status"] in ("Under Construction", "Complete",
                                              "Building Permit Granted"))
                record(c, pid, f, value=None,
                       outcome="null" if started else "not_yet_selected",
                       source_type="article80_pdf", source_url=url, source_name=name,
                       page_ref=str(page or ""), firm_sentence=passage,
                       address_sentence=addr_q, resolution_step=2,
                       reason="document names a general contractor only prospectively")
                stats[f"gc_prospective"] += 1
                continue

            if firm and label:
                record(c, pid, f, value=firm, outcome="resolved",
                       tier="document_confirmed", source_type="article80_pdf",
                       source_url=url, source_name=name, page_ref=str(page or ""),
                       firm_sentence=passage, address_sentence=addr_q,
                       resolution_step=2,
                       reason=("value is an individual, stored as stated"
                               if looks_like_person(firm) else None))
                stats[f"resolved_{f}"] += 1
            elif firm and not label:
                stats[f"unlabelled_{f}"] += 1

        if stats["parsed"] % 100 == 0:
            c.commit()

    c.commit()
    print("=== batch application ===")
    for k, v in sorted(stats.items()):
        print(f"  {k:42} {v}")
    live = c.execute("select count(*) from field_provenance where superseded=0").fetchone()[0]
    print(f"\nlive provenance rows: {live}")


if __name__ == "__main__":
    main()
