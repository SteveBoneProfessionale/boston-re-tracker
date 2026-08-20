"""Apply the Step 3 registry sources: Cambridge permit architect, Boston GC.

An individual is a legitimate architect value when the source labels them in
the role -- a sole practitioner really is the architect of record. What the
rule forbids is expanding that person into their employer, which never
happens here: the value is stored exactly as the record states it.
"""
import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from provenance import connect, looks_like_person, record

CAMB_URL = "https://data.cambridgema.gov/resource/9qm7-wbdc.json"
COMPLIANCE_URL = ("https://data.boston.gov/dataset/"
                  "boston-jobs-policy-compliance-reports")


def split_firm_person(v):
    """'CDM SMITH - MARK J SALVETTI' -> ('CDM SMITH', 'MARK J SALVETTI')."""
    if " - " in v:
        a, b = v.split(" - ", 1)
        if looks_like_person(b.strip()) and not looks_like_person(a.strip()):
            return a.strip(), b.strip()
    return v.strip(), None


def cambridge(c):
    m = json.loads(Path("data/cambridge_step3_matches.json").read_text())
    n = 0
    for pid, v in m.items():
        pid = int(pid)
        cands = [p for p in v["permits"] if (p.get("architect_firm") or "").strip()]
        if not cands:
            continue
        cands.sort(key=lambda p: float(p.get("total_cost_of_construction") or 0),
                   reverse=True)
        best = cands[0]
        firm, person = split_firm_person(best["architect_firm"].strip())
        others = sorted({p["architect_firm"].strip() for p in cands[1:]}
                        - {best["architect_firm"].strip()})
        is_person = looks_like_person(firm)
        stmt = (f'Cambridge New Construction permit {best.get("id")} for '
                f'{best.get("full_address")}: architect_firm = "{best["architect_firm"]}"'
                + (f', architect_name = "{best["architect_name"]}"'
                   if best.get("architect_name") else ""))
        reason = None
        if is_person:
            reason = ("permit names an individual; stored as given and not "
                      "expanded to an employer")
        if others:
            reason = (reason + " | " if reason else "") + \
                     "other permits at this address name: " + "; ".join(others)
        record(c, pid, "architect", value=firm, outcome="resolved",
               tier="registry_confirmed", source_type="permit",
               source_url=f"{CAMB_URL}?id={best.get('id')}",
               source_name="Cambridge Building Permits: New Construction (9qm7-wbdc)",
               source_date=(best.get("issue_date") or "")[:10],
               page_ref=f"permit id {best.get('id')}",
               firm_sentence=stmt,
               address_sentence=f'full_address = "{best.get("full_address")}"',
               resolution_step=3, reason=reason)
        if person or is_person:
            c.execute("update projects set architect_person=? where id=?",
                      (person or firm, pid))
        n += 1
    print(f"Cambridge architects recorded: {n}")
    return n


def boston_gc(c):
    m = json.loads(Path("data/gc_compliance_matches.json").read_text())
    n = 0
    for pid, v in m.items():
        pid = int(pid)
        named = [r for r in v["records"] if r.get("gc")]
        if not named:
            continue
        named.sort(key=lambda r: r.get("rows") or 0, reverse=True)
        best = named[0]
        others = sorted({r["gc"] for r in named[1:]} - {best["gc"]})
        stmt = (f'Boston Jobs Policy Compliance Report, project '
                f'"{best["project"]}" at {best["address"]}: '
                f'general_contractor_name = "{best["gc"]}" '
                f'({best["rows"]} worker-period rows, {best["first"]} to {best["last"]})')
        reason = ("other general contractors also reported on this project: "
                  + "; ".join(others)) if others else None
        record(c, pid, "general_contractor", value=best["gc"], outcome="resolved",
               tier="registry_confirmed", source_type="compliance_report",
               source_url=COMPLIANCE_URL,
               source_name="Boston Jobs Policy Compliance Reports",
               source_date=best.get("last"),
               page_ref=f'compliance project "{best["project"]}"',
               firm_sentence=stmt,
               address_sentence=f'project_address = "{best["address"]}" '
                                f'(matched by {v["how"]})',
               resolution_step=3, reason=reason)
        n += 1
    print(f"Boston GCs recorded: {n}")
    return n


def main():
    c = connect()
    a = cambridge(c)
    b = boston_gc(c)
    c.commit()
    tot = c.execute("select count(*) from field_provenance where superseded=0").fetchone()[0]
    print(f"checkpointed. live provenance rows: {tot}")


if __name__ == "__main__":
    main()
