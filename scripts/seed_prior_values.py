"""Carry every pre-run value into provenance so nothing reads as absent.

Values outside Boston were not checked against a primary document in this
run, so they enter as `unverified_prior`: kept, visibly weaker than a
document-confirmed value, and still eligible to be upgraded by the plan-set
batch or corroborated by search.
"""
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scraper"))
from provenance import connect, record

FIELDS = ("architect", "civil_engineer", "general_contractor")


def main():
    c = connect()
    base = sqlite3.connect("data/_baseline_prerun.db")
    base.row_factory = sqlite3.Row
    have = {(r["project_id"], r["field"]) for r in
            c.execute("select project_id,field from field_provenance")}
    n = 0
    for r in base.execute("select id,city,architect,civil_engineer,general_contractor "
                          "from projects where coalesce(excluded,0)=0"):
        for f in FIELDS:
            v = (r[f] or "").strip()
            if not v or v == "not_yet_selected" or (r["id"], f) in have:
                continue
            record(c, r["id"], f, value=v, outcome="resolved",
                   tier="unverified_prior", source_type="prior_value",
                   resolution_step=1,
                   reason="value present before this run; not verified against a "
                          "primary document in this pass")
            n += 1
    c.commit()
    print(f"seeded {n} pre-run values as unverified_prior")
    for row in c.execute("""select tier, count(*) from field_provenance
                            where superseded=0 and outcome='resolved'
                            group by 1 order by 2 desc"""):
        print("  ", tuple(row))


if __name__ == "__main__":
    main()
