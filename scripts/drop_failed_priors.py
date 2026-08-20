"""Drop prior values that failed verification, and label the ones we could not check.

The rule is: a value survives only if a primary document names the firm AND
labels it in that role. A document we could not read is not a failure, so
those values are kept but tiered `unverified_prior` and reported separately
rather than silently passing as confirmed.
"""
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scraper"))
from provenance import connect, record

FIELDS = ("architect", "civil_engineer")
FAIL = ("role_not_labelled", "firm_absent")


def main():
    c = connect()
    base = sqlite3.connect("data/_baseline_prerun.db")
    base.row_factory = sqlite3.Row
    prior = {r["id"]: r for r in base.execute(
        "select id,architect,civil_engineer,general_contractor,city,"
        "processed_filing_name,processed_filing_url from projects "
        "where coalesce(excluded,0)=0")}

    dropped, kept_replaced, unverified = 0, 0, 0
    for r in c.execute("select * from prior_value_audit where verdict in "
                       "('role_not_labelled','firm_absent')").fetchall():
        liveq = c.execute("select * from field_provenance where project_id=? and field=? "
                          "and superseded=0", (r["project_id"], r["field"])).fetchone()
        if liveq is not None and liveq["outcome"] == "resolved":
            kept_replaced += 1          # a better value already took its place
            continue
        p = prior.get(r["project_id"])
        record(c, r["project_id"], r["field"], value=None, outcome="null",
               source_type="article80_pdf",
               source_url=p["processed_filing_url"] if p else None,
               source_name=p["processed_filing_name"] if p else None,
               resolution_step=1,
               reason=f'prior value "{r["prior_value"]}" failed verification '
                      f'({r["verdict"]}); dropped')
        dropped += 1

    # Boston values that never met a readable document.
    audited = {(r["project_id"], r["field"]) for r in
               c.execute("select project_id,field from prior_value_audit")}
    for pid, p in prior.items():
        if p["city"] != "Boston":
            continue
        for f in FIELDS:
            v = (p[f] or "").strip()
            if not v or (pid, f) in audited:
                continue
            liveq = c.execute("select 1 from field_provenance where project_id=? and "
                              "field=? and superseded=0", (pid, f)).fetchone()
            if liveq:
                continue
            record(c, pid, f, value=v, outcome="resolved", tier="unverified_prior",
                   source_type="prior_value", resolution_step=1,
                   reason="no readable primary document in the corpus; value carried "
                          "forward unverified")
            unverified += 1
    c.commit()
    print(f"dropped (failed verification, nothing better): {dropped}")
    print(f"failed but already replaced by a document value: {kept_replaced}")
    print(f"Boston values kept as unverified_prior:         {unverified}")


if __name__ == "__main__":
    main()
