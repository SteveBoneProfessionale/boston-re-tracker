"""Park the general contractor on every project that has not broken ground.

A GC does not exist before construction starts, so searching for one on a
project still in Planning or Permitting cannot succeed. Those are recorded
as not_yet_selected without spending a search; only projects where
construction has started stay on the search list, and only those count
against the null denominator.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scraper"))
from provenance import connect, construction_started, record


def main():
    c = connect()
    rows = c.execute("select id,city,status,completion_stage from projects "
                     "where coalesce(excluded,0)=0").fetchall()
    live = {r["project_id"]: r for r in c.execute(
        "select * from field_provenance where field='general_contractor' "
        "and superseded=0")}
    n = 0
    for r in rows:
        if construction_started(r):
            continue
        l = live.get(r["id"])
        if l is not None and l["outcome"] in ("resolved", "not_yet_selected"):
            continue
        record(c, r["id"], "general_contractor", value=None,
               outcome="not_yet_selected", source_type="stage_rule",
               resolution_step=3,
               reason=f'construction has not started (status "{r["status"]}", '
                      f'completion_stage "{r["completion_stage"]}"); a general '
                      f'contractor does not exist yet')
        n += 1
    c.commit()
    print(f"parked {n} projects as not_yet_selected")
    for row in c.execute("""select outcome, count(*) from field_provenance
                            where field='general_contractor' and superseded=0
                            group by 1 order by 2 desc"""):
        print("  ", tuple(row))


if __name__ == "__main__":
    main()
