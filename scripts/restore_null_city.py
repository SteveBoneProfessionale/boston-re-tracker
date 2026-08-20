"""Leave the 13 null-city rows exactly as they were found.

They were excluded from this run by instruction. The bulk stage rule that
parks a general contractor as not_yet_selected did not filter on city, so it
reached them; this puts them back and removes the provenance rows written
against them, so "skipped" means skipped.
"""
import sqlite3

FIELDS = ("architect", "civil_engineer", "general_contractor")


def main():
    c = sqlite3.connect("data/boston_re.db")
    b = sqlite3.connect("data/_baseline_prerun.db")
    b.row_factory = sqlite3.Row
    ids = [r[0] for r in c.execute(
        "select id from projects where city is null and coalesce(excluded,0)=0")]
    print(f"{len(ids)} null-city rows")
    n = 0
    for pid in ids:
        base = b.execute("select architect,civil_engineer,general_contractor "
                         "from projects where id=?", (pid,)).fetchone()
        for f in FIELDS:
            c.execute(f"update projects set {f}=? where id=?", (base[f], pid))
        n += c.execute("delete from field_provenance where project_id=?", (pid,)).rowcount
        c.execute("delete from field_evidence where project_id=?", (pid,))
        c.execute("delete from prior_value_audit where project_id=?", (pid,))
    c.commit()
    print(f"restored to baseline; removed {n} provenance rows")


if __name__ == "__main__":
    main()
