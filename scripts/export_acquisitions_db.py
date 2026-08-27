r"""Export the transactions table to a READ-ONLY database the app never writes.

THE PROBLEM THIS SOLVES. data/boston_re.db is committed to the repo AND written
to at runtime -- the news scheduler inserts articles every three hours, and
init_db() can issue DDL on startup. On Streamlit Cloud that leaves the file
permanently modified in the deployment's git working tree, so a pull will not
overwrite it. The result is a deployed database frozen at whatever revision the
clone was created from, while every other file updates normally. That is exactly
what happened here:

    sqlite3.OperationalError: no such table: transactions

against a GitHub copy that has the table with 793 live rows. Renaming the file
would fix it once and then break again on the next data push, because the new
name would be written at runtime too and go stale the same way.

THE FIX. Acquisitions data lives in its own database that NOTHING WRITES. It is
generated here from the main database, committed, and only ever read. A file git
sees as clean is a file git will happily update, so every future push lands.

Run this after any change to the transactions table and before pushing:

    python scripts/export_acquisitions_db.py

The app falls back to data/boston_re.db if this file is missing, so a forgotten
export degrades to the old behaviour rather than an empty tab.
"""

import shutil
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
SRC = ROOT / "data" / "boston_re.db"
DST = ROOT / "data" / "acquisitions.db"


def main() -> int:
    if not SRC.exists():
        print(f"source database not found: {SRC}")
        return 1

    tmp = DST.with_suffix(".db.tmp")
    if tmp.exists():
        tmp.unlink()

    src = sqlite3.connect(f"file:{SRC}?mode=ro", uri=True)
    names = [r[0] for r in src.execute(
        "select name from sqlite_master where type='table'")]
    if "transactions" not in names:
        print("source database has no transactions table")
        src.close()
        return 1

    # sqlite's own backup API would copy all 16 tables. Only transactions is
    # wanted, so the table is recreated from its stored DDL and copied row by
    # row -- which also keeps the exported file small.
    ddl = src.execute(
        "select sql from sqlite_master where type='table' and name='transactions'"
    ).fetchone()[0]

    dst = sqlite3.connect(tmp)
    dst.execute(ddl)
    cols = [r[1] for r in src.execute("PRAGMA table_info(transactions)")]
    placeholders = ",".join("?" * len(cols))
    rows = src.execute("select * from transactions").fetchall()
    dst.executemany(f"insert into transactions values ({placeholders})", rows)
    for idx, in src.execute(
            "select sql from sqlite_master where type='index' "
            "and tbl_name='transactions' and sql is not null"):
        dst.execute(idx)
    dst.commit()

    live = dst.execute(
        "select count(*) from transactions where coalesce(quarantined,0)=0"
    ).fetchone()[0]
    quar = dst.execute(
        "select count(*) from transactions where coalesce(quarantined,0)=1"
    ).fetchone()[0]
    resolved = dst.execute(
        "select count(*) from transactions where coalesce(quarantined,0)=0 "
        "and coalesce(buyer_canonical,'') <> ''").fetchone()[0]
    dst.execute("VACUUM")
    dst.close()
    src.close()

    shutil.move(str(tmp), str(DST))
    print(f"wrote {DST.relative_to(ROOT)}")
    print(f"  {len(rows)} rows, {len(cols)} columns, "
          f"{DST.stat().st_size / 1048576:.2f} MB")
    print(f"  {live} live, {quar} quarantined, {resolved} with a resolved buyer")
    return 0


if __name__ == "__main__":
    sys.exit(main())
