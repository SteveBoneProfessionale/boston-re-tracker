"""Apply ONLY the rows marked confidence=confirmed in structural_corrections.csv.

The 56 `probable` and 6 `unresolved` rows are held, untouched, by explicit
instruction. total_gsf is never cleared: where a confirmed correction exists it
is overwritten with the BPDA page figure, and the previous value is preserved in
the row's notes so nothing is lost.

    python audit/_apply_confirmed.py            # dry run
    python audit/_apply_confirmed.py --apply
"""
import argparse
import csv
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from sqlalchemy import text

from db.database import engine, init_db

ROOT = pathlib.Path(__file__).parent
DATE = "2026-08-31"


def headline(conn, label):
    """Pipeline SF exactly as app/data.py computes it, under current precedence."""
    rows = list(conn.execute(text(
        "select id, total_gsf, bpda_gsf, status, coalesce(excluded,0), "
        "coalesce(out_of_scope,0) from projects")))
    DEAD = ("Complete", "Delivered", "Abandoned", "Cancelled",
            "Withdrawn - designation not renewed")
    live = [r for r in rows if not r[4] and not r[5] and (r[3] or "") not in DEAD]
    old = sum(r[1] or r[2] or 0 for r in live)      # total_gsf first (old order)
    new = sum(r[2] or r[1] or 0 for r in live)      # bpda_gsf first (new order)
    print(f"  [{label}] live rows {len(live)}   "
          f"total_gsf-first {old:,}   bpda_gsf-first {new:,}")
    return len(live), old, new


def main(dry):
    # Schema migration runs in both modes: it is idempotent, adds only nullable
    # columns, and the before/after measurement below has to be able to read
    # out_of_scope. No row data is touched by it.
    init_db()
    conn = engine.connect()
    rows = [r for r in csv.DictReader((ROOT / "structural_corrections.csv")
                                      .open(encoding="utf-8"))
            if r["confidence"] == "confirmed"]
    data = [r for r in rows if r["project_id"].isdigit()]
    print(f"confirmed rows: {len(rows)}  ({len(data)} touch a project row)")

    print("\nBEFORE:")
    before = headline(conn, "before")

    applied, skipped = 0, []
    for r in data:
        pid = int(r["project_id"])
        f, v = r["field"], r["proposed_value"]
        if str(v).startswith("(no change") or str(v).startswith("(null - cannot"):
            skipped.append((pid, f, "explicitly no-change"))
            continue
        if f == "total_gsf":
            old = conn.execute(text("select total_gsf from projects where id=:i"),
                               {"i": pid}).scalar()
            note = (f" | GSF CORRECTED {old:,} -> {int(v):,} on {DATE} from the BPDA "
                    f"project page ({r['source_url']}), which publishes Gross Floor "
                    f"Area per building. The superseded {old:,} came from an LLM "
                    f"extraction asked for the 'entire project', which returns the "
                    f"PHASE total on a component parcel. Prior value retained here.")
            if not dry:
                conn.execute(text(
                    "update projects set total_gsf=:v, total_gsf_source='bpda_page', "
                    "notes=coalesce(notes,'')||:n where id=:i"),
                    {"v": int(v), "n": note, "i": pid})
        elif f == "land_sq_ft":
            if not dry:
                conn.execute(text("update projects set land_sq_ft=:v where id=:i"),
                             {"v": int(v), "i": pid})
        elif f == "out_of_scope":
            if not dry:
                conn.execute(text(
                    "update projects set out_of_scope=1, "
                    "excluded_reason=coalesce(excluded_reason,'')||:n where id=:i"),
                    {"n": f" | OUT OF SCOPE: {v}", "i": pid})
        elif f == "excluded":
            if not dry:
                conn.execute(text("update projects set excluded=:v where id=:i"),
                             {"v": int(v), "i": pid})
        elif f == "excluded_reason":
            if not dry:
                conn.execute(text(
                    "update projects set excluded_reason=:v where id=:i"),
                    {"v": v, "i": pid})
        elif f == "notes":
            if not dry:
                conn.execute(text(
                    "update projects set notes=coalesce(notes,'')||:n where id=:i"),
                    {"n": " | " + v, "i": pid})
        elif f in ("status", "completion_stage", "phase_group", "developer"):
            val = v.replace(" (pending vocabulary)", "")
            if not dry:
                conn.execute(text(f"update projects set {f}=:v where id=:i"),
                             {"v": val, "i": pid})
                if f == "status":
                    conn.execute(text(
                        "update projects set notes=coalesce(notes,'')||:n where id=:i"),
                        {"n": f" | STATUS SET to '{val}' on {DATE}. Source: "
                              f"{r['source_url']} ({r['source_date']}).", "i": pid})
        else:
            skipped.append((pid, f, "no handler"))
            continue
        applied += 1

    if not dry:
        conn.commit()

    print(f"\napplied {applied}   skipped {len(skipped)}")
    for s in skipped:
        print(f"  skipped id={s[0]} {s[1]} -- {s[2]}")

    print("\nAFTER:")
    after = headline(conn, "after")
    conn.close()
    return before, after


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry=not ap.parse_args().apply)
