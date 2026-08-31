"""Clear the nine parity-crossed completion flags, and settle the seven
description conflicts.

    python audit/_apply_part1c.py            # dry run
    python audit/_apply_part1c.py --apply
"""
import argparse
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from sqlalchemy import text

from db.database import engine, init_db

# ---------------------------------------------------------------- PARITY
# Every one of these was marked Complete on a Certificate of Occupancy issued
# to a building on the OTHER SIDE OF THE STREET. Odd and even street numbers
# face each other; 176 Lincoln Street was retired on a CO at 179.
#
# `status` was never overwritten by the permit matcher, so clearing
# completion_stage restores each row to the status its BPDA page states. That
# is the honest resting place: not a new claim, just the removal of a false one.
PARITY = {
    247: ("176 Lincoln Street", 179),
    261: ("41 Berkeley Street", 40),
    67:  ("9 Geneva Street", 12),
    133: ("North Station Gateway Project", 226),
    140: ("151 Liverpool Street", 152),
    224: ("1188 Bennington Street", 1209),
    254: ("7 Channel Center", 10),
    273: ("75 Tremont Street", 78),
    370: ("1857-1859 Dorchester Avenue", 1854),
}

# ---------------------------------------------------------------- DESCRIPTIONS
# (id, restored total_gsf, the quote from the page's own description)
RESTORE = [
    (328, 535000, "a total gross floor area of approximately 535,000 sf"),
    (237, 295020, "a new multifamily residential building with amenity space "
                  "comprising approximately 295,020 square feet"),
    (231, 409395, "replacing it with a life science campus totaling "
                  "approximately 409,395 square feet"),
    (119, 420800, "approximately 435 residential units in two buildings, "
                  "totaling 420,800 square feet"),
    (252, 742000, "the construction of a new, two (2)-building, approximately "
                  "742,000 square foot"),
]

# Rows where picking a winner would destroy a real distinction. Both figures
# are true and they answer different questions.
TWO_FIGURES = [
    (228, 330600, 684450,
     "total building area including the 353,910 sf existing structure",
     "88 BLACK FALCON AVENUE MEASURES TWO DIFFERENT THINGS AND BOTH ARE KEPT. The "
     "page's own description gives a four-storey vertical addition of approximately "
     "330,600 sf onto an existing three-storey structure of approximately 353,910 sf. "
     "330,600 + 353,910 = 684,510, which is the 684,450 previously stored -- so the "
     "old value was the TOTAL BUILDING AREA and the page's 571,500 field is neither. "
     "total_gsf now holds the NEW CONSTRUCTION (330,600), which is what a pipeline "
     "figure should measure, and component_gsf holds the total building area."),
    (246, 810000, 700000,
     "office/life-science component; the balance is an 85-unit residential building",
     "1170-1190 SOLDIERS FIELD ROAD LIKEWISE MEASURES TWO THINGS. The description "
     "gives four new buildings: three for office and life science totalling "
     "approximately 700,000 sf, plus one 85-unit residential building. So the "
     "previously stored 700,000 was the LAB COMPONENT and the page's 810,000 is the "
     "whole project. total_gsf now holds 810,000 and component_gsf the 700,000 lab "
     "figure. NOTE THIS ROW IS RETIRED as abandoned (2024), so this corrects the "
     "record rather than any pipeline total."),
]


def main(dry):
    init_db()
    conn = engine.connect()

    print("=== clearing nine parity-crossed completion flags ===")
    for pid, (nm, pnum) in PARITY.items():
        r = conn.execute(text(
            "select status, completion_stage, total_gsf from projects where id=:i"),
            {"i": pid}).first()
        print(f"  id={pid:<5}{nm[:30]:<32}{int(r[2] or 0):>9,}  "
              f"stage {r[1]} -> null, reverts to status '{r[0]}'")
        if dry:
            continue
        conn.execute(text("""
            update projects
               set completion_stage = null, completion_basis = null,
                   completion_date = null, is_flagged = 1,
                   notes = coalesce(notes,'') || :n
             where id = :i"""), {"i": pid, "n":
            f" | COMPLETION FLAG CLEARED, UNVERIFIED. This row was marked Complete on "
            f"a Certificate of Occupancy issued at number {pnum} on the same street, "
            f"while the project is at {nm.split()[0]}. Odd and even street numbers "
            f"face each other, so that CO belongs to the building opposite. The "
            f"original match required coordinate proximity, street-name agreement and "
            f"street-number adjacency within 4, but did NOT test parity. No claim is "
            f"made about this project's real stage: it reverts to the status its BPDA "
            f"page states and is flagged for verification."})

    print("\n=== restoring five values the page description supports ===")
    for pid, val, quote in RESTORE:
        r = conn.execute(text(
            "select name,total_gsf,bpda_gsf from projects where id=:i"), {"i": pid}).first()
        print(f"  id={pid:<5}{str(r[0])[:30]:<32}{int(r[1]):>9,} -> {val:>9,}   "
              f"field {int(r[2] or 0):,} -> bpda_parcel_gsf")
        if dry:
            continue
        conn.execute(text("""
            update projects
               set total_gsf = :v, total_gsf_source = 'bpda_page_description',
                   bpda_parcel_gsf = bpda_gsf, bpda_gsf_is_partial = 1,
                   notes = coalesce(notes,'') || :n
             where id = :i"""), {"v": val, "i": pid, "n":
            f" | GSF RESTORED TO {val:,}. The BPDA page's Gross Floor Area field reads "
            f"{int(r[2] or 0):,}, but the SAME PAGE's description says \"{quote}\". As "
            f"at Austin Street Lots, the structured field describes less than the "
            f"project does. The field value is kept in bpda_parcel_gsf and "
            f"bpda_gsf_is_partial is set so the read path skips it."})

    print("\n=== two rows that measure two things ===")
    for pid, main_v, comp_v, label, note in TWO_FIGURES:
        r = conn.execute(text(
            "select name,total_gsf from projects where id=:i"), {"i": pid}).first()
        print(f"  id={pid:<5}{str(r[0])[:30]:<32}{int(r[1]):>9,} -> {main_v:>9,}  "
              f"+ component {comp_v:,} ({label[:44]})")
        if dry:
            continue
        conn.execute(text("""
            update projects
               set total_gsf = :v, component_gsf = :c, component_gsf_label = :l,
                   total_gsf_source = 'bpda_page_description',
                   notes = coalesce(notes,'') || :n
             where id = :i"""),
            {"v": main_v, "c": comp_v, "l": label, "i": pid, "n": " | " + note})

    if not dry:
        conn.commit()
        print("\napplied")
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry=not ap.parse_args().apply)
