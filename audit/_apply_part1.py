"""Part One writes: construction status, On the Dot units.

Every status below is set from the project's OWN address's permit history in the
Boston Approved Building Permits dataset, re-checked with exact street-number
matching. Nothing here rests on the loose match that originally produced the
permit_active flags.

    python audit/_apply_part1.py            # dry run
    python audit/_apply_part1.py --apply
"""
import argparse
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from sqlalchemy import text

from db.database import engine

ROOT = pathlib.Path(__file__).parent
RULE = json.loads((ROOT / "_permit_rule.json").read_text(encoding="utf-8"))
SRC = "https://data.boston.gov/dataset/approved-building-permits"

# id -> (status, completion_stage, completion_basis, note)
PLAN = {
    114: ("Complete", "Complete", "co_issued",
          "COMPLETE. Certificate of Occupancy COO1344375 issued 2023-01-23 at this "
          "address. The row read Board Approved with a completion_stage of Under "
          "Construction; both were wrong and the CO settles it."),
    307: ("Under Construction", "Under Construction", "construction_observed",
          "UNDER CONSTRUCTION, and this resolves a self-contradiction: status read "
          "Board Approved while completion_stage read Under Construction. Four trade "
          "permits (ELECTRICAL, FA, LVOLT, PLUMBING) follow the erect permit at this "
          "exact address, which is work begun rather than merely permitted."),
    299: ("Under Construction", "Under Construction", "construction_observed",
          "UNDER CONSTRUCTION, resolving a status/stage contradiction. Five trade "
          "permits follow the erect permit at this exact address."),
    126: (None, "Under Construction", "construction_observed",
          "CONFIRMED UNDER CONSTRUCTION on evidence rather than on permit issuance: "
          "eight trade permits (ELECTRICAL, FA, PLUMBING) follow the erect permit."),
    225: (None, "Under Construction", "construction_observed",
          "CONFIRMED UNDER CONSTRUCTION: six trade permits follow the erect permit."),
    111: (None, "Under Construction", "construction_observed",
          "CONFIRMED UNDER CONSTRUCTION: four trade permits follow the erect permit."),
    350: ("Permitted - Not Started", "Permitted - Not Started", "permit_active",
          "PERMITTED, NOT STARTED. An erect permit and nothing else at this address "
          "-- no trade permit, no CO. The row previously read Under Review as status "
          "with Under Construction as stage; neither described a site with a permit "
          "and no work."),
    165: ("Permitted - Not Started", "Permitted - Not Started", "permit_active",
          "PERMITTED, NOT STARTED. Erect permit only at this address; no trade "
          "permit and no CO."),
    329: ("Board Approved", None, None,
          "COMPLETION FLAG CLEARED. The row carried completion_stage Under "
          "Construction on a permit_active basis, but re-checked against the exact "
          "street number there is NO erect permit at this address at all -- one "
          "unrelated permit. The original flag came from a loose address match. "
          "Status returns to Board Approved with no completion claim."),
    390: ("Board Approved", None, None,
          "COMPLETION FLAG CLEARED. No permit exists at this exact street number; "
          "the Under Construction claim came from a loose address match."),
    106: ("Board Approved", None, None,
          "COMPLETION FLAG CLEARED. No permit exists at this exact street number; "
          "the Under Construction claim came from a loose address match."),
    358: (None, None, None,
          "STATUS LEFT AS IS AND FLAGGED AS UNRESOLVED. The $116.9M erect permit "
          "ERT1017169 issued 2020-08-06 is still Open six years on. Three ELECTRICAL "
          "permits at this address ($150k, $40k, $2.5k) argue some activity, but "
          "there is NO plumbing, NO fire alarm and NO Certificate of Occupancy -- and "
          "the address also carries two special-event permits (2024) and a RAZE "
          "permit (2025-06-06), which is site activity without a building going up. "
          "IT DID NOT FINISH: a CO would exist. Whether it ever started cannot be "
          "settled from the permit record. NOTE THE NEAR MISS: a Certificate of "
          "Occupancy DOES exist at 6 STACK ST, a different building on the same "
          "street, and a looser address match would have recorded this project as "
          "complete on the strength of it."),
}

UNITS_NOTE = (
    "RESIDENTIAL UNITS SET TO NULL, NOT ZERO. The row read 0, which is a claim that "
    "the building contains no housing, and every unit total in the app was counting "
    "it as such. The approved On the Dot phase one contains 331 homes -- 237 "
    "market-rate and 94 senior -- across five buildings at 505 Dorchester Avenue, 65 "
    "Ellery Street and 75 Ellery Street. NO BPDA PAGE PUBLISHES A UNIT COUNT for any "
    "of the three, so the per-building split cannot be established and is NOT being "
    "guessed at. Null is the honest state: unknown, rather than none."
)


def main(dry):
    conn = engine.connect()
    n = 0
    for pid, (st, stage, basis, note) in PLAN.items():
        cur = conn.execute(text(
            "select name,status,completion_stage,completion_basis from projects "
            "where id=:i"), {"i": pid}).first()
        print(f"  id={pid:<5}{str(cur[0])[:30]:<32}status {str(cur[1])[:20]:<22}"
              f"-> {str(st or cur[1])[:22]:<24}stage -> {stage}")
        if dry:
            continue
        sets, params = [], {"i": pid, "n": " | " + note}
        if st is not None:
            sets.append("status=:s"); params["s"] = st
        sets.append("completion_stage=:cs"); params["cs"] = stage
        sets.append("completion_basis=:cb"); params["cb"] = basis
        sets.append("completion_source_url=:u"); params["u"] = SRC
        sets.append("notes=coalesce(notes,'')||:n")
        conn.execute(text(f"update projects set {', '.join(sets)} where id=:i"), params)
        n += 1

    print("\n  On the Dot residential_units 0 -> NULL:")
    for pid in (168, 170, 172):
        cur = conn.execute(text("select name,residential_units from projects where id=:i"),
                           {"i": pid}).first()
        print(f"    id={pid:<5}{str(cur[0])[:34]:<36}{cur[1]} -> None")
        if not dry:
            conn.execute(text(
                "update projects set residential_units=null, is_flagged=1, "
                "notes=coalesce(notes,'')||:n where id=:i"),
                {"n": " | " + UNITS_NOTE, "i": pid})
            n += 1

    if not dry:
        conn.commit()
        print(f"\n{n} rows written")
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry=not ap.parse_args().apply)
