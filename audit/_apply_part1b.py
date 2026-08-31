"""Approved follow-ups: Austin Street, Faneuil Gardens, and two restored flags.

    python audit/_apply_part1b.py            # dry run
    python audit/_apply_part1b.py --apply
"""
import argparse
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from sqlalchemy import text

from db.database import engine, init_db

PERMITS = "https://data.boston.gov/dataset/approved-building-permits"

AUSTIN = (
    "GSF RESTORED TO 790,000, THE WHOLE-SITE FIGURE. The read path now prefers the "
    "BPDA page's Gross Floor Area field, and on this row that field reads 126,000 -- "
    "which is ONE PARCEL, not the project. The same BPDA page contradicts its own "
    "field in its description: \"The project consists of four new mixed-use buildings "
    "collectively containing up to 790,000 sf of Gross Floor Area\". The Project "
    "Notification Form puts it at 800,290 sf across four buildings on a 221,095 sf "
    "site, against a page land figure of 53,959 sf -- so BOTH structured fields on "
    "the page describe a single assessing parcel while the row is the whole "
    "redevelopment. The 126,000 is kept in bpda_parcel_gsf and bpda_gsf_is_partial "
    "is set so the read path skips it. THE OTHER THREE BUILDINGS ARE NOT IN THIS "
    "DATABASE and should not be added: BPDA carries one project page for the site "
    "(austin-street-lots-redevelopment) and no per-building pages exist -- "
    "100-168-new-rutherford-avenue, austin-street-lots and new-rutherford-avenue all "
    "return 404. This is a single Article 80 filing whose four buildings are "
    "components, not separate projects, so this row is correctly the whole "
    "redevelopment."
)

FANEUIL = (
    "PHASE 1 VALUE KEPT, MASTER PLAN RECORDED SEPARATELY. total_gsf is set to "
    "141,950, the BPDA page figure for Phase 1, which is what this row is named. "
    "The 547,200 previously stored here is the whole five-phase redevelopment and "
    "moves to master_plan_total_gsf. The Chapter 121A filing states the phases as "
    "141,950 (Phase 1) + 95,200 + 133,000 + 100,000 + 77,000 = 547,150, against a "
    "stated project total of approximately 547,200 -- a 50 sf rounding difference. "
    "Phases 2 to 5 are not in this database."
)

PLAN = [
    (390, "Under Construction", "construction_observed",
     "COMPLETION FLAG RESTORED -- I CLEARED IT IN ERROR. I re-tested this row on an "
     "EXACT street-number match and found no permit at '55 India St', then cleared "
     "the flag as a bad join. That was wrong: the permit is at 51-59 INDIA ST, a "
     "range that contains 55, and the original rule matched it by coordinate "
     "proximity, street-name agreement and street-number OVERLAP -- a sounder rule "
     "than the exact match I replaced it with. At that address there is one erect "
     "permit and six trade permits, so the project is under construction on "
     "evidence rather than on permit issuance."),
    (106, "Under Construction", "construction_observed",
     "COMPLETION FLAG RESTORED -- I CLEARED IT IN ERROR, as at 55 India Street. The "
     "permit is at 275-279 MAVERICK ST, a range containing 279. That address carries "
     "a FOUNDATION permit (FND1817868, 2026-02-13) ahead of the erect permit, which "
     "is direct evidence that work began rather than merely that it was permitted."),
]

NOTES = [
    (385, "PERMITTED - NOT STARTED STANDS, against a weak contrary signal. The permit "
          "is at 366-380 STUART ST, a range containing 380, so the original match was "
          "sound and my exact-number test was wrong to reject it. That address does "
          "carry one further permit -- A1679132, an AMENDMENT to a long-form sprinkler "
          "permit, July 2025 -- but an amendment is a paperwork act, and it is "
          "outweighed by specific reporting: the Globe (2025-01-10) recorded Skanska "
          "with no firm start date, and as of September 2025 the project had neither "
          "anchor tenant nor financing. No foundation permit, no trade sequence, no CO."),
    (329, "FLAG LEFT CLEARED, BUT THE EVIDENCE IS GENUINELY AMBIGUOUS AND THIS ROW "
          "NEEDS A HUMAN. The original match was to a permit at 18 GARDNER ST by "
          "street-number ADJACENCY (within 4), not overlap -- the weakest of the three "
          "match types in use. 18 Gardner St carries a full construction sequence: "
          "erect permit 2025-07-03, then RAZE, ELECTRICAL, LVOLT, FA and temporary "
          "electrical service through late 2025. So something IS being built at 18 "
          "Gardner Street. Whether it is this project at 14 Gardner Street cannot be "
          "settled from the permit record, and 14 and 18 are both even, so the parity "
          "test does not separate them either."),
]


def main(dry):
    init_db()
    conn = engine.connect()

    cur = conn.execute(text("select total_gsf,bpda_gsf from projects where id=69")).first()
    print(f"  id=69  Austin Street   total_gsf {cur[0]:,} -> 790,000   "
          f"bpda_gsf {cur[1]:,} -> bpda_parcel_gsf, flagged partial")
    cur = conn.execute(text("select total_gsf from projects where id=146")).first()
    print(f"  id=146 Faneuil Gardens total_gsf {cur[0]:,} -> 141,950   "
          f"master_plan_total_gsf -> 547,200")
    for pid, st, basis, _ in PLAN:
        print(f"  id={pid:<5} -> {st} ({basis})")
    for pid, _ in NOTES:
        print(f"  id={pid:<5} -> note only, no field change")

    if dry:
        conn.close()
        return

    conn.execute(text(
        "update projects set total_gsf=790000, total_gsf_source='bpda_page_description', "
        "bpda_parcel_gsf=bpda_gsf, bpda_gsf_is_partial=1, "
        "notes=coalesce(notes,'')||:n where id=69"), {"n": " | " + AUSTIN})
    conn.execute(text(
        "update projects set total_gsf=141950, master_plan_total_gsf=547200, "
        "total_gsf_source='bpda_page', notes=coalesce(notes,'')||:n where id=146"),
        {"n": " | " + FANEUIL})
    for pid, st, basis, note in PLAN:
        conn.execute(text(
            "update projects set status=:s, completion_stage=:s, completion_basis=:b, "
            "completion_source_url=:u, notes=coalesce(notes,'')||:n where id=:i"),
            {"s": st, "b": basis, "u": PERMITS, "n": " | " + note, "i": pid})
    for pid, note in NOTES:
        conn.execute(text(
            "update projects set is_flagged=1, notes=coalesce(notes,'')||:n where id=:i"),
            {"n": " | " + note, "i": pid})
    conn.commit()
    print("\napplied")
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry=not ap.parse_args().apply)
