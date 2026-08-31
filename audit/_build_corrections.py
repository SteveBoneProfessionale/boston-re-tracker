"""Build audit/structural_corrections.csv from findings 1-8.

READ ONLY against the database. Every GSF and land figure comes from
audit/_bpda_pages_20260831.json, which holds pages fetched from bostonplans.org
during this session -- not from the scraper's stored values.
"""
import csv
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from sqlalchemy import text

from db.database import engine

ROOT = pathlib.Path(__file__).parent
PAGES = json.loads((ROOT / "_bpda_pages_20260831.json").read_text(encoding="utf-8"))
FETCHED = {int(k): v for k, v in PAGES["fetched"].items()}
DATE = "2026-08-31"
H = ["project_id", "field", "current_value", "proposed_value",
     "source_url", "source_date", "confidence", "requires_filing_read"]
out = []


def add(pid, field, cur, prop, url, date, conf, filing):
    out.append(dict(zip(H, [pid, field, cur, prop, url, date, conf, filing])))


c = engine.connect()
db = {r[0]: r for r in c.execute(text(
    "select id,name,city,total_gsf,bpda_gsf,status,developer,residential_units,"
    "excluded,phase_group,notes,bpda_url from projects"))}

# ---------------------------------------------------------------- FINDING 8
# GSF corrections. The BPDA project page is Tier 1 under the hierarchy and each
# page below was retrieved this session. Rows where the page and the stored
# value already agree are omitted -- there is nothing to propose.
CONFIRMED = {
    168: "arithmetic proof: 487,400+510,900+388,200 = 1,386,500 exactly, and independently verified by you",
    170: "arithmetic proof",
    172: "arithmetic proof",
    182: "live fetch; sits outside the phase-one total, which the other three reconstruct exactly",
    363: "page 439,500 independently corroborated by Banker & Tradesman's 440,000 SF",
}
gsf_rows = 0
for pid, v in sorted(FETCHED.items(), key=lambda kv: -(kv[1]["db_total_gsf"] or 0)):
    gfa, stored = v["gfa"], v["db_total_gsf"]
    if not gfa or gfa == stored:
        continue
    add(pid, "total_gsf", stored, gfa, v["url"], DATE,
        "confirmed" if pid in CONFIRMED else "probable", "no")
    gsf_rows += 1

# land_sq_ft: new nullable column, value already parsed by the scraper and
# discarded. Proposed for every fetched page that publishes the field.
land_rows = 0
for pid, v in sorted(FETCHED.items()):
    if v["land"]:
        add(pid, "land_sq_ft", "(column does not exist)", v["land"], v["url"], DATE,
            "confirmed", "no")
        land_rows += 1

add("SCHEMA", "land_sq_ft", "(none)", "add nullable INTEGER; assign the already-parsed "
    "detail['land_sqft'] in bpda_scraper.py, which is currently dropped",
    "scraper/bpda_scraper.py:261", DATE, "confirmed", "no")
add("SCHEMA", "far", "(none)", "add computed column total_gsf / land_sq_ft; label as GROSS "
    "FAR against the BPDA land figure, which may not equal the zoning lot",
    "derived", DATE, "probable", "no")
add("CODE", "extract_projects.py prompt", "'gross square feet of entire project'",
    "ask for the GSF of the building at THIS address; return null where the document "
    "describes more than one building", "scraper/extract_projects.py:54", DATE,
    "confirmed", "no")
add("CODE", "app read path", "app reads total_gsf", "prefer bpda_gsf where present, fall "
    "back to total_gsf; corrects 45 rows at once and is reversible",
    "app/data.py", DATE, "confirmed", "no")

# ---------------------------------------------------------------- FINDING 1
for pid in (168, 170, 172):
    add(pid, "residential_units", db[pid][7], "(null - cannot allocate)",
        FETCHED[pid]["url"], DATE, "unresolved", "yes")
add("NOTE-1", "residential_units allocation", "0 on all three rows",
    "331 units total (237 market-rate + 94 senior) across the phase; NO BPDA page "
    "publishes a unit count, so the per-building split is not determinable without the "
    "Development Plan. Left null per instruction rather than distributed by guess.",
    "http://www.bostonplans.org/projects/development-projects/505-dorchester-avenue",
    DATE, "unresolved", "yes")

# ---------------------------------------------------------------- FINDING 3
add(372, "status", db[372][5], "Withdrawn - designation not renewed",
    "https://www.baystatebanner.com", "2019-10-17", "confirmed", "no")
add(372, "excluded", db[372][8], 1, "https://www.baystatebanner.com", "2019-10-17",
    "confirmed", "no")
add(372, "excluded_reason", "(null)",
    "BPDA declined to renew P-3 Partners' tentative designation on 2019-10-31, citing "
    "lack of firm financing. Removes 1,746,908 GSF from pipeline totals.",
    "https://www.baystatebanner.com", "2019-10-17", "confirmed", "no")
add(372, "notes", "designation expired January 2026",
    "CORRECTION: designation expired 2019-10-31, not January 2026. Bay State Banner "
    "2019-10-17 quotes BPDA Director Brian Golden; Boston Globe 2019-10-18; Bay State "
    "Banner confirmed the withdrawal again in May 2020.",
    "https://www.baystatebanner.com", "2019-10-17", "confirmed", "no")

# ---------------------------------------------------------------- FINDING 4
add(246, "status", db[246][5], "Abandoned", "https://www.universalhub.com", "2026-03-30",
    "confirmed", "no")
add(246, "excluded", db[246][8], 1, "https://www.universalhub.com", "2026-03-30",
    "confirmed", "no")
add(246, "excluded_reason", "(null)",
    "Three-building 700,000 SF lab/office plan abandoned in 2024 as the Boston life "
    "science market contracted. Harvard Crimson 2026-03-13; Universal Hub 2026-03-30. "
    "Removes 700,000 GSF from pipeline totals.",
    "https://www.universalhub.com", "2026-03-30", "confirmed", "no")
add(246, "developer", db[246][6],
    "National Development / Mount Vernon Company (JV; record entity 1170 SFR Associates, LLC)",
    "https://www.universalhub.com", "2026-03-30", "confirmed", "no")
add(246, "total_gsf", db[246][3], "(no change - CONFLICT, do not update)",
    FETCHED[246]["url"] if 246 in FETCHED else "", DATE, "unresolved", "yes")

# ---------------------------------------------------------------- FINDING 5
add(363, "status", db[363][5], "Cancelled", "https://www.bisnow.com", "2019-08-01",
    "confirmed", "no")
add(363, "excluded", db[363][8], 1, "https://www.bisnow.com", "2019-08-01",
    "confirmed", "no")
add(363, "excluded_reason", "(null)",
    "Weiner Ventures cancelled August 2019; MassDOT did not renew development rights. "
    "~$83M spent in predevelopment. Bisnow 2019-08 and 2021-03; Banker & Tradesman "
    "2024-01 on the Fish/Weiner settlement. Removes 689,000 GSF from pipeline totals.",
    "https://www.bisnow.com", "2019-08-01", "confirmed", "no")

# ---------------------------------------------------------------- FINDING 6
add("SCHEMA", "status vocabulary", "Under Construction covers both states",
    "add 'Permitted - Not Started'; permit_active must map to it, never to Under "
    "Construction. Add completion_basis 'construction_observed' as the only basis "
    "short of a CO that may set Under Construction.",
    "app/data.py:290", DATE, "confirmed", "no")
add(385, "status", db[385][5], "Permitted - Not Started (pending vocabulary)",
    "https://www.bostonglobe.com", "2025-01-10", "confirmed", "no")
add(385, "completion_stage", "Under Construction",
    "Permitted - Not Started (pending vocabulary)",
    "https://www.bostonglobe.com", "2025-01-10", "confirmed", "no")
add(385, "total_gsf", db[385][3], "(no change - 625,000 confirmed)",
    "https://www.bostonglobe.com", "2025-01-10", "confirmed", "no")
for pid in (358, 126, 225, 390, 106, 111, 165, 329, 350, 307, 299, 114):
    r = db.get(pid)
    if r:
        add(pid, "completion_stage", "Under Construction (basis=permit_active)",
            "review: permit issuance alone is not construction",
            "https://data.boston.gov/dataset/approved-building-permits", DATE,
            "probable", "no")

# ---------------------------------------------------------------- FINDING 2
add("SCHEMA", "master_plan_total_gsf", "(none)",
    "add nullable INTEGER on projects; populate ONLY from the PDA/master plan document, "
    "never from press", "schema", DATE, "confirmed", "no")
GROUPS = {"Suffolk Downs": (331, 332, 333, 334, 335, 382),
          "Seaport Square": (275, 276, 369, 277),
          "Allston Yards": (345, 346, 235, 270),
          "776 Summer Street": (267, 115)}
for g, ids in GROUPS.items():
    for pid in ids:
        if pid in db:
            add(pid, "phase_group", db[pid][9] or "(null)", g, "internal - site grouping",
                DATE, "confirmed", "no")
add("NOTE-2", "master_plan_total_gsf (Suffolk Downs Boston)", "(none)",
    "REQUIRES PDA READ. Five Boston phases sum to 10,520,000; Beachmont Square (Revere, "
    "id 382) adds 1,700,000; HYM publishes 16.2M site-wide across both municipalities. "
    "Do not populate from press. If the PDA gives a different Boston-side figure, flag "
    "rather than adjust the phase rows.", "BPDA PDA document", "", "unresolved", "yes")

# ---------------------------------------------------------------- FINDING 7
for pid, why in (
        (392, "Hudson MA, ~30 miles west; no Tier 1 source in the hierarchy covers it"),
        (779, "Providence RI; belongs to the separate RI pipeline. Its own note concedes "
              "total_gsf is web-sourced (Tier 3).")):
    add(pid, "out_of_scope", "(column does not exist)", f"1 - {why}", "internal", DATE,
        "confirmed", "no")
add("SCHEMA", "out_of_scope", "(none)",
    "add nullable BOOLEAN, separate from `excluded`, so Boston/Cambridge totals can drop "
    "these rows without asserting the project is dead", "schema", DATE, "confirmed", "no")

p = ROOT / "structural_corrections.csv"
with p.open("w", newline="", encoding="utf-8") as fh:
    w = csv.DictWriter(fh, fieldnames=H)
    w.writeheader()
    w.writerows(out)

print(f"wrote {p}  -- {len(out)} proposed changes")
print(f"  total_gsf corrections : {gsf_rows}")
print(f"  land_sq_ft population : {land_rows}")
import collections
for k, v in sorted(collections.Counter(r["confidence"] for r in out).items()):
    print(f"  confidence {k:<12}{v}")
print(f"  requires_filing_read=yes: {sum(1 for r in out if r['requires_filing_read']=='yes')}")
c.close()
