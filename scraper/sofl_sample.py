"""Regenerate south_florida_sample.md -- a readable spot-check of the market.

The sample exists so the data can be audited without opening the app: ten
projects per city with every stored field and every source URL behind them, so
a reader can pick a row, follow a link, and see whether the tracker says what
the document says.

The selection is random with a FIXED SEED, which matters more than it sounds.
Hand-picking ten rows per city would show the ten best rows; seeding the
shuffle means the sample includes the thin permit rows alongside the named
towers, and regenerating it after a research pass compares like with like
rather than reshuffling into a different ten.

Blank fields print as one of three things, never as an empty space:

    not found        researched and not published anywhere acceptable
    none             a stated zero -- an office tower with no residences
    not applicable   the asset class has no dwellings in it

That split is the point. A warehouse with no unit count and a tower whose unit
count nobody has published are both blank in the database and are not the same
fact, and a sample that renders them alike invites the reader to count 161
correct blanks as 161 gaps.

    python -m scraper.sofl_sample
"""

import os
import random
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

DB = os.path.join(ROOT, "data", "boston_re.db")
OUT = os.path.join(ROOT, "south_florida_sample.md")

CITIES = ["Miami", "Miami Beach", "West Palm Beach", "Coral Gables", "Doral"]
PER_CITY = 10
SEED = 20260301

# Asset classes with no dwellings in them, kept in step with the same set in
# app/tabs/project_table.py. Institutional and Other are deliberately absent:
# a dormitory is institutional and does have units.
NO_DWELLINGS = {"Retail", "Office", "Industrial", "Parking", "Lab"}

# The size floor the app applies by default, kept in step with
# app/data.py::SIZE_FLOOR_UNITS / SIZE_FLOOR_GSF. Coverage figures are quoted
# ABOVE the floor because that is the population the view describes: the City
# of Miami permit feed's long tail of sub-25,000 sq ft buildings dragged every
# percentage down without being gaps, since nobody publishes a lender or a
# height for them and they have no project name to search.
FLOOR_SQL = "(coalesce(residential_units,0) >= 50 or coalesce(total_gsf,0) >= 50000)"

# How an empty lender is labelled. See scraper/sofl_lender_status.py.
LENDER_LABEL = {"no_loan_reported": "no construction loan reported",
                "lender_unnamed": "loan reported, lender not named"}

FIELDS = ("id, name, address, parcel_id, parcel_id_aliases, status, lender_status, "
          "project_status_filing, asset_class, neighborhood, developer, "
          "architect, general_contractor, civil_engineer, lender, "
          "residential_units, hotel_rooms, building_height_ft, num_stories, "
          "total_gsf, completion_basis, completion_date, notes")


def _text(v):
    return str(v).strip() if v not in (None, "") else "not found"


def _num(v, suffix=""):
    if v is None:
        return None
    return f"{int(v):,}{suffix}" if float(v) == int(v) else f"{v:,.2f}{suffix}"


def _units(row):
    """The unit count, or which kind of blank it is."""
    if row["residential_units"] is not None:
        if int(row["residential_units"]) == 0:
            return "none (no residential component)"
        return _num(row["residential_units"])
    if (row["asset_class"] or "") in NO_DWELLINGS:
        return "not applicable"
    return "not found"


def _height(row):
    """Height in feet, or the storey count as the scale that IS known."""
    if row["building_height_ft"] is not None:
        return _num(row["building_height_ft"], " ft")
    if row["num_stories"] is not None:
        return f"not found ({int(row['num_stories'])} storeys known)"
    return "not found"


def _sources(con, pid):
    rows = con.execute(
        "select field_name, filing_name, pdf_url from extraction_sources "
        "where project_id=? order by field_name", (pid,)).fetchall()
    seen = {}
    for field, name, url in rows:
        if url:
            seen.setdefault(url, name or "")
    return len(rows), seen


def _coverage(con):
    """Per-city counts for the two fields the pipeline is actually read for.

    Printed with its denominators split, because a bare "82 of 452 have a
    height" invites the wrong conclusion. 161 of those rows are warehouses,
    showrooms, car parks and retail pads from the City of Miami permit feed,
    where a missing unit count is not a missing fact. The honest denominator
    for a unit count is the rows that can have dwellings in them.
    """
    out = ["## Coverage", "",
           "Counted ABOVE THE SIZE FLOOR — projects with 50 or more residential units "
           "or 50,000 or more square feet, which is what the app shows by default. "
           "The rows below the floor are still in the database and one toggle away; "
           "they are left out here because the City of Miami permit feed catalogues "
           "every permit in the city, and its tail of small buildings has no published "
           "lender, height or project name to find.", "",
           "| City | Projects | Units known | of residential-bearing | Height in feet | "
           "Storeys | Hotel keys | Lender |",
           "| --- | --: | --: | --: | --: | --: | --: | --: |"]
    tot = [0, 0, 0, 0, 0, 0, 0]
    for city in CITIES:
        r = con.execute(
            "select count(*), sum(residential_units is not null), "
            "sum(coalesce(asset_class,'') not in ('Retail','Office','Industrial',"
            "'Parking','Lab')), sum(building_height_ft is not null), "
            "sum(num_stories is not null), sum(hotel_rooms is not null), "
            "sum(lender is not null) "
            f"from projects where city=? and {FLOOR_SQL}", (city,)).fetchone()
        vals = [v or 0 for v in r]
        tot = [t + v for t, v in zip(tot, vals)]
        out.append(f"| {city} | {vals[0]:,} | {vals[1]:,} | {vals[1]:,} of {vals[2]:,} | "
                   f"{vals[3]:,} | {vals[4]:,} | {vals[5]:,} | {vals[6]:,} |")
    out.append(f"| **All five** | **{tot[0]:,}** | **{tot[1]:,}** | "
               f"**{tot[1]:,} of {tot[2]:,}** | **{tot[3]:,}** | **{tot[4]:,}** | "
               f"**{tot[5]:,}** | **{tot[6]:,}** |")
    out += ["",
            "Height in feet is the thinnest field in this market and will stay that way: "
            "the trade press prints storey counts and prints feet only when a project "
            "reaches a board approval that states one. Where feet are unpublished the "
            "storey count usually is not, which is why both columns are shown. The FAA "
            "Digital Obstacle File was calibrated as a bulk source for the missing feet "
            "and REJECTED at a 53 per cent hit rate — see scraper/sofl_faa_heights.py; "
            "it would have written 395 ft onto the 1,049 ft Waldorf Astoria, that being "
            "a 2018 survey of the building demolished to make room for it.", ""]
    return out


def _programme(con):
    """Every project of real size, with its programme, in one table.

    The point of a pipeline is the big schemes, and in the per-city random
    sample they are a minority of the rows. This lists every project with at
    least 100 dwellings, 100 hotel keys or 200 feet of height, so the tall and
    the large can be read in one place and checked against each other.
    """
    rows = con.execute(
        "select name, city, status, asset_class, residential_units, hotel_rooms, "
        "building_height_ft, num_stories from projects "
        "where city in ({}) and (residential_units >= 100 or hotel_rooms >= 100 "
        "or building_height_ft >= 200) "
        "order by coalesce(building_height_ft, 0) desc, "
        "coalesce(residential_units, 0) desc".format(
            ",".join("?" * len(CITIES))), CITIES).fetchall()
    out = [f"## Programme — the {len(rows)} largest projects", "",
           "Every project with 100 or more dwellings, 100 or more hotel keys, or a "
           "published height of 200 feet or more. Ordered by height, then by unit count, "
           "so the rows with no published height fall to the bottom.", "",
           "| Project | City | Status | Class | Units | Keys | Height | Storeys |",
           "| --- | --- | --- | --- | --: | --: | --: | --: |"]
    for r in rows:
        units = _units(r)
        if units == "not found":
            units = "—"
        elif units.startswith("none"):
            # The column header already says Units, so the long form is
            # redundant in a table cell and costs the row its readability.
            units = "none"
        elif units == "not applicable":
            units = "n/a"
        keys = (_num(r["hotel_rooms"]) if r["hotel_rooms"] is not None
                and int(r["hotel_rooms"]) > 0 else "—")
        ht = (_num(r["building_height_ft"], " ft")
              if r["building_height_ft"] is not None else "—")
        st = _num(r["num_stories"]) if r["num_stories"] is not None else "—"
        name = (r["name"] or "").replace("|", "/")
        out.append(f"| {name} | {r['city']} | {r['status']} | {r['asset_class'] or '—'} "
                   f"| {units} | {keys} | {ht} | {st} |")
    out.append("")
    return out


def build(verbose=True):
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    out = ["# South Florida — spot-check sample", "",
           f"Ten projects per city, drawn at random with a fixed seed ({SEED}) so the "
           "selection is reproducible and a regenerated sample compares like with like. "
           "Every filled value below has a citation in the tracker's FIELD SOURCES panel; "
           "the distinct source URLs for each project are listed.", "",
           "A blank is one of three things and is never printed as an empty space: "
           "**not found** means researched and not published by any acceptable source; "
           "**none** is a stated zero, such as an office tower with no residences; "
           "**not applicable** means the asset class has no dwellings in it. A warehouse "
           "with no unit count and a tower whose unit count nobody has published are both "
           "blank in the database and are not the same fact.", ""]
    out += _coverage(con)
    out += _programme(con)
    out += ["## Random sample by city", ""]

    for city in CITIES:
        rows = con.execute(
            f"select {FIELDS} from projects where city=? and {FLOOR_SQL} order by id",
            (city,)).fetchall()
        picked = sorted(random.Random(SEED).sample(
            list(rows), min(PER_CITY, len(rows))), key=lambda r: r["name"] or "")
        out += [f"## {city} — random {len(picked)} of {len(rows)}", ""]
        for r in picked:
            status = r["status"] or "not found"
            if r["project_status_filing"]:
                # Dead is the existing withdrawn/denied field, so it is shown
                # alongside the stage the project died at rather than
                # replacing it.
                status = f"{status} — **{r['project_status_filing']}** (dead)"
            out.append(f"### {r['name']}")
            out.append(f"- **Address:** {_text(r['address'])}")
            pid_line = _text(r["parcel_id"])
            if r["parcel_id_aliases"]:
                pid_line += f"  (also {r['parcel_id_aliases']})"
            out.append(f"- **Parcel id:** {pid_line}")
            out.append(f"- **Status:** {status}")
            out.append(f"- **Asset class:** {_text(r['asset_class'])}   "
                       f"**Neighbourhood:** {_text(r['neighborhood'])}")
            out.append(f"- Developer: {_text(r['developer'])}   |   "
                       f"Architect: {_text(r['architect'])}   |   "
                       f"General contractor: {_text(r['general_contractor'])}   |   "
                       f"Civil engineer: {_text(r['civil_engineer'])}   |   "
                       f"Lender: {_text(r['lender'])}")
            prog = [f"Residential units: {_units(r)}"]
            if r["hotel_rooms"] is not None and int(r["hotel_rooms"]) > 0:
                prog.append(f"Hotel keys: {_num(r['hotel_rooms'])}")
            prog += [f"Height: {_height(r)}",
                     f"Stories: {_num(r['num_stories']) or 'not found'}",
                     f"Total sq ft: {_num(r['total_gsf']) or 'not found'}"]
            out.append("- " + "   |   ".join(prog))
            lend = r["lender"] if r["lender"] else LENDER_LABEL.get(
                (r["lender_status"] or "").strip(), "not found")
            out.append(f"- **Construction lender:** {lend}")
            if r["completion_basis"]:
                when = f"  ({r['completion_date']})" if r["completion_date"] else ""
                out.append(f"- **Completion basis:** {r['completion_basis']}{when}")
            n, srcs = _sources(con, r["id"])
            out.append(f"- **Sources ({n} field citations, {len(srcs)} distinct):**")
            for url, name in srcs.items():
                out.append(f"    - {name or 'source'} — {url}")
            if r["notes"]:
                out.append(f"- *Note:* {' '.join(r['notes'].split())}")
            out.append("")
    con.close()

    text = "\n".join(out).rstrip() + "\n"
    with open(OUT, "w", encoding="utf-8") as fh:
        fh.write(text)
    if verbose:
        print(f"wrote {OUT} ({len(text):,} chars, "
              f"{sum(1 for L in out if L.startswith('### '))} projects)")
    return text


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    build()
