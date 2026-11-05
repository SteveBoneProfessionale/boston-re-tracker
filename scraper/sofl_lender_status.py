"""Label an empty lender field with WHY it is empty.

An empty lender meant three unrelated things and rendered as one blank:

  no_loan_reported   A source documents how the project IS financed -- sponsor
                     equity, presale deposits, conduit revenue bonds, a grant,
                     an owner-occupier building for itself -- or a source says
                     outright that no construction loan has been secured.
                     Rendered "No construction loan reported".

  lender_unnamed     A construction loan IS documented, and no acceptable
                     source names who made it. Rendered "lender not named".
                     These are the rows worth a county-records lookup, and
                     `report()` prints them with folio and address for exactly
                     that.

  NULL               Nothing was found. Rendered "not found".

"REPORTED" IS DOING REAL WORK IN THAT FIRST LABEL. No news of a loan is not
proof there is no loan. Melo Group's towers -- both Aria Reserve towers and
Downtown 6 -- show no facility in any acceptable outlet, and that is entirely
consistent with Melo self-funding its condominium towers from deposits. But no
source SAYS so. Those rows stay NULL, because "we found nothing" and "we found
out how it was paid for" are different claims and the second one needs a
citation. Several rows that an earlier pass would happily have called
equity-funded are NULL here for that reason.

Rows below the size floor are left alone: no lender work was done on them, so
NULL is the honest label and claiming otherwise would be worse than a blank.
"""

import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from db.database import get_session, init_db                 # noqa: E402
from db.models import Project, ExtractionSource              # noqa: E402
from scraper import sofl_changelog as clog                   # noqa: E402

NO_LOAN = "no_loan_reported"
UNNAMED = "lender_unnamed"

# ── a construction loan exists and nobody acceptable names the lender ───
# These are the eight for the county records: a recorded mortgage or a Notice
# of Commencement will name the lender outright.
UNNAMED_ROWS = {
    928: ("Security Benefit Life Insurance Corporation",
          "$243.3m construction loan, closed 1 Nov 2019",
          "Named only by The Real Deal, which returns HTTP 403 to every automated "
          "fetch. Bisnow independently confirms the $243.3m facility and its "
          "construction character but attributes the figure to TRD and does not name "
          "the lender. Borrowers were OKO Group, OB Group and Cain International.",
          "https://therealdeal.com/miami/2019/11/01/vlad-doronin-partners-score-243m-loan-for-edgewater-tower/"),
    995: ("Bank OZK",
          "$213.4m construction loan, originated Sept 2021, paid off Aug 2024",
          "Named only by The Real Deal. All five Florida YIMBY articles on 601 NE 1st "
          "Avenue were checked and none mentions a loan, lender or financing; no "
          "Commercial Observer or Bisnow article covers this financing.",
          "https://therealdeal.com/miami/2024/08/20/galbuts-pay-off-213m-construction-loan-for-miami-tower/"),
    1028: ("Citizens Bank",
           "$98.15m construction loan, arranged by Berkadia",
           "Named only by the South Florida Business Journal and its syndications. All "
           "three Florida YIMBY articles on the project were fetched -- 2022 plans, "
           "2024 topping out, 2025 finishing touches -- and none mentions financing; "
           "Commercial Observer, Bisnow and The Real Deal have nothing.",
           ""),
    1358: ("PNC Bank",
           "$80.6m construction financing, as a leasehold mortgage",
           "Named only by bldup, citybiz, cre-sources, multifamilybiz and yieldpro. "
           "Both Florida YIMBY articles were fetched, the July 2025 topping-out and "
           "the March 2026 nearing-completion piece, and neither names any lender.",
           ""),
    1324: ("not named by any source",
           "$94m construction loan, reported Jan 2026",
           "The cleanest case of the eight: Florida YIMBY states that an affiliate of "
           "Pacific Star Capital secured a $94 million construction loan, and the "
           "article was fetched in full -- the lender is simply not identified in it. "
           "No Commercial Observer, Bisnow or Real Deal article names it either.",
           "https://floridayimby.com/2026/01/mixed-use-71-nobe-planned-for-2027-completion-at-400-71st-st-miami-beach-florida.html"),
    1296: ("City National Bank of Florida",
           "$58m construction loan, reported Mar 2026",
           "Florida YIMBY confirms the facility and names no bank or financial "
           "institution anywhere in the article. The lender appears only in "
           "crenews.com, bldup, traded.co and a blog aggregator -- so this is not even "
           "a Real Deal-only finding, since none of the four acceptable outlets "
           "covered it.",
           "https://floridayimby.com/2026/03/developers-secure-construction-loan-for-the-oasis-at-doral-at-4090-n-w-97th-ave-doral-florida.html"),
    978: ("Mack Real Estate Credit Strategies",
          "$185m construction loan, September 2019, on the project then called "
          "Bayshore Grove",
          "Named only by The Real Deal, which cannot be fetched. Florida YIMBY's April "
          "2023 article on the address mentions no financing and there is no "
          "Commercial Observer or Bisnow coverage of the 2019 facility. CanAm "
          "Enterprises' $20m EB-5 redeployment INTO the existing Mack loan is a "
          "participation rather than the lender. Do not confuse with Mr. C Hotel & "
          "Residences WEST PALM BEACH, whose lender is Tyko Capital.",
          "https://therealdeal.com/miami/2019/09/26/terra-closes-on-185m-construction-loan-for-latest-coconut-grove-project/"),
    1207: ("New Wave Loans",
           "$21.5m construction loan, closed around 5 Jan 2026",
           "Named only by traded.co, a National Law Review press-release wire, "
           "einpresswire and multifamilypress. Florida YIMBY covers the project twice, "
           "in January 2023 and August 2024, and never mentions financing.",
           ""),
}

# ── the financing structure is documented; there is no construction loan ─
# Every entry names the source that documents it. A row whose financing I only
# INFER is deliberately absent and stays NULL.
NO_LOAN_ROWS = {
    1359: ("The $240m project reached financial close on 26 June 2025 entirely through "
           "a Palm Beach County CONDUIT REVENUE BOND issue on behalf of Provident "
           "Resources Group: $212.7m of senior bonds plus $23.1m of subordinate bonds, "
           "both 40-year and secured by project revenues. JPMorgan appears only as "
           "bond underwriter, which is an arranger role. There is no construction "
           "lender to find.",
           "https://commercialobserver.com/2025/06/palm-beach-atlantic-university-bond-dorm-student-housing-gilbane-florida/"),
    1242: ("Funded through the Miami-Dade County Educational Facilities Authority, "
           "which approved up to $500m of tax-exempt revenue bonds on behalf of the "
           "University of Miami to finance and refinance capital improvements across "
           "its campuses. That is a conduit bond programme rather than a project "
           "construction loan. Demolition is set for summer 2026 with construction "
           "immediately after, so a facility may yet be reported.",
           "https://www.miamidade.gov/govaction/legistarfiles/Matters/Y2024/241790.pdf"),
    1004: ("The only construction financing is $44m of multifamily mortgage revenue "
           "bonds authorised by the Housing Finance Authority of Miami-Dade County in "
           "February 2026. The Authority is the bond ISSUER rather than a lender, and "
           "no bank credit provider or bond purchaser is named. The 27,717 sq ft site "
           "was deeded to the developer by the county in 2020.",
           "https://www.bisnow.com/south-florida/news/affordable-housing/affordable-housing-project-wynwood-44m-public-construction-financing-133392"),
    1223: ("The $190m project is funded by a $130m loan from the U.S. Department of "
           "Housing and Urban Development plus a $27m grant and $15m of tax increment "
           "financing from the Omni Community Redevelopment Agency. No source "
           "characterises the HUD facility as construction financing or names an "
           "originating lender, and the grant and the tax increment financing are not "
           "loans at all.",
           "https://www.bisnow.com/news/south-florida/affordable-housing/nr-investments-groundbreaking-190-units-133494"),
    976: ("The only construction-side facility is a $200m C-PACE loan from Bayview "
          "PACE, closed January 2026. Commercial Observer calls it the first "
          "construction financing for the project, while Florida YIMBY describes the "
          "same facility as supporting qualifying energy-efficiency, water- "
          "conservation, building-envelope and resiliency components and completing "
          "the capital stack. It is not the construction lender, and the implication "
          "of its being the FIRST construction financing is that the vertical work "
          "from 2022 to 2026 was equity-funded.",
          "https://commercialobserver.com/2026/01/okan-tower-downtown-miami/"),
    1208: ("Banco Santander owns the site and is building the tower for its OWN "
           "OCCUPANCY, so there is no third-party construction lender to find. The "
           "only money publicly attached is equity and incentive rather than debt: a "
           "$6m State of Florida High-Impact Sector Performance Grant against a $630m "
           "Santander capital-investment commitment.",
           "https://floridayimby.com/2026/04/50-story-1401-brickell-by-santander-breaks-ground-in-miami.html"),
    1220: ("A City of Miami government building, financed by the City itself: the City "
           "Commission approved issuing up to $285m of bonds for the office. The $450m "
           "J.P. Morgan Chase loan reported at Miami Freedom Park finances Inter "
           "Miami's soccer stadium, a separate structure with a separate borrower, and "
           "is carried on the stadium's own row.",
           "https://www.miami.gov/My-Government/Departments/Department-of-Real-Estate-Asset-Management-DREAM/New-City-of-Miami-Administration-Building"),
    1356: ("Positively confirmed UNFINANCED at groundbreaking rather than merely "
           "unreported: Florida YIMBY's July 2026 groundbreaking article indicates the "
           "developers had not yet secured a construction loan as of that date. "
           "FrontRange Capital Partners is named as the capital and investment "
           "partner, which is equity rather than a lender. Worth re-checking after "
           "mid-2027.",
           "https://floridayimby.com/2026/07/construction-begins-on-alida-residences-and-tribute-portfolio-by-marriott-hotel-in-downtown-west-palm-beach.html"),
    1355: ("A lender said on the record that no construction facility existed: Florida "
           "YIMBY of September 2025 reports $62.5m from JVP Management explicitly as a "
           "PRE-construction loan, and quotes JVP saying the facility will support the "
           "project through its predevelopment phase and that it hopes to provide the "
           "construction financing in 2026. A $30m Windsor Private Capital facility "
           "that property records style a construction loan is a site-level mortgage "
           "on the 1.92-acre parcel, superseded five months later by that larger "
           "predevelopment loan.",
           "https://floridayimby.com/2025/09/the-berkeley-palm-beach-secures-62-5-million-pre-construction-loan-from-jvp-management-in-downtown-west-palm-beach.html"),
    1371: ("Philanthropically funded: the $150m expansion is paid for by grants and "
           "donations including $20m from Wendy and Howard Cox, $8m from Ken Griffin "
           "and $5m gifts from Leonard Lauder and the David H. Koch Foundation. "
           "Skanska's role is an $83m construction contract as general contractor, not "
           "financing. No debt facility exists.",
           ""),
}


def run(dry_run=False, verbose=True):
    init_db()
    db = get_session()
    entries = clog.load()
    counts = {NO_LOAN: 0, UNNAMED: 0}
    missing = []
    try:
        for pid, (note, url) in NO_LOAN_ROWS.items():
            row = db.get(Project, pid)
            if row is None:
                missing.append(pid)
                continue
            if row.lender:
                print(f"  SKIP {pid}: now carries a lender, {row.lender!r}")
                continue
            if row.lender_status != NO_LOAN:
                clog.record(entries, pid, row.name, "lender_status",
                            row.lender_status, NO_LOAN, url,
                            "a source documents how the project is financed, so the "
                            "empty lender is a finding rather than a gap")
            if not dry_run:
                row.lender_status = NO_LOAN
                _note(row, f"FINANCING: {note}")
                db.flush()
                if url:
                    _src(db, pid, "lender_status", "No construction loan reported",
                         "financing structure documented; see notes", url)
            counts[NO_LOAN] += 1

        for pid, (who, facility, why, url) in UNNAMED_ROWS.items():
            row = db.get(Project, pid)
            if row is None:
                missing.append(pid)
                continue
            if row.lender:
                print(f"  SKIP {pid}: now carries a lender, {row.lender!r}")
                continue
            if row.lender_status != UNNAMED:
                clog.record(entries, pid, row.name, "lender_status",
                            row.lender_status, UNNAMED, url,
                            "a construction loan is documented but no acceptable "
                            "source names the lender")
            if not dry_run:
                row.lender_status = UNNAMED
                _note(row, f"LENDER NOT NAMED. Facility: {facility}. "
                           f"Reported as {who}. {why}")
                db.flush()
            counts[UNNAMED] += 1
        if not dry_run:
            db.commit()
    finally:
        db.close()
    if not dry_run:
        clog.save(entries)
    if verbose:
        print(f"{'(dry run) ' if dry_run else ''}"
              f"{counts[NO_LOAN]} rows labelled 'No construction loan reported', "
              f"{counts[UNNAMED]} labelled 'lender not named'")
        if missing:
            print("  missing rows:", missing)
    return counts


def _note(row, text):
    cur = (row.notes or "").strip()
    if text not in cur:
        row.notes = f"{cur} {text}".strip() if cur else text


def _src(db, pid, field, value, filing, url):
    db.query(ExtractionSource).filter(
        ExtractionSource.project_id == pid,
        ExtractionSource.field_name == field).delete(synchronize_session=False)
    db.add(ExtractionSource(project_id=pid, field_name=field, field_value=value,
                            filing_name=filing, filing_date="", pdf_url=url))


def report():
    """The county-records worklist: a loan exists, nobody acceptable names it."""
    import sqlite3
    con = sqlite3.connect(os.path.join(ROOT, "data", "boston_re.db"))
    con.row_factory = sqlite3.Row
    out = ["# Lenders to look up in county records", "",
           "A construction loan is documented for each of these and no acceptable "
           "source names who made it. A recorded mortgage or a Notice of Commencement "
           "will name the lender outright. Nothing further was researched on them.", ""]
    for pid in sorted(UNNAMED_ROWS):
        r = con.execute("select id,name,address,city,parcel_id,permit_number,status,"
                        "total_gsf from projects where id=?", (pid,)).fetchone()
        if r is None:
            continue
        who, facility, why, url = UNNAMED_ROWS[pid]
        county = ("Miami-Dade" if r["city"] in ("Miami", "Miami Beach", "Doral",
                                                "Coral Gables") else "Palm Beach")
        out += [f"## {r['name']}  (id {r['id']})", "",
                f"- **Address:** {r['address']}, {r['city']}",
                f"- **Folio / PCN:** {r['parcel_id'] or 'not resolved'}  ({county} County)",
                f"- **Master permit:** {r['permit_number'] or 'none on record'}",
                f"- **Status:** {r['status']}"
                + (f"   **Floor area:** {r['total_gsf']:,} sq ft" if r["total_gsf"] else ""),
                f"- **Facility:** {facility}",
                f"- **Lender reported as:** {who}",
                f"- **Why not stored:** {why}"]
        if url:
            out += [f"- **Reference:** {url}"]
        out += [""]
    con.close()
    text = "\n".join(out).rstrip() + "\n"
    path = os.path.join(ROOT, "lenders_for_county_records.md")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text)
    print(f"wrote {path} ({len(UNNAMED_ROWS)} projects)")
    return text


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--report", action="store_true")
    a = ap.parse_args()
    if a.report:
        report()
    else:
        run(dry_run=a.dry_run)
