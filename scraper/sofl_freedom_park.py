"""Split the Miami Freedom Park records, which describe four different things.

THE CONFLATION
Three rows carried the SAME developer and the SAME architect, and the note on
each admitted where that came from: "Florida YIMBY's coverage of the master
plan is specifically about its eight-storey administration building." So one
article about the City's administration building had seeded the developer and
architect of the stadium, the garage and the commercial component alike.

WHAT THE PERMITS ACTUALLY SAY
Every permit on the 130-acre site was swept. The decisive records:

  BD25026033001B001  1896 NW 37 AV   724,470 sf  Moss & Associates
                     NEW CONSTRUCTION, work items PROFESSIONAL OFFICE / BANK
                     issued 2026-07-01, Active, no certificate, requires CO
      -> the CITY OF MIAMI ADMINISTRATION BUILDING, not Miami Freedom Park
         LLC's commercial space. Three independent locks: the City's own
         topping-out notice gives the Civic Center's address as exactly this
         permit's delivery address; the notice names Moss Construction, which
         is the permit's CompanyName; and the permit's issue date and cost,
         2026-07-01 and $193,000,000, match the trade press to the day and the
         dollar.

  BD23030080001B001  1000 STADIUM DR  920,470 sf  Lemartec, TCO 2026-04-10
      -> the stadium.

  BD24019509001B001  4000 NW 14 ST  1,372,696 sf  Finfrock, TCO 2026-04-10
      -> NOT the park, despite the row's name. Its work items are CHAIN LINK
         FENCE | POLE / BOLLARDS | PARKING GARAGE-MULTIPLE FLOORS and its
         contractor is a precast parking-structure builder. This is the
         stadium PARKING GARAGE.

  BD25003093001B001  1550 NW 37 AV  1,916,640 sf  LTG Sports Turf One
                     work items NEW PARK ONLY, issued 2026-03-10, no
                     certificate yet (requires CC)
      -> the actual park, which no row tracks.

AND THE ONE WITH NO PERMIT AT ALL
Miami Freedom Park LLC's Phase 1 office building, branded One Heron, has no
building permit anywhere on the site: BD25026033001B001 is the only permit of
any scope on any of the site's folios whose work items mention office, and
that one is the City's. The Real Deal of 10 April 2026 has One Heron still in
final design. So it is Planning, it gets its own row, and it inherits NOTHING
from the Civic Center -- not Adler, not Arquitectonica, not Moss. None of the
three is attested for it.

The appraiser cannot arbitrate any of this: the whole site is City of Miami
fee land (folios 0131320000090 and 0131320000095, TRUE_OWNER1 CITY OF MIAHI),
and Miami Freedom Park LLC holds a 99-year leasehold that never appears as
owner of record.

    python -m scraper.sofl_freedom_park --dry-run
    python -m scraper.sofl_freedom_park
"""

import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from db.database import get_session, init_db                  # noqa: E402
from db.models import Project, ExtractionSource               # noqa: E402
from scraper import sofl_changelog as clog                    # noqa: E402

CITY_NOTICE = ("https://www.miami.gov/Notices/Events-Activities/"
               "City-of-Miami-to-Celebrate-the-Topping-Out-of-the-New-Miami-Civic-Center")
FY_ADMIN = ("https://floridayimby.com/2025/01/city-of-miami-to-break-ground-on-"
            "eight-story-administration-building-at-miami-freedom-park.html")
TRD_ONE_HERON = ("https://therealdeal.com/miami/2026/04/10/"
                 "miami-freedom-park-retail-construction-kicks-off/")
DREAM = ("https://www.miami.gov/My-Government/Departments/"
         "Department-of-Real-Estate-Asset-Management-DREAM/"
         "New-City-of-Miami-Administration-Building")

CIVIC_NOTE = (
    "The City of Miami's own administration building, developed by Adler Development on a "
    "City-retained parcel of about two acres inside the Miami Freedom Park special area plan. "
    "It is NOT part of Miami Freedom Park LLC's 61-acre commercial ground lease: City of Miami "
    "Resolution R-22-0156 leases the stadium and commercial parcels to Miami Freedom Park, LLC, "
    "while the City's own plan page lists the administration building as a separate component. "
    "This row previously carried Miami Freedom Park, LLC as developer and was named the "
    "commercial component, which conflated the two projects; its permit, address, floor area and "
    "contractor always described the Civic Center. "
    "FLOOR AREA CONFLICT, unresolved on purpose: the master permit states 724,470 sq ft, which "
    "most likely covers the office plus the attached garage; the City's own project page and The "
    "Real Deal both put the OFFICE programme at 257,000 sq ft; and Florida YIMBY of January 2025 "
    "said 382,592 sq ft. The permit figure is stored because it is the latest primary document, "
    "and the other two are recorded here rather than averaged away. "
    "Address aliases on the same folio: 1802 NW 37 AV carries only the demolition permit and a "
    "gate permit, and 1822 NW 37 AV carries the construction-trailer permit and about thirty "
    "phased trade permits. Neither is a separate master permit, and the garage has none of its "
    "own. Vertical construction ran under phased permit BD25002535001B001, a full-vertical phased "
    "class I issued 2025-11-06 and later revoked, which is why the building topped off nine days "
    "after the master permit issued. Move-in is Q4 2027 per the City and spring 2028 per The Real "
    "Deal.")

ONE_HERON_NOTE = (
    "Miami Freedom Park LLC's Phase 1 office building, branded One Heron. The lessee entity is "
    "named verbatim in City of Miami Resolution R-22-0156, which approved a stadium ground lease "
    "over about twelve acres and a commercial ground lease over about sixty-one acres to Miami "
    "Freedom Park, LLC. "
    "NO BUILDING PERMIT EXISTS. Every permit on the site's folios was swept and permit "
    "BD25026033001B001 is the only one of any scope whose work items mention office -- and that "
    "permit is the City's Civic Center, now held on its own row. So the stage is Planning: a "
    "design stage never supports Under Construction, there is no notice of commencement, and no "
    "dated report of construction exists for this building. "
    "Architect, general contractor and storey count are NOT FOUND in any acceptable source, and "
    "are deliberately not inherited from the Civic Center -- Adler Development, Arquitectonica "
    "and Moss are attested for that building and for no other. "
    "The address is a site address rather than a building address: the City's plan page gives "
    "1400 NW 37 Avenue for Miami Freedom Park, and One Heron's own parcel is not identified in "
    "any primary record. "
    "Phase 1's separate 500,000 sq ft of retail is already partly permitted and open under other "
    "permits -- stadium retail inside BD23030080001B001 and a retail buildout at 1900 NW 37 AV "
    "under BD25023734001B001, both certificated in April 2026 -- and belongs to those permits' "
    "rows rather than to this one.")

GARAGE_NOTE = (
    "RENAMED. This row was called the park, which its own permit contradicts: permit "
    "BD24019509001B001 carries the work items CHAIN LINK FENCE, POLE / BOLLARDS and PARKING "
    "GARAGE-MULTIPLE FLOORS, is addressed 4000 NW 14 ST, and its contractor Finfrock Construction "
    "is a precast parking-structure builder. It is the stadium parking garage, and its TCO of "
    "10 April 2026 is what makes the row Complete. "
    "THE ACTUAL PARK IS NOT TRACKED BY ANY ROW: it is permit BD25003093001B001 at 1550 NW 37 AV, "
    "work items NEW PARK ONLY, 1,916,640 sq ft, contractor LTG Sports Turf One LLC, issued "
    "2026-03-10, with no certificate yet and a CC required. A park is not a building, so whether "
    "it belongs in a building pipeline at all is a judgement for a later pass.")

ARCH_REMOVAL = (
    "Architect CLEARED. The only source ever cited for it on this row is Florida YIMBY's coverage "
    "of the eight-storey ADMINISTRATION BUILDING, as the row's own note recorded, so "
    "Arquitectonica was this site's Civic Center architect attributed to the wrong building. No "
    "acceptable source names an architect for this structure.")


def _src(db, pid, field, value, filing, date, url):
    db.query(ExtractionSource).filter(
        ExtractionSource.project_id == pid,
        ExtractionSource.field_name == field).delete(synchronize_session=False)
    db.add(ExtractionSource(project_id=pid, field_name=field, field_value=str(value),
                            filing_name=filing, filing_date=date, pdf_url=url))


def _note(row, text):
    cur = (row.notes or "").strip()
    if text not in cur:
        row.notes = f"{cur} {text}".strip() if cur else text


def run(dry_run=False, verbose=True):
    init_db()
    db = get_session()
    entries = clog.load()
    log = []
    try:
        # ── 1220 becomes the Miami Civic Center ─────────────────────────
        civic = db.get(Project, 1220)
        for field, new, filing, date, url, why in [
            ("name", "Miami Civic Center",
             "City of Miami, topping-out notice for the new Miami Civic Center",
             "2026-07-09", CITY_NOTICE,
             "the row's permit, address, floor area and contractor all describe the "
             "City's administration building, not Miami Freedom Park LLC's commercial space"),
            ("developer", "Adler Development",
             "City of Miami, topping-out notice for the new Miami Civic Center",
             "2026-07-09", CITY_NOTICE,
             "the City's own notice names Adler Development; the previous value, Miami "
             "Freedom Park LLC, is the ground lessee of the OTHER parcels"),
            ("architect", "Arquitectonica",
             "Florida YIMBY, 13 Jan 2025", "2025-01-13", FY_ADMIN,
             "unchanged in value but re-sourced: Arquitectonica is the architect of THIS "
             "building, which is where the other two Freedom Park rows had wrongly "
             "inherited it from"),
        ]:
            old = getattr(civic, field)
            if old != new:
                clog.record(entries, 1220, old if field == "name" else civic.name,
                            field, old, new, url, why)
                log.append(f"1220 {field}: {old!r} -> {new!r}")
            if not dry_run:
                setattr(civic, field, new)
                db.flush()
                _src(db, 1220, field, new, filing, date, url)

        for field, new, filing, date, url, why in [
            ("num_stories", 8, "City of Miami DREAM, new administration building",
             "", DREAM, "the City's project page and Florida YIMBY both say eight storeys"),
            ("parking_spaces", 889, "Florida YIMBY, 13 Jan 2025", "2025-01-13", FY_ADMIN,
             "an attached garage for 889 vehicles; the City's page says up to 900, and the "
             "specific figure is stored"),
        ]:
            old = getattr(civic, field)
            if old != new:
                clog.record(entries, 1220, "Miami Civic Center", field, old, new, url, why)
                log.append(f"1220 {field}: {old!r} -> {new!r}")
            if not dry_run:
                setattr(civic, field, new)
                db.flush()
                _src(db, 1220, field, new, filing, date, url)
        if not dry_run:
            # The boilerplate master-plan note is what spread the wrong team
            # across three rows; it is replaced rather than appended to.
            civic.notes = CIVIC_NOTE
            db.flush()

        # ── 1203 is the stadium parking garage, not the park ────────────
        garage = db.get(Project, 1203)
        new_name = "Miami Freedom Park — stadium parking garage"
        if garage.name != new_name:
            clog.record(entries, 1203, garage.name, "name", garage.name, new_name,
                        "https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/"
                        "services/Building_Permits_Since_2014/FeatureServer/0/query"
                        "?where=PermitNumber%3D%27BD24019509001B001%27&outFields=*&f=pjson",
                        "the permit's work items are a parking garage and its contractor is a "
                        "precast parking-structure builder; the actual park is a different "
                        "permit that no row tracks")
            log.append(f"1203 name: {garage.name!r} -> {new_name!r}")
        if not dry_run:
            garage.name = new_name
            garage.notes = GARAGE_NOTE
            db.flush()
            _src(db, 1203, "name", new_name,
                 "City of Miami building permit BD24019509001B001, work items", "",
                 "https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/"
                 "Building_Permits_Since_2014/FeatureServer/0/query"
                 "?where=PermitNumber%3D%27BD24019509001B001%27&outFields=*&f=pjson")

        # ── the inherited architect comes off the stadium and the garage ─
        for pid in (1185, 1203):
            row = db.get(Project, pid)
            if row.architect:
                clog.record(entries, pid, row.name, "architect", row.architect, None,
                            FY_ADMIN, ARCH_REMOVAL)
                log.append(f"{pid} architect: {row.architect!r} -> None")
            if not dry_run:
                row.architect = None
                db.flush()
                db.query(ExtractionSource).filter(
                    ExtractionSource.project_id == pid,
                    ExtractionSource.field_name == "architect").delete(
                        synchronize_session=False)
                _note(row, ARCH_REMOVAL)

        # A stadium is not a shop. Its work items carry a RETAIL token for the
        # concourse retail, which is what put it in Retail.
        stadium = db.get(Project, 1185)
        if stadium.asset_class != "Other":
            clog.record(entries, 1185, stadium.name, "asset_class",
                        stadium.asset_class, "Other", "",
                        "a stadium is not retail: the permit's work items are STADIUM plus "
                        "RECREATIONAL ESTABLISHMENT plus a retail token for the concourse "
                        "retail, and the retail token is what had classified it")
            log.append(f"1185 asset_class: {stadium.asset_class!r} -> 'Other'")
        if not dry_run:
            stadium.asset_class = "Other"
            db.flush()

        # ── the new One Heron row ───────────────────────────────────────
        key = "sofl://miami/one-heron-freedom-park"
        existing = db.query(Project).filter(Project.bpda_url == key).one_or_none()
        if existing is None:
            row = Project(bpda_url=key)
            if not dry_run:
                db.add(row)
            row.city = "Miami"
            row.name = "Miami Freedom Park — One Heron (Phase 1 office)"
            row.address = "1400 NW 37 AV"
            row.status = "Planning"
            row.asset_class = "Office"
            row.developer = "Miami Freedom Park, LLC"
            row.commercial_gsf = 250000
            row.neighborhood = stadium.neighborhood
            row.latitude, row.longitude = stadium.latitude, stadium.longitude
            row.entry_type = "development"
            row.requires_extraction = False
            row.excluded = False
            row.out_of_scope = False
            row.review_scale_basis = "not_applicable"
            row.filing_type = "City of Miami special area plan component"
            row.description = ("Phase 1 office building of Miami Freedom Park's commercial "
                               "programme, branded One Heron, of approximately 250,000 sq ft, "
                               "in final design as of April 2026.")
            row.notes = ONE_HERON_NOTE
            log.append(f"NEW row: {row.name!r} (Planning, no permit)")
            if not dry_run:
                db.flush()
                _src(db, row.id, "developer", "Miami Freedom Park, LLC",
                     "City of Miami Resolution R-22-0156", "2022-04-28",
                     "https://miamifl.iqm2.com/Citizens/Detail_LegiFile.aspx?ID=11637")
                _src(db, row.id, "commercial_gsf", "250000",
                     "The Real Deal, 10 Apr 2026", "2026-04-10", TRD_ONE_HERON)
                _src(db, row.id, "status", "Planning",
                     "The Real Deal, 10 Apr 2026 — still in final design, and no building "
                     "permit exists on any folio of the site", "2026-04-10", TRD_ONE_HERON)
                clog.record(entries, row.id, row.name, "record created", None,
                            "Miami Freedom Park — One Heron (Phase 1 office)",
                            TRD_ONE_HERON,
                            "split out of the row that is now Miami Civic Center, which held "
                            "this project's name and developer against the Civic Center's "
                            "permit, address, floor area and contractor")
        elif verbose:
            log.append(f"One Heron row already exists (id {existing.id})")

        if not dry_run:
            db.commit()
    finally:
        db.close()

    if not dry_run:
        clog.save(entries)
    if verbose:
        print(f"{'(dry run) ' if dry_run else ''}{len(log)} change(s)")
        for line in log:
            print("  " + line)
    return log


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    run(**vars(ap.parse_args()))
