"""City of Miami ingest, from the City's own daily permit feed.

City of Miami goes first because it is the only one of the five municipalities
with a clean bulk feed: "Building Permits Since 2014" on the City's ArcGIS hub,
served through a FeatureServer that updates daily and carries the contractor
company, the cost, the square footage, the scope of work, the permit status,
the certificate of occupancy and the folio -- on one row, for 236,000 permits.

WHAT THE FEED DOES AND DOES NOT GIVE
It gives, with no research at all: general contractor, folio, address,
coordinates, permitted square footage, construction cost, use (and so asset
class), permit status, issue date and certificate of occupancy. Those fields
are filled here and cite the permit record.

It gives NO unit count, NO building height, NO developer and NO architect --
there are no such fields. Those stay blank after this module and are filled
only where a named source states them (see scraper/sofl_research.py). The
property appraiser would answer units and area for most of these parcels and
is deliberately not used for them: it describes the building that is standing,
which on a redevelopment site is the one being torn down.

HOW A PERMIT BECOMES A PROJECT
A single project generates one permit per trade -- the permit number is the
process number plus a trade code, so BD15008401001B001 is the building permit
and ...E001, ...P001, ...MA001 are its electrical, plumbing and mechanical
siblings. Only the B (building) permit is read; the siblings would otherwise
multiply one tower into six rows, each with a subcontractor in the contractor
field. Permits are then grouped by application number, and applications by
folio, so an assembled site is one project with its other parcels as aliases.
"""

import datetime as dt
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.database import get_session, init_db       # noqa: E402
from db.models import Project                      # noqa: E402
from scraper import sofl_common as sc              # noqa: E402

CITY = "Miami"

PERMITS_LAYER = ("https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/"
                 "services/Building_Permits_Since_2014/FeatureServer/0")
NEIGHBORHOODS_LAYER = ("https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/"
                       "services/Miami_Neighborhoods_Shapefile/FeatureServer/0")
PARCELS_LAYER = ("https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/"
                 "services/PaParcelView_gdb/FeatureServer/0")

DATASET_PAGE = ("https://datahub-miamigis.opendata.arcgis.com/datasets/"
                "building-permits-since-2014")

PERMIT_FIELDS = (
    "ObjectId,PermitNumber,ProcessNumber,ApplicationNumber,DeliveryAddress,"
    "PropertyType,TotalSQFT,AdditionSQFT,RemSQFT,TotalCost,NewAdditionCost,"
    "RemodelingCost,CompanyName,BuildingPermitStatusDescription,"
    "BuildingPermitStatusReasonDescription,IssuedDate,FirstSubmissionDate,"
    "PlanAcceptedDate,Statusdate,FolioNumber,Certificatecode,Certificatedate,"
    "RequiredCertificate,WorkItems,Miami21Zone,IsPermitFinal,ScopeofWork,"
    "Latitude,Longitude,BuildingFinalLastInspDate,BuildingFinalLastInspResult"
)

# Scopes that create or materially change a building (test 1). Demolition is
# fetched but never becomes a project on its own: a demolition permit states
# what is coming down and says nothing about a replacement, so it fails test 3.
# It is read only as corroboration that a site is being cleared.
BUILDING_SCOPES = ("NEW CONSTRUCTION", "ADDITION AND REMODELING", "PHASED PERMIT")
FETCH_SCOPES = BUILDING_SCOPES + ("DEMOLITION",)

# ── the City's own use vocabulary ───────────────────────────────────────
# WorkItems is a pipe-separated list drawn from a closed vocabulary of 219
# values, and roughly forty of them name a USE while the rest name a component
# (trusses, railings, post-tension, a pool). A use token is what satisfies
# tests 2 and 3 at once: it says the project is commercial and it states a
# building program. A permit whose work items are all components is a piece of
# a building, not a building.
USE_TOKENS = {
    # Rental and for-sale are separate asset classes -- the feed says which.
    "MULTI-FAMILY (RENTAL)": "Residential",
    "MULTI-FAMILY (CONDO)": "Condo",
    "CONDO HOTEL": "Hotel",
    "APARTMENT HOTEL": "Hotel",
    "26 OR MORE LODGING UNITS": "Hotel",
    "11-25 LODGING UNITS": "Hotel",
    "1-10 LODGING UNITS": "Hotel",
    "PROFESSIONAL OFFICE / BANK / FINANCIAL INSTITUTION": "Office",
    "GENERAL OFFICE": "Office",
    "MEDICAL OR DENTAL OFFICE": "Office",
    "RETAIL OF GOODS:SUPERMARKET/CLOTHING/FURNITURE/BOOK STORE/ART PRODUCTS": "Retail",
    "GENERAL RETAIL": "Retail",
    "GENERAL COMMERCIAL": "Retail",
    "RESTAURANT (50 OR MORE OCCUPANTS)": "Retail",
    "RESTAURANT (LESS THAN 50 OCCUPANTS)": "Retail",
    "BAR / TAVERN (50 OR MORE OCCUPANTS)": "Retail",
    "BAR / TAVERN (LESS THAN 50 OCCUPANTS)": "Retail",
    "JUICE BAR / COFFEE SHOP/BAKERY/DELI/ICE CREAM SHOP/CIGAR BAR": "Retail",
    "BARBER / BEAUTY SHOP / MANICURE-PEDICURE / WAXING/SPA/MASSAGE/TATTO SHOP": "Retail",
    "CAR/ TRUCK WASH": "Retail",
    "GAS STATION": "Retail",
    "AUTO/MARINE RELATED COMMERCIAL ESTABLISHMENT": "Retail",
    "COMMERCIAL STORAGE &amp; WAREHOUSING": "Industrial",
    "MINI STORAGE &amp; WAREHOUSING": "Industrial",
    "LIGHT INDUSTRIAL/MARINE REPAIRS": "Industrial",
    "MANUFACTURING AND PROCESSING": "Industrial",
    "CHURCH / RELIGIOUS FACILTY": "Institutional",
    "ELEMENTARY SCHOOL": "Institutional",
    "MIDDLE / HIGH SCHOOL": "Institutional",
    "COLLEGE / UNIVERSITY": "Institutional",
    "PRE-SCHOOL": "Institutional",
    "LEARNING CENTER": "Institutional",
    "CHILD DAYCARE / NURSERY SCHOOL": "Institutional",
    "ADULT DAYCARE": "Institutional",
    "HOSPITAL": "Institutional",
    "ANIMAL HOSPITAL/KENNEL/POUND": "Institutional",
    "COMMUNITY FACILITY": "Institutional",
    "COMMUNITY SUPPORT FACILITY": "Institutional",
    "FUNERAL HOME": "Institutional",
    "PARKING GARAGE-MULTIPLE FLOORS": "Parking",
    "PUBLIC PARKING": "Parking",
    "THEATER / CONCERT HALL": "Other",
    "BANQUET/EVENT HALL/DANCE HALL": "Other",
    "NIGHTCLUB": "Other",
    "SUPPERCLUB": "Other",
    "RECREATIONAL ESTABLISHMENT": "Other",
    "RECREATIONAL FACILITY": "Other",
    "ART GALLERY": "Other",
    "WORK - LIVE (W-51% L-49%)": "Mixed-Use",
}

# 1-to-4-family housing: in the vocabulary, and out of scope by test 2.
RESIDENTIAL_EXCLUDED_TOKENS = {
    "SINGLE FAMILY RESIDENCE",
    "TWO-FAMILY RESIDENCE",
    "SINGLE FAMILY ANCILLARY DWELLING UNIT",
}

# ── the floor ───────────────────────────────────────────────────────────
# Without a floor the feed's commercial new-construction set is dominated by
# jobs that are commercial and permitted and still not development projects:
# a $58,000 permit on one condo unit at 485 Brickell, a $187,600 twelve-room
# lodging conversion. Both have a use token and a square footage, so neither
# test 2 nor test 3 removes them.
#
# A project clears the floor on EITHER limb, because each catches what the
# other misses. Cost alone drops genuinely small new apartment buildings whose
# permits are cheap; area alone admits six-figure refits of large floor plates.
COST_FLOOR = 1_000_000
SQFT_FLOOR = 20_000

# A certificate code of C40 is the county's forty-year recertification, which
# is issued to buildings that have stood for four decades. Reading it as a
# completion would date a 1970s tower's delivery to last month.
COMPLETION_CERTIFICATES = {"CO", "CC", "TCO", "TCC"}


def _num(value) -> float:
    try:
        return float(str(value).replace(",", "").replace("$", "").strip() or 0)
    except (TypeError, ValueError):
        return 0.0


def _int(value):
    n = _num(value)
    return int(n) if n > 0 else None


def _ms_to_date(ms):
    if not ms:
        return None
    try:
        return dt.datetime.fromtimestamp(int(ms) / 1000, dt.UTC).date()
    except (TypeError, ValueError, OSError, OverflowError):
        return None


def _cert_date(value):
    """Certificatedate is a string, not the feed's date type. Parse loosely."""
    if not value:
        return None
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", str(value))
    if not m:
        return None
    try:
        return dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def trade_code(rec) -> str:
    """The trade letter a permit number carries after its process number."""
    pn = (rec.get("PermitNumber") or "").strip()
    proc = (rec.get("ProcessNumber") or "").strip()
    if proc and pn.startswith(proc):
        m = re.match(r"^([A-Z]+)", pn[len(proc):])
        return m.group(1) if m else ""
    return ""


def work_item_tokens(rec) -> list[str]:
    return [t.strip() for t in (rec.get("WorkItems") or "").split("|") if t.strip()]


def permit_url(rec) -> str:
    """A link that returns exactly this permit record.

    The City's permit portal is a session-driven search with no addressable
    per-permit page, so the citation is a REST query on the City's own
    published layer: it names the permit, it is stable, and clicking it shows
    the row the value came from.
    """
    pn = (rec.get("PermitNumber") or "").strip()
    return (f"{PERMITS_LAYER}/query?where=PermitNumber%3D%27{pn}%27"
            f"&outFields=*&f=pjson")


# ── intake ──────────────────────────────────────────────────────────────
def classify(rec) -> tuple[str, dict]:
    """("" and a record) if in scope, else (failed test, details).

    Every rejection carries the reason so the run log can list what was
    dropped and why. This is the only filter: a record that fails here is
    never written, so there is nothing to quarantine and nothing to review.
    """
    tokens = work_item_tokens(rec)
    scope = rec.get("ScopeofWork") or ""
    address = (rec.get("DeliveryAddress") or "").strip()

    uses = {USE_TOKENS[t] for t in tokens if t in USE_TOKENS}
    cost = max(_num(rec.get("TotalCost")), _num(rec.get("NewAdditionCost")))
    sqft = _int(rec.get("TotalSQFT")) or 0
    info = {"permit": rec.get("PermitNumber"), "address": address,
            "scope": scope, "cost": cost, "sqft": sqft,
            "uses": sorted(uses), "tokens": tokens}

    # Test 1 -- creates or materially changes a building.
    if scope not in BUILDING_SCOPES:
        return "test1_no_building_change", info
    if trade_code(rec) != "B":
        return "test1_not_a_building_permit", info
    # A permit addressed to a single unit inside a standing building is an
    # interior job however it is scoped, and interior fit-out is excluded by
    # name. The unit number in the address is the only reliable signal of it
    # in this feed.
    if re.search(r"#\s*\S", address):
        return "test1_single_unit_interior", info

    # Test 2 -- commercial, i.e. not 1-to-4-family.
    if not uses:
        if set(tokens) & RESIDENTIAL_EXCLUDED_TOKENS:
            return "test2_not_commercial", info
        return "test3_no_stated_program", info
    if set(tokens) & RESIDENTIAL_EXCLUDED_TOKENS and not (uses - {"Parking", "Other"}):
        return "test2_not_commercial", info

    # Test 3 -- a stated program.
    if not sqft:
        return "test3_no_stated_program", info

    failed = sc.passes_three_tests(changes_building=True, commercial=True,
                                   has_program=True)
    if failed:
        return failed, info

    # The floor, applied after the tests so the log can tell a record that is
    # out of scope apart from one that is in scope but too small to be a
    # development project.
    if cost < COST_FLOOR and sqft < SQFT_FLOOR:
        return "below_floor", info

    return "", info


def in_window(rec) -> tuple[bool, str]:
    """Whether the permit is live work or a 2025/2026 delivery.

    "Load active projects plus anything completed in 2025 or 2026." A permit
    the City finalled in 2019 is a building that has stood for years and is
    not what this tracker is for.
    """
    status = rec.get("BuildingPermitStatusDescription") or ""
    if status in ("Active", "Hold"):
        return True, "active_permit"
    if status == "Final":
        cert = _cert_date(rec.get("Certificatedate"))
        if cert and cert.year in (2025, 2026) and \
                (rec.get("Certificatecode") or "") in COMPLETION_CERTIFICATES:
            return True, "certificate_2025_26"
        closed = _ms_to_date(rec.get("Statusdate"))
        if closed and closed.year in (2025, 2026):
            return True, "permit_closed_2025_26"
        return False, "completed_before_2025"
    # Revoked and Expired permits are not live work and did not deliver a
    # building. They are dropped rather than recorded as dead projects: a
    # lapsed permit is a lapsed permit, not a denied application.
    return False, f"permit_{status.lower() or 'unknown'}"


# ── stage corrections found by hand ─────────────────────────────────────
# `stage_of` reads the permit record and nothing else, which is right for 313
# Miami rows and wrong for a handful where evidence OUTSIDE the permit feed
# contradicts it. An issued building permit is the brief's test for Under
# Construction, but a permit issued years ago that has never had an inspection
# finalled, on a parcel the County still assesses as vacant land with year
# built 0, is not a building going up -- it is an entitlement someone is
# holding. Recording it as Under Construction is the Rhode Island error in the
# other direction: a pipeline inflated by projects that never started.
#
# Pinned by folio rather than inferred, because the condition that justifies
# each one was checked by hand against the County record. Each entry carries
# the stage, the basis string stored as evidence, and why.
STAGE_REVIEW = {
    "0141380510260": {
        "stage": sc.STAGE_PERMITTING,
        "basis": "permit_issued_never_started",
        "why": ("Building permit BD18003273001B001 was issued on 6 March 2020 and is "
                "still Active with no finalised inspections six years later, and "
                "Miami-Dade assesses folio 01-4138-051-0260 as vacant land with year "
                "built 0. A 287,323 sq ft hotel has not been built here, so the row is "
                "Permitting -- a permit on file -- rather than Under Construction."),
    },
}


# ── stage ───────────────────────────────────────────────────────────────
def stage_of(rec) -> tuple[str, str, str, dt.date | None]:
    """(stage, completion_stage, basis, date) from the permit record alone.

    Order matters, and it is the brief's: check completion BEFORE construction.
    A permit that has been finalled and carries a certificate is a finished
    building, and calling it Under Construction because a permit was once
    issued is exactly the error that left 69 delivered buildings in the Rhode
    Island pipeline.
    """
    cert_code = (rec.get("Certificatecode") or "").strip().upper()
    cert_date = _cert_date(rec.get("Certificatedate"))
    final = (rec.get("IsPermitFinal") or "").strip().upper() == "YES"

    if cert_code in COMPLETION_CERTIFICATES:
        basis = ("certificate_of_occupancy" if cert_code in ("CO", "CC")
                 else "temporary_certificate")
        return (sc.STAGE_COMPLETE, sc.STAGE_COMPLETE,
                "co_issued" if basis == "certificate_of_occupancy" else "co_issued",
                cert_date)
    if final:
        # The City does not final a building permit until the work has passed
        # its final inspection, so a finalled permit is a completed building
        # even where no certificate is recorded against this row. Recorded
        # under its own basis, because it is weaker evidence than a
        # certificate and must not read like one.
        return (sc.STAGE_COMPLETE, sc.STAGE_COMPLETE, "permit_final",
                _ms_to_date(rec.get("Statusdate")))

    issued = _ms_to_date(rec.get("IssuedDate"))
    if issued:
        return (sc.STAGE_UNDER_CONSTRUCTION, "Under Construction",
                "permit_active", None)
    # An application on file that the City has not issued: Permitting, which
    # is where the brief puts a proposed project once a building permit
    # application exists.
    return (sc.STAGE_PERMITTING, "", "", None)


# ── assembly ────────────────────────────────────────────────────────────
def build_projects(records) -> tuple[list[dict], list[dict], dict]:
    """In-scope permits folded into projects, the rejection log, and the
    earlier completions on the same parcels.

    The third return value is the "is it already built?" check. A permit the
    City finalled before 2025 is out of the loading window and dropped, but
    the fact that a building on that parcel was completed is exactly what has
    to be known before a live permit on the same parcel is written down as
    Under Construction. Rhode Island skipped this check, and 69 finished
    buildings stayed in its pipeline as a result.
    """
    accepted, rejected = [], []
    prior_completions = {}
    for rec in records:
        reason, info = classify(rec)
        if reason:
            rejected.append({"reason": reason, **info})
            continue
        live, why = in_window(rec)
        if not live:
            rejected.append({"reason": why, **info})
            if why == "completed_before_2025":
                folio = sc.norm_folio(rec.get("FolioNumber"))
                if folio:
                    done = _cert_date(rec.get("Certificatedate")) or \
                        _ms_to_date(rec.get("Statusdate"))
                    prior_completions.setdefault(folio, []).append({
                        "permit": rec.get("PermitNumber"),
                        "sqft": _int(rec.get("TotalSQFT")),
                        "date": done.isoformat() if done else "",
                        "certificate": rec.get("Certificatecode") or "",
                    })
            continue
        rec["_uses"] = info["uses"]
        rec["_cost"] = info["cost"]
        rec["_window"] = why
        accepted.append(rec)

    # One application is one project. Where an application carries more than
    # one building permit (a phased tower: foundation, then vertical), the
    # costliest is the main building permit and the rest are its phases.
    by_app = {}
    for rec in accepted:
        by_app.setdefault(rec.get("ApplicationNumber") or rec.get("PermitNumber"), []).append(rec)

    projects = []
    for app, recs in by_app.items():
        recs.sort(key=lambda r: (r["_cost"], _int(r.get("TotalSQFT")) or 0), reverse=True)
        main = recs[0]
        folios = []
        for r in recs:
            f = sc.norm_folio(r.get("FolioNumber"))
            if f and f not in folios:
                folios.append(f)
        projects.append({"app": app, "main": main, "permits": recs,
                         "folio": folios[0] if folios else "",
                         "folio_aliases": folios[1:]})

    # Then dedupe on the folio, as the brief specifies. Two applications on
    # one parcel are one project; the later activity wins and the earlier
    # permit numbers are kept on the row so nothing is silently lost.
    merged = {}
    for proj in projects:
        key = proj["folio"] or f"app:{proj['app']}"
        prior = merged.get(key)
        if prior is None:
            merged[key] = proj
            continue
        if _latest_activity(proj) > _latest_activity(prior):
            proj["permits"] = proj["permits"] + prior["permits"]
            merged[key] = proj
        else:
            prior["permits"] = prior["permits"] + proj["permits"]

    # Last gate, and it can only be applied here: the loading window asks for
    # active work plus 2025-26 deliveries, and whether a project IS a delivery
    # is not known until its main permit has been chosen. A permit still
    # listed Active or Hold that already carries a 2019 temporary certificate
    # is a building that opened six years ago -- live-looking in the permit
    # system, and an older completion in fact.
    final = []
    for proj in merged.values():
        stage, _, _, done = stage_of(proj["main"])
        if stage == sc.STAGE_COMPLETE and (done is None or done.year < 2025):
            rejected.append({
                "reason": "completed_before_2025",
                "permit": proj["main"].get("PermitNumber"),
                "address": proj["main"].get("DeliveryAddress"),
                "completion_date": done.isoformat() if done else "",
            })
            continue
        final.append(proj)
    return final, rejected, prior_completions


def _latest_activity(proj) -> dt.date:
    best = dt.date(1900, 1, 1)
    for r in proj["permits"]:
        for d in (_cert_date(r.get("Certificatedate")),
                  _ms_to_date(r.get("Statusdate")),
                  _ms_to_date(r.get("IssuedDate")),
                  _ms_to_date(r.get("FirstSubmissionDate"))):
            if d and d > best:
                best = d
    return best


# ── write ───────────────────────────────────────────────────────────────
def ingest(limit_owner_lookups=None, verbose=True):
    init_db()
    s = sc.session()

    if verbose:
        print("fetching permits ...")
    where = "ScopeofWork IN (%s)" % ",".join(f"'{x}'" for x in FETCH_SCOPES)
    records = sc.arcgis_query(PERMITS_LAYER, where=where,
                              out_fields=PERMIT_FIELDS, sess=s,
                              order_by="ObjectId")
    if verbose:
        print(f"  {len(records):,} permit rows in construction scopes")

    projects, rejected, prior_completions = build_projects(records)
    if verbose:
        print(f"  {len(projects):,} projects after folio dedupe; "
              f"{len(rejected):,} permit rows rejected")

    if verbose:
        print("fetching neighborhood polygons ...")
    nbhd = sc.PolygonIndex(
        sc.arcgis_query(NEIGHBORHOODS_LAYER, out_fields="FID,LABEL",
                        geometry=True, sess=s),
        "LABEL")
    if verbose:
        print(f"  {len(nbhd)} neighborhood polygons")

    owners = _fetch_owners([p["folio"] for p in projects if p["folio"]], s,
                           limit=limit_owner_lookups, verbose=verbose)

    db = get_session()
    written = updated = 0
    try:
        for proj in projects:
            if _upsert(db, proj, nbhd, owners, prior_completions):
                written += 1
            else:
                updated += 1
        db.commit()
    finally:
        db.close()

    if verbose:
        print(f"wrote {written} new, updated {updated}")
    return {"projects": projects, "rejected": rejected,
            "prior_completions": prior_completions,
            "written": written, "updated": updated,
            "owners_found": len(owners)}


def _fetch_owners(folios, s, limit=None, verbose=True):
    """Owner of record per folio, from the county's published parcel layer.

    The owner of record is not the developer -- it is usually a single-purpose
    LLC -- so it is written to owner_or_agency, which is the column the
    tracker already keeps for a party that owns rather than builds. Resolving
    an LLC to its sponsor needs Sunbiz, which refuses automated requests, so
    where the press does not name a developer the chain stops here and
    developer stays blank. That is the brief's rule, not a shortfall.
    """
    folios = [f for f in dict.fromkeys(folios) if f]
    if limit:
        folios = folios[:limit]
    found = {}
    if verbose:
        print(f"fetching owner of record for {len(folios)} folios ...")
    for i in range(0, len(folios), 100):
        chunk = folios[i:i + 100]
        where = "FOLIO IN (%s)" % ",".join(f"'{f}'" for f in chunk)
        try:
            rows = sc.arcgis_query(PARCELS_LAYER, where=where,
                                   out_fields="FOLIO,TRUE_OWNER1,TRUE_SITE_ADDR,PARENT_FOLIO",
                                   sess=s)
        except RuntimeError as exc:
            if verbose:
                print(f"  owner chunk {i} failed: {exc}")
            continue
        for r in rows:
            if r.get("TRUE_OWNER1"):
                found[r["FOLIO"]] = r
    if verbose:
        print(f"  {len(found)} owners resolved")
    return found


def _describe(proj) -> str:
    main = proj["main"]
    tokens = [t for t in work_item_tokens(main) if t in USE_TOKENS]
    bits = []
    scope = (main.get("ScopeofWork") or "").title()
    if scope:
        bits.append(scope)
    if tokens:
        bits.append("; ".join(t.title() for t in tokens))
    sqft = _int(main.get("TotalSQFT"))
    if sqft:
        bits.append(f"{sqft:,} sq ft permitted")
    cost = main.get("_cost") or 0
    if cost:
        bits.append(f"${cost:,.0f} stated construction cost")
    zone = main.get("Miami21Zone")
    if zone:
        bits.append(f"Miami 21 zone {zone}")
    if len(proj["permits"]) > 1:
        bits.append(f"{len(proj['permits'])} building permits on this parcel")
    return ". ".join(bits) + "."


def _upsert(db, proj, nbhd, owners, prior_completions=None) -> bool:
    main = proj["main"]
    folio = proj["folio"]
    key = f"sofl://miami/{folio or main.get('PermitNumber')}"

    row = None
    if folio:
        row = db.query(Project).filter(Project.parcel_id == folio,
                                       Project.city == CITY).one_or_none()
    if row is None:
        row = db.query(Project).filter(Project.bpda_url == key).one_or_none()
    is_new = row is None
    if is_new:
        row = Project(bpda_url=key)
        db.add(row)

    stage, completion_stage, basis, completion_date = stage_of(main)
    fix = STAGE_REVIEW.get(folio)
    if fix:
        stage, completion_stage, basis = fix["stage"], "", fix["basis"]
        completion_date = None
    lat, lon = main.get("Latitude"), main.get("Longitude")
    uses = main.get("_uses") or []

    row.city = CITY
    row.parcel_id = folio
    row.parcel_id_aliases = ",".join(proj["folio_aliases"]) or None
    # No project name exists in a permit record, so the address IS the name
    # until a named source gives the building one. An invented name would be
    # worse than an address: it would look like a fact.
    addr = (main.get("DeliveryAddress") or "").strip()
    if is_new or not row.name:
        row.name = addr
    row.address = addr
    row.status = stage
    row.completion_stage = completion_stage or None
    row.completion_basis = basis or None
    row.completion_date = completion_date.isoformat() if completion_date else None
    if completion_date:
        row.delivered_date = completion_date
        row.delivered_precision = "day"
    row.completion_source_url = permit_url(main) if basis else None
    row.completion_evidence = (
        f"Permit {main.get('PermitNumber')} status "
        f"{main.get('BuildingPermitStatusDescription')}"
        + (f", certificate {main.get('Certificatecode')} "
           f"{main.get('Certificatedate')}" if main.get("Certificatecode") else "")
    ) if basis else None

    row.asset_class = sc.asset_class_from_uses(uses) or None
    row.asset_class_raw = main.get("WorkItems") or None
    row.total_gsf = _int(main.get("TotalSQFT"))
    row.total_gsf_source = "filing" if row.total_gsf else None
    row.permit_gsf = _int(main.get("TotalSQFT"))
    row.permit_number = main.get("PermitNumber")
    row.permit_url = permit_url(main)
    row.permit_cost = int(main.get("_cost") or 0) or None
    issued = _ms_to_date(main.get("IssuedDate"))
    row.permit_issued_date = issued.isoformat() if issued else None
    row.general_contractor = (main.get("CompanyName") or "").strip() or None
    row.zoning_district_raw = main.get("Miami21Zone") or None
    row.neighborhood = nbhd.name_at(lon, lat) or ""
    row.latitude = lat
    row.longitude = lon
    row.coords_approximate = False
    row.description = _describe(proj)
    row.requires_extraction = False      # structured feed, no PDF to read
    row.out_of_scope = False
    row.excluded = False
    row.entry_type = "development"
    row.filing_type = "Building permit"
    row.review_scale = None
    row.review_scale_basis = "not_applicable"
    row.building_permit_raw = ", ".join(
        p.get("PermitNumber") for p in proj["permits"] if p.get("PermitNumber"))

    # The "is it already built?" note. An earlier completed building permit on
    # the same parcel does not by itself mean THIS project is finished -- most
    # often it is the building being replaced, or an earlier phase -- so it is
    # recorded as a caveat rather than acted on. It must be visible, because a
    # reader checking an Under Construction row needs to know a building on
    # this parcel was already certified.
    earlier = (prior_completions or {}).get(folio) or []
    if earlier and stage != sc.STAGE_COMPLETE:
        row.notes = (
            "Earlier completed building permit(s) on this parcel, outside the "
            "2025-26 loading window: "
            + "; ".join(f"{e['permit']} ({e['certificate'] or 'closed'} "
                        f"{e['date'] or 'date not stated'}"
                        + (f", {e['sqft']:,} sq ft" if e['sqft'] else "") + ")"
                        for e in earlier[:4])
            + ". This may be the building being replaced, an earlier phase, or "
              "evidence that the site is already built out -- it is not treated "
              "as a completion of this project."
        )
    elif not earlier:
        row.notes = None

    owner = owners.get(folio)
    if owner:
        row.owner_or_agency = (owner.get("TRUE_OWNER1") or "").strip() or None

    db.flush()

    # Provenance. One citation per filled value, all pointing at the permit
    # record the value was read from.
    from db.models import ExtractionSource
    db.query(ExtractionSource).filter(
        ExtractionSource.project_id == row.id,
        ExtractionSource.filing_name.like("City of Miami building permit%"),
    ).delete(synchronize_session=False)

    sc.record_sources(
        db, row.id,
        {"address": row.address,
         "status": row.status,
         "asset_class": row.asset_class,
         "total_gsf": row.total_gsf,
         "general_contractor": row.general_contractor,
         "parcel_id": row.parcel_id,
         "neighborhood": row.neighborhood,
         "permit_number": row.permit_number,
         "completion_date": row.completion_date},
        f"City of Miami building permit {main.get('PermitNumber')}",
        permit_url(main),
        row.permit_issued_date or "",
    )
    if owner:
        sc.record_sources(
            db, row.id, {"owner_or_agency": row.owner_or_agency},
            f"Miami-Dade property boundary record, folio {folio}",
            f"{PARCELS_LAYER}/query?where=FOLIO%3D%27{folio}%27&outFields=*&f=pjson",
        )
    return is_new


if __name__ == "__main__":
    ingest()
