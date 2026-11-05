"""Re-pull square footage and contractor from the MASTER building permit.

Rules 3 and 4 of the review both turn on one question -- which permit is the
master -- so they are answered together here.

WHAT THE MASTER PERMIT IS
The City of Miami issues many building permits per project. All of them carry
the `B` trade letter, which is why filtering on trade alone is not enough:

    ScopeofWork = NEW CONSTRUCTION, TotalSQFT > 0     the master
    ScopeofWork = NEW CONSTRUCTION, TotalSQFT = 0     shoring, post-tension,
                                                      curtain wall, excavation
    ScopeofWork = PHASED PERMIT                       a trade's phased permit
    ScopeofWork = DEMOLITION                          the teardown

Okan Tower has 49 permits on its folio, of which 23 are `B` permits, of which
exactly ONE is the master. Its phased permits carry $100 costs and the
contractors of the glazing, plumbing, electrical and fire-sprinkler
subcontractors -- Ready Window, Suntech, Power Design, Sprinklermatic. Taking
a contractor from any of those would name a subcontractor as the general
contractor, which is what rule 4 forbids.

WHY THE EXISTING DATA MOSTLY ALREADY AGREES
The original ingest took the costliest `B` permit per application. That is a
proxy for the master and it lands on the right permit nearly every time,
because the master's cost dwarfs a $100 phased permit. This module replaces
the proxy with the thing itself and reports every row where they differ, so
the agreement is measured rather than assumed.

Nothing is written without `--apply`. The report is the point: a silent
re-pull that changed 300 numbers would be impossible to review.

    python -m scraper.sofl_master_permit
    python -m scraper.sofl_master_permit --apply
"""

import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from db.database import get_session                      # noqa: E402
from db.models import Project, ExtractionSource          # noqa: E402
from scraper import sofl_common as sc                    # noqa: E402
from scraper import sofl_changelog as clog               # noqa: E402
from scraper import sofl_miami as sm                     # noqa: E402

MASTER_SCOPES = {"NEW CONSTRUCTION", "ADDITION AND REMODELING"}

FIELDS = ("PermitNumber,ProcessNumber,ApplicationNumber,ScopeofWork,WorkItems,"
          "TotalSQFT,TotalCost,NewAdditionCost,CompanyName,IssuedDate,"
          "BuildingPermitStatusDescription,IsPermitFinal,Certificatecode,"
          "Certificatedate,RequiredCertificate,DeliveryAddress,FolioNumber")


def _num(v) -> float:
    try:
        return float(str(v).replace(",", ""))
    except (TypeError, ValueError):
        return 0.0


def _int(v):
    n = _num(v)
    return int(n) if n else None


def master_of(permits: list[dict], application: str | None = None) -> dict | None:
    """The master building permit for ONE project, or None.

    Two things make this harder than filtering on scope, and both of them bit
    on the first run:

    SCOPE IS NOT MAGNITUDE. Preferring NEW CONSTRUCTION over everything else
    picked a 943 sq ft ancillary building ahead of a 14,838 sq ft project, and
    a 355 sq ft remodel ahead of a tower whose plans state 1,667,604 sq ft.
    The master permit is the one that states the PROJECT's floor area, so the
    ranking is by area, with cost only as a tiebreak.

    A FOLIO IS NOT A PROJECT. One parcel accumulates permits for decades: at
    1500 NW North River Drive a 730,731 sq ft permit from 2015 sits beside a
    5,261 sq ft permit from 2023. Taking the larger would re-point a row at a
    building that was finished years before the project it tracks -- the same
    class of error as calling a delivered building Under Construction. So the
    search is confined to ONE APPLICATION, which is the unit the ingest already
    treats as a project ("one application is one project").
    """
    cands = [a for a in permits
             if sm.trade_code(a) == "B"
             and (a.get("ScopeofWork") or "").strip().upper() in MASTER_SCOPES
             and _num(a.get("TotalSQFT")) > 0]
    if application:
        same = [a for a in cands
                if (a.get("ApplicationNumber") or "").strip() == application]
        # Fall back to the whole folio only when the application states no area
        # anywhere, which happens for a row ingested from a permit that was
        # later superseded.
        cands = same or cands
    if not cands:
        return None
    cands.sort(key=lambda a: (_num(a.get("TotalSQFT")), _num(a.get("TotalCost"))),
               reverse=True)
    return cands[0]


def fetch_folio(folio: str, sess) -> list[dict]:
    feats = sc.arcgis_query(sm.PERMITS_LAYER, where=f"FolioNumber='{folio}'",
                            out_fields=FIELDS, order_by="PermitNumber ASC", sess=sess)
    return [f.get("attributes", f) for f in feats]


def audit(apply=False, verbose=True, limit=None):
    db = get_session()
    try:
        # Only permit-derived rows. The four press-overlay rows (619 Brickell,
        # 130 Biscayne, LILLI, 75 Broadway) hold a floor area from filed plans
        # and no permit at all; handing them the largest permit on their folio
        # gave 130 Biscayne a 355 sq ft remodel in place of its 1,667,604 sq ft
        # of filed plans.
        rows = db.query(Project).filter(
            Project.city == "Miami",
            Project.parcel_id.isnot(None),
            Project.permit_number.isnot(None)).order_by(Project.id).all()
        work = [(r.id, r.name, r.parcel_id, r.permit_number, r.total_gsf,
                 r.general_contractor) for r in rows]
    finally:
        db.close()
    if limit:
        work = work[:limit]

    sess = sc.session()
    entries = clog.load()
    agree = {"permit": 0, "sqft": 0, "gc": 0}
    diffs = []
    no_master = []
    failed = []

    for i, (pid, name, folio, cur_permit, cur_sqft, cur_gc) in enumerate(work, 1):
        # The permit feed stores a 12-digit folio; the tracker normalises to 13
        # with a leading zero, so the query needs the un-padded form.
        probe = folio[1:] if len(folio) == 13 and folio.startswith("0") else folio
        try:
            permits = fetch_folio(probe, sess)
        except Exception as exc:                                   # noqa: BLE001
            failed.append((pid, name, str(exc)[:70]))
            continue
        # The application the row's own permit belongs to defines the project.
        app = next(((p.get("ApplicationNumber") or "").strip() for p in permits
                    if (p.get("PermitNumber") or "").strip() == cur_permit), None)
        m = master_of(permits, app)
        if m is None:
            no_master.append((pid, name, folio, cur_permit, len(permits)))
            continue
        new_permit = (m.get("PermitNumber") or "").strip()
        new_sqft = _int(m.get("TotalSQFT"))
        new_gc = (m.get("CompanyName") or "").strip() or None

        agree["permit"] += (new_permit == cur_permit)
        agree["sqft"] += (new_sqft == cur_sqft)
        agree["gc"] += (new_gc == cur_gc)
        if new_permit != cur_permit or new_sqft != cur_sqft or new_gc != cur_gc:
            diffs.append({"id": pid, "name": name, "folio": folio,
                          "cur_permit": cur_permit, "new_permit": new_permit,
                          "cur_sqft": cur_sqft, "new_sqft": new_sqft,
                          "cur_gc": cur_gc, "new_gc": new_gc,
                          "scope": m.get("ScopeofWork"),
                          "cost": _int(m.get("TotalCost"))})
        if verbose and i % 25 == 0:
            print(f"  ...{i}/{len(work)}", file=sys.stderr)

    if verbose:
        n = len(work)
        print(f"\n{n} Miami rows with a folio checked against the master permit")
        print(f"  master permit already the one on the row : {agree['permit']:>4} / {n}")
        print(f"  square footage already the master's      : {agree['sqft']:>4} / {n}")
        print(f"  contractor already the master's          : {agree['gc']:>4} / {n}")
        print(f"  rows differing in at least one of those  : {len(diffs):>4}")
        print(f"  rows with no master permit on the folio  : {len(no_master):>4}")
        if failed:
            print(f"  rows whose folio query failed           : {len(failed):>4}")
        for d in diffs:
            print(f"\n  id {d['id']}  {str(d['name'])[:50]}   folio {d['folio']}")
            if d["new_permit"] != d["cur_permit"]:
                print(f"      permit  {d['cur_permit']}  ->  {d['new_permit']}"
                      f"   ({d['scope']}, cost {d['cost']:,})" if d["cost"]
                      else f"      permit  {d['cur_permit']}  ->  {d['new_permit']}")
            if d["new_sqft"] != d["cur_sqft"]:
                print(f"      sq ft   {d['cur_sqft']}  ->  {d['new_sqft']}")
            if d["new_gc"] != d["cur_gc"]:
                print(f"      GC      {d['cur_gc']!r}")
                print(f"           -> {d['new_gc']!r}")
        for pid, name, folio, permit, n_permits in no_master:
            print(f"  no master: id {pid} {str(name)[:44]:<44} folio {folio} "
                  f"({n_permits} permits, row holds {permit})")
        for pid, name, err in failed:
            print(f"  query failed: id {pid} {str(name)[:40]:<40} {err}")

    if apply and diffs:
        db = get_session()
        try:
            for d in diffs:
                row = db.get(Project, d["id"])
                url = sm.permit_url({"PermitNumber": d["new_permit"]})
                filing = f"City of Miami master building permit {d['new_permit']}"
                if d["new_sqft"] != d["cur_sqft"] and d["new_sqft"]:
                    clog.record(entries, row.id, row.name, "total_gsf",
                                d["cur_sqft"], d["new_sqft"], url,
                                "re-pulled from the master building permit's TotalSQFT, "
                                "which is gross area including the garage")
                    row.total_gsf = d["new_sqft"]
                    row.permit_gsf = d["new_sqft"]
                    _resource(db, row.id, "total_gsf", str(d["new_sqft"]), filing, url)
                if d["new_gc"] != d["cur_gc"] and d["new_gc"]:
                    clog.record(entries, row.id, row.name, "general_contractor",
                                d["cur_gc"], d["new_gc"], url,
                                "re-pulled from the master building permit's CompanyName; "
                                "the previous value came from a permit that is not the "
                                "master, which names a subcontractor rather than the "
                                "general contractor")
                    row.general_contractor = d["new_gc"]
                    _resource(db, row.id, "general_contractor", d["new_gc"], filing, url)
                if d["new_permit"] != d["cur_permit"] and d["new_permit"]:
                    clog.record(entries, row.id, row.name, "permit_number",
                                d["cur_permit"], d["new_permit"], url,
                                "the row was anchored to a permit that is not the "
                                "master building permit for this folio")
                    row.permit_number = d["new_permit"]
                    _resource(db, row.id, "permit_number", d["new_permit"], filing, url)
                db.flush()
            db.commit()
        finally:
            db.close()
        clog.save(entries)
        if verbose:
            print(f"\napplied changes to {len(diffs)} row(s)")

    return {"diffs": diffs, "no_master": no_master, "agree": agree, "failed": failed}


def _resource(db, pid, field, value, filing, url):
    db.query(ExtractionSource).filter(
        ExtractionSource.project_id == pid,
        ExtractionSource.field_name == field).delete(synchronize_session=False)
    db.add(ExtractionSource(project_id=pid, field_name=field, field_value=value,
                            filing_name=filing, filing_date="", pdf_url=url))


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int)
    a = ap.parse_args()
    audit(apply=a.apply, limit=a.limit)
