"""Backfill the permit facts the detail panel shows, once, into the database.

The panel's "Permit details" table should read like a permit record: process
number, scope, status, application and issue dates, contractor, area, cost,
folio, and whatever certificate exists. Most of that was already stored, but
the status, the application date, the final date and the certificate were not
-- they were only ever used transiently to decide a project's stage.

So they are fetched ONCE and stored, rather than called live when a panel
opens. A detail panel that makes a network request per click is slow, fails
when the City's service is down, and re-asks for a fact that does not change.

Permits are fetched in chunks by permit number rather than one at a time: 309
rows is 7 requests this way and 309 the obvious way.

    python -m scraper.sofl_permit_detail --dry-run
    python -m scraper.sofl_permit_detail
"""

import argparse
import datetime as dt
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from db.database import get_session, init_db           # noqa: E402
from db.models import Project                          # noqa: E402
from scraper import sofl_common as sc                  # noqa: E402
from scraper import sofl_miami as sm                   # noqa: E402

FIELDS = ("PermitNumber,ProcessNumber,ScopeofWork,BuildingPermitStatusDescription,"
          "FirstSubmissionDate,IssuedDate,IsPermitFinal,Statusdate,"
          "Certificatecode,Certificatedate,CompanyName,TotalSQFT,TotalCost,"
          "FolioNumber,PropertyType")
CHUNK = 50


def _date(value):
    """The City mixes epoch milliseconds and 'YYYY-MM-DD HH:MM:SS' strings."""
    if value in (None, ""):
        return None
    try:
        n = float(value)
        if n > 10_000_000_000:          # milliseconds
            n /= 1000.0
        return dt.datetime.utcfromtimestamp(n).date().isoformat()
    except (TypeError, ValueError):
        text = str(value).strip()
        return text[:10] if len(text) >= 10 else None


def fetch(permit_numbers, sess, verbose=True):
    out = {}
    nums = sorted({n for n in permit_numbers if n})
    for i in range(0, len(nums), CHUNK):
        batch = nums[i:i + CHUNK]
        where = "PermitNumber IN (" + ",".join(f"'{n}'" for n in batch) + ")"
        feats = sc.arcgis_query(sm.PERMITS_LAYER, where=where, out_fields=FIELDS,
                                order_by="PermitNumber ASC", sess=sess)
        for f in feats:
            a = f.get("attributes", f)
            key = (a.get("PermitNumber") or "").strip()
            if key:
                out[key] = a
        if verbose:
            print(f"  fetched {len(out)}/{len(nums)}", file=sys.stderr)
    return out


def run(dry_run=False, verbose=True):
    init_db()
    db = get_session()
    try:
        rows = db.query(Project).filter(
            Project.city == "Miami", Project.permit_number.isnot(None)).all()
        wanted = [r.permit_number for r in rows]
    finally:
        db.close()
    if verbose:
        print(f"{len(wanted)} Miami rows carry a permit number")

    found = fetch(wanted, sc.session(), verbose=verbose)
    if verbose:
        print(f"{len(found)} matched in the City feed")

    db = get_session()
    written = 0
    try:
        for row in db.query(Project).filter(
                Project.city == "Miami", Project.permit_number.isnot(None)).all():
            a = found.get(row.permit_number)
            if not a:
                continue
            row.permit_process_number = (a.get("ProcessNumber") or "").strip() or None
            row.permit_scope = (a.get("ScopeofWork") or "").strip() or None
            row.permit_status = (a.get("BuildingPermitStatusDescription") or "").strip() or None
            row.permit_application_date = _date(a.get("FirstSubmissionDate"))
            row.permit_certificate_code = (a.get("Certificatecode") or "").strip() or None
            row.permit_certificate_date = _date(a.get("Certificatedate"))
            # "Final" is a status the City sets when the work has passed its
            # final inspection; Statusdate is when that happened.
            row.permit_final_date = (_date(a.get("Statusdate"))
                                     if (a.get("IsPermitFinal") or "").upper() == "YES"
                                     else None)
            written += 1
        if not dry_run:
            db.commit()
    finally:
        db.close()
    if verbose:
        print(f"{'(dry run) ' if dry_run else ''}wrote permit detail onto {written} row(s)")
    return written


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    run(**vars(ap.parse_args()))
