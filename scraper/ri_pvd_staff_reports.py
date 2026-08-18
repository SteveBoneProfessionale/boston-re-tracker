r"""
Harvest Providence City Plan Commission STAFF REPORTS as a second document type.

Why
---
The RI Secretary of State open-meetings portal hosts only notices and minutes.
An agenda is a notice of a hearing: it states who, where and what stage, but it
does not state parking counts, floor counts or gross square footage, because
nobody needs those to attend a hearing. That is why those fields sit at 2%, 1%
and 36%, and why no amount of pattern work moves them -- measured, not assumed.

Providence publishes the staff report for each CPC item on the city's own site,
and those DO carry the program detail:

    https://www.providenceri.gov/wp-content/uploads/2026/06/
        24-069MA-70-Houghton-Street-prelim-staff-report.pdf

The filename encodes the case number the agenda also prints, so matching is on
a real key rather than fuzzy address similarity. Enumeration is via the site's
WordPress media endpoint (wp-json/wp/v2/media).

Precedence
----------
Staff reports FILL NULLS ONLY. A value the agenda already stated is left alone,
so this can never silently rewrite the primary filing -- it reaches only fields
the agenda was silent about.
"""

import re
import sys
import json
import time
import logging
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import init_db, get_session
from db.models import Project
from scraper.ri_extract import (
    _UNITS, _PARKING, _SF, _ACRES, _STORIES, _BUILDINGS, _num, zoning, applicant,
)
from scraper.ri_meeting_docs import HEADERS, pdf_text

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

ROOT = Path(__file__).parent.parent
INDEX = ROOT / "data" / "ri_pvd_staff_reports.json"
PDF_DIR = ROOT / "data" / "ri_pdfs" / "pvd_reports"
MEDIA = "https://www.providenceri.gov/wp-json/wp/v2/media"


def enumerate_reports(terms=("staff-report", "staff report", "staff_report")) -> dict:
    """Every staff-report PDF the media endpoint knows, keyed by URL."""
    out = {}
    if INDEX.exists():
        out.update(json.loads(INDEX.read_text(encoding="utf-8")))
    with httpx.Client(headers=HEADERS, follow_redirects=True) as c:
        for term in terms:
            page = 1
            while True:
                r = c.get(MEDIA, params={"search": term, "per_page": 100,
                                         "page": page}, timeout=90)
                if r.status_code != 200:
                    break
                j = r.json()
                if not isinstance(j, list) or not j:
                    break
                for it in j:
                    su = it.get("source_url") or ""
                    if su.lower().endswith(".pdf"):
                        out[su] = {"date": (it.get("date") or "")[:10]}
                if page >= int(r.headers.get("X-WP-TotalPages") or 1):
                    break
                page += 1
                time.sleep(0.4)
    INDEX.write_text(json.dumps(out, indent=1), encoding="utf-8")
    return out


def case_key(text):
    """"24-069MA" / "26-022MIL" -> "24-069".

    The letter suffix differs between the agenda and the report filename for
    the same case (24-076UDR on the agenda, 24-076MI on the report), so it is
    deliberately not part of the key.
    """
    if not text:
        return None
    m = re.search(r"(\d{2})-(\d{2,4})", str(text).upper())
    return f"{m.group(1)}-{m.group(2)}" if m else None


def ref_key(text):
    m = re.search(r"REFERRAL[\s\-_]*(\d{3,4})", str(text or "").upper())
    return f"REF{m.group(1)}" if m else None


_STREET_WORD = {
    "STREET", "ST", "AVENUE", "AVE", "ROAD", "RD", "DRIVE", "DR", "LANE", "LN",
    "PLACE", "PL", "COURT", "CT", "BOULEVARD", "BLVD", "WAY", "TERRACE", "TER",
    "SQUARE", "SQ", "PIKE", "HIGHWAY", "HWY", "ROW", "CIRCLE", "CIR", "WHARF"}
# Words that appear in report filenames around the address and are not part of it.
_FILENAME_NOISE = {
    "REFERRAL", "STAFF", "REPORT", "PRELIM", "PRELIMINARY", "PLAN", "MASTER",
    "FINAL", "MAJOR", "MINOR", "CHANGE", "ORDINANCE", "AMENDMENT", "PDF",
    "UDR", "DPR", "MIL", "MIS", "REVISED", "UPDATED", "AND", "THE", "OF",
    "SUBDIVISION", "DEVELOPMENT", "OVERLAY", "HOUSING", "STUDENT", "BILLBOARD"}


def addr_key(text):
    """"70 Houghton Street" -> "70|HOUGHTON".

    Report filenames carry the address as well as the case number, which
    recovers matches where the agenda printed no case number. BOTH the house
    number and the street name must agree -- street alone would attach a report
    to any project on the same street, exactly the wrong attribution this
    pipeline exists to avoid.

    The number taken is the LAST bare number before the street word, so a
    filename's leading case or referral number is not mistaken for a house
    number ("Referral 3618 1331 Eddy St" is 1331 Eddy, not 3618), and a hyphen
    range resolves the same way on both sides ("16-22 Grove" -> 22|GROVE).
    """
    if not text:
        return None
    toks = re.sub(r"[^\w\s]", " ", str(text)).split()
    num = None
    for tok in toks:
        up = tok.upper()
        if tok.isdigit():
            num = tok
            continue
        if up in _STREET_WORD or up in _FILENAME_NOISE or not up.isalpha() or len(up) < 3:
            continue
        if num:
            return f"{num}|{up}"
    return None


def index_by_key(reports: dict) -> dict:
    idx = {}
    for su in reports:
        fn = su.rsplit("/", 1)[-1]
        slug = fn.replace("-", " ").replace("_", " ")
        for k in filter(None, [case_key(fn), ref_key(fn), addr_key(slug)]):
            idx.setdefault(k, []).append(su)
    return idx


def extract_fields(text: str) -> dict:
    """Program fields a staff report can supply that an agenda usually cannot."""
    acres = _ACRES.search(text)
    try:
        acreage = float(acres.group(1)) if acres else None
    except (ValueError, AttributeError):
        acreage = None
    return {
        "residential_units":   _num(_UNITS.search(text)),
        "parking_spaces":      _num(_PARKING.search(text)),
        "total_gsf":           _num(_SF.search(text)),
        "num_stories":         _num(_STORIES.search(text)),
        "building_count":      _num(_BUILDINGS.search(text)),
        "zoning_district_raw": zoning(text),
        "applicant_entity":    applicant(text),
        "site_acreage":        acreage,
    }


def run(dry_run: bool = False, refresh: bool = False) -> dict:
    init_db()
    if refresh or not INDEX.exists():
        reports = enumerate_reports()
    else:
        reports = json.loads(INDEX.read_text(encoding="utf-8"))
    idx = index_by_key(reports)
    log.info("Staff reports indexed: %d PDFs, %d case keys", len(reports), len(idx))

    PDF_DIR.mkdir(parents=True, exist_ok=True)
    session = get_session()
    stats = {"matched": 0, "downloaded": 0, "parsed": 0, "filled": 0, "no_match": 0}
    per_field = {}
    try:
        projects = [p for p in session.query(Project)
                    .filter(Project.bpda_url.like("manual:ri-%")).all()
                    if p.city == "Providence"]
        with httpx.Client(headers=HEADERS, follow_redirects=True) as client:
            for p in projects:
                keys = [k for k in (case_key(p.case_number), ref_key(p.case_number),
                                    ref_key(p.description), addr_key(p.address)) if k]
                urls = [u for k in keys for u in idx.get(k, [])]
                if not urls:
                    stats["no_match"] += 1
                    continue
                stats["matched"] += 1

                url = urls[0]
                name = re.sub(r"[^\w.\-]", "_", url.rsplit("/", 1)[-1])
                path = PDF_DIR / name
                if not path.exists():
                    try:
                        r = client.get(url, timeout=120)
                        r.raise_for_status()
                        path.write_bytes(r.content)
                        stats["downloaded"] += 1
                        time.sleep(0.5)
                    except Exception as exc:
                        log.warning("  download failed %s: %s", name[:48], exc)
                        continue
                try:
                    text = pdf_text(path, max_pages=25)
                except Exception as exc:
                    log.warning("  pdf parse failed %s: %s", name[:48], exc)
                    continue
                if len(text) < 400:
                    continue
                stats["parsed"] += 1

                for f, v in extract_fields(text).items():
                    if v in (None, "", 0):
                        continue
                    # Fill nulls only: the agenda stays primary wherever it spoke.
                    if getattr(p, f, None) not in (None, "", False):
                        continue
                    if not dry_run:
                        setattr(p, f, v)
                    per_field[f] = per_field.get(f, 0) + 1
                    stats["filled"] += 1
        if not dry_run:
            session.commit()
    finally:
        session.close()

    log.info("\n=== Providence staff reports ===")
    for k, v in stats.items():
        log.info("  %-12s %4d", k, v)
    log.info("  fields filled (nulls only):")
    for f, n in sorted(per_field.items(), key=lambda x: -x[1]):
        log.info("     %-22s %3d", f, n)
    return stats


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--refresh", action="store_true")
    a = ap.parse_args()
    run(a.dry_run, a.refresh)
