r"""
Harvest Cranston City Plan Commission STAFF REPORTS and APPLICATION MATERIALS.

Why
---
Cranston's applicant coverage sits at ~12% and that is a real property of the
source: only 12 of 93 documents on the Secretary of State portal carry an
Applicant or Owner label. The portal hosts notices and minutes only, and a
Cranston agenda item names the proposal and the parcel but usually not the
sponsor.

Cranston publishes the rest on its own site. Every Plan Commission meeting has
a page listing, per project, an APPLICATION MATERIALS pdf and a STAFF REPORT
pdf, with the project's address in the link text:

    116 Shaw Avenue - Application Materials
    116 Shaw Avenue - Staff Memo

The application is where the applicant is actually named, so this is the
document type that closes Cranston's gap -- not a better regex over agendas.

Structure
---------
Meeting pages are indexed by year ("past meeting materials" pages) and have
inconsistent slugs (/city-plan-commission6.3.25/, /november-4-2025-city-plan-
commission/, /city-plan-commisson-1.6.26/ -- including a typo'd "commisson"),
so they are discovered by crawling the yearly index rather than constructed.

Precedence
----------
FILL NULLS ONLY, exactly as the Providence staff-report harvester does. A value
an agenda already stated is left alone; these documents reach only the fields
the agenda was silent about. Matching is on house number AND street name, never
street alone.
"""

import re
import sys
import json
import time
import logging
from pathlib import Path
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import init_db, get_session
from db.models import Project
from scraper.ri_extract import (
    _UNITS, _PARKING, _SF, _ACRES, _STORIES, _BUILDINGS, _num, zoning, applicant,
)
from scraper.ri_meeting_docs import HEADERS, pdf_text
from scraper.ri_pvd_staff_reports import addr_key

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

ROOT = Path(__file__).parent.parent
INDEX = ROOT / "data" / "ri_cranston_materials.json"
PDF_DIR = ROOT / "data" / "ri_pdfs" / "cranston_materials"
BASE = "https://www.cranstonri.gov"

YEAR_INDEXES = [
    f"{BASE}/past-meeting-materials-2026/default.aspx",
    f"{BASE}/2025-past-meetings-materials/",
    f"{BASE}/2024-past-meetings-materials/",
    f"{BASE}/2023-past-meeting-materials/",
]

# A meeting page slug, however it is spelled. "commisson" is the city's typo and
# is matched deliberately rather than corrected.
_MEETING_SLUG = re.compile(
    r"(city[-\s]?plan[-\s]?commiss?[io]?on|development[-\s]?plan[-\s]?review|cpc)",
    re.I)

# Documents worth reading. Public comment, affidavits and minutes are skipped:
# they carry no program data and public comment is what polluted the pipeline
# with speakers' home addresses in the first place.
_WANTED = re.compile(r"(staff\s*(report|memo)|application\s*materials|"
                     r"combined\s*application)", re.I)
_SKIP = re.compile(r"(public\s*comment|affidavit|exhibit|minutes|agenda|"
                   r"canvassing|notice)", re.I)


def meeting_pages(client: httpx.Client) -> list[str]:
    """Every Plan Commission / DPR meeting page linked from the year indexes."""
    out = []
    for idx in YEAR_INDEXES:
        try:
            r = client.get(idx, timeout=60)
            if r.status_code != 200:
                log.warning("  index %s -> HTTP %d", idx.rsplit('/', 2)[-2], r.status_code)
                continue
        except Exception as exc:
            log.warning("  index failed %s: %s", idx, exc)
            continue
        s = BeautifulSoup(r.text, "html.parser")
        for a in s.find_all("a", href=True):
            href = a["href"]
            if not _MEETING_SLUG.search(href):
                continue
            url = urljoin(BASE, href)
            if url not in out:
                out.append(url)
        time.sleep(0.5)
    return out


def documents_on(client: httpx.Client, page: str) -> list[dict]:
    """Staff reports and application materials linked from one meeting page."""
    try:
        r = client.get(page, timeout=60)
        if r.status_code != 200:
            return []
    except Exception:
        return []
    s = BeautifulSoup(r.text, "html.parser")
    docs = []
    for a in s.find_all("a", href=True):
        href = a["href"]
        if ".pdf" not in href.lower():
            continue
        label = a.get_text(" ", strip=True)
        if not label or _SKIP.search(label) or not _WANTED.search(label):
            continue
        docs.append({"label": label, "url": urljoin(page, href), "page": page})
    return docs


def build_index(refresh: bool = False) -> list[dict]:
    if INDEX.exists() and not refresh:
        return json.loads(INDEX.read_text(encoding="utf-8"))
    docs = []
    with httpx.Client(headers=HEADERS, follow_redirects=True) as client:
        pages = meeting_pages(client)
        log.info("Cranston meeting pages found: %d", len(pages))
        for i, pg in enumerate(pages, 1):
            found = documents_on(client, pg)
            docs.extend(found)
            if i % 10 == 0:
                log.info("  %d/%d pages, %d documents", i, len(pages), len(docs))
            time.sleep(0.4)
    INDEX.write_text(json.dumps(docs, indent=1), encoding="utf-8")
    log.info("Cranston project documents indexed: %d", len(docs))
    return docs


def extract_fields(text: str) -> dict:
    acres = _ACRES.search(text)
    try:
        acreage = float(acres.group(1)) if acres else None
    except (ValueError, AttributeError):
        acreage = None
    return {
        "applicant_entity":    applicant(text),
        "residential_units":   _num(_UNITS.search(text)),
        "parking_spaces":      _num(_PARKING.search(text)),
        "total_gsf":           _num(_SF.search(text)),
        "num_stories":         _num(_STORIES.search(text)),
        "building_count":      _num(_BUILDINGS.search(text)),
        "zoning_district_raw": zoning(text),
        "site_acreage":        acreage,
    }


def run(dry_run: bool = False, refresh: bool = False) -> dict:
    init_db()
    docs = build_index(refresh)
    if not docs:
        log.warning("No Cranston documents indexed")
        return {}

    # Index by address key taken from the LINK TEXT, which names the project.
    idx = {}
    for d in docs:
        k = addr_key(d["label"])
        if k:
            idx.setdefault(k, []).append(d)
    log.info("Documents indexed under %d address keys", len(idx))

    PDF_DIR.mkdir(parents=True, exist_ok=True)
    session = get_session()
    stats = {"matched": 0, "downloaded": 0, "parsed": 0, "filled": 0, "no_match": 0}
    per_field = {}
    try:
        projects = [p for p in session.query(Project)
                    .filter(Project.bpda_url.like("manual:ri-%")).all()
                    if p.city == "Cranston"]
        with httpx.Client(headers=HEADERS, follow_redirects=True) as client:
            for p in projects:
                k = addr_key(p.address)
                hits = idx.get(k, []) if k else []
                if not hits:
                    stats["no_match"] += 1
                    continue
                stats["matched"] += 1

                # Application materials first: that is where the sponsor signs.
                hits = sorted(hits, key=lambda d: 0 if re.search(
                    r"application", d["label"], re.I) else 1)
                for d in hits[:2]:
                    name = re.sub(r"[^\w.\-]", "_", d["url"].rsplit("/", 1)[-1])[:110]
                    path = PDF_DIR / name
                    if not path.exists():
                        try:
                            r = client.get(d["url"], timeout=180)
                            r.raise_for_status()
                            path.write_bytes(r.content)
                            stats["downloaded"] += 1
                            time.sleep(0.5)
                        except Exception as exc:
                            log.warning("  download failed %s: %s", name[:44], exc)
                            continue
                    try:
                        text = pdf_text(path, max_pages=25)
                    except Exception as exc:
                        log.warning("  pdf parse failed %s: %s", name[:44], exc)
                        continue
                    if len(text) < 400:
                        continue
                    stats["parsed"] += 1

                    for f, v in extract_fields(text).items():
                        if v in (None, "", 0):
                            continue
                        if getattr(p, f, None) not in (None, "", False):
                            continue          # agenda stays primary
                        if not dry_run:
                            setattr(p, f, v)
                        per_field[f] = per_field.get(f, 0) + 1
                        stats["filled"] += 1
        if not dry_run:
            session.commit()
    finally:
        session.close()

    log.info("\n=== Cranston materials ===")
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
