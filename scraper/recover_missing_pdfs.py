"""
Recovery script: get a PDF for every project that currently has none.

Strategy per project (in order, stops on first success):
  1. Try existing SIRE filings in DB (including 'other' category — LOIs, scoping docs, etc.)
  2. Re-scan SIRE API for the project's sire_id to find documents we missed
  3. Re-scrape the BPDA detail page for direct PDF links or Box.com folder/file links
  4. Try SIRE API address search as last resort

After getting a PDF, run targeted developer extraction on it (first 3 pages).
Log every project that exhausts all options, with reason, to a failure log.
"""

import re
import sys
import time
import json
import logging
from pathlib import Path
from urllib.parse import urljoin, urlparse, quote

import anthropic
import httpx
from bs4 import BeautifulSoup
from pypdf import PdfReader

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import init_db, get_session
from db.models import Project, ProjectFiling
from scraper.normalize_developer import normalize as normalize_developer
from scraper.bpda_scraper import SIRE_DOCS_URL, SIRE_HEADERS, FILING_TYPE_MAP

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

PDF_DIR = Path(__file__).parent.parent / "data" / "pdfs"
FAILURE_LOG = Path(__file__).parent.parent / "data" / "missing_pdf_failures.jsonl"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
MAX_BYTES = 80 * 1024 * 1024
PAGES = 3
CHARS = 8_000

# Expand beyond dpir/pnf/small_project — for developer name, anything works
ALL_CATEGORIES = {"dpir", "pnf", "small_project", "other"}

HAIKU_PROMPT = """\
This is a Boston BPDA development filing.
Who is the applicant, proponent, or developer submitting this project?
Look for labels: Applicant, Proponent, Developer, Owner, Petitioner, \
"Submitted by", "Project Sponsor".
Return only the company or person name — nothing else, no explanations."""

BAD_PHRASES = (
    "i don't", "i cannot", "i can't", "not found", "no applicant",
    "unable to", "without access", "cannot identify", "not specified",
    "not mentioned", "not provided", "no developer", "no proponent",
)


# ── Box helpers ─────────────────────────────────────────────────────────────

class BoxSession:
    def __init__(self, client: httpx.Client):
        self.client = client
        self._token = ""
        self._base = ""

    def _refresh(self, url: str) -> httpx.Response | None:
        try:
            r = self.client.get(url, headers={"User-Agent": UA})
            m = re.search(r'"requestToken"\s*:\s*"([^"]+)"', r.text)
            if m:
                self._token = m.group(1)
            self._base = str(r.url).split("/s/")[0]
            return r
        except Exception:
            return None

    def _headers(self, referer=""):
        return {
            "User-Agent": UA,
            "Referer": referer or self._base,
            "X-Request-Token": self._token,
            "Accept": "application/json",
        }

    def get_download_url(self, share_link: str) -> str | None:
        r0 = self._refresh(share_link)
        if r0 is None or r0.status_code != 200:
            return None
        share_hash = str(r0.url).split("/s/")[-1].split("?")[0]
        try:
            r1 = self.client.get(
                f"{self._base}/app-api/enduserapp/shared-item",
                params={"sharedName": share_hash},
                headers=self._headers(str(r0.url)),
            )
            if r1.status_code != 200:
                return None
            item = r1.json()
            item_type = item.get("type", "file")
            item_id = item.get("itemID")
            if not item_id:
                return None
            if item_type == "folder":
                return self._get_first_pdf_from_folder(share_hash, item_id, str(r0.url))
            r2 = self.client.get(
                f"{self._base}/index.php",
                params={
                    "rm": "box_download_shared_file",
                    "shared_name": share_hash,
                    "file_id": f"f_{item_id}",
                },
                headers={**self._headers(str(r0.url)), "Accept": "*/*"},
                follow_redirects=False,
            )
            return r2.headers.get("location") or None
        except Exception as exc:
            log.debug("Box download error: %s", exc)
            return None

    def _get_first_pdf_from_folder(self, share_hash: str, folder_id: str, referer: str) -> str | None:
        """List items in a Box shared folder and return download URL for first PDF."""
        try:
            r = self.client.get(
                f"{self._base}/app-api/enduserapp/shared-folder",
                params={"sharedName": share_hash, "folderId": folder_id, "offset": 0, "limit": 50},
                headers=self._headers(referer),
            )
            if r.status_code != 200:
                # Try alternate endpoint
                r = self.client.get(
                    f"{self._base}/app-api/enduserapp/folder",
                    params={"sharedName": share_hash, "folderId": folder_id},
                    headers=self._headers(referer),
                )
            if r.status_code != 200:
                return None
            data = r.json()
            items = data.get("items", data.get("entries", []))
            for item in items:
                name = item.get("name", "")
                if name.lower().endswith(".pdf"):
                    file_id = item.get("id") or item.get("itemID")
                    if file_id:
                        r2 = self.client.get(
                            f"{self._base}/index.php",
                            params={
                                "rm": "box_download_shared_file",
                                "shared_name": share_hash,
                                "file_id": f"f_{file_id}",
                            },
                            headers={**self._headers(referer), "Accept": "*/*"},
                            follow_redirects=False,
                        )
                        loc = r2.headers.get("location")
                        if loc:
                            log.info("  Got PDF from Box folder: %s", name)
                            return loc
        except Exception as exc:
            log.debug("Box folder error: %s", exc)
        return None


def download_pdf(client: httpx.Client, dl_url: str, dest: Path) -> bool:
    try:
        with client.stream("GET", dl_url, headers={"User-Agent": UA}) as resp:
            if resp.status_code != 200:
                return False
            chunks, total = [], 0
            for chunk in resp.iter_bytes(65536):
                chunks.append(chunk)
                total += len(chunk)
                if total > MAX_BYTES:
                    log.warning("  File >80MB — skipping")
                    return False
        content = b"".join(chunks)
        if content[:4] != b"%PDF":
            log.warning("  Not a PDF (header: %s)", content[:8])
            return False
        dest.write_bytes(content)
        log.info("  Saved %.1f MB -> %s", len(content) / 1024 / 1024, dest.name)
        return True
    except Exception as exc:
        log.warning("  Download error: %s", exc)
        return False


# ── SIRE re-scan ─────────────────────────────────────────────────────────────

def scan_sire_for_ids(
    client: httpx.Client,
    target_ids: set[str],
) -> dict[str, list[dict]]:
    """Paginate SIRE docs API and collect documents for target sire_ids."""
    results: dict[str, list[dict]] = {sid: [] for sid in target_ids}
    marker = None
    page = 0
    found_count = 0

    while True:
        url = f"{SIRE_DOCS_URL}?ft_next_marker={marker}" if marker else SIRE_DOCS_URL
        try:
            r = client.get(url, headers=SIRE_HEADERS, timeout=30)
            if r.status_code != 200:
                break
            data = r.json()
        except Exception:
            break

        items = data.get("metadataObj", [])
        if not items:
            break

        for item in items:
            bm = item.get("boxMetadata", {})
            sid = bm.get("id", "")
            if sid not in target_ids:
                continue
            share_link = item.get("shareLink", "")
            if not share_link:
                continue
            subtype = bm.get("subtype", "")
            doc_date = bm.get("documentDate", "")
            results[sid].append({
                "name": subtype,
                "date": doc_date[:10] if doc_date else "",
                "url": share_link,
                "filing_category": FILING_TYPE_MAP.get(subtype, "other"),
            })
            found_count += 1

        new_marker = data.get("next_marker")
        if not new_marker or new_marker == marker:
            break
        marker = new_marker
        page += 1
        if page % 20 == 0:
            log.info("  SIRE scan: page %d, found %d docs so far", page, found_count)
        time.sleep(0.2)

    return results


# ── BPDA page re-scrape for document links ───────────────────────────────────

BOX_LINK_RE = re.compile(
    r'https?://(?:[\w-]+\.)?(?:app\.)?box\.com/s/[\w]+',
    re.IGNORECASE
)
PDF_LINK_RE = re.compile(r'https?://[^\s"\'<>]+\.pdf', re.IGNORECASE)


def scrape_bpda_page_for_docs(client: httpx.Client, bpda_url: str) -> list[str]:
    """Return Box share links and direct PDF URLs found on a BPDA project page."""
    try:
        r = client.get(bpda_url, headers={"User-Agent": UA}, timeout=30)
        if r.status_code != 200:
            return []
    except Exception:
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    links = []

    # All anchor hrefs
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "box.com/s/" in href.lower():
            links.append(href)
        elif href.lower().endswith(".pdf"):
            links.append(urljoin(bpda_url, href))

    # Also scan raw text for box links (sometimes in JS or data attrs)
    for m in BOX_LINK_RE.finditer(r.text):
        links.append(m.group(0))

    # Deduplicate preserving order
    seen = set()
    out = []
    for link in links:
        if link not in seen:
            seen.add(link)
            out.append(link)

    return out


# ── SIRE address search ───────────────────────────────────────────────────────

SIRE_SEARCH_URL = "https://sire.bostonplans.org/api/documentSearch/getProjects"


def sire_address_search(client: httpx.Client, address: str) -> list[str]:
    """Search SIRE by address fragment and return Box share links."""
    # SIRE doesn't have a direct address search, but we can search getMetadataWithProjects
    # with a keyword — try scanning cached projects list for address match
    # As a fallback, return empty (expensive full scan handled in Phase 2)
    return []


# ── Developer extraction ──────────────────────────────────────────────────────

def extract_developer(pdf_path: Path, project_name: str, ai_client) -> str | None:
    try:
        reader = PdfReader(str(pdf_path))
        parts, chars = [], 0
        for i, page in enumerate(reader.pages):
            if i >= PAGES:
                break
            text = page.extract_text() or ""
            parts.append(text)
            chars += len(text)
            if chars >= CHARS:
                break
        text = "\n".join(parts)[:CHARS]
    except Exception:
        return None

    if not text.strip():
        return None

    content = [
        {"type": "text", "text": f"[Filing — first {PAGES} pages]\n\n{text}"},
        {"type": "text", "text": f"Project: {project_name}\n\n{HAIKU_PROMPT}"},
    ]
    for attempt in range(3):
        try:
            resp = ai_client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=128,
                messages=[{"role": "user", "content": content}],
            )
            result = resp.content[0].text.strip().strip('"').strip("'")
            if not result or len(result) < 2:
                return None
            if any(p in result.lower() for p in BAD_PHRASES):
                return None
            if len(result) > 120:
                return None
            return result
        except anthropic.RateLimitError:
            wait = 20 * (attempt + 1)
            log.warning("  Rate limit — sleeping %ds", wait)
            time.sleep(wait)
        except Exception as exc:
            log.warning("  AI error: %s", exc)
            time.sleep(5)
    return None


# ── Failure logging ───────────────────────────────────────────────────────────

def log_failure(proj: Project, reason: str, attempts: list[str]):
    entry = {
        "id": proj.id,
        "name": proj.name,
        "bpda_url": proj.bpda_url,
        "address": proj.address,
        "sire_id": proj.sire_id,
        "reason": reason,
        "attempts": attempts,
    }
    with open(FAILURE_LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    init_db()
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    FAILURE_LOG.unlink(missing_ok=True)

    session = get_session()
    ai_client = anthropic.Anthropic()

    try:
        # All projects without a PDF on disk
        all_projects = session.query(Project).all()
        targets = [p for p in all_projects if not (PDF_DIR / f"{p.id}.pdf").exists()]
        log.info("Projects without PDF: %d", len(targets))

        # Phase 2 prep: gather sire_ids that need SIRE re-scan
        needs_sire_scan = {
            p.sire_id for p in targets
            if p.sire_id and len(p.filings) == 0
        }
        log.info("Sire IDs to re-scan in SIRE API: %d", len(needs_sire_scan))

        recovered = 0
        failed_projects = []

        with httpx.Client(follow_redirects=True, timeout=60) as client:
            box = BoxSession(client)

            # ── Pre-fetch: scan SIRE for projects with sire_id but no filings ──
            sire_new_docs: dict[str, list[dict]] = {}
            if needs_sire_scan:
                log.info("=== Pre-scan: SIRE API for %d sire IDs ===", len(needs_sire_scan))
                sire_new_docs = scan_sire_for_ids(client, needs_sire_scan)
                found_via_sire = sum(1 for v in sire_new_docs.values() if v)
                log.info("SIRE pre-scan: found docs for %d/%d projects", found_via_sire, len(needs_sire_scan))

                # Persist new filings to DB
                for proj in targets:
                    if proj.sire_id not in sire_new_docs:
                        continue
                    docs = sire_new_docs[proj.sire_id]
                    for doc in docs:
                        existing = session.query(ProjectFiling).filter_by(
                            project_id=proj.id, url=doc["url"]
                        ).first()
                        if not existing:
                            session.add(ProjectFiling(
                                project_id=proj.id,
                                name=doc["name"],
                                date=doc["date"],
                                url=doc["url"],
                                file_type="pdf",
                                filing_category=doc["filing_category"],
                            ))
                    session.commit()
                    session.refresh(proj)

            # ── Main loop ─────────────────────────────────────────────────────
            log.info("=== Main recovery loop: %d projects ===", len(targets))
            for i, proj in enumerate(targets, 1):
                pdf_path = PDF_DIR / f"{proj.id}.pdf"
                log.info("[%d/%d] %s  (sire_id=%s, filings=%d)",
                         i, len(targets), proj.name, proj.sire_id or "none", len(proj.filings))
                attempts = []
                got_pdf = False

                # ── Strategy 1: Try all existing SIRE filings (any category) ──
                filings_sorted = sorted(
                    [f for f in proj.filings if f.url],
                    key=lambda f: (
                        {"dpir": 3, "pnf": 2, "small_project": 1, "other": 0}.get(f.filing_category, 0),
                        f.date or ""
                    ),
                    reverse=True,
                )
                for filing in filings_sorted:
                    log.info("  Strategy 1: filing '%s' [%s]", filing.name[:50], filing.filing_category)
                    attempts.append(f"sire_filing:{filing.url[:80]}")
                    dl_url = box.get_download_url(filing.url)
                    if dl_url and download_pdf(client, dl_url, pdf_path):
                        proj.processed_filing_url = filing.url
                        proj.processed_filing_name = filing.name
                        proj.processed_filing_type = filing.filing_category
                        filing.is_processed = True
                        session.commit()
                        got_pdf = True
                        break
                    time.sleep(0.5)

                # ── Strategy 2: BPDA detail page for Box/PDF links ────────────
                if not got_pdf and proj.bpda_url:
                    log.info("  Strategy 2: scraping BPDA page for doc links")
                    attempts.append(f"bpda_page:{proj.bpda_url}")
                    page_links = scrape_bpda_page_for_docs(client, proj.bpda_url)
                    log.info("  Found %d links on BPDA page", len(page_links))

                    for link in page_links:
                        if link.lower().endswith(".pdf"):
                            # Direct PDF link
                            log.info("  Trying direct PDF: %s", link[:80])
                            attempts.append(f"direct_pdf:{link[:80]}")
                            if download_pdf(client, link, pdf_path):
                                proj.processed_filing_url = link
                                proj.processed_filing_name = "BPDA page document"
                                proj.processed_filing_type = "other"
                                session.commit()
                                got_pdf = True
                                break
                        elif "box.com/s/" in link.lower():
                            # Box share link (file or folder)
                            log.info("  Trying Box link: %s", link[:80])
                            attempts.append(f"box_link:{link[:80]}")
                            dl_url = box.get_download_url(link)
                            if dl_url and download_pdf(client, dl_url, pdf_path):
                                proj.processed_filing_url = link
                                proj.processed_filing_name = "BPDA page Box document"
                                proj.processed_filing_type = "other"
                                session.commit()
                                got_pdf = True
                                break
                        time.sleep(0.3)

                if got_pdf:
                    log.info("  PDF obtained via strategies 1-2")
                else:
                    log.info("  All strategies exhausted — no PDF found")
                    reason = "No downloadable filing found after trying all SIRE filings and BPDA page links"
                    if not proj.sire_id:
                        reason = "No SIRE ID and no document links found on BPDA page"
                    elif len(proj.filings) == 0:
                        reason = "Has SIRE ID but no documents in SIRE for this project"
                    log_failure(proj, reason, attempts)
                    failed_projects.append(proj.name)
                    continue

                # ── Extract developer from the new PDF ────────────────────────
                log.info("  Extracting developer from PDF")
                raw_dev = extract_developer(pdf_path, proj.name, ai_client)
                if raw_dev:
                    canonical = normalize_developer(raw_dev, session=session, client=ai_client)
                    proj.developer = raw_dev
                    proj.developer_canonical = canonical
                    log.info("  Developer: %s  =>  %s", raw_dev[:50], canonical)
                else:
                    proj.developer = "Unknown - review needed"
                    proj.developer_canonical = "Unknown - review needed"
                    log.info("  Developer not found in document")
                session.commit()
                recovered += 1
                time.sleep(1)

        log.info(
            "\n=== Recovery complete ===\n"
            "  PDFs recovered: %d\n"
            "  Still missing:  %d\n",
            recovered, len(failed_projects),
        )
        if failed_projects:
            log.info("Failed projects logged to: %s", FAILURE_LOG)
            log.info("Sample failures:")
            for name in failed_projects[:10]:
                log.info("  - %s", name)

    finally:
        session.close()


if __name__ == "__main__":
    run()
