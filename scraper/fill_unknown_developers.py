"""
Fill-in developer names for all projects where developer_canonical is null/unknown.

Per-project strategy (stops at first success):
  1. PDF on disk  -> extract from first 5 pages (label-targeted prompt)
  2. No PDF       -> scrape BPDA page for doc links, download PDF, then step 1
  3. Either path  -> also try extracting proponent from BPDA page HTML
  4. Normalize every found name through canonical rules + Haiku
  5. Log every still-unknown project with reason for manual review
"""

import re
import sys
import time
import json
import logging
from pathlib import Path
from urllib.parse import urljoin

import anthropic
import httpx
from bs4 import BeautifulSoup
from pypdf import PdfReader

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import init_db, get_session
from db.models import Project
from scraper.normalize_developer import (
    normalize, is_real_company, _rule_match,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

PDF_DIR    = Path(__file__).parent.parent / "data" / "pdfs"
FAIL_LOG   = Path(__file__).parent.parent / "data" / "unknown_developer_failures.jsonl"
MODEL      = "claude-haiku-4-5-20251001"
MAX_PAGES  = 5
MAX_CHARS  = 12_000
MAX_BYTES  = 80 * 1024 * 1024

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

_BAD_PHRASES = (
    "i don't", "i cannot", "i can't", "not found", "no applicant",
    "unable to", "without access", "cannot identify", "not specified",
    "not mentioned", "not provided", "no developer", "no proponent",
    "not identified", "not listed", "not available",
)

PDF_PROMPT = """\
This is a Boston BPDA development filing (PNF, DPIR, or Small Project Review).

Find the applicant, proponent, developer, or project sponsor.

Look for these exact labels in the document:
  Applicant, Proponent, Developer, Owner, Project Sponsor,
  "Submitted by", "Prepared by", "On behalf of", Petitioner

Return ONLY the company or person name that follows one of those labels.
No explanations. No punctuation other than what is part of the name.
If multiple names appear, return the primary applicant or project sponsor."""

HAIKU_NORM_PROMPT = """\
This is the legal entity name from a Boston real estate development filing. \
What is the well-known parent development company behind this entity? \
Common Boston developers include HYM Investment Group, Marcus Partners, \
The Davis Companies, WS Development, The Fallon Company, Related Beal, \
National Development, Samuels and Associates, BioMed Realty, Oxford Properties, \
CIM Group, The Abbey Group, Skanska, Cabot Cabot and Forbes, Accordia Development, \
HRP, Carr Properties, Lendlease, Greystar, and others. \
Return only the canonical company name. \
If it is a joint venture between two real companies, return both separated by a slash. \
If you genuinely cannot identify it, return UNKNOWN."""

BOX_RE  = re.compile(r'https?://(?:[\w-]+\.)?(?:app\.)?box\.com/s/[\w]+', re.I)
PDF_RE  = re.compile(r'https?://[^\s"\'<>]+\.pdf', re.I)

# Labels we look for in BPDA page HTML
_HTML_LABELS = re.compile(
    r'(?:Applicant|Proponent|Developer|Owner|Project\s+Sponsor|Submitted\s+by|'
    r'Prepared\s+by|On\s+behalf\s+of)\s*[:\-]?\s*([^\n<]{3,80})',
    re.IGNORECASE,
)


# ── Box session ──────────────────────────────────────────────────────────────

class BoxSession:
    def __init__(self, client: httpx.Client):
        self.client  = client
        self._token  = ""
        self._base   = ""

    def _refresh(self, url: str):
        try:
            r = self.client.get(url, headers={"User-Agent": UA})
            m = re.search(r'"requestToken"\s*:\s*"([^"]+)"', r.text)
            if m:
                self._token = m.group(1)
            self._base = str(r.url).split("/s/")[0]
            return r
        except Exception:
            return None

    def _hdrs(self, referer=""):
        return {
            "User-Agent": UA,
            "Referer": referer or self._base,
            "X-Request-Token": self._token,
            "Accept": "application/json",
        }

    def get_download_url(self, share_link: str) -> str | None:
        r0 = self._refresh(share_link)
        if not r0 or r0.status_code != 200:
            return None
        share_hash = str(r0.url).split("/s/")[-1].split("?")[0]
        try:
            r1 = self.client.get(
                f"{self._base}/app-api/enduserapp/shared-item",
                params={"sharedName": share_hash},
                headers=self._hdrs(str(r0.url)),
            )
            if r1.status_code != 200:
                return None
            item = r1.json()
            if item.get("type") == "folder":
                return self._folder_first_pdf(share_hash, item.get("itemID"), str(r0.url))
            r2 = self.client.get(
                f"{self._base}/index.php",
                params={
                    "rm": "box_download_shared_file",
                    "shared_name": share_hash,
                    "file_id": f"f_{item.get('itemID')}",
                },
                headers={**self._hdrs(str(r0.url)), "Accept": "*/*"},
                follow_redirects=False,
            )
            return r2.headers.get("location") or None
        except Exception:
            return None

    def _folder_first_pdf(self, share_hash: str, folder_id: str, referer: str) -> str | None:
        try:
            r = self.client.get(
                f"{self._base}/app-api/enduserapp/shared-folder",
                params={"sharedName": share_hash, "folderId": folder_id, "offset": 0, "limit": 50},
                headers=self._hdrs(referer),
            )
            if r.status_code != 200:
                return None
            items = r.json().get("items", r.json().get("entries", []))
            for item in items:
                if item.get("name", "").lower().endswith(".pdf"):
                    fid = item.get("id") or item.get("itemID")
                    r2 = self.client.get(
                        f"{self._base}/index.php",
                        params={"rm": "box_download_shared_file",
                                "shared_name": share_hash, "file_id": f"f_{fid}"},
                        headers={**self._hdrs(referer), "Accept": "*/*"},
                        follow_redirects=False,
                    )
                    loc = r2.headers.get("location")
                    if loc:
                        return loc
        except Exception:
            pass
        return None


# ── Download ─────────────────────────────────────────────────────────────────

def download_pdf(http: httpx.Client, url: str, dest: Path) -> bool:
    try:
        with http.stream("GET", url, headers={"User-Agent": UA}) as resp:
            if resp.status_code != 200:
                return False
            chunks, total = [], 0
            for chunk in resp.iter_bytes(65536):
                chunks.append(chunk)
                total += len(chunk)
                if total > MAX_BYTES:
                    return False
        data = b"".join(chunks)
        if data[:4] != b"%PDF":
            return False
        dest.write_bytes(data)
        log.info("  Saved %.1f MB -> %s", len(data) / 1048576, dest.name)
        return True
    except Exception as exc:
        log.debug("  Download error: %s", exc)
        return False


# ── PDF text extraction ──────────────────────────────────────────────────────

def pdf_text(pdf_path: Path) -> str:
    try:
        reader = PdfReader(str(pdf_path))
        parts, chars = [], 0
        for i, page in enumerate(reader.pages):
            if i >= MAX_PAGES:
                break
            t = page.extract_text() or ""
            parts.append(t)
            chars += len(t)
            if chars >= MAX_CHARS:
                break
        return "\n".join(parts)[:MAX_CHARS]
    except Exception as exc:
        log.debug("  pypdf error: %s", exc)
        return ""


# ── AI extraction from PDF text ──────────────────────────────────────────────

def ai_from_pdf(text: str, project_name: str, client: anthropic.Anthropic) -> str | None:
    if not text.strip():
        return None
    content = [
        {"type": "text", "text": f"[Filing — first {MAX_PAGES} pages]\n\n{text}"},
        {"type": "text", "text": f"Project: {project_name}\n\n{PDF_PROMPT}"},
    ]
    for attempt in range(3):
        try:
            resp = client.messages.create(
                model=MODEL, max_tokens=128,
                messages=[{"role": "user", "content": content}],
            )
            result = resp.content[0].text.strip().strip('"').strip("'")
            if not result or len(result) < 2 or len(result) > 130:
                return None
            if any(p in result.lower() for p in _BAD_PHRASES):
                return None
            return result
        except anthropic.RateLimitError:
            wait = 20 * (attempt + 1)
            log.warning("  Rate limit — sleeping %ds", wait)
            time.sleep(wait)
        except Exception as exc:
            log.debug("  AI error attempt %d: %s", attempt + 1, exc)
            time.sleep(5)
    return None


# ── BPDA page HTML scraping ───────────────────────────────────────────────────

def scrape_bpda_page(http: httpx.Client, bpda_url: str) -> tuple[str | None, list[str]]:
    """
    Returns (proponent_from_html, [doc_links]).
    doc_links are Box share links and direct PDF URLs found on the page.
    """
    try:
        r = http.get(bpda_url, headers={"User-Agent": UA}, timeout=30)
        if r.status_code != 200:
            return None, []
    except Exception:
        return None, []

    soup = BeautifulSoup(r.text, "html.parser")

    # ── Try to find proponent in structured HTML ──────────────────────────────
    proponent = None

    # Pattern 1: <dt>Applicant</dt><dd>Name</dd> or <th>...</th><td>Name</td>
    for tag in soup.find_all(["dt", "th", "label", "strong", "b"]):
        label_text = tag.get_text(strip=True).lower()
        if any(kw in label_text for kw in
               ("applicant", "proponent", "developer", "project sponsor",
                "submitted by", "prepared by", "on behalf")):
            _bad_vals = ("zoning petition", "text amendment", "article 80",
                         "n/a", "tbd", "bpda", "boston planning")
            # Try sibling or next element
            sibling = tag.find_next_sibling(["dd", "td", "span", "p"])
            if sibling:
                val = sibling.get_text(strip=True)
                if 2 < len(val) < 120 and not any(b in val.lower() for b in _bad_vals):
                    proponent = val
                    break
            # Try parent row
            parent = tag.find_parent(["tr", "li", "div"])
            if parent and not proponent:
                text = parent.get_text(" ", strip=True)
                m = _HTML_LABELS.search(text)
                if m:
                    val = m.group(1).strip().rstrip(".,;")
                    if 2 < len(val) < 120 and not any(b in val.lower() for b in _bad_vals):
                        proponent = val
                        break

    # Pattern 2: regex scan of visible page text
    if not proponent:
        page_text = soup.get_text(" ")
        m = _HTML_LABELS.search(page_text)
        if m:
            val = m.group(1).strip().rstrip(".,;")
            # Make sure it's not a generic phrase
            bad = ("the following", "this project", "bpda", "boston planning",
                   "n/a", "tbd", "see attached", "multiple",
                   "zoning petition", "text amendment", "article 80")
            if 2 < len(val) < 120 and not any(b in val.lower() for b in bad):
                proponent = val

    # ── Collect document links ────────────────────────────────────────────────
    links = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "box.com/s/" in href.lower():
            href = href.split("?")[0]
            if href not in seen:
                seen.add(href)
                links.append(href)
        elif href.lower().endswith(".pdf"):
            full = urljoin(bpda_url, href)
            if full not in seen:
                seen.add(full)
                links.append(full)
    for m in BOX_RE.finditer(r.text):
        href = m.group(0).split("?")[0]
        if href not in seen:
            seen.add(href)
            links.append(href)

    return proponent, links


# ── Canonical normalization ────────────────────────────────────────────────────

def canonicalize(raw: str, session, ai_client: anthropic.Anthropic) -> str | None:
    """Run raw name through rules then Haiku; return canonical or None."""
    if not raw or not raw.strip():
        return None

    # Rules first
    rule_result = _rule_match(raw)
    if rule_result and is_real_company(rule_result):
        return rule_result

    # normalize() does cache + rules + Haiku
    canonical = normalize(raw, session=session, client=ai_client)
    if canonical and is_real_company(canonical) and canonical != raw:
        return canonical

    # Last resort: direct Haiku with the stronger normalization prompt
    content = f"Legal entity name: {raw}\n\n{HAIKU_NORM_PROMPT}"
    for attempt in range(3):
        try:
            resp = ai_client.messages.create(
                model=MODEL, max_tokens=80,
                messages=[{"role": "user", "content": content}],
            )
            result = resp.content[0].text.strip().strip('"').strip("'")
            if result.upper() == "UNKNOWN" or not result:
                return None
            bad = ("i don't", "i cannot", "i can't", "without access",
                   "cannot identify", "unable to")
            if any(p in result.lower() for p in bad) or len(result) > 100:
                return None
            if is_real_company(result):
                return result
            return None
        except anthropic.RateLimitError:
            time.sleep(20 * (attempt + 1))
        except Exception:
            time.sleep(5)
    return None


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    init_db()
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    FAIL_LOG.parent.mkdir(parents=True, exist_ok=True)
    FAIL_LOG.unlink(missing_ok=True)

    session    = get_session()
    ai_client  = anthropic.Anthropic()

    try:
        all_projects = session.query(Project).all()
        targets = [
            p for p in all_projects
            if not p.developer_canonical
            or p.developer_canonical.strip() in ("", "Unknown", "UNKNOWN",
                                                   "Unknown - review needed")
        ]
        log.info("Projects needing developer fill: %d", len(targets))

        has_pdf  = [p for p in targets if (PDF_DIR / f"{p.id}.pdf").exists()]
        no_pdf   = [p for p in targets if not (PDF_DIR / f"{p.id}.pdf").exists()]
        log.info("  Has PDF: %d  |  No PDF: %d", len(has_pdf), len(no_pdf))

        found_pdf_ai   = 0
        found_html     = 0
        recovered_pdf  = 0
        still_unknown  = 0
        failures       = []

        with httpx.Client(follow_redirects=True, timeout=60) as http:
            box = BoxSession(http)

            # ── Process projects that already have PDFs ──────────────────────
            log.info("=== Phase 1: extract from existing PDFs (%d) ===", len(has_pdf))
            for i, proj in enumerate(has_pdf, 1):
                pdf_path = PDF_DIR / f"{proj.id}.pdf"
                log.info("[%d/%d] %s", i, len(has_pdf), proj.name[:60])

                text  = pdf_text(pdf_path)
                raw   = ai_from_pdf(text, proj.name, ai_client) if text.strip() else None

                if raw:
                    canonical = canonicalize(raw, session, ai_client)
                    proj.developer = raw
                    proj.developer_canonical = canonical
                    log.info("  PDF -> %-50s  =>  %s", raw[:50], canonical or "None")
                    session.commit()
                    found_pdf_ai += 1
                    time.sleep(0.5)
                    continue

                # PDF extraction failed — also try BPDA page HTML
                if proj.bpda_url:
                    html_dev, _ = scrape_bpda_page(http, proj.bpda_url)
                    if html_dev:
                        canonical = canonicalize(html_dev, session, ai_client)
                        proj.developer = html_dev
                        proj.developer_canonical = canonical
                        log.info("  HTML -> %-50s  =>  %s", html_dev[:50], canonical or "None")
                        session.commit()
                        found_html += 1
                        time.sleep(0.3)
                        continue

                # Nothing worked
                log.info("  No developer found")
                proj.developer_canonical = None
                session.commit()
                still_unknown += 1
                failures.append({
                    "id": proj.id, "name": proj.name,
                    "bpda_url": proj.bpda_url,
                    "reason": "PDF exists but AI extraction returned nothing; HTML also blank",
                    "developer_raw": proj.developer,
                })
                time.sleep(0.3)

            # ── Process projects without PDFs ────────────────────────────────
            log.info("=== Phase 2: recover PDF or use HTML (%d) ===", len(no_pdf))
            for i, proj in enumerate(no_pdf, 1):
                pdf_path = PDF_DIR / f"{proj.id}.pdf"
                log.info("[%d/%d] %s", i, len(no_pdf), proj.name[:60])

                got_pdf    = False
                html_dev   = None
                doc_links  = []

                if proj.bpda_url:
                    html_dev, doc_links = scrape_bpda_page(http, proj.bpda_url)

                # Try to download a PDF from page links
                for link in doc_links:
                    if link.lower().endswith(".pdf"):
                        if download_pdf(http, link, pdf_path):
                            proj.processed_filing_url  = link
                            proj.processed_filing_type = "other"
                            session.commit()
                            got_pdf = True
                            break
                    elif "box.com/s/" in link.lower():
                        dl = box.get_download_url(link)
                        if dl and download_pdf(http, dl, pdf_path):
                            proj.processed_filing_url  = link
                            proj.processed_filing_type = "other"
                            session.commit()
                            got_pdf = True
                            break
                    time.sleep(0.3)

                # Try SIRE filings if still no PDF
                if not got_pdf:
                    filings = sorted(
                        [f for f in proj.filings if f.url],
                        key=lambda f: (
                            {"dpir": 3, "pnf": 2, "small_project": 1}.get(f.filing_category, 0),
                            f.date or ""
                        ),
                        reverse=True,
                    )
                    for filing in filings:
                        dl = box.get_download_url(filing.url)
                        if dl and download_pdf(http, dl, pdf_path):
                            proj.processed_filing_url  = filing.url
                            proj.processed_filing_type = filing.filing_category
                            filing.is_processed = True
                            session.commit()
                            got_pdf = True
                            break
                        time.sleep(0.3)

                if got_pdf:
                    recovered_pdf += 1
                    text = pdf_text(pdf_path)
                    raw  = ai_from_pdf(text, proj.name, ai_client) if text.strip() else None
                    if raw:
                        canonical = canonicalize(raw, session, ai_client)
                        proj.developer = raw
                        proj.developer_canonical = canonical
                        log.info("  Recovered PDF -> %-45s  =>  %s",
                                 raw[:45], canonical or "None")
                        session.commit()
                        time.sleep(0.5)
                        continue

                # Use HTML proponent if we have it
                if html_dev:
                    canonical = canonicalize(html_dev, session, ai_client)
                    proj.developer = html_dev
                    proj.developer_canonical = canonical
                    log.info("  HTML -> %-50s  =>  %s", html_dev[:50], canonical or "None")
                    session.commit()
                    found_html += 1
                    time.sleep(0.3)
                    continue

                # Exhausted all options
                log.info("  No developer found — logging failure")
                proj.developer_canonical = None
                session.commit()
                still_unknown += 1
                failures.append({
                    "id": proj.id, "name": proj.name,
                    "bpda_url": proj.bpda_url,
                    "reason": "No PDF and no proponent found in BPDA page HTML",
                    "developer_raw": proj.developer,
                })
                time.sleep(0.2)

        # ── Write failure log ─────────────────────────────────────────────────
        with open(FAIL_LOG, "w", encoding="utf-8") as f:
            for entry in failures:
                f.write(json.dumps(entry) + "\n")

        log.info(
            "\n=== Fill-unknown complete ===\n"
            "  Found via PDF extraction: %d\n"
            "  Found via BPDA HTML:      %d\n"
            "  Recovered new PDFs:       %d\n"
            "  Still unknown:            %d\n"
            "  Failure log:              %s\n",
            found_pdf_ai, found_html, recovered_pdf, still_unknown, FAIL_LOG,
        )

        # Show final state
        remaining = [
            p for p in session.query(Project).all()
            if not p.developer_canonical
            or p.developer_canonical.strip() in ("", "Unknown", "UNKNOWN",
                                                   "Unknown - review needed")
        ]
        log.info("Projects still without canonical developer: %d / %d",
                 len(remaining), session.query(Project).count())

    finally:
        session.close()


if __name__ == "__main__":
    run()
