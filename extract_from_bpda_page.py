"""
Extract structured data from BPDA project web pages for projects that have
no qualifying PDF filings (or whose PDFs could not be processed).

Fetches the full BPDA detail page, strips to key text, sends to Claude.
Sets extraction_timestamp + processed_filing_type = 'bpda_page' so the
main extract_projects.py pipeline knows these records are done.
"""
import sys, re, json, time, logging
from pathlib import Path
from datetime import datetime, timezone

import httpx
from bs4 import BeautifulSoup
import anthropic
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, '.')
from db.database import init_db, get_session
from db.models import Project
from scraper.normalize_developer import normalize as normalize_developer, is_real_company

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

MODEL = "claude-haiku-4-5-20251001"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"

SYSTEM_PROMPT = """\
You are a real estate analyst extracting structured data from a Boston BPDA (Boston Planning and
Development Agency) project webpage. The text below is from the project's official BPDA page.

Return a single valid JSON object with exactly these keys. Use null for any field not found.
{
  "developer": "applicant / developer company name",
  "asset_class": "one of: Residential, Office, Mixed-Use, Hotel, Lab/Research, Institutional, Industrial, Retail, Parking, Other",
  "total_gsf": integer gross square feet,
  "residential_units": integer number of residential units,
  "commercial_gsf": integer square feet of commercial space,
  "building_height_ft": numeric height in feet,
  "num_stories": integer number of stories,
  "parking_spaces": integer parking spaces,
  "architect": "architecture firm name",
  "civil_engineer": "civil engineering firm name",
  "expected_delivery": "anticipated completion year or quarter",
  "description": "2-3 sentence factual summary of what is being built"
}
Return only the JSON object — no prose, no markdown fences."""


def scrape_bpda_page(url: str, client: httpx.Client) -> str:
    """Fetch BPDA project page and return cleaned text content."""
    try:
        resp = client.get(url, headers={"User-Agent": UA}, timeout=20, follow_redirects=True)
        if resp.status_code != 200:
            log.warning("  HTTP %s fetching %s", resp.status_code, url)
            return ""
    except Exception as exc:
        log.warning("  Fetch error: %s", exc)
        return ""

    soup = BeautifulSoup(resp.text, "html.parser")

    # Remove nav, footer, scripts, styles
    for tag in soup(["script", "style", "nav", "footer", "header"]):
        tag.decompose()

    # Pull the main content area
    main = soup.find("main") or soup.find("div", id="content") or soup.find("div", class_="page-content") or soup
    text = main.get_text(separator="\n", strip=True)

    # Collapse blank lines
    lines = [l for l in text.splitlines() if l.strip()]
    return "\n".join(lines)[:8000]  # cap at ~2K tokens


def extract_json(text: str) -> dict | None:
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    try:
        return json.loads(text)
    except Exception:
        m = re.search(r"\{.*\}", text, re.DOTALL)
        if m:
            try:
                return json.loads(m.group())
            except Exception:
                pass
    return None


def _int(v):
    if v is None: return None
    try: return int(str(v).replace(",", "").split(".")[0])
    except: return None

def _float(v):
    if v is None: return None
    try: return float(str(v).replace(",", ""))
    except: return None


def run():
    init_db()
    session = get_session()
    client_ai = anthropic.Anthropic()

    # IDs 142 and 276 have qualifying filings being processed by recover_large_pdfs.py
    SKIP_IDS = {142, 276}

    targets = [
        p for p in session.query(Project).filter(
            Project.extraction_timestamp.is_(None)
        ).order_by(Project.id).all()
        if p.id not in SKIP_IDS
    ]

    log.info("Unextracted projects to attempt via BPDA page: %d", len(targets))

    with httpx.Client(follow_redirects=True, timeout=20) as http:
        for proj in targets:
            log.info("Processing [%d] %s", proj.id, proj.name)

            page_text = scrape_bpda_page(proj.bpda_url, http)
            if not page_text:
                log.warning("  Could not fetch BPDA page — skipping")
                continue

            # Build prompt with existing known fields as context
            context = (
                f"Project name: {proj.name}\n"
                f"Address: {proj.address or 'unknown'}\n"
                f"Neighborhood: {proj.neighborhood or 'unknown'}\n"
                f"BPDA reported GSF: {proj.bpda_gsf or 'unknown'}\n"
                f"Status: {proj.status or 'unknown'}\n\n"
                f"BPDA page content:\n{page_text}"
            )

            try:
                resp = client_ai.messages.create(
                    model=MODEL,
                    max_tokens=1024,
                    system=[{"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}],
                    messages=[{"role": "user", "content": context}],
                )
                data = extract_json(resp.content[0].text)
            except Exception as exc:
                log.warning("  API error: %s", exc)
                time.sleep(5)
                continue

            if not data:
                log.warning("  Could not parse JSON response")
                continue

            try:
                proj.developer = data.get("developer")
                if proj.developer:
                    canonical = normalize_developer(proj.developer, session=session, client=client_ai)
                    proj.developer_canonical = canonical if is_real_company(canonical) else None
                proj.asset_class = data.get("asset_class")
                proj.total_gsf = _int(data.get("total_gsf")) or proj.bpda_gsf
                proj.residential_units = _int(data.get("residential_units"))
                proj.commercial_gsf = _int(data.get("commercial_gsf"))
                proj.building_height_ft = _float(data.get("building_height_ft"))
                proj.num_stories = _int(data.get("num_stories"))
                proj.parking_spaces = _int(data.get("parking_spaces"))
                proj.architect = data.get("architect")
                proj.civil_engineer = data.get("civil_engineer")
                proj.expected_delivery = data.get("expected_delivery")
                if data.get("description"):
                    proj.description = data["description"]
                proj.extraction_model = MODEL
                proj.extraction_timestamp = datetime.now(timezone.utc)
                proj.processed_filing_type = "bpda_page"
                session.commit()

                log.info("  OK: dev=%s  class=%s  units=%s  gsf=%s",
                         (proj.developer or "null")[:30],
                         (proj.asset_class or "null"),
                         proj.residential_units or "null",
                         f"{proj.total_gsf:,}" if proj.total_gsf else "null")
            except Exception as exc:
                log.warning("  DB write error: %s", exc)
                session.rollback()

            time.sleep(8)

    session.close()

    # Final count
    session2 = get_session()
    total = session2.query(Project).count()
    done = session2.query(Project).filter(Project.extraction_timestamp.isnot(None)).count()
    session2.close()
    log.info("\n=== Done ===  %d / %d projects extracted", done, total)


if __name__ == "__main__":
    run()
