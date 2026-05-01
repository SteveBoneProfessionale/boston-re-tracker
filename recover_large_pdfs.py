"""
Download and extract the two projects whose PDFs exceed the 80MB limit.
Streams the file, writes it, then truncates to first 25 pages before sending
to Claude — same approach as the scanned-PDF fix in extract_projects.py.
"""
import sys, io, base64, time, json, re, logging
from pathlib import Path
from datetime import datetime, timezone

import httpx
import anthropic
from pypdf import PdfReader, PdfWriter
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, '.')
from db.database import init_db, get_session
from db.models import Project, ProjectFiling
from scraper.normalize_developer import normalize as normalize_developer, is_real_company

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

PDF_DIR = Path("data/pdfs")
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
MODEL = "claude-haiku-4-5-20251001"
PAGE_CAP = 25

SYSTEM_PROMPT = """\
You are a real estate analyst extracting structured data from Boston BPDA project filings.
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
  "description": "2-3 sentence factual summary"
}
Return only the JSON object — no prose, no markdown fences."""


def get_box_download_url(client, share_link):
    r0 = client.get(share_link, headers={"User-Agent": UA})
    if r0.status_code != 200:
        return None
    m = re.search(r'"requestToken"\s*:\s*"([^"]+)"', r0.text)
    request_token = m.group(1) if m else ""
    base_url = str(r0.url).split("/s/")[0]
    share_hash = str(r0.url).split("/s/")[-1].split("?")[0]

    r1 = client.get(
        f"{base_url}/app-api/enduserapp/shared-item",
        params={"sharedName": share_hash},
        headers={"User-Agent": UA, "X-Request-Token": request_token, "Accept": "application/json"},
    )
    if r1.status_code != 200:
        return None
    item_id = r1.json().get("itemID")
    if not item_id:
        return None

    r2 = client.get(
        f"{base_url}/index.php",
        params={"rm": "box_download_shared_file", "shared_name": share_hash, "file_id": f"f_{item_id}"},
        headers={"User-Agent": UA, "X-Request-Token": request_token, "Accept": "*/*"},
        follow_redirects=False,
    )
    return r2.headers.get("location")


def pdf_to_content(pdf_path):
    reader = PdfReader(str(pdf_path))
    total = len(reader.pages)
    cap = min(total, PAGE_CAP)
    writer = PdfWriter()
    for page in reader.pages[:cap]:
        writer.add_page(page)
    buf = io.BytesIO()
    writer.write(buf)
    data = base64.standard_b64encode(buf.getvalue()).decode("utf-8")
    log.info("  Sending first %d of %d pages as binary", cap, total)
    return [{"type": "document", "source": {"type": "base64", "media_type": "application/pdf", "data": data}}]


def extract_json(text):
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


def run():
    init_db()
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    session = get_session()
    client_ai = anthropic.Anthropic()

    # Target: projects with qualifying filings but no PDF downloaded and no extraction
    targets = []
    for proj in session.query(Project).filter(Project.extraction_timestamp.is_(None)).all():
        best = None
        priority = {"dpir": 3, "pnf": 2, "small_project": 1}
        candidates = [f for f in proj.filings if f.filing_category in priority]
        if candidates:
            best = max(candidates, key=lambda f: (priority[f.filing_category], f.date or ""))
        if best:
            targets.append((proj, best))

    log.info("Projects with qualifying filings to process: %d", len(targets))

    with httpx.Client(follow_redirects=True, timeout=120) as http:
        for proj, filing in targets:
            pdf_path = PDF_DIR / f"{proj.id}.pdf"
            log.info("Processing: %s", proj.name)

            # Download (no size limit — we'll truncate before sending to API)
            if not pdf_path.exists():
                log.info("  Downloading from Box...")
                dl_url = get_box_download_url(http, filing.url)
                if not dl_url:
                    log.warning("  Could not get download URL")
                    continue

                try:
                    with http.stream("GET", dl_url, headers={"User-Agent": UA}) as resp:
                        if resp.status_code != 200:
                            log.warning("  HTTP %s", resp.status_code)
                            continue
                        content = resp.read()

                    if content[:4] != b"%PDF":
                        log.warning("  Not a PDF")
                        continue

                    pdf_path.write_bytes(content)
                    size_mb = len(content) / 1024 / 1024
                    log.info("  Downloaded %.1f MB", size_mb)

                    proj.processed_filing_url = filing.url
                    proj.processed_filing_name = filing.name
                    proj.processed_filing_type = filing.filing_category
                    filing.is_processed = True
                    session.commit()

                except Exception as exc:
                    log.warning("  Download error: %s", exc)
                    continue

            # Extract
            log.info("  Extracting with Claude...")
            content_blocks = pdf_to_content(pdf_path)
            content_blocks.append({
                "type": "text",
                "text": f"Project: {proj.name}\nAddress: {proj.address or 'unknown'}\nNeighborhood: {proj.neighborhood or 'unknown'}\nFiling type: {proj.processed_filing_type}\n\nExtract the structured data from this filing."
            })

            try:
                resp = client_ai.messages.create(
                    model=MODEL,
                    max_tokens=1024,
                    system=[{"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}],
                    messages=[{"role": "user", "content": content_blocks}],
                )
                data = extract_json(resp.content[0].text)
            except Exception as exc:
                log.warning("  API error: %s", exc)
                continue

            if not data:
                log.warning("  Could not parse JSON response")
                continue

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
            session.commit()

            log.info("  Done: dev=%s  class=%s  units=%s  gsf=%s",
                     (proj.developer or "?")[:30],
                     (proj.asset_class or "?"),
                     proj.residential_units or "?",
                     f"{proj.total_gsf:,}" if proj.total_gsf else "?")
            time.sleep(12)

    session.close()


def _int(v):
    if v is None: return None
    try: return int(str(v).replace(",", "").split(".")[0])
    except: return None

def _float(v):
    if v is None: return None
    try: return float(str(v).replace(",", ""))
    except: return None


if __name__ == "__main__":
    run()
