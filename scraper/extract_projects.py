"""
AI extraction: send each project's PDF to Claude and populate the extracted fields.

- PDFs < 32 MB: sent as native PDF (base64) — Claude reads the actual document
- PDFs >= 32 MB: pypdf text extraction, sent as text
- Model: claude-haiku-4-5-20251001 (fast, cheap for bulk extraction)
- Writes: developer, asset_class, total_gsf, residential_units, commercial_gsf,
          building_height_ft, num_stories, parking_spaces, architect,
          civil_engineer, expected_delivery, description
"""

import sys
import json
import time
import base64
import logging
import re
from pathlib import Path
from datetime import datetime, timezone

import anthropic
from pypdf import PdfReader
from dotenv import load_dotenv
load_dotenv()

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import init_db, get_session
from db.models import Project
from scraper.normalize_developer import normalize as normalize_developer, is_real_company

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

PDF_DIR = Path(__file__).parent.parent / "data" / "pdfs"
PDF_SIZE_LIMIT = 32 * 1024 * 1024   # 32 MB — Anthropic PDF limit
PDF_MAX_PAGES = 25                   # cap pages sent to API to control token usage
TEXT_MAX_CHARS = 60_000              # ~15K tokens for text fallback
MODEL = "claude-haiku-4-5-20251001"

SYSTEM_PROMPT = """\
You are a real estate analyst extracting structured data from Boston BPDA project filings \
(Project Notification Forms, Draft Project Impact Reports, Small Project Review Applications).

Return a single valid JSON object with exactly these keys. Use null for any field not found.

{
  "developer": "applicant / developer company name",
  "asset_class": "one of: Residential, Office, Mixed-Use, Hotel, Lab/Research, Institutional, Industrial, Retail, Parking, Other",
  "total_gsf": integer gross square feet of entire project,
  "residential_units": integer number of residential units,
  "commercial_gsf": integer square feet of commercial/office/retail space,
  "building_height_ft": numeric height in feet,
  "num_stories": integer number of floors/stories,
  "parking_spaces": integer number of parking spaces,
  "architect": "architecture firm name",
  "civil_engineer": "civil engineering firm name",
  "expected_delivery": "anticipated completion/delivery year or quarter, e.g. '2027' or '2026 Q3'",
  "description": "2-3 sentence factual summary: what is being built, where, and key program elements"
}

Return only the JSON object — no prose, no markdown fences."""


def pdf_to_content(pdf_path: Path) -> list[dict]:
    """Return Anthropic message content blocks for the given PDF.
    Always caps at PDF_MAX_PAGES to keep token usage manageable."""
    size = pdf_path.stat().st_size

    # Always extract text via pypdf, capped to first PDF_MAX_PAGES pages.
    # For small PDFs we could send the binary, but text is far cheaper and
    # works well for structured PNF/DPIR forms which are text-based.
    try:
        reader = PdfReader(str(pdf_path))
        pages = []
        chars = 0
        for i, page in enumerate(reader.pages):
            if i >= PDF_MAX_PAGES:
                break
            text = page.extract_text() or ""
            pages.append(text)
            chars += len(text)
            if chars >= TEXT_MAX_CHARS:
                break
        full_text = "\n".join(pages)[:TEXT_MAX_CHARS]
        if full_text.strip():
            page_note = f"[First {min(len(reader.pages), PDF_MAX_PAGES)} of {len(reader.pages)} pages]"
            return [{"type": "text", "text": f"[BPDA filing PDF — {page_note}]\n\n{full_text}"}]
    except Exception as exc:
        log.warning("  pypdf text extraction failed (%s), falling back to binary", exc)

    # Fallback: send binary. For oversized PDFs (scanned/image), extract a page subset
    # so the binary stays well under the 32 MB API limit regardless of original size.
    cap = min(100, PDF_MAX_PAGES) if size >= PDF_SIZE_LIMIT else 100
    try:
        import io
        from pypdf import PdfWriter
        reader = PdfReader(str(pdf_path))
        total_pages = len(reader.pages)
        if total_pages > cap:
            writer = PdfWriter()
            for page in reader.pages[:cap]:
                writer.add_page(page)
            buf = io.BytesIO()
            writer.write(buf)
            data = base64.standard_b64encode(buf.getvalue()).decode("utf-8")
            log.info("  Truncated %d-page PDF to %d pages for binary send", total_pages, cap)
        else:
            data = base64.standard_b64encode(pdf_path.read_bytes()).decode("utf-8")
        return [{"type": "document", "source": {"type": "base64", "media_type": "application/pdf", "data": data}}]
    except Exception as exc:
        log.warning("  Binary PDF prep failed: %s", exc)

    log.warning("  PDF could not be prepared for extraction — skipping")
    return []


def extract_json(text: str) -> dict | None:
    """Parse JSON from Claude's response, stripping any accidental markdown."""
    text = text.strip()
    # Strip ```json ... ``` fences if present
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Try to find a {...} block
        m = re.search(r"\{.*\}", text, re.DOTALL)
        if m:
            try:
                return json.loads(m.group())
            except json.JSONDecodeError:
                pass
    return None


def _call_with_retry(client: anthropic.Anthropic, content: list[dict]) -> dict | None:
    """Call the API with exponential backoff on rate limits. Returns parsed dict, {} on bad JSON, None on total failure."""
    for attempt in range(4):
        try:
            resp = client.messages.create(
                model=MODEL,
                max_tokens=1024,
                system=[{"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}],
                messages=[{"role": "user", "content": content}],
            )
            raw = resp.content[0].text
            parsed = extract_json(raw)
            return parsed if parsed is not None else {}
        except anthropic.RateLimitError:
            wait = 30 * (attempt + 1)
            log.warning("  Rate limit (attempt %d) — sleeping %ds", attempt + 1, wait)
            time.sleep(wait)
        except anthropic.APIError as exc:
            log.warning("  API error (attempt %d): %s", attempt + 1, exc)
            time.sleep(10)
    return None


def run_extraction(limit: int | None = None, reprocess: bool = False):
    init_db()
    session = get_session()
    client = anthropic.Anthropic()

    try:
        projects = (
            session.query(Project)
            .filter(Project.processed_filing_url.isnot(None))
            .order_by(Project.name)
            .all()
        )

        if not reprocess:
            projects = [p for p in projects if p.extraction_timestamp is None]

        log.info("Projects to extract: %d", len(projects))

        if limit:
            projects = projects[:limit]
            log.info("Limited to %d", limit)

        success = skipped = failed = 0

        for i, proj in enumerate(projects, 1):
            pdf_path = PDF_DIR / f"{proj.id}.pdf"
            if not pdf_path.exists():
                log.warning("[%d/%d] No PDF on disk: %s", i, len(projects), proj.name)
                skipped += 1
                continue

            log.info("[%d/%d] %s  (%s)", i, len(projects), proj.name,
                     proj.processed_filing_type)

            content = pdf_to_content(pdf_path)
            if not content:
                log.warning("  Could not build content — skipping")
                skipped += 1
                continue

            content.append({
                "type": "text",
                "text": f"Project: {proj.name}\nAddress: {proj.address or 'unknown'}\nNeighborhood: {proj.neighborhood or 'unknown'}\nFiling type: {proj.processed_filing_type}\n\nExtract the structured data from this filing."
            })

            data = _call_with_retry(client, content)

            if data is None:
                log.warning("  Extraction failed after retries — skipping")
                failed += 1
                continue

            if data == {}:
                log.warning("  Could not parse JSON — skipping")
                failed += 1
                continue

            try:
                proj.developer = data.get("developer")
                if proj.developer:
                    canonical = normalize_developer(
                        proj.developer, session=session, client=client
                    )
                    proj.developer_canonical = canonical if is_real_company(canonical) else None
                proj.asset_class = data.get("asset_class")
                proj.total_gsf = _to_int(data.get("total_gsf"))
                proj.residential_units = _to_int(data.get("residential_units"))
                proj.commercial_gsf = _to_int(data.get("commercial_gsf"))
                proj.building_height_ft = _to_float(data.get("building_height_ft"))
                proj.num_stories = _to_int(data.get("num_stories"))
                proj.parking_spaces = _to_int(data.get("parking_spaces"))
                proj.architect = data.get("architect")
                proj.civil_engineer = data.get("civil_engineer")
                proj.expected_delivery = data.get("expected_delivery")
                if data.get("description"):
                    proj.description = data["description"]
                proj.extraction_model = MODEL
                proj.extraction_timestamp = datetime.now(timezone.utc)
                session.commit()

                log.info(
                    "  dev=%-30s  class=%-15s  units=%s  gsf=%s",
                    (proj.developer or "?")[:30],
                    (proj.asset_class or "?")[:15],
                    proj.residential_units or "?",
                    f"{proj.total_gsf:,}" if proj.total_gsf else "?",
                )
                success += 1
            except Exception as exc:
                log.warning("  DB write error: %s", exc)
                failed += 1
                session.rollback()

            time.sleep(12)  # base delay — keeps token/min under rate limit

        log.info(
            "\n=== Extraction complete ===\n"
            "  Success: %d\n  Skipped: %d\n  Failed:  %d\n",
            success, skipped, failed,
        )

        # Quick summary
        extracted = session.query(Project).filter(
            Project.extraction_timestamp.isnot(None)
        ).count()
        log.info("Total projects with extracted data: %d", extracted)

    finally:
        session.close()


def _to_int(v) -> int | None:
    if v is None:
        return None
    try:
        return int(str(v).replace(",", "").split(".")[0])
    except (ValueError, TypeError):
        return None


def _to_float(v) -> float | None:
    if v is None:
        return None
    try:
        return float(str(v).replace(",", ""))
    except (ValueError, TypeError):
        return None


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--reprocess", action="store_true",
                        help="Re-extract even if already done")
    args = parser.parse_args()
    run_extraction(limit=args.limit, reprocess=args.reprocess)
