"""
Targeted developer-name extraction for projects with blank developer fields.

- Uses only the first 3 pages of each PDF (fast, focused)
- Prompt looks specifically for Applicant / Proponent / Developer labels
- Runs result through normalize_developer for canonical name
- Sets developer to "Unknown - review needed" if nothing found after all attempts
- Does NOT touch extraction_timestamp, so full extraction can still run normally
"""

import sys
import re
import time
import logging
from pathlib import Path

import anthropic
from pypdf import PdfReader

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import init_db, get_session
from db.models import Project
from scraper.normalize_developer import normalize

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

PDF_DIR = Path(__file__).parent.parent / "data" / "pdfs"
MODEL = "claude-haiku-4-5-20251001"
PAGES = 3          # only first 3 pages needed
CHARS = 8_000      # ~2K tokens — enough for a cover page

PROMPT = """\
This is a Boston BPDA development filing (Project Notification Form, Draft Project Impact \
Report, or Small Project Review Application).

Who is the applicant, proponent, or developer submitting this project?

Look for labels such as: Applicant, Proponent, Developer, Owner, Petitioner, \
"Submitted by", "Project Sponsor", or similar.

Return only the company or person name — nothing else. No explanations, no punctuation \
other than what is part of the name. If you find multiple names (e.g. owner and developer \
are different entities), return the primary applicant or project sponsor."""

UNKNOWN = "Unknown - review needed"


def _extract_text(pdf_path: Path) -> str:
    try:
        reader = PdfReader(str(pdf_path))
        parts = []
        chars = 0
        for i, page in enumerate(reader.pages):
            if i >= PAGES:
                break
            text = page.extract_text() or ""
            parts.append(text)
            chars += len(text)
            if chars >= CHARS:
                break
        return "\n".join(parts)[:CHARS]
    except Exception as exc:
        log.warning("  pypdf failed for %s: %s", pdf_path.name, exc)
        return ""


def _call_haiku(client: anthropic.Anthropic, text: str, project_name: str) -> str | None:
    if not text.strip():
        return None
    content = [
        {"type": "text", "text": f"[Filing text — first {PAGES} pages]\n\n{text}"},
        {"type": "text", "text": f"Project name: {project_name}\n\n{PROMPT}"},
    ]
    for attempt in range(3):
        try:
            resp = client.messages.create(
                model=MODEL,
                max_tokens=128,
                messages=[{"role": "user", "content": content}],
            )
            result = resp.content[0].text.strip().strip('"').strip("'")
            # Reject non-answers
            if not result or len(result) < 2:
                return None
            bad_phrases = (
                "i don't", "i cannot", "i can't", "not found", "no applicant",
                "unable to", "without access", "cannot identify", "not specified",
                "not mentioned", "not provided", "no developer", "no proponent",
            )
            if any(p in result.lower() for p in bad_phrases):
                return None
            if len(result) > 120:
                return None
            return result
        except anthropic.RateLimitError:
            wait = 20 * (attempt + 1)
            log.warning("  Rate limit — sleeping %ds", wait)
            time.sleep(wait)
        except Exception as exc:
            log.warning("  API error (attempt %d): %s", attempt + 1, exc)
            time.sleep(5)
    return None


def run():
    init_db()
    session = get_session()
    client = anthropic.Anthropic()

    try:
        projects = (
            session.query(Project)
            .filter(
                (Project.developer == None) | (Project.developer == "")
            )
            .order_by(Project.name)
            .all()
        )

        candidates = [p for p in projects if (PDF_DIR / f"{p.id}.pdf").exists()]
        no_pdf = [p for p in projects if not (PDF_DIR / f"{p.id}.pdf").exists()]

        log.info("Projects missing developer: %d total", len(projects))
        log.info("  With PDF: %d  |  No PDF: %d", len(candidates), len(no_pdf))

        found = 0
        flagged = 0

        for i, proj in enumerate(candidates, 1):
            pdf_path = PDF_DIR / f"{proj.id}.pdf"
            log.info("[%d/%d] %s", i, len(candidates), proj.name)

            text = _extract_text(pdf_path)
            if not text.strip():
                log.warning("  No text extracted from PDF")
                proj.developer = UNKNOWN
                proj.developer_canonical = UNKNOWN
                session.commit()
                flagged += 1
                continue

            raw = _call_haiku(client, text, proj.name)

            if raw:
                canonical = normalize(raw, session=session, client=client)
                proj.developer = raw
                proj.developer_canonical = canonical
                log.info("  Found: %-50s  =>  %s", raw[:50], canonical)
                found += 1
            else:
                proj.developer = UNKNOWN
                proj.developer_canonical = UNKNOWN
                log.info("  Not found in document — flagged")
                flagged += 1

            session.commit()
            time.sleep(1)

        log.info(
            "\n=== Developer extraction complete ===\n"
            "  Found:    %d\n"
            "  Flagged:  %d (PDF exists, developer not found)\n"
            "  Skipped:  %d (no PDF — will be handled when PDF is available)\n",
            found, flagged, len(no_pdf),
        )

    finally:
        session.close()


if __name__ == "__main__":
    run()
