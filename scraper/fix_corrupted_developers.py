"""
One-time fix: clear the 71 projects where developer was overwritten with
'Zoning Petitions for Text Amendments' and re-extract from their PDFs.
"""
import sys
import time
import logging
from pathlib import Path

import anthropic
from pypdf import PdfReader

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import init_db, get_session
from db.models import Project
from scraper.normalize_developer import normalize, is_real_company

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

PDF_DIR = Path(__file__).parent.parent / "data" / "pdfs"
MODEL   = "claude-haiku-4-5-20251001"

PROMPT = (
    "This is a Boston BPDA development filing.\n"
    "Find the applicant, proponent, developer, or project sponsor.\n"
    "Look for these labels: Applicant, Proponent, Developer, Owner, Project Sponsor, "
    "Submitted by, Prepared by, On behalf of, Petitioner.\n"
    "Return ONLY the company or person name. No explanations."
)

_BAD = (
    "i don", "i cannot", "i can't", "not found", "no applicant",
    "unable to", "without access", "cannot identify", "not specified",
    "not mentioned", "not provided", "no developer", "no proponent", "not identified",
)


def _extract_from_pdf(pdf_path: Path, project_name: str, client: anthropic.Anthropic) -> str | None:
    try:
        reader = PdfReader(str(pdf_path))
        parts, chars = [], 0
        for i, page in enumerate(reader.pages):
            if i >= 5:
                break
            t = page.extract_text() or ""
            parts.append(t)
            chars += len(t)
            if chars >= 12000:
                break
        text = "\n".join(parts)[:12000]
    except Exception:
        return None
    if not text.strip():
        return None

    content = f"[Filing first 5 pages]\n\n{text}\n\nProject: {project_name}\n\n{PROMPT}"
    for attempt in range(3):
        try:
            resp = client.messages.create(
                model=MODEL, max_tokens=128,
                messages=[{"role": "user", "content": content}],
            )
            r = resp.content[0].text.strip().strip('"').strip("'")
            if not r or len(r) < 2 or len(r) > 130:
                return None
            if any(p in r.lower() for p in _BAD):
                return None
            return r
        except anthropic.RateLimitError:
            time.sleep(30)
        except Exception:
            time.sleep(5)
    return None


def run():
    init_db()
    s = get_session()
    client = anthropic.Anthropic()

    try:
        corrupted = [
            p for p in s.query(Project).all()
            if p.developer and "Zoning Petitions" in p.developer
        ]
        log.info("Corrupted records to fix: %d", len(corrupted))

        fixed = cleared = 0
        for i, p in enumerate(corrupted, 1):
            pdf_path = PDF_DIR / f"{p.id}.pdf"
            log.info("[%d/%d] %s", i, len(corrupted), p.name[:60])

            p.developer = None
            p.developer_canonical = None

            if pdf_path.exists():
                raw = _extract_from_pdf(pdf_path, p.name, client)
                if raw:
                    canonical = normalize(raw, session=s, client=client)
                    p.developer = raw
                    p.developer_canonical = canonical if is_real_company(canonical) else None
                    log.info("  -> %-45s  =>  %s", raw[:45], p.developer_canonical or "None")
                    fixed += 1
                else:
                    log.info("  -> no developer found in PDF")
                    cleared += 1
            else:
                log.info("  -> no PDF on disk")
                cleared += 1

            s.commit()
            time.sleep(0.5)

        log.info(
            "\n=== Fix complete ===\n"
            "  Re-extracted successfully: %d\n"
            "  Cleared (no PDF or no match): %d\n",
            fixed, cleared,
        )

        # Final tally
        total = s.query(Project).count()
        null_can = sum(
            1 for p in s.query(Project).all()
            if not p.developer_canonical or p.developer_canonical.strip() in
            ("", "Unknown", "UNKNOWN", "Unknown - review needed")
        )
        has_raw = sum(
            1 for p in s.query(Project).all()
            if p.developer and p.developer.strip() and p.developer.strip() not in
            ("Unknown - review needed", "Unknown", "UNKNOWN", "Zoning Petitions for Text Amendments")
        )
        log.info("Projects with null canonical: %d / %d", null_can, total)
        log.info("Projects with any raw developer name: %d / %d", has_raw, total)

    finally:
        s.close()


if __name__ == "__main__":
    run()
