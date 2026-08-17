r"""
OCR scanned Rhode Island meeting minutes that carry no text layer.

Providence CPC files most of its minutes as scanned images: 26 of 37 minutes
documents in the harvested corpus extract fewer than 200 characters, averaging
4. Without OCR, Providence project status can only be "heard at stage X" from
the agenda -- never "approved", because the outcome lives only in those images.

Toolchain note: Tesseract, poppler and PyMuPDF-based OCR are all unavailable on
this machine, and PyMuPDF confirmed the documents are genuinely image-only (it
recovered 0 of 36 where pypdf found nothing). Rather than require a system
install, this sends the PDF to Claude as a native document block -- the same
mechanism scraper/extract_projects.py already uses. No rasterization step, no
image libraries, no system dependencies.

Model: claude-haiku-4-5, matching the existing bulk-extraction convention in
extract_projects.py. Transcription is a low-judgment task and Haiku is ~5x
cheaper than Opus on it; pass --model to override.

Every result is cached permanently to the same text file the harvester writes,
so a document is never OCR'd twice. Real token usage is accumulated and
reported so the cost is measured rather than estimated.

    python scraper/ri_ocr_minutes.py --limit 3      # validate + measure cost
    python scraper/ri_ocr_minutes.py                # full depth
"""

import re
import sys
import json
import time
import base64
import logging
from pathlib import Path

import anthropic
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

CORPUS = Path(__file__).parent.parent / "data" / "ri_agenda_corpus.json"
PDF_DIR = Path(__file__).parent.parent / "data" / "ri_pdfs"
TEXT_DIR = PDF_DIR / "text"
OCR_LOG = Path(__file__).parent.parent / "data" / "ri_ocr_log.json"

MODEL = "claude-haiku-4-5"
TEXT_THRESHOLD = 200          # chars below which a document counts as image-only
PDF_SIZE_LIMIT = 30 * 1024 * 1024

# Haiku 4.5 list pricing, $/MTok — used only to report measured spend.
PRICE_IN, PRICE_OUT = 1.00, 5.00

SYSTEM_PROMPT = """\
You are transcribing a scanned municipal meeting-minutes document from a Rhode \
Island planning or zoning board.

Transcribe the text verbatim, preserving the reading order, agenda item \
numbering, and any recorded votes exactly as they appear. Vote outcomes are the \
most important content in these documents: preserve wording such as APPROVED, \
DENIED, CONTINUED, WITHDRAWN, TABLED, "motion carried", "unanimous", and any \
recorded vote tallies, exactly as written.

Transcribe only what is legibly on the page. If a passage is illegible, write \
[illegible] rather than guessing at it. Do not summarize, correct, reorder, or \
add commentary. Output the transcription only."""


def load_targets(limit: int | None) -> list[dict]:
    """Corpus documents whose cached text is empty -- i.e. image-only scans."""
    corpus = json.loads(CORPUS.read_text(encoding="utf-8"))
    out = []
    for key, meeting in corpus.items():
        for doc in meeting["documents"]:
            tp = TEXT_DIR / doc["text_file"]
            if not tp.exists():
                continue
            if len(tp.read_text(encoding="utf-8", errors="replace")) >= TEXT_THRESHOLD:
                continue
            pdf = PDF_DIR / doc["text_file"].replace(".txt", ".pdf")
            if not pdf.exists():
                continue
            out.append({
                "key": key, "municipality": meeting["municipality"],
                "board": meeting["board"], "date": meeting["date"],
                "kind": doc["kind"], "pdf": pdf, "text_file": doc["text_file"],
            })
    out.sort(key=lambda d: d["date"], reverse=True)
    return out[:limit] if limit else out


def ocr(client: anthropic.Anthropic, pdf: Path, model: str) -> tuple[str, dict] | None:
    size = pdf.stat().st_size
    if size > PDF_SIZE_LIMIT:
        log.warning("  %s is %.1f MB — over the request limit, skipping", pdf.name, size / 1e6)
        return None
    data = base64.standard_b64encode(pdf.read_bytes()).decode("utf-8")

    for attempt in range(4):
        try:
            resp = client.messages.create(
                model=model,
                max_tokens=16000,
                system=[{"type": "text", "text": SYSTEM_PROMPT,
                         "cache_control": {"type": "ephemeral"}}],
                messages=[{"role": "user", "content": [
                    {"type": "document", "source": {
                        "type": "base64", "media_type": "application/pdf", "data": data}},
                    {"type": "text", "text": "Transcribe this document."},
                ]}],
            )
            if resp.stop_reason == "refusal":
                log.warning("  refused — skipping")
                return None
            text = "".join(b.text for b in resp.content if b.type == "text")
            usage = {
                "in": resp.usage.input_tokens,
                "out": resp.usage.output_tokens,
                "cache_read": getattr(resp.usage, "cache_read_input_tokens", 0) or 0,
                "truncated": resp.stop_reason == "max_tokens",
            }
            return text, usage
        except anthropic.RateLimitError:
            wait = 30 * (attempt + 1)
            log.warning("  rate limited — sleeping %ds", wait)
            time.sleep(wait)
        except anthropic.APIError as exc:
            log.warning("  API error (attempt %d): %s", attempt + 1, exc)
            time.sleep(10)
    return None


def main(limit: int | None, model: str):
    targets = load_targets(limit)
    if not targets:
        log.info("Nothing to OCR — every harvested document already has text.")
        return

    log.info("Documents to OCR: %d", len(targets))

    client = anthropic.Anthropic()
    ocr_log = json.loads(OCR_LOG.read_text(encoding="utf-8")) if OCR_LOG.exists() else {}

    tin = tout = tcache = 0
    done = failed = 0

    for i, t in enumerate(targets, 1):
        if t["text_file"] in ocr_log:
            continue
        log.info("[%d/%d] %s %s %s (%s)", i, len(targets), t["municipality"],
                 t["date"], t["kind"], t["pdf"].name)

        result = ocr(client, t["pdf"], model)
        if result is None:
            failed += 1
            continue
        text, usage = result

        # Overwrite the empty cached text so every downstream consumer -- the
        # vocabulary analysis, the eventual ingestion -- picks it up with no
        # special-casing for OCR'd documents.
        (TEXT_DIR / t["text_file"]).write_text(text, encoding="utf-8", errors="replace")
        ocr_log[t["text_file"]] = {
            "model": model, "chars": len(text), "usage": usage,
            "municipality": t["municipality"], "board": t["board"],
            "date": t["date"], "kind": t["kind"],
        }
        OCR_LOG.write_text(json.dumps(ocr_log, indent=1), encoding="utf-8")

        tin += usage["in"]; tout += usage["out"]; tcache += usage["cache_read"]
        done += 1
        if usage["truncated"]:
            log.warning("  hit max_tokens — transcription may be incomplete")
        log.info("  %d chars   in=%d out=%d", len(text), usage["in"], usage["out"])
        time.sleep(1.0)

    cost = (tin / 1e6) * PRICE_IN + (tout / 1e6) * PRICE_OUT
    log.info("")
    log.info("=== OCR complete ===")
    log.info("  transcribed : %d", done)
    log.info("  failed      : %d", failed)
    log.info("  input tokens : %d", tin)
    log.info("  output tokens: %d", tout)
    log.info("  cache reads  : %d", tcache)
    log.info("  MEASURED COST: $%.2f  (%s at $%.2f/$%.2f per MTok)",
             cost, model, PRICE_IN, PRICE_OUT)
    if done:
        log.info("  per document : $%.4f", cost / done)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--model", default=MODEL)
    a = ap.parse_args()
    main(a.limit, a.model)
