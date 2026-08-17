r"""
Resolve a Rhode Island Open Meetings Portal meeting to its agenda/minutes PDFs.

The document path must never be constructed by hand. The folder number inside
FilePath is NOT the EntityID -- confirmed: Pawtucket Board of Appeals is
EntityID 2506 but files under folder 4009 -- and the relationship is unverified.

The path is not an href. It is the string argument of an onclick handler, which
is why an <a href> scan returns nothing on these pages:

    <a onclick="DownloadMeetingFiles('\\Notices\\4009\\2026\\562476.pdf')">
      Agenda filed on Jul 24 2026, 11:37AM by Carl Johnson</a>

Agendas live under \Notices\, minutes under \Minutes\. The FilePath query
parameter is built from that parsed argument verbatim.

    python scraper/ri_meeting_docs.py 1091758
    python scraper/ri_meeting_docs.py 1091758 --dump
"""

import re
import sys
import time
import logging
from pathlib import Path
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format="%(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

BASE = "https://opengov.sos.ri.gov"
MEETING = BASE + "/OpenMeetingsPublic/MeetingInformation?MeetingID={mid}"
DOWNLOAD = BASE + "/Common/DownloadMeetingFiles"

# The single-quoted argument of DownloadMeetingFiles('...'), e.g.
# \\Notices\\4009\\2026\\562476.pdf  (doubled backslashes are JS escaping)
ONCLICK_PATH = re.compile(r"DownloadMeetingFiles\('([^']+)'\)")

# "Public Body Name:" is a labelled field on this page; there is no <h1> here,
# unlike the dashboard and BoardMembers pages.
FIELD_LABELS = ("Public Body Name", "Date", "Time", "Address", "Filed on")
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    )
}
CACHE_DIR = Path(__file__).parent.parent / "data" / "ri_pdfs"


def polite_get(client: httpx.Client, url: str, timeout: int = 60,
               attempts: int = 5) -> httpx.Response:
    """GET with exponential backoff on 429 / 5xx.

    The portal rate-limits and returns 429 under sustained scraping. Backing
    off rather than failing keeps a long harvest from silently losing whole
    boards -- a 429 mid-run previously dropped four of six Tier 1 boards.
    """
    delay = 5.0
    last = None
    for i in range(attempts):
        r = client.get(url, timeout=timeout)
        if r.status_code not in (429, 500, 502, 503, 504):
            return r
        last = r
        log.warning("  HTTP %d — backing off %.0fs (attempt %d/%d)",
                    r.status_code, delay, i + 1, attempts)
        time.sleep(delay)
        delay *= 2
    return last


def meeting_documents(client: httpx.Client, meeting_id: int) -> dict:
    """Board name, meeting metadata and every document link for one meeting."""
    r = polite_get(client, MEETING.format(mid=meeting_id))
    r.raise_for_status()
    s = BeautifulSoup(r.text, "html.parser")

    docs = []
    for a in s.find_all("a", onclick=True):
        m = ONCLICK_PATH.search(a["onclick"])
        if not m:
            continue
        # Un-escape the doubled backslashes the JS literal carries, then pass
        # the path through verbatim as FilePath. Nothing here is synthesized.
        raw_path = m.group(1).replace("\\\\", "\\")
        docs.append({
            "label": a.get_text(" ", strip=True) or "(no label)",
            "path": raw_path,
            "kind": ("agenda" if raw_path.lstrip("\\").lower().startswith("notices")
                     else "minutes" if raw_path.lstrip("\\").lower().startswith("minutes")
                     else "other"),
            "url": f"{DOWNLOAD}?FilePath={raw_path}",
        })

    # Labelled fields rather than an <h1>; each label's value is the next cell.
    meta = {}
    for label in FIELD_LABELS:
        node = s.find(string=re.compile(rf"^\s*{re.escape(label)}\s*:?\s*$"))
        if node:
            holder = node.find_parent(["td", "th", "div", "label", "p"])
            nxt = holder.find_next(["td", "div", "span", "p"]) if holder else None
            if nxt:
                meta[label] = nxt.get_text(" ", strip=True)[:120]

    return {
        "meeting_id": meeting_id,
        "board": meta.get("Public Body Name", ""),
        "meta": meta,
        "documents": docs,
        "html_len": len(r.text),
    }


def fetch_pdf(client: httpx.Client, url: str, meeting_id: int, idx: int) -> Path | None:
    """Download a document, cached permanently by meeting id and position."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / f"{meeting_id}_{idx}.pdf"
    if path.exists():
        return path
    try:
        r = client.get(url, timeout=120)
        r.raise_for_status()
        path.write_bytes(r.content)
        return path
    except Exception as exc:
        log.warning("  download failed: %s", exc)
        return None


def pdf_text(path: Path, max_pages: int = 4) -> str:
    from pypdf import PdfReader
    reader = PdfReader(str(path))
    out = []
    for i, page in enumerate(reader.pages):
        if i >= max_pages:
            break
        out.append(page.extract_text() or "")
    return "\n".join(out)


def main():
    mid = int(sys.argv[1])
    dump = "--dump" in sys.argv
    with httpx.Client(headers=HEADERS, follow_redirects=True) as client:
        info = meeting_documents(client, mid)
        log.info("Board:    %s", info["board"])
        log.info("Meeting:  %s", mid)
        log.info("Documents: %d", len(info["documents"]))
        for i, d in enumerate(info["documents"]):
            log.info("  [%d] %s", i, d["label"])
            log.info("      %s", d["url"])

        if dump:
            for i, d in enumerate(info["documents"]):
                p = fetch_pdf(client, d["url"], mid, i)
                if not p:
                    continue
                log.info("\n===== DOC %d (%s) — %d bytes =====", i, d["label"], p.stat().st_size)
                log.info(pdf_text(p)[:4000])


if __name__ == "__main__":
    main()
