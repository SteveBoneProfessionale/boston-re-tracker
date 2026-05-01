"""
Download the best available filing PDF for each qualifying Article 80 project.

Priority: dpir > pnf > small_project  (ties broken by most recent date)
Max file size: 80 MB — if the chosen filing exceeds this, fall back to next priority.
Saves to: data/pdfs/{project_id}.pdf
Updates: project.processed_filing_url/name/type and ProjectFiling.is_processed
"""

import re
import sys
import time
import logging
from pathlib import Path
from datetime import datetime

import httpx
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import init_db, get_session
from db.models import Project, ProjectFiling

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

PDF_DIR = Path(__file__).parent.parent / "data" / "pdfs"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
PRIORITY = {"dpir": 3, "pnf": 2, "small_project": 1}
MAX_BYTES = 80 * 1024 * 1024   # 80 MB
CRAWL_DELAY = 1.5


# ---------------------------------------------------------------------------
# Box download helpers
# ---------------------------------------------------------------------------

class BoxSession:
    """Maintains a Box browser session for downloading shared files."""

    def __init__(self, client: httpx.Client):
        self.client = client
        self._request_token: str = ""
        self._base_url: str = ""
        self._initialized = False

    def _init_session(self, any_share_link: str):
        r = self.client.get(any_share_link, headers={"User-Agent": UA})
        m = re.search(r'"requestToken"\s*:\s*"([^"]+)"', r.text)
        self._request_token = m.group(1) if m else ""
        self._base_url = str(r.url).split("/s/")[0]
        self._initialized = True
        log.info("Box session established (base=%s)", self._base_url)

    def _headers(self, referer: str = "") -> dict:
        return {
            "User-Agent": UA,
            "Referer": referer or self._base_url,
            "X-Request-Token": self._request_token,
            "Accept": "application/json",
        }

    def get_download_url(self, share_link: str) -> str | None:
        """Return a signed public.boxcloud.com URL for the given Box share link."""
        # Resolve the canonical share URL (bpda.box.com → bpda.app.box.com)
        r0 = self.client.get(share_link, headers={"User-Agent": UA})
        if r0.status_code != 200:
            log.warning("Share link returned %s: %s", r0.status_code, share_link)
            return None

        # Refresh session token from this page
        m = re.search(r'"requestToken"\s*:\s*"([^"]+)"', r0.text)
        if m:
            self._request_token = m.group(1)
        self._base_url = str(r0.url).split("/s/")[0]
        share_hash = str(r0.url).split("/s/")[-1].split("?")[0]

        # Get item ID
        r1 = self.client.get(
            f"{self._base_url}/app-api/enduserapp/shared-item",
            params={"sharedName": share_hash},
            headers=self._headers(str(r0.url)),
        )
        if r1.status_code != 200:
            log.warning("shared-item API failed (%s) for %s", r1.status_code, share_link)
            return None
        item_id = r1.json().get("itemID")
        if not item_id:
            log.warning("No itemID in shared-item response for %s", share_link)
            return None

        # Get download redirect
        r2 = self.client.get(
            f"{self._base_url}/index.php",
            params={
                "rm": "box_download_shared_file",
                "shared_name": share_hash,
                "file_id": f"f_{item_id}",
            },
            headers={**self._headers(str(r0.url)), "Accept": "*/*"},
            follow_redirects=False,
        )
        location = r2.headers.get("location", "")
        if not location:
            log.warning("No redirect location for %s (status %s)", share_link, r2.status_code)
            return None
        return location


# ---------------------------------------------------------------------------
# Filing selection
# ---------------------------------------------------------------------------

def best_filing(project: Project) -> ProjectFiling | None:
    """Return the highest-priority, most-recent filing for a project."""
    candidates = [f for f in project.filings if f.filing_category in PRIORITY]
    if not candidates:
        return None
    return max(candidates, key=lambda f: (PRIORITY[f.filing_category], f.date or ""))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_downloader(limit: int | None = None, skip_existing: bool = True):
    init_db()
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    session = get_session()

    try:
        projects = session.query(Project).filter(Project.filings.any()).order_by(Project.name).all()
        log.info("Projects with filings: %d", len(projects))

        # Filter to those with downloadable (non-other) filings
        to_download = []
        for p in projects:
            filing = best_filing(p)
            if not filing:
                continue
            pdf_path = PDF_DIR / f"{p.id}.pdf"
            if skip_existing and pdf_path.exists():
                log.info("SKIP (exists): %s", p.name)
                continue
            to_download.append((p, filing))

        if limit:
            to_download = to_download[:limit]

        log.info("PDFs to download: %d", len(to_download))
        if not to_download:
            log.info("Nothing to do.")
            return

        downloaded = skipped = failed = 0

        with httpx.Client(follow_redirects=True, timeout=60) as client:
            box = BoxSession(client)

            for i, (proj, filing) in enumerate(to_download, 1):
                pdf_path = PDF_DIR / f"{proj.id}.pdf"
                log.info(
                    "[%d/%d] %s  |  %s (%s)",
                    i, len(to_download), proj.name, filing.name, filing.date or "?",
                )

                dl_url = box.get_download_url(filing.url)
                if not dl_url:
                    log.warning("  Could not get download URL — skipping")
                    failed += 1
                    continue

                # Stream download with size check
                try:
                    with client.stream("GET", dl_url, headers={"User-Agent": UA}) as resp:
                        if resp.status_code != 200:
                            log.warning("  HTTP %s from boxcloud", resp.status_code)
                            failed += 1
                            continue

                        chunks = []
                        total = 0
                        for chunk in resp.iter_bytes(65536):
                            chunks.append(chunk)
                            total += len(chunk)
                            if total > MAX_BYTES:
                                break

                        if total > MAX_BYTES:
                            log.warning(
                                "  File >80 MB (%s) — skipping (will try smaller filing next run)",
                                proj.name,
                            )
                            failed += 1
                            continue

                    content = b"".join(chunks)
                    if content[:4] != b"%PDF":
                        log.warning("  Not a PDF (got %s) — skipping", content[:8])
                        failed += 1
                        continue

                    pdf_path.write_bytes(content)
                    size_mb = len(content) / 1024 / 1024
                    log.info("  Saved %.1f MB → %s", size_mb, pdf_path.name)

                    # Update DB
                    proj.processed_filing_url = filing.url
                    proj.processed_filing_name = filing.name
                    proj.processed_filing_type = filing.filing_category
                    filing.is_processed = True
                    session.commit()
                    downloaded += 1

                except (httpx.RequestError, OSError) as exc:
                    log.warning("  Download error: %s", exc)
                    failed += 1
                    continue

                time.sleep(CRAWL_DELAY)

        log.info(
            "\n=== Download complete ===\n"
            "  Downloaded: %d\n"
            "  Skipped:    %d\n"
            "  Failed:     %d\n",
            downloaded, skipped, failed,
        )

        # Summary
        have_pdf = sum(1 for p in session.query(Project).all()
                       if (PDF_DIR / f"{p.id}.pdf").exists())
        log.info("PDFs on disk: %d", have_pdf)

    finally:
        session.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None,
                        help="Only download first N PDFs")
    parser.add_argument("--no-skip", action="store_true",
                        help="Re-download even if PDF already exists")
    args = parser.parse_args()
    run_downloader(limit=args.limit, skip_existing=not args.no_skip)
