"""
Backfill script: populate sire_id on existing projects and fill project_filings
using the correct ft_next_marker cursor pagination.

Run once after the initial scrape to recover the filings that were missed
due to the wrong pagination parameter (marker vs ft_next_marker).
"""

import sys
import time
import logging
from pathlib import Path

import httpx
from sqlalchemy.exc import IntegrityError

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import init_db, get_session, engine
from db.models import Base, Project, ProjectFiling
from scraper.bpda_scraper import (
    SIRE_PROJECTS_URL, SIRE_DOCS_URL, SIRE_HEADERS, FILING_TYPE_MAP,
    safe_get, make_client,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def normalize_url(u: str) -> str:
    return u.rstrip("/").lower().replace("https://", "http://")


def backfill():
    # Ensure sire_id column exists (idempotent via SQLAlchemy)
    init_db()
    session = get_session()

    try:
        with make_client() as client:
            # --- Step 1: Load SIRE project registry ---
            log.info("Fetching SIRE project list...")
            resp = safe_get(client, SIRE_PROJECTS_URL, extra_headers=SIRE_HEADERS)
            if resp is None:
                log.error("Cannot reach SIRE")
                return

            sire_projects = resp.json()
            log.info("  %d SIRE projects", len(sire_projects))

            sire_by_url: dict[str, str] = {}
            for p in sire_projects:
                raw = p.get("Website_URL__c") or ""
                url = normalize_url(raw)
                if url:
                    sire_by_url[url] = p["Id"]

            # --- Step 2: Match DB projects to SIRE IDs ---
            projects = session.query(Project).all()
            matched = 0
            for proj in projects:
                norm = normalize_url(proj.bpda_url)
                sire_id = sire_by_url.get(norm)
                if not sire_id:
                    # Try path-only match
                    path = norm.split("bostonplans.org")[-1]
                    for k, v in sire_by_url.items():
                        if k.endswith(path):
                            sire_id = v
                            break
                if sire_id and proj.sire_id != sire_id:
                    proj.sire_id = sire_id
                    matched += 1

            session.commit()
            log.info("Matched/updated %d projects with SIRE IDs (%d total in DB)",
                     matched, len(projects))

            with_sire = [p for p in projects if p.sire_id]
            target_ids = {p.sire_id for p in with_sire}
            log.info("Projects with SIRE ID: %d / %d", len(with_sire), len(projects))

            # --- Step 3: Paginate all SIRE docs with correct ft_next_marker ---
            log.info("Paginating SIRE document list (ft_next_marker)...")
            docs_by_sire: dict[str, list[dict]] = {sid: [] for sid in target_ids}
            marker = None
            page = 0
            total_fetched = 0
            hits = 0

            while True:
                url = f"{SIRE_DOCS_URL}?ft_next_marker={marker}" if marker else SIRE_DOCS_URL
                resp = safe_get(client, url, extra_headers=SIRE_HEADERS)
                if resp is None:
                    log.warning("Request failed at page %d, stopping", page + 1)
                    break

                data = resp.json()
                items = data.get("metadataObj", [])
                if not items:
                    break

                total_count = data.get("totalcount", 0)
                new_marker = data.get("next_marker")

                for item in items:
                    bm = item.get("boxMetadata", {})
                    sid = bm.get("id", "")
                    if sid not in target_ids:
                        continue
                    subtype = bm.get("subtype", "")
                    doc_date = bm.get("documentDate", "")
                    share_link = item.get("shareLink", "")
                    if not share_link:
                        continue
                    docs_by_sire[sid].append({
                        "name": subtype,
                        "date": doc_date[:10] if doc_date else "",
                        "url": share_link,
                        "file_type": "pdf",
                        "filing_category": FILING_TYPE_MAP.get(subtype),
                    })
                    hits += 1

                total_fetched += len(items)
                page += 1
                log.info("  page %3d: %d docs fetched (%d/%d total), %d hits so far",
                         page, len(items), total_fetched, total_count, hits)

                if not new_marker or new_marker == marker:
                    log.info("  Pagination complete")
                    break
                marker = new_marker
                time.sleep(0.2)

            log.info("Done paginating. Total docs: %d, hits for our projects: %d",
                     total_fetched, hits)

            # --- Step 4: Write filings ---
            new_filings = 0
            proj_by_sire = {p.sire_id: p for p in with_sire}

            for sid, docs in docs_by_sire.items():
                proj = proj_by_sire.get(sid)
                if not proj:
                    continue
                for f in docs:
                    if f["filing_category"] is None:
                        continue
                    existing = (
                        session.query(ProjectFiling)
                        .filter_by(project_id=proj.id, url=f["url"])
                        .first()
                    )
                    if existing:
                        continue
                    try:
                        session.add(ProjectFiling(
                            project_id=proj.id,
                            name=f["name"],
                            date=f["date"],
                            url=f["url"],
                            file_type=f["file_type"],
                            filing_category=f["filing_category"],
                        ))
                        session.flush()
                        new_filings += 1
                    except IntegrityError:
                        session.rollback()

            session.commit()
            log.info("Added %d new filings to database", new_filings)

            # --- Summary ---
            total_proj = session.query(Project).count()
            proj_with_filings = session.query(Project).filter(Project.filings.any()).count()
            total_filings = session.query(ProjectFiling).count()
            cat_rows = session.execute(
                __import__("sqlalchemy").text(
                    "SELECT filing_category, COUNT(*) FROM project_filings GROUP BY filing_category ORDER BY COUNT(*) DESC"
                )
            ).fetchall()

            print(f"\n=== Backfill complete ===")
            print(f"  Projects in DB:          {total_proj}")
            print(f"  Projects with filings:   {proj_with_filings}")
            print(f"  Total filings:           {total_filings}")
            print(f"  By category:")
            for cat, cnt in cat_rows:
                print(f"    {cat:<20} {cnt}")

    finally:
        session.close()


if __name__ == "__main__":
    backfill()
