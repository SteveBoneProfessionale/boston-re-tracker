"""
BPDA development project index scraper.

Data sources:
  1. https://www.bostonplans.org/projects/development-projects  (project index & detail pages)
  2. https://sire.bostonplans.org/api/documentSearch/getProjects  (Salesforce project IDs)
  3. https://sire.bostonplans.org/api/documentSearch/getMetadataWithProjects  (document list)

Step 2 goal: populate the `projects` and `project_filings` tables.
No PDF downloading or AI extraction happens here.
"""

import re
import sys
import time
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from sqlalchemy.exc import IntegrityError

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import init_db, get_session
from db.models import Project, ProjectFiling

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_URL = "https://www.bostonplans.org"
PROJECTS_INDEX_URL = "https://www.bostonplans.org/projects/development-projects"
SIRE_PROJECTS_URL = "https://sire.bostonplans.org/api/documentSearch/getProjects"
SIRE_DOCS_URL = "https://sire.bostonplans.org/api/documentSearch/getMetadataWithProjects"

APPROVED_CUTOFF_MONTHS = 24
CRAWL_DELAY = 0.8

# SIRE document subtypes to include as filings (maps to our categories)
FILING_TYPE_MAP = {
    "Draft Project Impact Report (DPIR)": "dpir",
    "Final Project Impact Report (FPIR)": "dpir",
    "Revised Project Impact Report (RPIR)": "dpir",
    "Project Impact Report (PIR)": "dpir",
    "Project Notification Form (PNF)": "pnf",
    "Institutional Master Plan Notification Form-Project Notification Form (IMPNF-PNF)": "pnf",
    "Small Project Review Application (SPRA)": "small_project",
    "Small Project Change (SPC)": "small_project",
    # Include other useful doc types as "other"
    "Letter of Intent (LOI)": "other",
    "Adequacy Determination": "other",
    "Preliminary Adequacy Determination (PAD)": "other",
    "Scoping Determination": "other",
    "Supplemental Filing": "other",
    "Additional Information": "other",
}

# Filing priority for PDF selection (Step 3)
FILING_PRIORITY = {"dpir": 3, "pnf": 2, "small_project": 1, "other": 0}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}
SIRE_HEADERS = {
    "User-Agent": HEADERS["User-Agent"],
    "Accept": "application/json",
    "Referer": "http://apps.bostonplans.org/recordslibrary/",
}


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def make_client() -> httpx.Client:
    return httpx.Client(headers=HEADERS, timeout=30, follow_redirects=True)


def safe_get(client: httpx.Client, url: str, extra_headers: dict | None = None) -> httpx.Response | None:
    for attempt in range(3):
        try:
            h = dict(client.headers)
            if extra_headers:
                h.update(extra_headers)
            resp = client.get(url, headers=h)
            if resp.status_code == 200:
                return resp
            log.warning("HTTP %s for %s", resp.status_code, url)
            return None
        except httpx.RequestError as exc:
            log.warning("Request error (attempt %d): %s", attempt + 1, exc)
            time.sleep(2 ** attempt)
    return None


# ---------------------------------------------------------------------------
# BPDA project index scraping
# ---------------------------------------------------------------------------

def get_total_pages(soup: BeautifulSoup) -> int:
    """Read the last page number from the paging widget."""
    paging = soup.find("aside", class_="paging")
    if not paging:
        return 1
    last = paging.find("a", href=re.compile(r"page=\d+"))
    page_nums = []
    for link in paging.find_all("a", href=re.compile(r"page=(\d+)")):
        m = re.search(r"page=(\d+)", link["href"])
        if m:
            page_nums.append(int(m.group(1)))
    return max(page_nums) if page_nums else 1


def parse_index_page(soup: BeautifulSoup) -> list[dict]:
    """
    Parse one BPDA project index page.

    Each project is a <table class="devprojectTable"> with:
      <caption><a href="...">Project Name</a></caption>
      <thead><tr>
        <th><h2>STATUS<span class="tableSubHeader">Project Status</span></h2></th>
        <th><h2>TYPE<span class="tableSubHeader">Project Type</span></h2></th>
        <th><h2>DATE<span class="tableSubHeader">Latest Filed Date</span></h2></th>
      </tr></thead>
    """
    projects = []
    tables = soup.find_all("table", class_="devprojectTable")

    for table in tables:
        caption = table.find("caption")
        if not caption:
            continue
        link = caption.find("a")
        if not link:
            continue

        name = link.get_text(strip=True)
        href = link.get("href", "")
        project_url = urljoin(BASE_URL, href)

        # Parse thead columns
        status = project_type = latest_date = ""
        for th in table.find_all("th"):
            h2 = th.find("h2")
            if not h2:
                continue
            sub = h2.find("span", class_="tableSubHeader")
            label = sub.get_text(strip=True) if sub else ""
            if sub:
                sub.extract()          # remove span to isolate the value text
            value = h2.get_text(strip=True)
            if label == "Project Status":
                status = value
            elif label == "Project Type":
                project_type = value
            elif label == "Latest Filed Date":
                latest_date = value

        projects.append({
            "name": name,
            "status": status,
            "project_type": project_type,
            "latest_date": latest_date,
            "project_url": project_url,
        })

    return projects


def status_is_active(status: str) -> bool:
    s = status.strip().lower()
    return s in ("letter of intent", "under review", "board approved", "under construction")


def scrape_index_all_pages(client: httpx.Client) -> list[dict]:
    """Fetch every page of the project index. Returns all projects found."""
    log.info("Fetching page 1 to determine total pages…")
    resp = safe_get(client, PROJECTS_INDEX_URL)
    if resp is None:
        log.error("Could not reach BPDA project index")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    total_pages = get_total_pages(soup)
    log.info("Total pages: %d", total_pages)

    all_projects = parse_index_page(soup)
    log.info("  Page 1: %d projects", len(all_projects))

    for page in range(2, total_pages + 1):
        url = f"{PROJECTS_INDEX_URL}?page={page}"
        resp = safe_get(client, url)
        if resp is None:
            log.warning("Failed to fetch page %d, stopping", page)
            break
        soup = BeautifulSoup(resp.text, "html.parser")
        page_projects = parse_index_page(soup)
        all_projects.extend(page_projects)
        log.info("  Page %d: %d projects (running total: %d)", page, len(page_projects), len(all_projects))
        time.sleep(CRAWL_DELAY)

    return all_projects


# ---------------------------------------------------------------------------
# BPDA project detail page
# ---------------------------------------------------------------------------

def scrape_detail_page(client: httpx.Client, url: str) -> dict:
    """
    Fetch a project detail page and return:
      {address, neighborhood, description, current_status}

    The detail page uses:
      <div class="projATimelineDetails">
        <div class="detailsContainer">
          <div class="bpdaPrjHeader">Neighborhood</div>
          <div class="bpdaPrjDetails">South Boston</div>
        </div>
        ...
      </div>
      <ul class="projectPhaseList">
        <li class="completed active">Letter of Intent</li>
        ...
      </ul>
    """
    resp = safe_get(client, url)
    if resp is None:
        return {}

    soup = BeautifulSoup(resp.text, "html.parser")
    detail = {}

    # Key-value pairs in the details section
    details_div = soup.find("div", class_="projATimelineDetails")
    if details_div:
        containers = details_div.find_all("div", class_="detailsContainer")
        for container in containers:
            header = container.find("div", class_="bpdaPrjHeader")
            value_el = container.find("div", class_="bpdaPrjDetails")
            if not header or not value_el:
                continue
            key = header.get_text(strip=True).lower()
            value = value_el.get_text(strip=True)
            if "address" in key:
                detail["address"] = value
            elif "neighborhood" in key:
                detail["neighborhood"] = value
            elif "gross floor area" in key or "floor area" in key:
                detail["bpda_gsf"] = parse_gsf(value)
            elif "land sq" in key or "land square" in key:
                detail["land_sqft"] = parse_gsf(value)

        # Description is in a styled div inside the details section
        desc_div = details_div.find("div", style=re.compile(r"font-size"))
        if desc_div:
            detail["description"] = desc_div.get_text(strip=True)

    # Current phase: the li that has class="completed active" or just "active"
    phase_list = soup.find("ul", class_="projectPhaseList")
    if phase_list:
        active_li = phase_list.find("li", class_="active")
        if active_li:
            detail["current_status"] = active_li.get_text(strip=True)

    # Project scale tag: "Large Project" or "Small Project" (from tl_tags spans)
    for tag_el in soup.find_all("span", class_="tl_tags"):
        tag_text = tag_el.get_text(strip=True)
        if "large project" in tag_text.lower():
            detail["project_scale"] = "Large Project"
            break
        elif "small project" in tag_text.lower():
            detail["project_scale"] = "Small Project"
            break

    # Neighborhood tag (fallback)
    if "neighborhood" not in detail:
        tag_el = soup.find("span", class_="tl_tags_url")
        if tag_el:
            a = tag_el.find("a")
            if a:
                detail["neighborhood"] = a.get_text(strip=True)

    # BPDA internal project ID (used for cross-referencing)
    timeline_id_el = soup.find(class_="timeline-project-id")
    if timeline_id_el:
        detail["bpda_internal_id"] = timeline_id_el.get_text(strip=True)

    return detail


def parse_gsf(text: str) -> int | None:
    """Parse a GSF string like '219,839 sq ft' or '50000' into an integer."""
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", text.split("sq")[0].split("SF")[0].split("sf")[0])
    return int(digits) if digits else None


# ---------------------------------------------------------------------------
# SIRE API — project list and document discovery
# ---------------------------------------------------------------------------

def fetch_sire_projects(client: httpx.Client) -> dict[str, dict]:
    """
    Fetch all projects from the SIRE API.
    Returns dict keyed by normalized website URL -> {id, name, neighborhood}.
    """
    log.info("Fetching SIRE project list…")
    resp = safe_get(client, SIRE_PROJECTS_URL, extra_headers=SIRE_HEADERS)
    if resp is None:
        log.warning("Could not reach SIRE project API")
        return {}

    projects = resp.json()
    log.info("  SIRE returned %d projects", len(projects))

    # Build lookup by normalized website URL
    by_url: dict[str, dict] = {}
    for p in projects:
        raw_url = p.get("Website_URL__c") or ""
        url = raw_url.rstrip("/").lower()
        if url:
            by_url[url] = {
                "sire_id": p.get("Id", ""),
                "name": p.get("Name", ""),
                "neighborhood": p.get("Neighborhood__c", ""),
            }
    return by_url


def fetch_sire_docs_for_projects(
    client: httpx.Client,
    target_sire_ids: set[str],
    max_pages: int = 200,
) -> dict[str, list[dict]]:
    """
    Paginate through the SIRE document list and collect documents
    for the given set of Salesforce project IDs.

    Returns dict: sire_id -> [list of document dicts]
    """
    log.info("Fetching SIRE documents for %d active projects…", len(target_sire_ids))
    docs_by_project: dict[str, list[dict]] = {sid: [] for sid in target_sire_ids}
    marker = None
    page = 0
    total_fetched = 0

    while page < max_pages:
        full_url = f"{SIRE_DOCS_URL}?ft_next_marker={marker}" if marker else SIRE_DOCS_URL
        resp = safe_get(client, full_url, extra_headers=SIRE_HEADERS)
        if resp is None:
            break

        data = resp.json()
        items = data.get("metadataObj", [])
        if not items:
            break

        total_count = data.get("totalcount", 0)
        new_marker = data.get("next_marker")

        for item in items:
            bm = item.get("boxMetadata", {})
            project_sire_id = bm.get("id", "")
            if project_sire_id not in target_sire_ids:
                continue

            subtype = bm.get("subtype", "")
            doc_date = bm.get("documentDate", "")
            share_link = item.get("shareLink", "")

            if not share_link:
                continue

            docs_by_project[project_sire_id].append({
                "name": subtype,
                "date": doc_date[:10] if doc_date else "",
                "url": share_link,
                "file_type": "pdf",
                "filing_category": FILING_TYPE_MAP.get(subtype, None),
            })

        total_fetched += len(items)
        log.info(
            "  SIRE docs page %d: fetched %d docs (%d/%d total)",
            page + 1, len(items), total_fetched, total_count,
        )

        if not new_marker or new_marker == marker:
            log.info("  No more pages")
            break

        marker = new_marker
        page += 1
        time.sleep(0.3)

    log.info("SIRE: collected docs for %d projects", sum(1 for v in docs_by_project.values() if v))
    return docs_by_project


# ---------------------------------------------------------------------------
# Database persistence
# ---------------------------------------------------------------------------

def upsert_project(session, index_data: dict, detail_data: dict, sire_data: dict,
                   skip_reason: str | None = None) -> tuple:
    """Insert or update a project record. Returns (project, is_new)."""
    now = datetime.utcnow()
    project_url = index_data["project_url"]

    project = session.query(Project).filter_by(bpda_url=project_url).first()
    is_new = project is None

    if is_new:
        project = Project(bpda_url=project_url, first_seen_date=now)
        session.add(project)

    project.name = (
        detail_data.get("name")
        or sire_data.get("name")
        or index_data.get("name")
    )
    project.address = detail_data.get("address") or index_data.get("name")
    project.neighborhood = (
        detail_data.get("neighborhood")
        or sire_data.get("neighborhood")
        or ""
    )
    project.status = detail_data.get("current_status") or index_data.get("status")
    project.description = detail_data.get("description")
    project.bpda_gsf = detail_data.get("bpda_gsf")
    project.project_scale = detail_data.get("project_scale")
    project.sire_id = sire_data.get("sire_id") or project.sire_id
    project.skip_reason = skip_reason
    project.last_checked_date = now

    return project, is_new


def upsert_filings(session, project: Project, filings: list[dict]):
    """Insert new filings, skip duplicates (handles re-runs gracefully)."""
    seen_urls = set()
    for f in filings:
        if f.get("filing_category") is None:
            continue
        url = f["url"]
        if url in seen_urls:
            continue
        seen_urls.add(url)

        existing = (
            session.query(ProjectFiling)
            .filter_by(project_id=project.id, url=url)
            .first()
        )
        if existing:
            continue
        try:
            session.add(ProjectFiling(
                project_id=project.id,
                name=f["name"],
                date=f["date"],
                url=url,
                file_type=f.get("file_type", "pdf"),
                filing_category=f["filing_category"],
            ))
            session.flush()
        except IntegrityError:
            session.rollback()
            log.debug("Duplicate filing skipped: %s", url)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_scraper(limit: int | None = None, active_limit: int | None = None):
    """
    Full scrape:
      1. BPDA project index → all projects with status
      2. Filter to active
      3. SIRE getProjects → Salesforce IDs
      4. Detail pages → address / neighborhood / description
      5. SIRE getMetadataWithProjects → filings
      6. Persist to SQLite
    """
    init_db()
    session = get_session()

    try:
        with make_client() as client:
            # --- Step 1: Get all index entries ---
            log.info("=== Phase 1: Scraping BPDA project index ===")
            all_index = scrape_index_all_pages(client)
            log.info("Total projects in index: %d", len(all_index))

            # --- Step 2: Filter to active ---
            active = [p for p in all_index if status_is_active(p["status"])]
            log.info("Active projects: %d", len(active))

            if not active:
                log.error("No active projects found — check BPDA site structure")
                return

            # --- Step 3: SIRE project list (for Salesforce IDs) ---
            log.info("=== Phase 2: Fetching SIRE project registry ===")
            sire_by_url = fetch_sire_projects(client)

            # Build reverse lookup: project_url -> sire_id
            def normalize_url(u):
                return u.rstrip("/").lower().replace("https://", "http://")

            sire_id_map: dict[str, dict] = {}
            for p in active:
                norm = normalize_url(p["project_url"])
                sire_data = sire_by_url.get(norm, {})
                if not sire_data:
                    # Try without subdomain differences
                    for k, v in sire_by_url.items():
                        if k.endswith(norm.split("bostonplans.org")[-1]):
                            sire_data = v
                            break
                sire_id_map[p["project_url"]] = sire_data

            matched = sum(1 for v in sire_id_map.values() if v.get("sire_id"))
            log.info("Matched %d/%d active projects to SIRE records", matched, len(active))

            # --- Step 4: Fetch detail pages for all active projects, then apply Article 80 filter ---
            # The Article 80 review type (Large/Small Project) is only available on detail pages
            # (tl_tags span), not in the index-level project_type column (which is asset/use class).
            if active_limit:
                active = active[:active_limit]
                log.info("active_limit: capping to first %d active projects (test mode)", active_limit)
            log.info("=== Phase 3: Fetching detail pages for %d active projects ===", len(active))
            detail_cache: dict[str, dict] = {}
            for i, proj in enumerate(active, 1):
                log.info("[%d/%d] Detail: %s", i, len(active), proj["name"])
                detail = scrape_detail_page(client, proj["project_url"])
                detail_cache[proj["project_url"]] = detail
                scale = detail.get("project_scale", "")
                gsf_str = f"{detail['bpda_gsf']:,}" if detail.get("bpda_gsf") else "unknown"
                log.info("  scale: %-14s  GSF: %s  nbhd: %s",
                         scale or "?", gsf_str, detail.get("neighborhood", "?"))
                time.sleep(CRAWL_DELAY)

            qualifying = [
                p for p in active
                if detail_cache.get(p["project_url"], {}).get("project_scale") in
                   ("Large Project", "Small Project")
            ]
            log.info(
                "Article 80 filter: %d qualifying (Large/Small Project tagged) / %d no tag (of %d active)",
                len(qualifying), len(active) - len(qualifying), len(active),
            )

            if limit:
                qualifying = qualifying[:limit]
                log.info("Limited to first %d qualifying projects", limit)

            # --- Step 5: SIRE documents (only for qualifying projects) ---
            log.info("=== Phase 4: Fetching SIRE document list ===")
            target_sire_ids = {
                sire_id_map[p["project_url"]]["sire_id"]
                for p in qualifying
                if sire_id_map.get(p["project_url"], {}).get("sire_id")
            }
            docs_by_sire = fetch_sire_docs_for_projects(client, target_sire_ids)

            # --- Step 6: Persist qualifying Article 80 projects ---
            log.info("=== Phase 5: Persisting to database ===")
            new_count = updated_count = 0

            for proj in qualifying:
                url = proj["project_url"]
                detail = detail_cache.get(url, {})
                sire_info = sire_id_map.get(url, {})
                sire_id = sire_info.get("sire_id", "")
                filings = docs_by_sire.get(sire_id, [])

                project, is_new = upsert_project(session, proj, detail, sire_info,
                                                 skip_reason=None)
                session.flush()
                upsert_filings(session, project, filings)
                session.commit()

                if is_new:
                    new_count += 1
                    log.info("  NEW: %s (%d filings)", project.name, len(filings))
                else:
                    updated_count += 1

            log.info(
                "\n=== Scrape complete ===\n"
                "  New:     %d\n"
                "  Updated: %d\n",
                new_count, updated_count,
            )
            _print_summary(session)

    finally:
        session.close()


def _print_summary(session):
    projects = session.query(Project).order_by(Project.name).all()

    print(f"\n{'PROJECT NAME':<50} {'STATUS':<22} {'TYPE':<35} {'GSF':>8} {'FILINGS':>7} {'NEIGHBORHOOD':<22}")
    print("-" * 150)
    for p in projects:
        gsf_str = f"{p.bpda_gsf:,}" if p.bpda_gsf else "?"
        print(
            f"{(p.name or 'Unknown')[:49]:<50} "
            f"{(p.status or '')[:21]:<22} "
            f"{(p.project_scale or '')[:34]:<35} "
            f"{gsf_str:>8} "
            f"{len(p.filings):>7} "
            f"{(p.neighborhood or '')[:21]:<22}"
        )
    print(f"\nTotal Article 80 projects: {len(projects)}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="BPDA project index scraper")
    parser.add_argument("--limit", type=int, default=None,
                        help="Persist only first N qualifying Article 80 projects")
    parser.add_argument("--active-limit", type=int, default=None,
                        help="Fetch detail pages for first N active projects only (test mode)")
    args = parser.parse_args()
    run_scraper(limit=args.limit, active_limit=args.active_limit)
