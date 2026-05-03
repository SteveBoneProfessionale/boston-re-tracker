"""
Fetch development news from RSS feeds and link articles to projects.

Sources:
  - Boston.gov news
  - Banker & Tradesman
  - The Real Deal Boston
  - Curbed Boston
  - Boston.com Real Estate
  - Boston Real Estate Times (HTML scrape — no RSS)
  - Bisnow Boston

Run on demand or on a schedule. Idempotent — skips already-seen URLs.
"""

import json
import re
import sys
import time
import logging
from datetime import datetime, timezone
from pathlib import Path

import feedparser
import httpx
from bs4 import BeautifulSoup
from rapidfuzz import fuzz
from scraper.classifier import classify_topics

_SCRAPE_LOG = Path(__file__).parent.parent / "data" / "scrape_log.json"

# ── Relevance filtering ────────────────────────────────────────────────────────
# Multi-word phrases — substring matching is safe (short phrases won't appear
# in unrelated articles by accident).
_RE_PHRASES = frozenset({
    "real estate", "development", "housing", "construction",
    "mixed-use", "mixed use",
    "affordable housing", "income-restricted",
    "redevelopment", "ground lease", "cap rate",
    "sq ft", "square feet", "square foot",
    "boston planning", "article 80",
    "logistics", "industrial park",
    "residential units", "landlord",
    "hotel development", "hotel project", "hotel construction",
    "hotel acquisition", "hotel deal", "hotel lease",
    "hotel conversion", "hotel renovation",
    "hospitality development",
    "foreclosure", "sale-leaseback", "leaseback",
    "absorption rate", "vacancy rate", "net lease",
    "financing", "refinanc",
    "commercial property", "commercial real estate",
    "property tax", "property value",
    "transit-oriented", "transit oriented",
    "urban planning", "master plan",
    "homebuying", "homebuyer", "home buyer", "homeowner", "homeownership",
    "lending arm", "loan portfolio", "distressed loan",
    "office space", "office building", "office tower", "office park",
    "office loan", "office deal", "office market", "office complex",
    "office lease", "office sale", "office portfolio", "office sector",
    "retail space", "retail tenant", "retail lease",
    "lab space", "life science", "life sciences",
    "multifamily", "multi-family",
    "reit", "mortgage",
})

# Single words — checked with \b word boundaries to avoid substring false
# positives (e.g. "tenant" must not match "lieutenant").
_RE_WORDS = frozenset({
    "lease", "leasing",
    "tenant", "tenants",
    "developer", "developers",
    "bpda", "zoning", "rezoning",
    "permit", "permitting", "permitted",
    "condo", "condos", "condominium", "condominiums",
    "apartment", "apartments",
    "warehouse", "demolition", "groundbreaking",
    "parcel",
})

# Curbed's RSS is national; require at least one Boston-area term
# to accept articles from that source.
_BOSTON_TERMS = frozenset({
    "boston", "massachusetts", "cambridge", "somerville", "brookline",
    "newton", "quincy", "waltham", "woburn", "malden", "medford",
    "charlestown", "dorchester", "south end", "back bay", "south boston",
    "east boston", "fenway", "allston", "brighton", "jamaica plain",
    "roxbury", "mission hill", "seaport", "waterfront", "kendall",
    "longwood", "mattapan", "roslindale", "west roxbury", "hyde park",
    "north end", "beacon hill",
})

# Sources whose articles must also pass the Boston geo-filter
_GEO_FILTERED_SOURCES = {"curbed"}


def _is_relevant(title: str, summary: str) -> bool:
    """Return True if the article is real-estate-related."""
    haystack = (title + " " + summary).lower()
    if any(phrase in haystack for phrase in _RE_PHRASES):
        return True
    return any(re.search(r'\b' + re.escape(w) + r'\b', haystack) for w in _RE_WORDS)


def _is_boston(title: str, summary: str) -> bool:
    """Return True if the article mentions a Boston-area location."""
    haystack = (title + " " + summary).lower()
    return any(term in haystack for term in _BOSTON_TERMS)


def _write_run_log(run_time: datetime, source_results: list[dict]) -> None:
    """Append one run record to data/scrape_log.json, keeping last 100 runs."""
    try:
        existing = json.loads(_SCRAPE_LOG.read_text()) if _SCRAPE_LOG.exists() else []
    except Exception:
        existing = []
    existing.append({
        "run_time": run_time.isoformat(),
        "sources": source_results,
    })
    try:
        _SCRAPE_LOG.write_text(json.dumps(existing[-100:], indent=2))
    except Exception as exc:
        log.warning("Could not write scrape log: %s", exc)


# Generic location/structure words that appear in nearly every article and
# must not be counted as meaningful project-name signals.
_MATCH_STOP = frozenset({
    "boston", "massachusetts", "street", "avenue", "road", "boulevard",
    "highway", "parkway", "place", "court", "drive", "circle", "square",
    "south", "north", "east", "west", "upper", "lower", "phase",
    "building", "project", "development", "district", "center", "centre",
    "crossing", "station", "village", "gardens", "landing", "harbor",
    "wharf", "commons", "height", "heights", "point", "park",
    "washington", "state", "lincoln", "atlantic",
})


def _name_key_tokens(name: str) -> list[str]:
    """Return significant tokens (5+ chars, not generic) from a project name."""
    return [t for t in re.findall(r'\b[a-z]{5,}\b', name.lower()) if t not in _MATCH_STOP]

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import init_db, get_session
from db.models import NewsItem, Project

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

FEEDS = [
    {
        "name": "Boston.gov News",
        "url": "https://www.boston.gov/rss.xml",
        "source": "boston_gov",
    },
    {
        "name": "Banker & Tradesman",
        "url": "https://bankerandtradesman.com/feed/",
        "source": "banker_tradesman",
    },
    {
        "name": "The Real Deal Boston",
        "url": "https://therealdeal.com/boston/feed/",
        "source": "the_real_deal",
    },
    {
        "name": "Curbed",
        "url": "https://www.curbed.com/rss/index.xml",
        "source": "curbed",
    },
    {
        "name": "Boston.com Real Estate",
        "url": "https://www.boston.com/tag/real-estate/feed/",
        "source": "boston_com",
    },
    {
        "name": "Bisnow Boston",
        "url": "https://www.bisnow.com/rss/boston",
        "source": "bisnow_boston",
    },
]

# Sources scraped via HTML (no RSS available)
HTML_SCRAPERS = [
    {
        "name": "Boston Real Estate Times",
        "url": "https://bostonrealestatetimes.com/",
        "source": "boston_re_times",
    },
]

_BRE_SKIP = {"/category/", "/tag/", "/page/", "/author/", "/feed", "/events/", "/about", "/advertising"}

MATCH_THRESHOLD = 65
ADDR_ONLY_THRESHOLD = 75


def _parse_date(entry) -> datetime | None:
    for attr in ("published_parsed", "updated_parsed"):
        t = getattr(entry, attr, None)
        if t:
            try:
                return datetime(*t[:6], tzinfo=timezone.utc)
            except Exception:
                pass
    return None


def _summary(entry) -> str:
    for attr in ("summary", "description", "content"):
        val = getattr(entry, attr, None)
        if isinstance(val, list):
            val = val[0].get("value", "") if val else ""
        if val:
            text = re.sub(r"<[^>]+>", " ", str(val))
            return " ".join(text.split())[:400]
    return ""


def _best_project_match(title: str, summary: str, projects: list) -> tuple[int | None, float]:
    haystack = (title + " " + summary).lower()
    best_id = None
    best_score = 0.0

    for proj in projects:
        name = (proj.name or "").lower()
        address = (proj.address or "").lower()
        nbhd = (proj.neighborhood or "").lower()

        score_name = fuzz.partial_ratio(name, title.lower())
        score_addr = fuzz.partial_ratio(address, haystack) if address else 0
        score = max(score_name, score_addr * 0.85)

        if nbhd and nbhd in haystack:
            score = min(100, score * 1.08)

        key_toks = _name_key_tokens(name)
        if key_toks:
            if not any(t in haystack for t in key_toks):
                continue
            threshold = MATCH_THRESHOLD
        else:
            threshold = ADDR_ONLY_THRESHOLD

        if score >= threshold and score > best_score:
            best_score = score
            best_id = proj.id

    return (best_id, best_score) if best_id else (None, 0.0)


def _scrape_bre_times(resp: httpx.Response) -> list[dict]:
    """Parse article cards from bostonrealestatetimes.com homepage."""
    soup = BeautifulSoup(resp.text, "html.parser")
    seen = set()
    articles = []

    for mod in soup.find_all("div", class_=re.compile(r"^td_module_")):
        time_el = mod.find("time", class_="td-module-date")
        if not time_el:
            continue

        link = mod.find("a", title=True, href=re.compile(r"^https://bostonrealestatetimes\.com/"))
        if not link:
            continue

        url = link["href"]
        if url in seen or any(s in url for s in _BRE_SKIP):
            continue
        seen.add(url)

        title = (link.get("title") or link.get_text(strip=True)).strip()
        dt_str = time_el.get("datetime", "")
        pub_date = None
        if dt_str:
            try:
                pub_date = datetime.fromisoformat(dt_str).astimezone(timezone.utc).replace(tzinfo=None)
                pub_date = pub_date.replace(tzinfo=timezone.utc)
            except Exception:
                pass

        excerpt_el = mod.find(class_="td-excerpt")
        excerpt = excerpt_el.get_text(strip=True) if excerpt_el else ""

        articles.append({"title": title, "url": url, "pub_date": pub_date, "summary": excerpt})

    return articles


def purge_irrelevant(session) -> int:
    """Delete articles from DB that fail the relevance filter. Returns purge count."""
    items = session.query(NewsItem).all()
    purged = 0
    for item in items:
        title = item.title or ""
        summary = item.summary or ""
        source = item.source or ""
        if not _is_relevant(title, summary):
            session.delete(item)
            purged += 1
        elif source in _GEO_FILTERED_SOURCES and not _is_boston(title, summary):
            session.delete(item)
            purged += 1
    session.commit()
    log.info("Purged %d irrelevant articles from DB", purged)
    return purged


def fetch_news():
    init_db()
    session = get_session()
    run_time = datetime.now(timezone.utc)
    source_results: list[dict] = []

    try:
        projects = session.query(Project).all()
        log.info("Loaded %d projects for matching", len(projects))

        headers = {"User-Agent": "Mozilla/5.0 (compatible; BostonCRETracker/1.0)"}
        total_new = 0
        total_linked = 0
        total_skipped = 0

        # ── RSS feeds ────────────────────────────────────────────────────────
        for feed_def in FEEDS:
            log.info("Fetching: %s", feed_def["name"])
            error_msg = None
            fetched = 0
            new_count = 0
            skipped = 0
            try:
                resp = httpx.get(feed_def["url"], headers=headers, timeout=15, follow_redirects=True)
                parsed = feedparser.parse(resp.content)
                entries = parsed.get("entries", [])
                fetched = len(entries)
                log.info("  %d entries", fetched)

                for entry in entries:
                    url = entry.get("link", "")
                    if not url:
                        continue
                    if session.query(NewsItem).filter_by(url=url).first():
                        continue

                    title = entry.get("title", "").strip()
                    summary = _summary(entry)

                    # Relevance filter — discard non-real-estate articles
                    if not _is_relevant(title, summary):
                        skipped += 1
                        continue
                    # Geo filter for national feeds
                    if feed_def["source"] in _GEO_FILTERED_SOURCES:
                        if not _is_boston(title, summary):
                            skipped += 1
                            continue

                    pub_date = _parse_date(entry)
                    proj_id, score = _best_project_match(title, summary, projects)

                    session.add(NewsItem(
                        title=title,
                        url=url,
                        published_date=pub_date,
                        source=feed_def["source"],
                        summary=summary,
                        linked_project_id=proj_id,
                        match_score=score if proj_id else None,
                        topics=classify_topics(title, summary),
                    ))
                    new_count += 1
                    if proj_id:
                        total_linked += 1

                try:
                    session.commit()
                except Exception as exc:
                    session.rollback()
                    log.warning("  DB error: %s", exc)
                    error_msg = str(exc)

            except Exception as exc:
                log.warning("  Failed to fetch %s: %s", feed_def["name"], exc)
                error_msg = str(exc)

            total_new += new_count
            total_skipped += skipped
            log.info("  Added %d new, skipped %d irrelevant", new_count, skipped)
            source_results.append({
                "name": feed_def["name"],
                "source": feed_def["source"],
                "fetched": fetched,
                "new": new_count,
                "skipped": skipped,
                "error": error_msg,
            })
            time.sleep(1)

        # ── HTML scrapers ────────────────────────────────────────────────────
        for scraper_def in HTML_SCRAPERS:
            log.info("Scraping: %s", scraper_def["name"])
            error_msg = None
            fetched = 0
            new_count = 0
            skipped = 0
            try:
                resp = httpx.get(scraper_def["url"], headers=headers, timeout=15, follow_redirects=True)
                articles = _scrape_bre_times(resp)
                fetched = len(articles)
                log.info("  %d articles found", fetched)

                for art in articles:
                    if not art["url"] or not art["title"]:
                        continue
                    if session.query(NewsItem).filter_by(url=art["url"]).first():
                        continue

                    title = art["title"]
                    summary = art["summary"]

                    # Relevance filter
                    if not _is_relevant(title, summary):
                        skipped += 1
                        continue

                    proj_id, score = _best_project_match(title, summary, projects)

                    session.add(NewsItem(
                        title=title,
                        url=art["url"],
                        published_date=art["pub_date"],
                        source=scraper_def["source"],
                        summary=summary,
                        linked_project_id=proj_id,
                        match_score=score if proj_id else None,
                        topics=classify_topics(title, summary),
                    ))
                    new_count += 1
                    if proj_id:
                        total_linked += 1

                try:
                    session.commit()
                except Exception as exc:
                    session.rollback()
                    log.warning("  DB error: %s", exc)
                    error_msg = str(exc)

            except Exception as exc:
                log.warning("  Failed to scrape %s: %s", scraper_def["name"], exc)
                error_msg = str(exc)

            total_new += new_count
            total_skipped += skipped
            log.info("  Added %d new, skipped %d irrelevant", new_count, skipped)
            source_results.append({
                "name": scraper_def["name"],
                "source": scraper_def["source"],
                "fetched": fetched,
                "new": new_count,
                "skipped": skipped,
                "error": error_msg,
            })
            time.sleep(1)

        log.info("News fetch complete: %d new, %d linked, %d skipped", total_new, total_linked, total_skipped)
        total = session.query(NewsItem).count()
        linked = session.query(NewsItem).filter(NewsItem.linked_project_id.isnot(None)).count()
        log.info("DB totals: %d articles, %d project-linked", total, linked)

    finally:
        session.close()
        _write_run_log(run_time, source_results)


if __name__ == "__main__":
    fetch_news()
