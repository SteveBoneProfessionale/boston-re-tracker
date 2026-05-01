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

# Generic location/structure words that appear in nearly every article and
# must not be counted as meaningful project-name signals.
_MATCH_STOP = frozenset({
    "boston", "massachusetts", "street", "avenue", "road", "boulevard",
    "highway", "parkway", "place", "court", "drive", "circle", "square",
    "south", "north", "east", "west", "upper", "lower", "phase",
    "building", "project", "development", "district", "center", "centre",
    "crossing", "station", "village", "gardens", "landing", "harbor",
    "wharf", "commons", "height", "heights", "point", "park",
    # Common proper nouns that appear in many articles unrelated to specific projects
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
        "name": "Curbed Boston",
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

MATCH_THRESHOLD = 65          # lowered from 72; token guard below filters noise
ADDR_ONLY_THRESHOLD = 75      # higher bar when project name has no key tokens


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

        # Token guard: if the project has meaningful name words (not generic location
        # terms), at least one must appear in the article — prevents "Massachusetts Ave"
        # matching any article that mentions Massachusetts, etc.
        key_toks = _name_key_tokens(name)
        if key_toks:
            if not any(t in haystack for t in key_toks):
                continue
            threshold = MATCH_THRESHOLD
        else:
            # Pure address names (e.g. "101 Boston Street") have no guard available,
            # so require a higher score to compensate.
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


def fetch_news():
    init_db()
    session = get_session()

    try:
        projects = session.query(Project).all()
        log.info("Loaded %d projects for matching", len(projects))

        headers = {"User-Agent": "Mozilla/5.0 (compatible; BostonCRETracker/1.0)"}
        total_new = 0
        total_linked = 0

        # ── RSS feeds ────────────────────────────────────────────────────────
        for feed_def in FEEDS:
            log.info("Fetching: %s", feed_def["name"])
            try:
                resp = httpx.get(feed_def["url"], headers=headers, timeout=15, follow_redirects=True)
                parsed = feedparser.parse(resp.content)
            except Exception as exc:
                log.warning("  Failed to fetch %s: %s", feed_def["name"], exc)
                continue

            entries = parsed.get("entries", [])
            log.info("  %d entries", len(entries))
            new_count = 0

            for entry in entries:
                url = entry.get("link", "")
                if not url:
                    continue
                if session.query(NewsItem).filter_by(url=url).first():
                    continue

                title = entry.get("title", "").strip()
                summary = _summary(entry)
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

            total_new += new_count
            log.info("  Added %d new articles", new_count)
            time.sleep(1)

        # ── HTML scrapers ────────────────────────────────────────────────────
        for scraper_def in HTML_SCRAPERS:
            log.info("Scraping: %s", scraper_def["name"])
            try:
                resp = httpx.get(scraper_def["url"], headers=headers, timeout=15, follow_redirects=True)
                articles = _scrape_bre_times(resp)
            except Exception as exc:
                log.warning("  Failed to scrape %s: %s", scraper_def["name"], exc)
                continue

            log.info("  %d articles found", len(articles))
            new_count = 0

            for art in articles:
                if not art["url"] or not art["title"]:
                    continue
                if session.query(NewsItem).filter_by(url=art["url"]).first():
                    continue

                proj_id, score = _best_project_match(art["title"], art["summary"], projects)

                session.add(NewsItem(
                    title=art["title"],
                    url=art["url"],
                    published_date=art["pub_date"],
                    source=scraper_def["source"],
                    summary=art["summary"],
                    linked_project_id=proj_id,
                    match_score=score if proj_id else None,
                    topics=classify_topics(art["title"], art["summary"]),
                ))
                new_count += 1
                if proj_id:
                    total_linked += 1

            try:
                session.commit()
            except Exception as exc:
                session.rollback()
                log.warning("  DB error: %s", exc)

            total_new += new_count
            log.info("  Added %d new articles", new_count)
            time.sleep(1)

        log.info("News fetch complete: %d new articles, %d linked to projects", total_new, total_linked)

        total = session.query(NewsItem).count()
        linked = session.query(NewsItem).filter(NewsItem.linked_project_id.isnot(None)).count()
        log.info("DB totals: %d articles, %d project-linked", total, linked)

    finally:
        session.close()


if __name__ == "__main__":
    fetch_news()
