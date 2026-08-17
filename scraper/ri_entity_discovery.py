"""
Discover Rhode Island Open Meetings Portal EntityIDs by board name.

The portal exposes no public body search or enumeration endpoint -- the
GovDirectory page is POST-driven and renders nothing server-side, the
dashboard has no entity picker, and /RSSFeed/Rss returns only an HTML shell.
So board IDs have to be resolved by probing.

Probes /OpenMeetingsPublic/BoardMembers rather than the dashboard: it is 28 KB
against the dashboard's 151 KB, carries the board name in <h1>, and returns an
empty <h1> for an ID that has no entity behind it.

Results are cached to data/ri_entity_scan.json and the scan is resumable, so a
range is never re-fetched. Deliberately polite: a small worker pool and a
per-request delay, against a government transparency portal built for public
access.

    python scraper/ri_entity_discovery.py --ranges 2240-2770
    python scraper/ri_entity_discovery.py --report
"""

import sys
import json
import time
import logging
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import httpx
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)   # one line per probe is noise at scan scale

BASE = "https://opengov.sos.ri.gov"
BOARD_URL = BASE + "/OpenMeetingsPublic/BoardMembers?subtopmenuId=203&EntityID={eid}&MeetingID=0"
CACHE = Path(__file__).parent.parent / "data" / "ri_entity_scan.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    )
}

WORKERS = 2
DELAY = 0.6          # per-request, per-worker
TIMEOUT = 60

_lock = threading.Lock()


def load_cache() -> dict:
    if CACHE.exists():
        try:
            return json.loads(CACHE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            log.warning("Cache unreadable — starting fresh")
    return {}


def save_cache(cache: dict):
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    CACHE.write_text(json.dumps(cache, indent=1, sort_keys=True), encoding="utf-8")


def board_name(client: httpx.Client, eid: int) -> str | None:
    """Board name for an EntityID, or None if no entity exists there.

    Returns None on transport failure too; the caller leaves those out of the
    cache so a later run retries them rather than recording a false negative.
    """
    try:
        r = client.get(BOARD_URL.format(eid=eid), timeout=TIMEOUT)
        if r.status_code != 200:
            return None
        h1 = BeautifulSoup(r.text, "html.parser").find("h1")
        name = h1.get_text(" ", strip=True) if h1 else ""
        return name or ""          # "" means "probed, no entity"
    except Exception as exc:
        log.debug("EntityID %d: %s", eid, exc)
        return None


def parse_ranges(spec: str) -> list[int]:
    out: list[int] = []
    for part in spec.split(","):
        part = part.strip()
        if "-" in part:
            a, b = part.split("-", 1)
            out.extend(range(int(a), int(b) + 1))
        elif part:
            out.append(int(part))
    return out


def scan(ids: list[int]) -> dict:
    cache = load_cache()
    todo = [i for i in ids if str(i) not in cache]
    log.info("Requested %d ids; %d already cached; %d to fetch",
             len(ids), len(ids) - len(todo), len(todo))
    if not todo:
        return cache

    done = 0
    with httpx.Client(headers=HEADERS, follow_redirects=True) as client:
        def work(eid: int):
            nonlocal done
            name = board_name(client, eid)
            time.sleep(DELAY)
            with _lock:
                if name is not None:          # transport failures stay uncached
                    cache[str(eid)] = name
                done += 1
                if done % 50 == 0:
                    save_cache(cache)
                    hits = sum(1 for v in cache.values() if v)
                    log.info("  %d/%d probed — %d entities known", done, len(todo), hits)

        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            list(pool.map(work, todo))

    save_cache(cache)
    log.info("Scan complete — %d ids cached, %d are real entities",
             len(cache), sum(1 for v in cache.values() if v))
    return cache


# Municipalities in scope for the Rhode Island market.
TARGET_CITIES = ["Providence", "Cranston", "Pawtucket", "Newport", "Warwick", "Central Falls"]


def report(cache: dict, cities: list[str] | None = None):
    cities = cities or TARGET_CITIES
    log.info("")
    for city in cities:
        rows = sorted(
            ((int(k), v) for k, v in cache.items() if v and city.lower() in v.lower()),
            key=lambda kv: kv[0],
        )
        log.info("=== %s (%d bodies found) ===", city.upper(), len(rows))
        for eid, name in rows:
            log.info("  %-6d %s", eid, name)
        log.info("")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--ranges", help="e.g. 700-780,2190-2260")
    parser.add_argument("--report", action="store_true")
    args = parser.parse_args()

    c = load_cache()
    if args.ranges:
        c = scan(parse_ranges(args.ranges))
    if args.report or not args.ranges:
        report(c)
