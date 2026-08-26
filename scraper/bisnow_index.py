r"""Walk the Bisnow Boston news index and build a dated article list.

Searching finds articles matching the search words. Paging the index finds what
was published. That difference is not academic: a type-led search sweep missed
the Kensington at $234M, the largest Boston transaction of 2026, because the
report never used the words the search did.

Bisnow's robots.txt disallows /newsletters/*, /more-news, /user/* and a handful
of admin paths, and allows /news/*. This reads only /news/boston, one page at a
time with a delay, and stops at a date boundary rather than crawling the whole
archive.

The tag page (/tags/boston-deal-sheet) is NOT the archive -- it holds nine
articles, all 2021-2022, and appears abandoned. The sitemap is section-level
only, 475 entries and no articles. The paginated section index is the only
complete listing, running to roughly 141 pages at ~30 articles each.

    python scraper/bisnow_index.py --until 2026-01-01
"""

import argparse
import json
import logging
import re
import sys
import time
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

BASE = "https://www.bisnow.com/news/boston"
UA = {"User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                     "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36")}
OUT = Path("data/bisnow_boston_index.json")

MONTHS = {m: i + 1 for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
     "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])}

ART = re.compile(r"<article\b.*?</article>", re.S)
HREF = re.compile(r'href="(https://www\.bisnow\.com/news/boston/[^"?#]+)"')
DATE = re.compile(r'<span class="font-bold text-black">\s*([A-Z][a-z]{2}) (\d{1,2}), (\d{4})\s*</span>')


def parse_page(html: str) -> list[dict]:
    out = []
    for block in ART.findall(html):
        h = HREF.search(block)
        d = DATE.search(block)
        if not h:
            continue
        date = None
        if d:
            mon, day, yr = d.groups()
            if mon in MONTHS:
                date = f"{yr}-{MONTHS[mon]:02d}-{int(day):02d}"
        out.append({"url": h.group(1), "date": date})
    return out


def main(until: str, max_pages: int):
    seen, rows = set(), []
    with httpx.Client(headers=UA, timeout=60, follow_redirects=True) as c:
        for page in range(1, max_pages + 1):
            r = c.get(BASE, params={"page": page})
            if r.status_code != 200:
                log.warning("page %d -> HTTP %d, stopping", page, r.status_code)
                break
            items = parse_page(r.text)
            fresh = [i for i in items if i["url"] not in seen]
            for i in fresh:
                seen.add(i["url"])
                rows.append(i)
            dated = [i["date"] for i in items if i["date"]]
            oldest = min(dated) if dated else None
            log.info("page %3d  %2d new  oldest on page %s  (total %d)",
                     page, len(fresh), oldest or "?", len(rows))
            if not fresh:
                log.info("no new articles on page %d, stopping", page)
                break
            if oldest and oldest < until:
                log.info("reached %s, past the %s boundary, stopping", oldest, until)
                break
            time.sleep(1.0)          # deliberate pace on someone else's server

    rows = [r for r in rows if r["date"] and r["date"] >= until]
    rows.sort(key=lambda x: x["date"], reverse=True)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(rows, indent=1), encoding="utf-8")
    log.info("\n%d articles dated >= %s written to %s", len(rows), until, OUT)

    ds = [r for r in rows if "/deal-sheet/" in r["url"]]
    log.info("of which %d are deal-sheet issues", len(ds))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--until", default="2026-01-01")
    ap.add_argument("--max-pages", type=int, default=80)
    a = ap.parse_args()
    main(a.until, a.max_pages)
