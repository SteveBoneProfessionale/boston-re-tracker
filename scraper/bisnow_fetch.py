r"""Fetch the article bodies for the indexed Bisnow Boston stories.

Text is cached to data/bisnow/*.txt so the archive is read once and can be
re-parsed without hitting the site again. Requests are paced.

The extracted text keeps the article's OWN dateline where one is present,
because the undated-press trap has fired repeatedly: articles for Urban Spaces
($103M, May 2022), 30 Hampshire Street ($25.1M, November 2025), 2400
Massachusetts Avenue ($12.5M, February 2024) and Berklee ($28.1M, November 2025)
all surfaced from 2026 queries and read as current. The index supplies the
publication date; the body has to supply the transaction date.

    python scraper/bisnow_fetch.py --only-deal-sheets
"""

import argparse
import html
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

INDEX = Path("data/bisnow_boston_index.json")
CACHE = Path("data/bisnow")
UA = {"User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                     "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36")}

_DROP = re.compile(r"<(script|style|noscript|svg)\b.*?</\1>", re.S | re.I)
_TAG = re.compile(r"<[^>]+>")


def to_text(raw: str) -> str:
    t = _DROP.sub(" ", raw)
    # Article bodies live in <p> and <h2>; keep paragraph breaks so the deal
    # list stays readable one item per line.
    t = re.sub(r"</(p|h2|h3|li|div)>", "\n", t, flags=re.I)
    t = _TAG.sub(" ", t)
    t = html.unescape(t)
    t = t.replace("’", "'").replace("“", '"').replace("”", '"')
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n\s*\n+", "\n", t)
    return t.strip()


def slug(url: str) -> str:
    return url.rstrip("/").split("/")[-1][:80]


def main(only_ds: bool, limit: int):
    rows = json.loads(INDEX.read_text(encoding="utf-8"))
    if only_ds:
        rows = [r for r in rows if "/deal-sheet/" in r["url"]]
    CACHE.mkdir(parents=True, exist_ok=True)
    done = new = 0
    with httpx.Client(headers=UA, timeout=60, follow_redirects=True) as c:
        for r in rows[:limit]:
            p = CACHE / f"{r['date']}_{slug(r['url'])}.txt"
            if p.exists():
                done += 1
                continue
            try:
                resp = c.get(r["url"])
                if resp.status_code != 200:
                    log.warning("%s -> HTTP %d", r["url"], resp.status_code)
                    continue
                body = to_text(resp.text)
                p.write_text(f"URL: {r['url']}\nINDEX_DATE: {r['date']}\n\n{body}",
                             encoding="utf-8")
                new += 1
                log.info("%s  %6d chars  %s", r["date"], len(body), slug(r["url"])[:60])
            except Exception as e:
                log.warning("%s -> %s", r["url"], e)
            time.sleep(1.2)
    log.info("\n%d newly fetched, %d already cached, %d total in %s",
             new, done, len(list(CACHE.glob('*.txt'))), CACHE)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--only-deal-sheets", action="store_true")
    ap.add_argument("--limit", type=int, default=200)
    a = ap.parse_args()
    main(a.only_deal_sheets, a.limit)
