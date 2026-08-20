"""Download the matched Rhode Island board documents that are not cached yet.

One request a second, and anything that fails is recorded and skipped rather
than retried into the ground.
"""
import json
import re
import time
from pathlib import Path
from urllib.parse import unquote

import requests

OUT = Path("data/ri_plansets")
LOG = Path("data/ri_fetch_log.json")
UA = {"User-Agent": "Mozilla/5.0 (research; contact via project owner)"}


def safe_name(url):
    base = unquote(url.rsplit("/", 1)[-1])
    if not base.lower().endswith(".pdf"):
        m = re.search(r"([^\/]+\.pdf)", unquote(url), re.I)
        base = m.group(1) if m else (re.sub(r"\W+", "_", url)[-60:] + ".pdf")
    return re.sub(r"[^\w.\-]+", "_", base)[:120]


def main():
    m = json.loads(Path("data/ri_zbr_matches.json").read_text())
    urls = sorted({u for v in m.values() for u in v["urls"]})
    log = {}
    got = skipped = failed = 0
    for i, u in enumerate(urls, 1):
        p = OUT / safe_name(u)
        if p.exists() and p.stat().st_size > 2000:
            skipped += 1
            log[u] = {"file": p.name, "status": "cached"}
            continue
        try:
            r = requests.get(u, headers=UA, timeout=60)
            if r.status_code == 200 and r.content[:4] == b"%PDF":
                p.write_bytes(r.content)
                got += 1
                log[u] = {"file": p.name, "status": "ok", "bytes": len(r.content)}
            else:
                failed += 1
                log[u] = {"status": f"http {r.status_code}, {len(r.content)}b, "
                                    f"not a pdf" if r.content[:4] != b"%PDF" else "bad"}
        except Exception as e:
            failed += 1
            log[u] = {"status": f"error {type(e).__name__}"}
        time.sleep(1.0)
        if i % 25 == 0:
            print(f"  {i}/{len(urls)}  got={got} cached={skipped} failed={failed}", flush=True)
    LOG.write_text(json.dumps(log, indent=1))
    print(f"done: downloaded {got}, already cached {skipped}, failed {failed}")


if __name__ == "__main__":
    main()
