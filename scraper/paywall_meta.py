r"""Read the head, not the body: recover facts from behind a paywall legitimately.

The Real Reporter gates its article text. It does not gate its <head>, and the
head is where the address lives:

    <meta property="twitter:alt"   content="26 Hichborn St., Boston">
    <meta property="og:image"      content=".../article_images/26HICHBORNsT.jpg">

That is the whole unlock. "ARX Buys Hub Apartments" names no address anywhere in
the readable lead, and the ARX row sat with no address and no seller through
four resolution passes because I only ever read the rendered text. The publisher
put the address in the social-preview card so it would render on Twitter, and
the social-preview card is served to everyone.

Nothing here defeats the paywall. The body stays unread; these are the tags a
publisher deliberately exposes for link previews, plus the filenames of images
served openly from a CDN. It is reading what was published, not circumventing
what was not.

WHAT TO PARSE, in rough order of usefulness:

    twitter:alt / og:image:alt   often a literal caption: address, city
    og:image filename            addresses get slugged into image names
    og:description               the lead paragraph, which the body hides
    og:title / twitter:title     the headline, often carrying the price
    <img alt=...>                in-body captions sometimes survive

CAVEAT, and it bit elsewhere in this project: og:description is sometimes STALE
or belongs to a different article entirely. The 11 Beacon page served the
Chick-fil-A description. Treat metadata as a lead to be corroborated, never as
a finished fact.

    python scraper/paywall_meta.py <url>
    python scraper/paywall_meta.py --scan          # every cached TRR article
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

UA = {"User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                     "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36")}

META = re.compile(
    r'<meta\s+(?:property|name)="((?:og|twitter)[^"]*)"\s+content="([^"]*)"', re.I)
IMG = re.compile(r'(?:src|content)="([^"]*\.(?:jpg|jpeg|png|webp))[^"]*"', re.I)
IMGALT = re.compile(r'<img[^>]+alt="([^"]{4,120})"', re.I)

# A street address in a caption or a slugged filename.
ADDR = re.compile(
    r"\b(\d{1,5}(?:[-–]\d{1,5})?)\s*"
    r"([A-Z][A-Za-z'.]*(?:\s+[A-Z][A-Za-z'.]*){0,3})\s*"
    r"(St|Street|Ave|Avenue|Rd|Road|Dr|Drive|Blvd|Boulevard|Pl|Place|Sq|Square|"
    r"Ln|Lane|Ct|Court|Way|Ter|Terrace|Pkwy|Row|Wharf)\b", re.I)


def unslug(name: str) -> str:
    """26HICHBORNsT.jpg -> '26 HICHBORN sT'."""
    stem = Path(name.split("?")[0]).stem
    stem = re.sub(r"[_-]+", " ", stem)
    # split camel/number boundaries: 26HICHBORNsT -> 26 HICHBORN sT
    stem = re.sub(r"(?<=\d)(?=[A-Za-z])", " ", stem)
    stem = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", stem)
    return re.sub(r"\s+", " ", stem).strip()


def harvest(raw: str) -> dict:
    out = {"meta": {}, "images": [], "img_alts": [], "address_candidates": []}
    for k, v in META.findall(raw):
        out["meta"][k.lower()] = html.unescape(v)
    for src in IMG.findall(raw):
        if "/ads/" in src or "logo" in src.lower() or "golden_rectangle" in src:
            continue
        out["images"].append(src)
    out["img_alts"] = [html.unescape(a) for a in IMGALT.findall(raw)]

    blobs = list(out["meta"].values()) + out["img_alts"] + \
        [unslug(s) for s in out["images"]]
    seen = set()
    for b in blobs:
        for m in ADDR.finditer(b):
            a = re.sub(r"\s+", " ", m.group(0)).strip()
            if a.lower() not in seen:
                seen.add(a.lower())
                out["address_candidates"].append(a)
    return out


def show(url: str, raw: str):
    h = harvest(raw)
    log.info("\n%s", url)
    for k in ("og:title", "twitter:title", "twitter:alt", "og:image:alt",
              "og:description"):
        if h["meta"].get(k):
            log.info("   %-16s %s", k, h["meta"][k][:150])
    for s in h["images"][:3]:
        log.info("   %-16s %s   -> %r", "image", s.split("/")[-1][:44], unslug(s))
    if h["address_candidates"]:
        log.info("   %-16s %s", "ADDRESSES", ", ".join(h["address_candidates"][:5]))
    return h


def main(url: str, scan: bool):
    if scan:
        idx = json.loads(Path("data/trr_index.json").read_text(encoding="utf-8"))
        cache = Path("data/trr/art")
        cache.mkdir(parents=True, exist_ok=True)
        found = 0
        with httpx.Client(headers=UA, timeout=45, follow_redirects=True) as c:
            for row in idx:
                slug = row["url"].rstrip("/").split("/")[-1][:80]
                p = cache / f"{slug}.html"
                if not p.exists():
                    try:
                        r = c.get(row["url"])
                        if r.status_code != 200:
                            continue
                        p.write_text(r.text, encoding="utf-8")
                        time.sleep(1.0)
                    except Exception as e:
                        log.warning("%s -> %s", slug[:40], e)
                        continue
                h = harvest(p.read_text(encoding="utf-8", errors="ignore"))
                if h["address_candidates"]:
                    found += 1
                    log.info("%s  %-52s %s", row["date"], row["title"][:52],
                             ", ".join(h["address_candidates"][:3]))
        log.info("\n%d of %d articles yielded an address from metadata alone",
                 found, len(idx))
        return

    with httpx.Client(headers=UA, timeout=45, follow_redirects=True) as c:
        show(url, c.get(url).text)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("url", nargs="?")
    ap.add_argument("--scan", action="store_true")
    a = ap.parse_args()
    main(a.url, a.scan)
