r"""Harvest the I-195 Redevelopment District's project pages.

The district owns the land under a cluster of the largest Providence projects
in the tracker and publishes a page per parcel with a "Completion" field. Like
the RIHousing crawl this is a bulk source: the landowner's own record, one
fetch per project instead of a search per project, and the same field for
every one of them so the values are comparable.

A completion year here is a forecast while a project is under construction and
a statement of fact once it is finished. The page does not say which, so this
only harvests; deciding whether a value is a TARGET or a DELIVERED is done
against the tracker's own stage in ri_195_match.py.

    python scraper/ri_195district.py
"""

import json
import re
import time
from pathlib import Path

import httpx

INDEX = "https://www.195district.com/projects/"
OUT = Path("data/ri_195district.json")
UA = {"User-Agent": "Mozilla/5.0 (compatible; boston-re-tracker/1.0)"}

FIELDS = {
    "developer":  r"Developer[:\s]*\n?\s*([^\n]{2,80})",
    "completion": r"Completion[:\s]*\n?\s*([^\n]{2,40})",
    "units":      r"(?:Total\s+)?Units[:\s]*\n?\s*([^\n]{1,30})",
    "parcel":     r"Parcel[:\s]*\n?\s*([^\n]{1,40})",
    "size":       r"(?:Size|Square\s+Feet|SF)[:\s]*\n?\s*([^\n]{1,40})",
    "status":     r"Status[:\s]*\n?\s*([^\n]{2,40})",
}


def _text(html: str) -> str:
    html = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html, flags=re.S | re.I)
    html = re.sub(r"<br\s*/?>|</p>|</div>|</li>|</td>|</tr>|</h\d>|</span>", "\n",
                  html, flags=re.I)
    html = re.sub(r"<[^>]+>", " ", html)
    html = (html.replace("&#8217;", "'").replace("&#8211;", "-")
                .replace("&amp;", "&").replace("&nbsp;", " ").replace("\xa0", " "))
    return re.sub(r"[ \t]+", " ", html)


def main():
    client = httpx.Client(headers=UA, timeout=30, follow_redirects=True)
    idx = client.get(INDEX)
    idx.raise_for_status()
    links = sorted({("https://www.195district.com" + h) if h.startswith("/") else h
                    for h in re.findall(r'href="([^"]*?/projects/[^"#?]+?)"', idx.text)
                    if h.rstrip("/").rsplit("/", 1)[-1] not in ("projects", "")})
    print(f"{len(links)} project pages linked from {INDEX}")

    out = []
    for i, url in enumerate(links, 1):
        try:
            r = client.get(url)
            r.raise_for_status()
        except Exception as exc:
            print(f"  [{i}/{len(links)}] {url} -> {exc}")
            continue
        t = _text(r.text)
        m = re.search(r"<title>([^<|]+)", r.text)
        rec = {"url": url, "name": (m.group(1).strip() if m else
                                    url.rstrip("/").rsplit("/", 1)[-1])}
        for key, pat in FIELDS.items():
            mm = re.search(pat, t, re.I)
            rec[key] = mm.group(1).strip() if mm else None
        out.append(rec)
        print(f"  [{i}/{len(links)}] {rec['name'][:40]:<42} "
              f"{str(rec['completion'])[:12]:<14} {str(rec['developer'])[:30]}")
        time.sleep(0.4)

    OUT.write_text(json.dumps(out, indent=1, ensure_ascii=False), encoding="utf-8")
    dated = sum(1 for r in out if r.get("completion"))
    print(f"\n{len(out)} projects, {dated} carry a completion value")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
