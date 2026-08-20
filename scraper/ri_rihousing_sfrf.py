r"""Harvest RIHousing's State Fiscal Recovery Funds development pages.

Searching one project at a time is the wrong shape for this job. RIHousing
publishes a page per funded development carrying exactly the fields a TARGET
needs -- "Anticipated Completion Date: Q3 2027" -- from the state housing
finance agency rather than from a listing site, and a "Last Updated" stamp that
gives every forecast on the page its vintage.

Much of the Rhode Island pipeline is affordable or mixed-income, so one crawl
answers what would otherwise be dozens of searches, and answers it better: a
quarter stated by the agency financing the construction beats a sentence in a
press release, and it arrives with a construction percentage beside it.

Matching to the tracker is NOT done here. This only harvests; attaching a
development to a row is a separate decision made in ri_sfrf_match.py, where a
wrong match can be seen and argued with.

    python scraper/ri_rihousing_sfrf.py
"""

import json
import re
import time
from pathlib import Path

import httpx

INDEX = "https://rihousing.com/sfrf"
OUT = Path("data/ri_rihousing_sfrf.json")
UA = {"User-Agent": "Mozilla/5.0 (compatible; boston-re-tracker/1.0)"}

# The fields as the pages label them, one pattern each rather than one big
# regex, so a page that renames a label fails visibly on that field instead of
# silently returning nothing for the whole record.
FIELDS = {
    "developer":        r"Developer:\s*([^\n]{2,80})",
    "units_total":      r"Total\s+Units:\s*([^\n]{1,30})",
    "units_affordable": r"Total\s+Affordable\s+Units:\s*([^\n]{1,30})",
    "affordability":    r"Affordability:\s*([^\n]{1,40})",
    "completion":       r"Anticipated\s+Completion\s+Date:\s*([^\n]{2,40})",
    "status":           r"Construction\s+Status:\s*([^\n]{1,40})",
    "groundbreaking":   r"Groundbreaking\s+([\d.]{6,10})",
}


def _text(html: str) -> str:
    """Strip tags but keep line structure, so the label/value regexes work."""
    html = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html, flags=re.S | re.I)
    html = re.sub(r"<br\s*/?>|</p>|</div>|</li>|</td>|</tr>|</h\d>", "\n",
                  html, flags=re.I)
    html = re.sub(r"<[^>]+>", " ", html)
    html = (html.replace("&#8217;", "'").replace("&#8211;", "-")
                .replace("&amp;", "&").replace("&nbsp;", " ").replace("\xa0", " "))
    return re.sub(r"[ \t]+", " ", html)


def main():
    client = httpx.Client(headers=UA, timeout=30, follow_redirects=True)
    idx = client.get(INDEX)
    idx.raise_for_status()
    # The index links are site-relative, so absolutise rather than requiring a
    # scheme -- matching only on https:// found nothing at all.
    links = sorted({("https://rihousing.com" + h) if h.startswith("/") else h
                    for h in re.findall(r'href="([^"]*?/sfrf-development-[^"]+?)"',
                                        idx.text)})
    print(f"{len(links)} development pages linked from {INDEX}")

    out = []
    for i, url in enumerate(links, 1):
        try:
            r = client.get(url)
            r.raise_for_status()
        except Exception as exc:
            print(f"  [{i}/{len(links)}] {url} -> {exc}")
            continue
        t = _text(r.text)
        m = re.search(r"SFRF Development\s*[-–]\s*([^\n|]{3,90})", t)
        rec = {"url": url, "name": m.group(1).strip() if m else ""}
        for key, pat in FIELDS.items():
            mm = re.search(pat, t, re.I)
            rec[key] = mm.group(1).strip() if mm else None
        # "Last Updated: 06.23.26" -- the vintage of every forecast on the page.
        mm = re.search(r"Last\s+Updated:\s*(\d{2})\.(\d{2})\.(\d{2})", t, re.I)
        rec["page_updated"] = (f"20{mm.group(3)}-{mm.group(1)}-{mm.group(2)}"
                               if mm else None)
        out.append(rec)
        print(f"  [{i}/{len(links)}] {rec['name'][:42]:<44} "
              f"{str(rec['completion'])[:10]:<12} {str(rec['status'])[:10]:<12} "
              f"{str(rec['developer'])[:30]}")
        time.sleep(0.4)

    OUT.write_text(json.dumps(out, indent=1, ensure_ascii=False), encoding="utf-8")
    dated = sum(1 for r in out if r.get("completion"))
    print(f"\n{len(out)} developments, {dated} carry an anticipated completion date")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
