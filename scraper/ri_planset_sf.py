r"""
Building square footage from the plan sets the board actually reviewed.

The agendas do not state building size -- 73% carry no square-foot figure at
all, and where one appears it is almost always lot area. The plan set and its
staff report DO state it, they are the same document the commission read, and
Providence hosts them publicly. That makes them the right source: they
describe the PROPOSED programme, unlike an assessor record or an energy
benchmarking file, both of which describe what is standing today.

RULES, unchanged from the agenda pass and enforced here in code:

  * GSF only where the document STATES it. Never derived from unit count,
    footprint, floor count or acreage.
  * Every figure stores its source document, page number and document date.
  * A plan set may be a REVISION, and revisions change the programme. Figures
    are taken from the most recent set; where an earlier set disagrees, the
    later figure wins and the earlier one is recorded rather than discarded.
  * The same classifier the agenda pass uses decides whether a figure is a
    building or a lot -- a plan set is, if anything, denser in lot areas,
    setbacks and parking dimensions than an agenda is.

    python scraper/ri_planset_sf.py --dry-run
"""

import re
import sys
import json
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pymupdf

from scraper.ri_sf_extract import candidates_for, building_sf

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

CACHE = Path(__file__).parent.parent / "data" / "ri_plansets"
OUT = Path(__file__).parent.parent / "data" / "ri_planset_sf.json"
CACHE.mkdir(parents=True, exist_ok=True)

# Document date, for deciding which of two revisions is current. Preference
# order: an explicit revision date in the text, then the date in the URL path
# Providence uses (/wp-content/uploads/2023/05/...).
URL_DATE = re.compile(r"/uploads/(\d{4})/(\d{2})/")
REV_DATE = re.compile(
    r"\b(?:rev(?:ised|ision)?|dated|issue[d]?)\s*:?\s*"
    r"(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|[A-Z][a-z]+\s+\d{1,2},?\s+\d{4})", re.I)



# Consultant studies argue from OTHER buildings. 50 Branch Avenue's parking
# analysis offered a 113,187 sq ft facility in Newtonville and a 117,500 sq ft
# one in Waltham -- neither in Rhode Island, both one step from being written
# as this project's floor area. No whitelist of town names can win that; the
# document type is the reliable signal.
SKIP_DOC = re.compile(
    r"parking[-_ ]?(?:analysis|study)|traffic|trip[-_ ]?generation|"
    r"peer[-_ ]?review|drainage|stormwater|utility|environmental", re.I)

# A floor-by-floor schedule that states its own scope is not a building total.
PARTIAL_TOTAL = re.compile(
    r"based\s+on\s+[^.]{0,40}\s+only|residential\s+floor\s+space\s+only|"
    r"excludes?\s+", re.I)

def fetch(url):
    """Download once, cache on disk. Returns the local path or None."""
    name = re.sub(r"[^A-Za-z0-9._-]", "_", url.rsplit("/", 1)[-1])[:120]
    dest = CACHE / name
    if dest.exists() and dest.stat().st_size > 1000:
        return dest
    try:
        import urllib.request
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=90) as r:
            data = r.read()
        if not data.startswith(b"%PDF"):
            log.warning("    not a PDF: %s", url[:90])
            return None
        dest.write_bytes(data)
        return dest
    except Exception as e:                                  # noqa: BLE001
        log.warning("    fetch failed (%s): %s", type(e).__name__, url[:90])
        return None


def doc_date(url, text):
    m = REV_DATE.search(text[:4000])
    if m:
        return m.group(1)
    m = URL_DATE.search(url)
    return f"{m.group(1)}-{m.group(2)}" if m else ""


def scan(path, url):
    """Every building-area figure in the document, with its page."""
    try:
        doc = pymupdf.open(path)
    except Exception as e:                                  # noqa: BLE001
        log.warning("    unreadable PDF (%s)", type(e).__name__)
        return [], "", 0
    hits, alltext = [], []
    for i, page in enumerate(doc, start=1):
        t = page.get_text()
        if not t.strip():
            continue
        alltext.append(t)
        if PARTIAL_TOTAL.search(t):
            continue
        sf, ev = building_sf(candidates_for(t))
        if sf:
            hits.append({"sf": sf, "page": i, "quote": ev[0]["quote"][:240]})
    joined = "\n".join(alltext)
    return hits, doc_date(url, joined), doc.page_count


def best_figure(hits):
    """The figure to use, and the reason.

    Several different building figures in one set usually means floor-by-floor
    areas plus a total. The largest is taken as the total; summing them would
    be deriving a number the document never stated.
    """
    if not hits:
        return None, None
    top = max(hits, key=lambda h: h["sf"])
    return top, sorted({h["sf"] for h in hits}, reverse=True)


def run(targets, dry_run=False):
    results = []
    for t in targets:
        log.info("  id=%-4d %s", t["id"], t["label"])
        per_doc = []
        for d in t["docs"]:
            if SKIP_DOC.search(d["url"].rsplit("/", 1)[-1]):
                log.info("      %-58s SKIPPED: consultant study, cites other buildings",
                         d["url"].rsplit("/", 1)[-1][:58])
                continue
            path = fetch(d["url"])
            if not path:
                continue
            hits, ddate, pages = scan(path, d["url"])
            log.info("      %-58s %2d pages, %d figure(s)%s",
                     d["url"].rsplit("/", 1)[-1][:58], pages, len(hits),
                     f", dated {ddate}" if ddate else "")
            if hits:
                top, allvals = best_figure(hits)
                per_doc.append({"url": d["url"], "kind": d.get("kind", ""),
                                "date": ddate, "sf": top["sf"], "page": top["page"],
                                "quote": top["quote"], "all_values": allvals})
        if not per_doc:
            results.append({"id": t["id"], "label": t["label"], "sf": None,
                            "reason": "no building-area figure stated in any plan set"})
            continue
        # Revisions: the most recent document wins; earlier ones are recorded.
        per_doc.sort(key=lambda d: d["date"], reverse=True)
        current, earlier = per_doc[0], per_doc[1:]
        disagree = [e for e in earlier if e["sf"] != current["sf"]]
        results.append({
            "id": t["id"], "label": t["label"], "sf": current["sf"],
            "page": current["page"], "url": current["url"], "date": current["date"],
            "quote": current["quote"],
            "superseded": [{"sf": e["sf"], "date": e["date"], "url": e["url"]}
                           for e in disagree],
        })
        log.info("      -> %s sf (page %s, %s)%s", f'{current["sf"]:,}',
                 current["page"], current["date"] or "undated",
                 "  SUPERSEDES " + ", ".join(f'{e["sf"]:,} ({e["date"]})' for e in disagree)
                 if disagree else "")
    OUT.write_text(json.dumps(results, indent=1, ensure_ascii=False), encoding="utf-8")
    return results
