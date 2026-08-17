r"""
Test whether Providence CPC staff reports carry gross SF where agendas do not.

Agenda summaries state GSF for only ~11% of development items. Before accepting
that as Providence's ceiling, this checks the deeper materials -- staff reports
and plan submissions -- which are published on providenceri.gov.

Finding: providenceri.gov is NOT behind a JavaScript challenge. Plain httpx gets
full HTML (the /planning/ page returns ~88 KB of real content). The meeting
materials ARCHIVE page is JS-rendered and yields no links to a plain fetch, but
the site is WordPress and its REST media API enumerates every uploaded PDF:

    /wp-json/wp/v2/media?search=<case number>&media_type=application

That returns staff reports directly, so no browser automation is required for
this source at all.

Never infers SF. Only records whether a document states it explicitly.

    python scraper/ri_pvd_enrichment_test.py --limit 10
"""

import re
import sys
import json
import time
import logging
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))

from scraper.ri_meeting_docs import pdf_text

logging.basicConfig(level=logging.INFO, format="%(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

MEDIA = "https://www.providenceri.gov/wp-json/wp/v2/media"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    )
}
CACHE = Path(__file__).parent.parent / "data" / "ri_pdfs" / "pvd_enrich"
CORPUS = Path(__file__).parent.parent / "data" / "ri_agenda_corpus.json"
TEXT_DIR = Path(__file__).parent.parent / "data" / "ri_pdfs" / "text"

# Only cases introduced as a numbered agenda item ("1. Case no 26-047MIL - ...").
# Matching bare case numbers anywhere sweeps in the Administrative Officer's
# run-on list of administrative approvals, which is dominated by MIS/A lot
# reconfigurations that involve no building and correctly carry no SF -- a
# sample drawn from there measures nothing about SF availability.
CASE_RE = re.compile(r"Case\s*no\.?\s*(\d{2}-\d{2,4}\s*[A-Z]{1,4})\b", re.I)

# Building-bearing case types. MA = Major Land Development, MIL = Minor Land
# Development, UDR = Unified Development Review. MIS (Minor Subdivision) and
# A (Administrative) are lot-line actions, excluded by design.
BUILDING_CASE = re.compile(r"(MA|MIL|UDR)$", re.I)
SF_RE = re.compile(
    r"([\d,]{3,})\s*(?:\+/-\s*)?(?:square\s*(?:feet|foot)|sq\.?\s*ft\.?|s\.?f\.?)\b", re.I)
UNIT_RE = re.compile(r"(\d{1,4})\s*(?:residential\s*|dwelling\s*)?units?\b", re.I)

# Documents that are forms/regulations rather than case materials
BOILERPLATE = re.compile(
    r"application\.pdf$|regulations|meeting-schedule|checklist|handbook", re.I)


def cases_from_corpus(limit: int) -> list[dict]:
    """Providence development items that have a case number, newest first."""
    corpus = json.loads(CORPUS.read_text(encoding="utf-8"))
    seen, out = set(), []
    rows = sorted(
        (v for v in corpus.values() if v["municipality"] == "Providence"),
        key=lambda v: v["date"], reverse=True,
    )
    for v in rows:
        for d in v["documents"]:
            if d["kind"] != "agenda":
                continue
            p = TEXT_DIR / d["text_file"]
            if not p.exists():
                continue
            text = p.read_text(encoding="utf-8", errors="replace")
            for m in CASE_RE.finditer(text):
                case = m.group(1).replace(" ", "")
                if case in seen or not BUILDING_CASE.search(case):
                    continue
                seg = text[m.start(): m.start() + 700]
                # Only development items, not ordinance referrals
                if not re.search(r"construct|units?|land development|"
                                 r"mixed[\s-]use|building", seg, re.I):
                    continue
                seen.add(case)
                out.append({
                    "case": case,
                    "agenda_has_sf": bool(SF_RE.search(seg)),
                    "agenda_has_units": bool(UNIT_RE.search(seg)),
                    "snippet": re.sub(r"\s+", " ", seg[:150]),
                })
                if len(out) >= limit:
                    return out
    return out


def find_docs(client: httpx.Client, case: str) -> list[str]:
    """Staff reports / plan submissions for a case, via the WP media API."""
    urls = []
    # Case numbers appear both as "26-047MIL" and "26-047-MIL" in filenames
    variants = {case, re.sub(r"(\d)([A-Z])", r"\1-\2", case)}
    for v in variants:
        try:
            r = client.get(MEDIA, params={"search": v, "per_page": 20,
                                          "media_type": "application"}, timeout=45)
            if r.status_code != 200:
                continue
            for m in r.json():
                u = m.get("source_url", "")
                if u.lower().endswith(".pdf") and not BOILERPLATE.search(u) and u not in urls:
                    urls.append(u)
        except Exception as exc:
            log.warning("  media search failed for %s: %s", v, exc)
        time.sleep(0.4)
    return urls


def main(limit: int):
    CACHE.mkdir(parents=True, exist_ok=True)
    cases = cases_from_corpus(limit)
    log.info("Testing %d Providence development cases\n", len(cases))

    agenda_sf = enriched_sf = with_docs = 0

    with httpx.Client(headers=HEADERS, follow_redirects=True) as client:
        for i, c in enumerate(cases, 1):
            urls = find_docs(client, c["case"])
            found_sf, sf_vals, best = False, [], None

            for u in urls[:4]:
                fn = CACHE / (u.rsplit("/", 1)[-1])
                if not fn.exists():
                    try:
                        rr = client.get(u, timeout=120)
                        if rr.status_code != 200:
                            continue
                        fn.write_bytes(rr.content)
                    except Exception:
                        continue
                    time.sleep(0.5)
                try:
                    txt = pdf_text(fn, max_pages=25)
                except Exception:
                    continue
                hits = SF_RE.findall(txt)
                if hits:
                    found_sf = True
                    sf_vals.extend(hits[:4])
                    best = fn.name

            if urls:
                with_docs += 1
            if c["agenda_has_sf"]:
                agenda_sf += 1
            if c["agenda_has_sf"] or found_sf:
                enriched_sf += 1

            log.info("[%2d] %-12s docs=%-2d agenda_sf=%-5s report_sf=%-5s %s",
                     i, c["case"], len(urls), c["agenda_has_sf"], found_sf,
                     (", ".join(sf_vals[:3]) + f"  <- {best}") if found_sf else "")

    n = len(cases) or 1
    log.info("\n=== PROVIDENCE SF COVERAGE TEST (n=%d) ===", len(cases))
    log.info("  cases with any published doc : %d (%.0f%%)", with_docs, 100 * with_docs / n)
    log.info("  SF from agenda alone         : %d (%.0f%%)", agenda_sf, 100 * agenda_sf / n)
    log.info("  SF from agenda + staff report: %d (%.0f%%)", enriched_sf, 100 * enriched_sf / n)
    log.info("  delta                        : +%.0f pts", 100 * (enriched_sf - agenda_sf) / n)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=10)
    a = ap.parse_args()
    main(a.limit)
