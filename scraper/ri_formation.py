r"""
Formation date and filing history for RI registry entities (shell rule 5).

An entity organised shortly before the filing it appears on, with nothing else
in its name, was created to hold that one project. That is rule 5 of
scraper/ri_shell.py, and it needs two facts the search results do not carry:
the date of organization and whether the entity has any other filings.

Both live on the CorpSummary page:

    Date of Organization in Rhode Island:  04-14-2022

Results are cached permanently in data/ri_corp_cache.json under "formation",
keyed by FEIN, so a resolved entity is never re-queried -- the same rule the
rest of the registry work follows.
"""

import re
import sys
import time
import logging
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))

from scraper.ri_corp_registry import HEADERS, load_cache, save_cache

log = logging.getLogger(__name__)

SUMMARY = ("https://business.sos.ri.gov/CorpWeb/CorpSearch/CorpSummary.aspx"
           "?FEIN={fein}&SEARCH_TYPE=1")

_ORG_DATE = re.compile(
    r"Date of Organization[^<]*?:?\s*(?:<[^>]+>\s*)*(\d{2}-\d{2}-\d{4})", re.I | re.S)
_ANY_DATE = re.compile(r"\b(\d{2})-(\d{2})-(\d{4})\b")
# Filings that show an entity has a life beyond its formation.
_OTHER_FILING = re.compile(
    r"\b(Annual Report|Amendment|Merger|Conversion|Reinstatement|"
    r"Change of Agent|Fictitious Business Name)\b", re.I)


def _iso(mmddyyyy: str) -> str | None:
    m = _ANY_DATE.search(mmddyyyy or "")
    if not m:
        return None
    mm, dd, yy = m.groups()
    return f"{yy}-{mm}-{dd}"


def formation_of(client: httpx.Client, fein: str, cache: dict) -> dict:
    """{'formed': ISO date or None, 'other_filings': int or None} for an entity."""
    cache.setdefault("formation", {})
    key = str(fein or "").strip()
    if not key:
        return {"formed": None, "other_filings": None}
    if key in cache["formation"]:
        return cache["formation"][key]

    rec = {"formed": None, "other_filings": None}
    try:
        r = client.get(SUMMARY.format(fein=key), timeout=45)
        if r.status_code == 429:
            time.sleep(4)
            r = client.get(SUMMARY.format(fein=key), timeout=45)
        html = r.text
        m = _ORG_DATE.search(html)
        if m:
            rec["formed"] = _iso(m.group(1))
        else:
            # Fall back to the earliest date shown, which is the organization
            # date on every summary page seen so far.
            dates = sorted(filter(None, (_iso(d) for d in
                                         set(_ANY_DATE.findall(html) and
                                             re.findall(r"\d{2}-\d{2}-\d{4}", html)))))
            rec["formed"] = dates[0] if dates else None
        rec["other_filings"] = len(_OTHER_FILING.findall(html))
    except Exception as exc:
        log.debug("formation lookup failed for %s: %s", key, exc)

    cache["formation"][key] = rec
    return rec


def enrich(feins: list[str], cache: dict | None = None) -> dict:
    """Fetch formation records for a bounded set of entities, politely."""
    cache = cache if cache is not None else load_cache()
    out = {}
    with httpx.Client(headers=HEADERS, follow_redirects=True) as client:
        for i, f in enumerate(feins, 1):
            out[f] = formation_of(client, f, cache)
            if f not in cache.get("formation", {}) or i % 10 == 0:
                save_cache(cache)
            time.sleep(1.2)          # rate limit, per the portal rules
    save_cache(cache)
    return out
