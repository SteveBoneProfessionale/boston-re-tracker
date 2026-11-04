"""Read the named trade press for the South Florida fields no record states.

The government sources in this market give a project's identity, program and
construction status and give nothing at all for four of the fields the brief
asks for: Florida YIMBY, The Real Deal, Commercial Observer, Bisnow, the South
Florida Business Journal and the Coral Gables Gazette are where a developer, an
architect, a construction lender and a building height are actually published.

Florida YIMBY runs WordPress with its REST API open, which makes it the one
press source readable as data rather than as scraped HTML -- posts come back
as JSON with a date, a canonical link and the full body, and the site files
every article under a per-project category ("1-alhambra-plaza"). So it is the
primary press source here and the others are read by hand where it is silent.

WHAT THIS MODULE DOES NOT DO
It does not decide anything. It returns the sentences that mention a project
team, a program figure or a loan, each with its article URL and date, and the
judgement about what the sentence actually says is made by the person or
process reading the output. An extractor that guessed a developer from
"...which neighbors a Related Group tower..." is exactly the failure mode the
field rules are written to prevent.
"""

import html
import re
import time
import urllib.parse

from scraper import sofl_common as sc

FY_API = "https://floridayimby.com/wp-json/wp/v2"

# Sentences worth returning, by what they can establish. The patterns are
# deliberately loose -- this is a recall filter whose output a reader checks,
# not a precision parser whose output is stored.
CUES = {
    "developer": re.compile(
        r"\b(develop(?:er|ed|ing|ment)\s+(?:by|is|are)?|developer[s]?[,:]?|"
        r"is being developed|being developed by|a joint venture|"
        r"applicant|owner of record|sponsor(?:ed by)?)\b", re.I),
    "architect": re.compile(
        r"\b(architect(?:ure|s|ural)?\s*(?:of record)?|designed by|design(?:ed)? "
        r"architect|architect of record)\b", re.I),
    "contractor": re.compile(
        r"\b(general contractor|contractor|construction manager|"
        r"build(?:er|ing) contractor)\b", re.I),
    "civil_engineer": re.compile(r"\bcivil engineer(?:ing)?\b", re.I),
    "lender": re.compile(
        r"\b(construction loan|construction financing|loan|lender|financing|"
        r"mortgage|provided a|refinanc)\b", re.I),
    "units": re.compile(r"\b([\d,]{2,7})\s*(?:residential\s+|rental\s+|condo\s+)?"
                        r"(?:units|apartments|residences|condominiums|keys|rooms)\b", re.I),
    "height": re.compile(r"\b([\d,]{2,6})\s*(?:feet|foot|ft\.?)\b|"
                         r"\b([\d]{1,3})[- ]stor(?:y|ies|ey|eys)\b", re.I),
    "status": re.compile(
        r"\b(topp(?:ed|ing) (?:off|out)|broke ground|groundbreaking|"
        r"under construction|vertical construction|completed|complete[d]?|"
        r"certificate of occupancy|now leasing|closings|delivered|"
        r"approved|denied|withdrawn|rejected)\b", re.I),
}

_SENT_SPLIT = re.compile(r"(?<=[.!?])\s+(?=[A-Z0-9“\"'])")


def _clean(text: str) -> str:
    text = re.sub(r"<script.*?</script>|<style.*?</style>", " ", text or "",
                  flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def fy_search(query: str, per_page=8, sess=None) -> list[dict]:
    """Florida YIMBY posts matching a free-text query, newest first."""
    s = sess or sc.session()
    url = (f"{FY_API}/posts?search={urllib.parse.quote(query)}"
           f"&per_page={per_page}&orderby=date&order=desc"
           f"&_fields=date,link,title,content")
    for attempt in range(3):
        try:
            data = s.get(url, timeout=60).json()
            break
        except Exception:
            time.sleep(2 + 2 * attempt)
    else:
        return []
    if not isinstance(data, list):
        return []
    out = []
    for post in data:
        out.append({
            "date": (post.get("date") or "")[:10],
            "url": post.get("link") or "",
            "title": _clean((post.get("title") or {}).get("rendered", "")),
            "text": _clean((post.get("content") or {}).get("rendered", "")),
        })
    return out


def cue_sentences(text: str, fields=None, address_terms=()) -> dict:
    """Sentences that could establish each requested field.

    `address_terms` is used only to mark which sentences name the project's own
    address, because the single biggest source of a wrong developer name is a
    sentence about the building next door.
    """
    wanted = fields or list(CUES)
    sentences = _SENT_SPLIT.split(text)
    out = {f: [] for f in wanted}
    for sent in sentences:
        s_low = sent.lower()
        on_address = any(t.lower() in s_low for t in address_terms if t)
        for field in wanted:
            if CUES[field].search(sent):
                out[field].append({"sentence": sent.strip()[:400],
                                   "names_address": on_address})
    return {k: v for k, v in out.items() if v}


def research(query: str, address_terms=(), fields=None, per_page=6,
             max_sentences=4, sess=None) -> list[dict]:
    """Press evidence for one project, as sentences with their citation.

    Returns one entry per article, each holding only the cue sentences, so the
    caller reads a few hundred characters per article rather than the article.
    """
    results = []
    for post in fy_search(query, per_page=per_page, sess=sess):
        cues = cue_sentences(post["text"], fields=fields,
                             address_terms=address_terms)
        if not cues:
            continue
        results.append({
            "source": "Florida YIMBY",
            "date": post["date"],
            "url": post["url"],
            "title": post["title"],
            "cues": {f: v[:max_sentences] for f, v in cues.items()},
        })
    return results


def fy_category_posts(slug: str, per_page=30, sess=None) -> list[dict]:
    """Every Florida YIMBY post filed under one project category.

    The site files articles under a category per project, which is the closest
    thing in this market to a project page: it collects the proposal, the
    approval, the loan and the topping-out under one slug.
    """
    s = sess or sc.session()
    try:
        cats = s.get(f"{FY_API}/categories?slug={urllib.parse.quote(slug)}"
                     f"&_fields=id,slug,name,count", timeout=60).json()
    except Exception:
        return []
    if not isinstance(cats, list) or not cats:
        return []
    cid = cats[0]["id"]
    try:
        posts = s.get(f"{FY_API}/posts?categories={cid}&per_page={per_page}"
                      f"&orderby=date&order=desc&_fields=date,link,title,content",
                      timeout=60).json()
    except Exception:
        return []
    if not isinstance(posts, list):
        return []
    return [{"date": (p.get("date") or "")[:10], "url": p.get("link") or "",
             "title": _clean((p.get("title") or {}).get("rendered", "")),
             "text": _clean((p.get("content") or {}).get("rendered", ""))}
            for p in posts]
