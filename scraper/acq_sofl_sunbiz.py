r"""Read Florida Division of Corporations records for a named entity.

Sunbiz is the state's own public register and the second source in the brief's
order: look up the LLC, read its managers or authorized persons, its principal
address and its mailing address, and follow a chain of LLCs upward until an
operating company appears.

WHY THIS SHELLS OUT TO CURL. Sunbiz sits behind Cloudflare, and the challenge
is inconsistent across clients: urllib is refused 403 on the very first page
while curl, sending an ordinary browser User-Agent and carrying a cookie jar,
is served normally. Nothing here solves a challenge, replays a token or
pretends to be a logged-in user -- it is a plain anonymous GET with the headers
a browser sends, and when a page DOES come back challenged the fetch raises
Blocked and the caller stops. That is the line: behave like a browser, never
defeat a block.

THE TWO-STEP IS FORCED BY THE SITE. A detail page is addressed by an
aggregateId that only the search results page knows, so every lookup is a
search followed by a fetch of the matching row. Both are cached on disk, so a
re-run costs nothing and the register is asked once per entity.

WHAT COMES BACK. Document number, FEI, filing date, home state, status, the
principal and mailing addresses, the registered agent, and the authorized
persons or managers with their own addresses. Those are the raw signals; the
judgement about which of them identifies a firm is made in
acq_sofl_resolve.py, not here.
"""

import json
import os
import re
import subprocess
import sys
import time
import urllib.parse

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE = os.path.join(ROOT, "data", "cache", "sunbiz")

BASE = "https://search.sunbiz.org"
SEARCH = (BASE + "/Inquiry/CorporationSearch/SearchResults"
          "?inquiryType=EntityName&inquiryDirectionType=Initial"
          "&searchNameOrder={key}&searchTerm={term}")
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
JAR = os.path.join(CACHE, "cookies.txt")
# Slow on purpose. The register began challenging NEW sessions partway through
# this work while an already-warm one kept being served, so the cost of being
# impatient is not a slow run, it is losing access entirely. Every call is also
# cached, so this pause is paid once per entity for the life of the repository.
PAUSE = 3.5


class Blocked(RuntimeError):
    """The register answered with a challenge instead of a record."""


def _norm(name):
    """Sunbiz's own search key: letters and digits only, upper case."""
    return re.sub(r"[^A-Z0-9]", "", str(name or "").upper())


def _cache_path(kind, key):
    os.makedirs(CACHE, exist_ok=True)
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", key)[:120]
    return os.path.join(CACHE, f"{kind}_{safe}.html")


# Set once the register challenges us. Every later fetch that is not already
# on disk gives up immediately instead of spending a 45-second timeout
# rediscovering the same block, and nothing retries behind it -- a block is an
# answer, not a transient.
BLOCKED_ONCE = False


def _get(url, referer=None):
    global BLOCKED_ONCE
    if BLOCKED_ONCE:
        raise Blocked("Sunbiz already refused this session; not retrying")
    os.makedirs(CACHE, exist_ok=True)
    cmd = ["curl", "-s", "--max-time", "45", "-A", UA,
           "-c", JAR, "-b", JAR, "-L", url]
    if referer:
        cmd[1:1] = ["-H", f"Referer: {referer}"]
    out = subprocess.run(cmd, capture_output=True, timeout=120)
    html = out.stdout.decode("utf-8", "replace")
    if "just a moment" in html.lower() or "enable javascript and cookies" in html.lower():
        BLOCKED_ONCE = True
        raise Blocked("Sunbiz returned a Cloudflare challenge")
    return html


def _cached_get(kind, key, url, referer=None, refresh=False):
    path = _cache_path(kind, key)
    if not refresh and os.path.exists(path):
        with open(path, encoding="utf-8") as fh:
            return fh.read()
    html = _get(url, referer)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(html)
    time.sleep(PAUSE)
    return html


def _strip(html):
    text = re.sub(r"<br\s*/?>", "\n", html)
    text = re.sub(r"<[^>]+>", "", text)
    text = text.replace("&nbsp;", " ").replace("&amp;", "&")
    return "\n".join(l.strip() for l in text.split("\n") if l.strip())


def search(name, refresh=False):
    """[(entity name, detail href)] for a name, best match first."""
    key = _norm(name)
    if not key:
        return []
    url = SEARCH.format(key=key, term=urllib.parse.quote(str(name)))
    html = _cached_get("search", key, url, refresh=refresh)
    out = []
    for m in re.finditer(
            r'<a href="(/Inquiry/CorporationSearch/SearchResultDetail\?[^"]+)"[^>]*>(.*?)</a>',
            html, re.S):
        out.append((_strip(m.group(2)), m.group(1).replace("&amp;", "&")))
    return out


def detail(href, refresh=False):
    """The parsed register record behind a search result."""
    key = _norm(href.split("searchNameOrder=")[-1][:80])
    html = _cached_get("detail", key, BASE + href,
                       referer=BASE + "/Inquiry/CorporationSearch/SearchResults",
                       refresh=refresh)
    return parse(html)


def parse(html):
    """Pull the register's fields out of a detail page.

    Parsed from the page's own structure -- labelled detailSection blocks and
    `<label for="Detail_X">` pairs -- rather than by walking flattened text,
    because the flattened form loses which address belongs to which heading
    the moment a record has no mailing address.
    """
    rec = {"name": None, "entity_type": None, "document_number": None,
           "fei": None, "date_filed": None, "state": None, "status": None,
           "principal_address": None, "mailing_address": None,
           "registered_agent": None, "persons": []}

    m = re.search(r'<div class="detailSection corporationName">(.*?)</div>',
                  html, re.S)
    if m:
        lines = _strip(m.group(1)).split("\n")
        if lines:
            rec["entity_type"] = lines[0]
        if len(lines) > 1:
            rec["name"] = lines[1]

    for key, label in (("document_number", "Detail_DocumentId"),
                       ("fei", "Detail_FeiEinNumber"),
                       ("date_filed", "Detail_FileDate"),
                       ("state", "Detail_EntityStateCountry"),
                       ("status", "Detail_Status")):
        m = re.search(r'<label for="' + label + r'">[^<]*</label>\s*<span>(.*?)</span>',
                      html, re.S)
        if m:
            rec[key] = _strip(m.group(1)) or None

    for m in re.finditer(r'<div class="detailSection">\s*<span>(.*?)</span>(.*?)</div>',
                         html, re.S):
        head = _strip(m.group(1)).rstrip(":")
        body = _strip(m.group(2))
        if head.startswith("Principal Address"):
            rec["principal_address"] = body or None
        elif head.startswith("Mailing Address"):
            rec["mailing_address"] = body or None
        elif head.startswith("Registered Agent"):
            rec["registered_agent"] = body or None

    # Authorized persons and managers. The block repeats name, title and
    # address per person; titles arrive as "Title AP" or "Title MGR".
    m = re.search(r'(Authorized Person\(s\) Detail|Manager/Member Detail|'
                  r'Officer/Director Detail)(.*?)(<div class="detailSection">\s*<span>'
                  r'Annual Reports|$)', html, re.S)
    if m:
        chunk = _strip(m.group(2))
        cur = None
        for line in chunk.split("\n"):
            if line.startswith("Name & Address"):
                continue
            t = re.match(r"^Title\s+(.+)$", line)
            if t:
                if cur:
                    rec["persons"].append(cur)
                cur = {"title": t.group(1).strip(), "name": None, "address": []}
                continue
            if cur is None:
                continue
            if cur["name"] is None:
                cur["name"] = line
            else:
                cur["address"].append(line)
        if cur:
            rec["persons"].append(cur)
    for p in rec["persons"]:
        p["address"] = ", ".join(p["address"]) or None
    return rec


def lookup(name, refresh=False):
    """The register record for an entity name, or None if it is not found.

    Matching is EXACT on the normalised name. Sunbiz search is a prefix walk
    and returns neighbours alphabetically, so the first row is frequently a
    different company that merely sorts nearby -- accepting it would attribute
    a deal to whoever happens to file next to the buyer, which is precisely the
    guessing the brief forbids.
    """
    want = _norm(name)
    rows = search(name, refresh=refresh)
    for label, href in rows:
        if _norm(label) == want:
            rec = detail(href, refresh=refresh)
            rec["sunbiz_url"] = BASE + href
            return rec
    # Sunbiz drops the comma in "545 WYN OWNER, LLC" when normalising, so an
    # exact match on the normalised form already covers punctuation. A second
    # pass allows only the suffix to differ, which is how the same company
    # appears as LLC and L.L.C.
    base = re.sub(r"(LLC|LC|INC|LP|LLP|LTD|CORP|CO|PA|PLLC)$", "", want)
    if len(base) >= 8:
        for label, href in rows:
            if re.sub(r"(LLC|LC|INC|LP|LLP|LTD|CORP|CO|PA|PLLC)$", "",
                      _norm(label)) == base:
                rec = detail(href, refresh=refresh)
                rec["sunbiz_url"] = BASE + href
                return rec
    return None


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    for arg in sys.argv[1:]:
        print(json.dumps(lookup(arg), indent=1))
