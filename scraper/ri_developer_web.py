r"""
Tier 2 developer resolution: web research, corroborated across sources.

Tier order for a shell applicant:
  1. RI Corporate Database (scraper/ri_corp_registry.py) -> registry_confirmed
  2. This module, web research                           -> web_corroborated
  3. Null

Tier 2 runs in two stages, cheapest first:
  2a. The local news corpus (news_items). Curated, locally reported, already
      on disk. RI trade press was added to news_fetcher.FEEDS for this --
      Providence Business News, PBN Real Estate, Rhode Island Current, plus
      Banker & Tradesman which already covered New England.
  2b. Claude's server-side web_search tool via the Anthropic API. No separate
      search API key is needed, so this runs unattended.

THE RULES ARE ENFORCED IN CODE, NOT ONLY IN THE PROMPT
------------------------------------------------------
A model asked to "find two independent sources" will sometimes report two when
it has one. Every rule below is therefore re-checked against the returned
citations before anything is written:

  * >= 2 sources, counted by REGISTRABLE DOMAIN. Two URLs on one site are one
    source. A wire story and its syndication are one source.
  * Aggregators, listing sites and press-release wires are dropped before
    counting, so they can never make up the second source.
  * The address must be confirmed. A source naming a developer for a different
    parcel is not evidence about this one, so the street number and street name
    must both appear in the cited evidence.
  * Disagreement -> null, recording every candidate with its URLs. The more
    frequently mentioned name is NOT chosen; frequency is not evidence.

Resolutions are cached permanently in data/ri_developer_web.json.

    python scraper/ri_developer_web.py --sample     # the 20-entity audit set
    python scraper/ri_developer_web.py --applicant "525 Broadway LLC" --address "525 Broadway, Newport RI"
"""

import re
import sys
import json
import logging
from pathlib import Path
from urllib.parse import urlparse
from collections import defaultdict

import anthropic
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format="%(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

CACHE = Path(__file__).parent.parent / "data" / "ri_developer_web.json"

# Judgment-heavy work on the field with the highest cost of being wrong, so
# this tier does not use the cheap transcription model.
MODEL = "claude-opus-5"

MIN_INDEPENDENT_SOURCES = 2

# Hosts that republish rather than report. Excluded BEFORE counting sources so
# they can never supply the second one. Listing/aggregator sites also routinely
# attach a "developer" to a building with no reporting behind it.
_EXCLUDED_HOSTS = re.compile(
    r"(loopnet|crexi|costar\.com|zillow|realtor\.com|redfin|trulia|apartments\.com|"
    r"rentcafe|yelp|bizapedia|opencorporates|dnb\.com|bloomberg\.com/profile|"
    r"buzzfile|manta|corporationwiki|zoominfo|crunchbase|globenewswire|prnewswire|"
    r"businesswire|einpresswire|accesswire|patch\.com|wikipedia|facebook|linkedin|"
    r"twitter|x\.com|reddit|medium\.com|substack)", re.I)

# Sources whose reporting is trusted for this market. Not required -- a source
# outside this list still counts if it is not excluded -- but recorded so the
# review can see how much weight rests on genuinely local reporting.
_PREFERRED_HOSTS = re.compile(
    r"(pbn\.com|rhodeislandcurrent|bankerandtradesman|golocalprov|providencejournal|"
    r"wpri\.com|turnto10|abc6|ecori|providenceri\.gov|pawtucketri\.com|"
    r"cityofnewport|newportri\.gov|warwickri\.gov|cranstonri\.(gov|com)|"
    r"\.gov$|\.gov/)", re.I)

SYSTEM_PROMPT = """\
You research who the actual development company behind a Rhode Island real \
estate project is, given a single-purpose LLC applicant and a project address.

These applicants are parcel holding entities ("525 Broadway LLC"). The question \
is which real development company is behind that entity FOR THAT SPECIFIC \
ADDRESS.

Requirements:

1. The source must describe the SAME project at the SAME address. A source \
naming a developer for a different parcel, or for the same developer's other \
work, is not evidence about this address. Quote the sentence that ties the \
developer to this address.

2. Prefer local trade press, the developer's own site, city or state \
announcements, and financing or permit records. Do not rely on listing sites, \
aggregators, business-directory sites, or press-release wires.

3. Report every source you find that names a developer for this address, with \
its URL and the quoted evidence. Report sources even when they disagree.

4. If different sources name different developers, report all of them. Do not \
choose between them and do not prefer the one mentioned more often.

5. If you cannot find sources tying a named development company to this \
specific address, say so. Reporting nothing is the correct answer for an \
obscure project, and is much better than a plausible guess.

Return your findings as JSON with this shape:

{
  "candidates": [
    {"developer": "<company name>",
     "sources": [{"url": "<url>", "publisher": "<publisher>",
                  "quote": "<sentence tying developer to this address>",
                  "address_confirmed": true|false}]}
  ],
  "notes": "<anything the reviewer should know>"
}

Return only the JSON object."""


def registrable_domain(url: str) -> str:
    """Host reduced to its registrable domain, for counting independence.

    news.example.com and www.example.com are ONE source, not two.
    """
    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        return ""
    host = host.removeprefix("www.")
    parts = host.split(".")
    if len(parts) <= 2:
        return host
    # Handle the common two-part public suffixes seen here (.co.uk, .gov.uk).
    if parts[-2] in {"co", "com", "gov", "org", "net", "ac"} and len(parts[-1]) == 2:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def _address_tokens(address: str) -> tuple[str, list[str]]:
    """Street number and significant street-name words from an address."""
    a = re.sub(r"\s+", " ", (address or "").strip())
    m = re.match(r"^\s*(\d+[A-Za-z]?)\b", a)
    number = m.group(1) if m else ""
    words = [w for w in re.findall(r"[A-Za-z]{3,}", a)
             if w.upper() not in {"STREET", "AVENUE", "ROAD", "DRIVE", "LANE", "LLC",
                                  "RHODE", "ISLAND", "PROVIDENCE", "NEWPORT", "WARWICK",
                                  "CRANSTON", "PAWTUCKET", "THE", "AND"}]
    return number, words[:3]


def address_is_confirmed(address: str, quote: str) -> bool:
    """True when the quoted evidence actually names this parcel.

    Requires the street number AND a distinctive street-name word. The model's
    own address_confirmed flag is advisory; this is the check that counts.
    """
    number, words = _address_tokens(address)
    hay = (quote or "").lower()
    if not number or number not in hay:
        return False
    return any(w.lower() in hay for w in words) if words else False


def _search_local_news(address: str, applicant: str) -> list[dict]:
    """Stage 2a: the already-harvested news corpus."""
    from db.database import get_session
    from db.models import NewsItem

    number, words = _address_tokens(address)
    if not number:
        return []
    session = get_session()
    try:
        hits = []
        for item in session.query(NewsItem).all():
            blob = f"{item.title or ''} {item.summary or ''}"
            if number in blob and any(w.lower() in blob.lower() for w in words):
                hits.append({"url": item.url, "publisher": item.source,
                             "quote": blob[:400], "address_confirmed": True,
                             "via": "local_news_corpus"})
        return hits
    finally:
        session.close()


def _web_research(client: anthropic.Anthropic, applicant: str, address: str,
                  description: str) -> dict:
    """Stage 2b: Claude with the server-side web_search tool."""
    prompt = (
        f"Applicant entity: {applicant}\n"
        f"Project address: {address}\n"
        f"Project description: {description or '(none available)'}\n\n"
        f"Who is the development company behind this project?"
    )
    resp = client.messages.create(
        model=MODEL,
        max_tokens=8000,
        system=[{"type": "text", "text": SYSTEM_PROMPT,
                 "cache_control": {"type": "ephemeral"}}],
        tools=[{"type": "web_search_20260209", "name": "web_search"}],
        messages=[{"role": "user", "content": prompt}],
    )
    if resp.stop_reason == "refusal":
        return {"candidates": [], "notes": "refused"}
    text = "".join(b.text for b in resp.content if b.type == "text")
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if not m:
        return {"candidates": [], "notes": "no JSON in response"}
    try:
        return json.loads(m.group())
    except json.JSONDecodeError:
        return {"candidates": [], "notes": "unparseable JSON"}


def adjudicate(address: str, candidates: list[dict]) -> dict:
    """Apply the corroboration rules to raw findings.

    Returns the resolution plus the full evidence, including for rejections --
    a null has to be auditable, not merely empty.
    """
    result = {"developer": None, "method": None, "sources": [],
              "rejected": [], "candidates_considered": {}, "reason": None}

    per_dev: dict[str, list[dict]] = defaultdict(list)
    for cand in candidates or []:
        dev = (cand.get("developer") or "").strip()
        if not dev:
            continue
        for src in cand.get("sources") or []:
            url = (src.get("url") or "").strip()
            if not url:
                continue
            dom = registrable_domain(url)
            quote = src.get("quote") or ""
            rec = {"url": url, "domain": dom, "publisher": src.get("publisher") or dom,
                   "quote": quote[:300], "preferred": bool(_PREFERRED_HOSTS.search(url)),
                   "via": src.get("via", "web_search")}
            if _EXCLUDED_HOSTS.search(url):
                rec["dropped"] = "aggregator / listing / wire — excluded before counting"
                result["rejected"].append({**rec, "developer": dev})
                continue
            if not address_is_confirmed(address, quote):
                rec["dropped"] = "quote does not name this street number and street"
                result["rejected"].append({**rec, "developer": dev})
                continue
            per_dev[dev].append(rec)

    # Independence is counted by distinct registrable domain.
    scored = {dev: {"sources": srcs, "domains": sorted({s["domain"] for s in srcs})}
              for dev, srcs in per_dev.items()}
    result["candidates_considered"] = {
        dev: {"independent_domains": len(v["domains"]), "domains": v["domains"],
              "urls": [s["url"] for s in v["sources"]]}
        for dev, v in scored.items()
    }

    qualified = {d: v for d, v in scored.items()
                 if len(v["domains"]) >= MIN_INDEPENDENT_SOURCES}

    if len(qualified) > 1:
        result["reason"] = (
            f"sources disagree — {len(qualified)} developers each corroborated "
            f"({', '.join(qualified)}); frequency is not evidence, so this stays null")
        return result
    if not qualified:
        best = max((len(v["domains"]) for v in scored.values()), default=0)
        result["reason"] = (
            f"no developer reached {MIN_INDEPENDENT_SOURCES} independent sources "
            f"for this address (best was {best})")
        return result

    dev, v = next(iter(qualified.items()))
    result["developer"] = dev
    result["method"] = "web_corroborated"
    result["sources"] = v["sources"]
    result["reason"] = (
        f"corroborated by {len(v['domains'])} independent sources "
        f"({', '.join(v['domains'])}), each naming this address")
    return result


def resolve_web(client, applicant: str, address: str, description: str = "",
                cache: dict | None = None) -> dict:
    cache = cache if cache is not None else {}
    key = f"{applicant.upper()}|{address.upper()}"
    if key in cache:
        return cache[key]

    findings = {"candidates": [], "notes": ""}

    local = _search_local_news(address, applicant)
    if local:
        findings["candidates"].append({"developer": "(local corpus hit — see sources)",
                                       "sources": local})
        findings["notes"] = f"{len(local)} local news corpus hits"

    web = _web_research(client, applicant, address, description)
    findings["candidates"].extend(web.get("candidates") or [])
    findings["notes"] = (findings["notes"] + " " + (web.get("notes") or "")).strip()

    out = adjudicate(address, findings["candidates"])
    out["applicant"] = applicant
    out["address"] = address
    out["notes"] = findings["notes"]
    cache[key] = out
    return out


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--applicant")
    ap.add_argument("--address")
    ap.add_argument("--description", default="")
    ap.add_argument("--sample", action="store_true")
    args = ap.parse_args()

    cache = json.loads(CACHE.read_text(encoding="utf-8")) if CACHE.exists() else {}
    client = anthropic.Anthropic()
    try:
        if args.sample:
            from scraper.ri_developer_sample import OUT as SAMPLE_OUT
            rows = json.loads(Path(SAMPLE_OUT).read_text(encoding="utf-8"))
            targets = [r for r in rows if not r["developer"]]
            log.info("Unresolved from registry tier: %d\n", len(targets))
            for r in targets:
                addr = r["evidence"].get("entity", {}).get("address") or r["applicant"]
                res = resolve_web(client, r["applicant"], addr,
                                  r["source"].get("context", ""), cache)
                log.info("%-30s -> %s", r["applicant"],
                         res["developer"] or f"NULL — {res['reason']}")
        else:
            res = resolve_web(client, args.applicant, args.address, args.description, cache)
            log.info(json.dumps(res, indent=1))
    finally:
        CACHE.write_text(json.dumps(cache, indent=1), encoding="utf-8")
