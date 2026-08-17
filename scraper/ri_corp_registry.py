r"""
Rhode Island Corporate Database client and single-purpose-LLC resolver.

Applicants on RI planning agendas file under single-purpose LLCs named for the
parcel ("180 Weeden St LLC"). Those are holding entities, not developers, and
ingesting them raw would destroy the Most Active Developers and Developer
Market Share charts. This resolves a shell to the sponsor behind it, or returns
null with a recorded reason.

HOW RESOLUTION WORKS, AND WHY IT IS INDIRECT
--------------------------------------------
The obvious path -- read the members off the entity record -- usually does not
work. Rhode Island LLCs are typically "managed by its Members", and for those
the summary page's Manager table is EMPTY. Member names appear only in Annual
Reports or Articles of Organization, and a recently-organized entity has filed
neither (180 Weeden St LLC, organized 2024-06-05, has an empty filings grid).

So resolution runs through the PRINCIPAL OFFICE ADDRESS instead. Sponsors
register their shells to one back-office address, and the Corporate Database
supports searching by address. Querying the shell's principal address returns
the sponsor's whole portfolio, and the named operating company usually sits in
it alongside the shells:

    8 WEST FARM RD, SMITHFIELD  ->  11 address-named shells
                                 +  DUO DEVELOPMENT CORP
                                 +  J INVESTMENTS LLC
                                 +  Jacavone Investment Corp
                                 +  SAFE AND SOUND SECURITY, LLC

That cluster is evidence, not an answer. Two guards keep it honest:

  1. SERVICE-ADDRESS GUARD. A large cluster is usually a lawyer's, accountant's
     or registered-agent's office serving unrelated clients, not one sponsor's
     portfolio. Above SERVICE_ADDRESS_THRESHOLD entities the address carries no
     sponsor signal and resolution abstains.
  2. SINGLE-CANDIDATE RULE. Resolution succeeds only when exactly ONE plausible
     operating company sits in the cluster. The example above has three, so it
     resolves to NULL -- picking "Duo Development" over "Jacavone Investment"
     would be a guess. A blank developer is correct; a guess is not.

Everything is cached permanently in data/ri_corp_cache.json. A resolved entity
is never re-queried, and the crawl is rate-limited -- this is a public database
run by a small office.
"""

import re
import sys
import json
import time
import logging
from pathlib import Path

import httpx
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

BASE = "https://business.sos.ri.gov/CorpWeb/CorpSearch"
SEARCH_URL = f"{BASE}/CorpSearch.aspx"
SUMMARY_URL = BASE + "/CorpSummary.aspx?FEIN={fein}&SEARCH_TYPE=1"
CACHE = Path(__file__).parent.parent / "data" / "ri_corp_cache.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    )
}
DELAY = 1.5          # deliberately slow; public database, small office

# Above this many entities, a shared address is a service provider's office
# rather than one sponsor's portfolio, and carries no sponsor signal.
SERVICE_ADDRESS_THRESHOLD = 40

# A name that is just a street address plus a suffix is a single-purpose shell.
_SHELL_LEADING_NUM = re.compile(r"^\s*\d")
_STREET_WORDS = re.compile(
    r"\b(ST|STREET|AVE|AVENUE|RD|ROAD|DR|DRIVE|LN|LANE|BLVD|BOULEVARD|WAY|"
    r"PL|PLACE|CT|COURT|TER|TERRACE|HWY|HIGHWAY|PIKE|SQ|SQUARE)\b", re.I)

# Entities that are plausible operating companies rather than shells or
# unrelated businesses at a shared address.
_OPERATING_HINT = re.compile(
    r"\b(DEVELOPMENT|DEVELOPERS?|PROPERTIES|REALTY|REAL ESTATE|CAPITAL|"
    r"PARTNERS?|INVESTMENTS?|MANAGEMENT|GROUP|HOLDINGS?|BUILDERS?|"
    r"CONSTRUCTION|COMPANIES|ENTERPRISES)\b", re.I)

# Businesses that share an address but are clearly not the development sponsor.
_NON_SPONSOR = re.compile(
    r"\b(SECURITY|INSURANCE|LANDSCAP\w*|PLUMBING|ELECTRIC\w*|HVAC|AUTO|"
    r"RESTAURANT|SALON|CLEANING|TRUCKING|LAW|ATTORNEY|ACCOUNTING|CPA|"
    r"DENTAL|MEDICAL|PHARMACY)\b", re.I)


_LEGAL_SUFFIX = re.compile(
    r"\b(LLC|L\.?L\.?C\.?|INC|CORP|CO|LP|L\.?P\.?|LTD|LLP|TRUST|COMPANY)\.?\s*$", re.I)


def is_single_purpose_shell(name: str) -> bool:
    """True when a name reads as a parcel holding entity, not a company.

    A leading street number plus a legal suffix is the signal. Requiring a
    street-type word too was wrong: "525 Broadway LLC" and "1 Ship Street LP"
    carry none, and were passing through as their own developer -- the exact
    single-purpose-LLC contamination this module exists to prevent. Operating
    companies essentially never begin with a street number.
    """
    n = (name or "").strip()
    if not n:
        return False
    if not _SHELL_LEADING_NUM.match(n):
        return False
    return bool(_STREET_WORDS.search(n) or _LEGAL_SUFFIX.search(n))


def _norm_name(name: str) -> str:
    """Comparison key: drop punctuation, legal suffix and spacing."""
    # Hyphens and ampersands are styling, not identity: "Celtic-Roman Group"
    # and "CELTIC ROMAN GROUP" are the same registrant.
    n = re.sub(r"[.,&'\-/]", " ", (name or "").upper())
    n = _LEGAL_SUFFIX.sub("", n)
    n = re.sub(r"\b(LLC|INC|CORP|CO|LP|LTD|LLP|COMPANY|TRUST)\b", " ", n)
    return re.sub(r"\s+", " ", n).strip()


# ── Cache ───────────────────────────────────────────────────────────────

def load_cache() -> dict:
    if CACHE.exists():
        try:
            return json.loads(CACHE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            log.warning("corp cache unreadable — starting fresh")
    return {"entities": {}, "addresses": {}, "resolutions": {}}


def save_cache(cache: dict):
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    CACHE.write_text(json.dumps(cache, indent=1, sort_keys=True), encoding="utf-8")


# ── Portal client ───────────────────────────────────────────────────────

def _hidden(soup) -> dict:
    return {i["name"]: i.get("value", "")
            for i in soup.find_all("input", type="hidden") if i.get("name")}


def _search(client: httpx.Client, mode: str, status: str = "rdbActive", **fields) -> list[dict]:
    """Run one CorpSearch query. `mode` is the rdoBy* radio value.

    EntityStatus is required -- omitting it silently returns the unsearched
    form rather than an error or a result grid.
    """
    soup = BeautifulSoup(client.get(SEARCH_URL, timeout=60).text, "html.parser")
    data = _hidden(soup)
    data.update({
        "ctl00$MainContent$EntityStatus": status,
        "ctl00$MainContent$CorpSearch": mode,
        "ctl00$MainContent$ddBeginsWithEntityName": "B",
        "ctl00$MainContent$ddBeginsWithIndividual": "B",
        "ctl00$MainContent$ddBeginsWithAgent": "B",
        "ctl00$MainContent$ddRecordsPerPage": "All items",
        "ctl00$MainContent$btnSearch": "Search",
    })
    data.update(fields)
    r = client.post(SEARCH_URL, data=data, headers={"Referer": SEARCH_URL}, timeout=90)
    time.sleep(DELAY)

    grid = BeautifulSoup(r.text, "html.parser").find(
        "table", id="MainContent_SearchControl_grdSearchResultsEntity")
    if not grid:
        return []
    out = []
    for tr in grid.find_all("tr")[1:]:
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue
        link = tds[0].find("a", href=True)
        fein = ""
        if link:
            m = re.search(r"FEIN=(\d+)", link["href"])
            fein = m.group(1) if m else ""
        out.append({
            "name": tds[0].get_text(" ", strip=True),
            "fein": fein or tds[1].get_text(" ", strip=True),
            "naics": tds[2].get_text(" ", strip=True),
            "inactive": tds[3].get_text(" ", strip=True),
            "address": tds[4].get_text(" ", strip=True),
        })
    return out


def find_entity(client: httpx.Client, name: str, cache: dict) -> dict | None:
    """Locate an entity by name, trying active then inactive."""
    key = name.strip().upper()
    if key in cache["entities"]:
        return cache["entities"][key]

    cleaned = re.sub(r"[.,]", " ", name)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    # Search on a prefix -- suffix punctuation and "LLC"/"L.L.C." vary between
    # the agenda text and the registry record.
    prefix = re.sub(r"\s+(LLC|L L C|INC|CORP|CO|LP|LTD)\.?$", "", cleaned, flags=re.I).strip()

    hit = None
    for status in ("rdbActive", "rdbInactive"):
        rows = _search(client, "rdoByEntityName", status=status,
                       **{"ctl00$MainContent$txtEntityName": prefix})
        # Require the base names to match exactly once punctuation and legal
        # suffix are normalised away. A prefix match is not good enough:
        # "Jan Co Inc" prefix-matches "JAN CO. CENTRAL, INC.", a different
        # company, and silently attributing a project to it is exactly the
        # wrong-developer failure this module has to avoid.
        want = _norm_name(prefix)
        exact = [r for r in rows if _norm_name(r["name"]) == want]
        if exact:
            hit = dict(exact[0])
            hit["status_searched"] = status
            hit["match"] = "exact"
            break
        if rows and hit is None:
            # Remember the closest candidate so the null has evidence.
            near = dict(rows[0])
            near["status_searched"] = status
            near["match"] = "near-miss"
            hit = near
    if hit is not None and hit.get("match") == "near-miss":
        cache["entities"][key] = hit
        return hit
    cache["entities"][key] = hit
    return hit


def entity_summary(client: httpx.Client, fein: str, cache: dict) -> dict:
    """Principal address, resident agent, managers and NAICS for an entity."""
    ck = f"summary:{fein}"
    if ck in cache["entities"]:
        return cache["entities"][ck]

    r = client.get(SUMMARY_URL.format(fein=fein), timeout=60)
    time.sleep(DELAY)
    soup = BeautifulSoup(r.text, "html.parser")

    def field(el_id: str) -> str:
        el = soup.find(id=el_id)
        return (el.get("value") or el.get_text(" ", strip=True)) if el else ""

    managers = []
    grd = soup.find("table", id="MainContent_grdManagers")
    if grd:
        for tr in grd.find_all("tr")[1:]:
            tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
            if len(tds) >= 2 and any(tds):
                managers.append({"title": tds[0], "name": tds[1],
                                 "address": tds[2] if len(tds) > 2 else ""})

    text = soup.get_text(" ", strip=True)
    out = {
        "fein": fein,
        "managers": managers,
        "member_managed": "managed by its Members" in text,
        "naics": field("MainContent_txtNIACS"),
        "raw_text_len": len(text),
    }
    cache["entities"][ck] = out
    return out


def entities_at_address(client: httpx.Client, address: str, cache: dict) -> list[dict]:
    """Every entity sharing a principal-office address."""
    key = address.strip().upper()
    if key in cache["addresses"]:
        return cache["addresses"][key]
    rows = _search(client, "rdoByAddress",
                   **{"ctl00$MainContent$txtAddress": address})
    cache["addresses"][key] = rows
    return rows


# ── Resolution ──────────────────────────────────────────────────────────

# Registry addresses render as "8 WEST FARM RD SMITHFIELD, RI  02917  USA" --
# the city is separated from the street by a single space, not a comma, so
# splitting on punctuation leaves the city attached and the address search
# returns nothing. Match through the street-type token instead.
_STREET_CAPTURE = re.compile(
    r"^\s*(\d+[A-Z]?\s+.*?\b(?:ST|STREET|AVE|AVENUE|RD|ROAD|DR|DRIVE|LN|LANE|"
    r"BLVD|BOULEVARD|WAY|PL|PLACE|CT|COURT|TER|TERRACE|HWY|HIGHWAY|PIKE|"
    r"SQ|SQUARE|CIR|CIRCLE|PKWY|PARKWAY))\b", re.I)


def _street_of(address: str) -> str:
    """The street portion of a registry address, for clustering."""
    a = (address or "").strip()
    m = _STREET_CAPTURE.match(a)
    if m:
        return m.group(1).strip()
    # No recognizable street type — fall back to the pre-comma portion.
    return re.split(r",", a)[0].strip()


_SUFFIX_CASE = {"LLC": "LLC", "L.L.C.": "LLC", "INC": "Inc.", "INC.": "Inc.",
                "CORP": "Corp.", "CORP.": "Corp.", "LP": "LP", "LTD": "Ltd.",
                "CO": "Co.", "CO.": "Co.", "LLP": "LLP", "PC": "PC"}


def display_name(raw: str) -> str:
    """Title-case a registry name while keeping legal suffixes correct.

    str.title() turns "JK EQUITIES, INC." into "Jk Equities, Inc." -- both the
    initialism and the suffix come out wrong, and these names are shown to
    users in the developer charts.
    """
    out = []
    for i, word in enumerate(re.split(r"(\s+)", (raw or "").strip())):
        if not word.strip():
            out.append(word)
            continue
        bare = word.strip(",.")
        up = bare.upper()
        if up in _SUFFIX_CASE:
            # Rebuild rather than substring-replace: the source may already
            # carry the trailing period ("INC."), and replacing the bare token
            # inside it yields "Inc..".
            lead = "," if word.startswith(",") else ""
            out.append(lead + _SUFFIX_CASE[up])
        elif up in _LOWER_WORDS and i > 0:
            out.append(word.replace(bare, bare.lower()))
        elif up in _EXPANDED:                      # ST -> St, AVE -> Ave
            out.append(word.replace(bare, _EXPANDED[up]))
        elif bare.isupper() and (any(c.isdigit() for c in bare) or
                                 (bare.isalpha() and len(bare) <= 4)):
            # Initialisms and alphanumeric codes: JK, WS, HRP, JPMW, JC131.
            # Capitalising these produces "Jpmw" / "Jc131", which is wrong in a
            # user-facing developer chart.
            out.append(word)
        else:
            out.append(word.replace(bare, bare.capitalize()) if bare.isupper() else word)
    return "".join(out).strip().rstrip(",")


# Words that stay lowercase inside a name, and short tokens that are ordinary
# words rather than initialisms -- without this, "HOUSES FOR THE COMMUNITY LLC"
# keeps FOR and THE uppercase because they are three letters and all-caps.
_LOWER_WORDS = {"OF", "FOR", "THE", "AND", "AT", "ON", "IN", "TO", "BY"}
_EXPANDED = {"ST": "St", "AVE": "Ave", "RD": "Rd", "DR": "Dr", "LN": "Ln",
             "CT": "Ct", "PL": "Pl", "SQ": "Sq", "TER": "Ter"}


def resolve(client: httpx.Client, applicant: str, cache: dict) -> dict:
    """Resolve an applicant entity to a sponsor, or to null with a reason.

    Returns a record carrying the evidence either way, so a null is auditable
    rather than just absent.
    """
    key = applicant.strip().upper()
    if key in cache["resolutions"]:
        return cache["resolutions"][key]

    rec = {
        "applicant": applicant, "developer": None, "confidence": None,
        "reason": None, "evidence": {},
    }

    ent = find_entity(client, applicant, cache)
    if not ent:
        rec["reason"] = "not found in the RI Corporate Database"
        cache["resolutions"][key] = rec
        return rec

    rec["evidence"]["entity"] = {
        "name": ent["name"], "id": ent["fein"], "address": ent["address"],
        "naics": ent.get("naics", ""), "match": ent.get("match", "exact"),
    }

    if ent.get("match") == "near-miss":
        rec["reason"] = (
            f"no exact name match in the registry (closest: {ent['name']}) — "
            f"attributing to a similarly-named entity would be a guess")
        cache["resolutions"][key] = rec
        return rec

    # An applicant that is already a named operating company needs no resolving.
    if not is_single_purpose_shell(ent["name"]):
        rec["developer"] = display_name(ent["name"])
        rec["confidence"] = "direct"
        rec["reason"] = "applicant is already a named operating company, not a shell"
        cache["resolutions"][key] = rec
        return rec

    street = _street_of(ent["address"])
    if not street:
        rec["reason"] = "no principal address on the entity record"
        cache["resolutions"][key] = rec
        return rec

    cluster = entities_at_address(client, street, cache)
    rec["evidence"]["cluster_address"] = street
    rec["evidence"]["cluster_size"] = len(cluster)

    if len(cluster) > SERVICE_ADDRESS_THRESHOLD:
        rec["reason"] = (
            f"principal address is shared by {len(cluster)} entities — reads as a "
            f"service provider's office, not one sponsor's portfolio")
        cache["resolutions"][key] = rec
        return rec

    shells = [e for e in cluster if is_single_purpose_shell(e["name"])]
    named = [e for e in cluster
             if not is_single_purpose_shell(e["name"])
             and not _NON_SPONSOR.search(e["name"])]
    operating = [e for e in named if _OPERATING_HINT.search(e["name"])]

    rec["evidence"]["shells"] = [e["name"] for e in shells]
    rec["evidence"]["named_entities"] = [e["name"] for e in named]
    rec["evidence"]["operating_candidates"] = [e["name"] for e in operating]

    if len(operating) == 1:
        rec["developer"] = display_name(operating[0]["name"])
        rec["confidence"] = "address-cluster"
        rec["reason"] = (
            f"sole operating company among {len(cluster)} entities registered at "
            f"{street}, alongside {len(shells)} single-purpose shells")
    elif len(operating) > 1:
        rec["reason"] = (
            f"ambiguous — {len(operating)} operating companies share {street} "
            f"({', '.join(e['name'] for e in operating[:4])}); choosing between "
            f"them would be a guess")
    else:
        rec["reason"] = f"no named operating company among {len(cluster)} entities at {street}"

    cache["resolutions"][key] = rec
    return rec


if __name__ == "__main__":
    names = sys.argv[1:] or ["180 Weeden St LLC", "JK Equities LLC"]
    cache = load_cache()
    try:
        with httpx.Client(headers=HEADERS, follow_redirects=True) as client:
            for n in names:
                r = resolve(client, n, cache)
                log.info("%-34s -> %s", n, r["developer"] or f"NULL ({r['reason']})")
    finally:
        save_cache(cache)
