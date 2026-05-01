"""
Normalize raw LLC developer names to canonical parent company names.

Resolution order:
  1. Exact cache hit (SQLite developer_cache table)
  2. Rule-based substring matching
  3. Claude Haiku API call (result is cached permanently)
"""

import re
import sys
import logging
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

log = logging.getLogger(__name__)

# ── Company validation ──────────────────────────────────────────────────────

_ADDRESS_LLC = re.compile(r'^\d')   # starts with digit = address-based shell LLC
_AI_FAILURES = (
    "not identified", "applicant/proponent", "review needed",
    "not found in", "not specified", "cannot be identified",
    "provided pages", "three pages",
    "an affiliate of", "joint venture between", "joint venture:",
    "does not identify", "the document does not", "does not contain",
    "zoning petition", "text amendment", "boston planning",
    "boston redevelopment", "article 80",
)

def is_real_company(name: str) -> bool:
    """Return True if this canonical name is a real development company suitable for the dropdown."""
    if not name or not name.strip():
        return False
    n = name.strip()
    if _ADDRESS_LLC.match(n):
        return False
    nl = n.lower()
    if any(p in nl for p in _AI_FAILURES):
        return False
    if nl in ("unknown", "unknown - review needed", "llc", "inc", "corp", "trust"):
        return False
    return True


_SUFFIX_RE = re.compile(
    r',?\s*(LLC|L\.L\.C\.|Inc\.?|Corp\.?|Ltd\.?|LLP|L\.P\.|LP|'
    r'Company|Co\.|and\s+(its\s+)?affiliates?)$',
    re.IGNORECASE,
)

def suffix_stripped(name: str) -> str:
    """Strip legal suffixes for deduplication comparison. Returns lowercased key."""
    n = name.strip()
    n = re.sub(r'\s+and\s+(its\s+)?affiliates?\.?$', '', n, flags=re.I).strip()
    n = _SUFFIX_RE.sub('', n).strip()
    # Run twice to catch ", LLC" after "Company" etc.
    n = _SUFFIX_RE.sub('', n).strip()
    return n.lower()

# ── Rule table ─────────────────────────────────────────────────────────────
# Each entry: (substring_to_match_lowercase, canonical_name)
# Evaluated in order — first match wins.
RULES: list[tuple[str, str]] = [
    # User-specified majors
    ("rhino capital",               "Rhino Capital Advisors"),
    ("hym investment",              "HYM Investment Group"),
    ("hym ",                        "HYM Investment Group"),
    (" hym",                        "HYM Investment Group"),
    ("samuels and associates",      "Samuels and Associates"),
    ("samuels",                     "Samuels and Associates"),
    ("fallon company",              "The Fallon Company"),
    ("fallon",                      "The Fallon Company"),
    ("ws development",              "WS Development"),
    ("wsd ",                        "WS Development"),
    ("biomed realty",               "BioMed Realty"),
    ("biomed",                      "BioMed Realty"),
    ("related beal",                "Related Beal"),
    ("marcus partners",             "Marcus Partners"),
    ("marcus",                      "Marcus Partners"),
    ("skanska",                     "Skanska"),
    ("national development",        "National Development"),
    ("cabot, cabot",                "Cabot Cabot and Forbes"),
    ("cabot cabot",                 "Cabot Cabot and Forbes"),

    # Major Boston / national developers
    ("hines",                       "Hines"),
    ("iqhq",                        "IQHQ"),
    ("iqho",                        "IQHQ"),
    ("cim group",                   "CIM Group"),
    ("lincoln property",            "Lincoln Property Company"),
    ("mill creek residential",      "Mill Creek Residential"),
    ("mill creek",                  "Mill Creek Residential"),
    ("druker",                      "The Druker Company"),
    ("mount vernon company",        "The Mount Vernon Company"),
    ("synergy investments",         "Synergy Investments"),
    ("howard stein hudson",         "Howard Stein Hudson"),
    ("transom real estate",         "Transom Real Estate"),
    ("harbor run development",      "Harbor Run Development"),
    ("trinity financial",           "Trinity Financial"),
    ("trinity acquisitions",        "Trinity Financial"),
    ("cruz development",            "Cruz Development Corporation"),
    ("city realty group",           "City Realty"),
    ("city realty",                 "City Realty"),
    ("alpha management",            "Alpha Management Corp"),
    ("connelly construction",       "Connelly Construction"),
    ("hood park",                   "Hood Park LLC"),
    ("hub parking",                 "Hub Parking LLC"),
    ("boylston properties",         "Boylston Properties"),
    ("primary development group",   "Primary Development Group"),
    ("thompson square partners",    "Thompson Square Partners"),
    ("volnay capital",              "Volnay Capital"),
    ("prc group",                   "PRC Group"),
    ("benenson capital",            "Benenson Capital Partners"),
    ("calare",                      "Calare Properties"),
    ("greystar",                    "Greystar"),
    ("boston properties",           "Boston Properties"),
    ("winn development",            "WinnDevelopment"),
    ("winndev",                     "WinnDevelopment"),
    ("suffolk downs",               "The HYM Investment Group"),  # HYM led Suffolk Downs
    ("northeast real estate",       "Northeast Real Estate"),
    ("gerding edlen",               "Gerding Edlen"),
    ("elkus manfredi",              "Elkus Manfredi"),            # architect but sometimes listed
    ("gilbane",                     "Gilbane Development"),
    ("leggat mcall",                "Leggat McCall Properties"),
    ("nrdc",                        "NRDC Equity Partners"),
    ("cedar realty",                "Cedar Realty Trust"),
    ("east boston community development", "East Boston CDC"),
    ("fenway community development",      "Fenway CDC"),
    ("south boston neighborhood development", "South Boston NDOC"),
    ("caribbean integration",       "Caribbean Integration CDC"),
    ("notre dame development",      "Notre Dame Development"),
    ("community development corporation", "CDC"),
    ("hub on causeway",             "Delaware North / Boston Properties"),
    ("city point",                  "WS Development"),
    ("ws-fenway",                   "WS Development"),

    # More Boston developers seen in data
    ("abbey group",                 "The ABBEY Group"),
    ("peebles",                     "The Peebles Corporation"),
    ("chiofaro",                    "The Chiofaro Company"),
    ("tishman speyer",              "Tishman Speyer"),
    ("oxford properties",           "Oxford Properties Group"),
    ("community builders",          "The Community Builders"),
    ("king street properties",      "King Street Properties"),
    ("new england development",     "New England Development"),
    ("equity residential",          "Equity Residential"),
    ("davis companies",             "The Davis Companies"),
    ("anchor line",                 "Anchor Line Partners"),
    ("madison park",                "Madison Park Development Corporation"),
    ("hebrew seniorlife",           "Hebrew SeniorLife"),
    ("the rmr group",               "The RMR Group"),
    ("rmr group",                   "The RMR Group"),
    ("trademark partners",          "Trademark Partners"),
    ("wentworth institute",         "Wentworth Institute of Technology"),
    ("northeastern university",     "Northeastern University"),
    ("franciscan hospital",         "Franciscan Hospital for Children"),
    ("boston housing authority",    "Boston Housing Authority"),
    ("core investments",            "Core Investments"),
    ("new atlantic development",    "New Atlantic Development"),
    ("nuestra comunidad",           "Nuestra Comunidad Development Corporation"),
    ("codman square",               "Codman Square Neighborhood Development Corporation"),
    ("allston brighton cdc",        "Allston Brighton CDC"),
    ("allston brighton community",  "Allston Brighton CDC"),
    ("dorchester bay",              "Dorchester Bay Economic Development Corporation"),
    ("dorchester house",            "Dorchester House Multi-Service Center"),
    ("rogerson communities",        "Rogerson Communities"),
    ("historic boston",             "Historic Boston Incorporated"),
    ("jefferson apartment",         "Jefferson Apartment Group"),
    ("scape development",           "Scape Development"),
    ("scape beacon",                "Scape Development"),
    ("pappas enterprises",          "Pappas Enterprises"),
    ("cronin development",          "Cronin Development"),
    ("nordblom",                    "Nordblom Company"),
    ("wingate development",         "Wingate Development"),
    ("tishman",                     "Tishman Speyer"),
    ("suffolk construction",        "Suffolk Construction"),
    ("michaels organization",       "The Michaels Organization"),
    ("grossman companies",          "The Grossman Companies"),
    ("mildred hailey",              "Mildred Hailey 121A Corporation"),
    ("brighton fg",                 "Brighton FG Revitalization"),
    ("bartlett station",            "Nuestra Comunidad Development Corporation"),
    ("seaport square development",  "WS Development"),
    ("seaport d title",             "WS Development"),
    ("seaport d investors",         "WS Development"),

    # Known abbreviation patterns
    ("mp properties",               "Marcus Partners"),
    ("adg ",                        "Accordia Development Group"),
    ("adg s",                       "Accordia Development Group"),
    ("accordia",                    "Accordia Development Group"),
    ("hrp 776",                     "HRP"),
    ("hrp properties",              "HRP"),
    ("bp hancock",                  "Boston Properties"),
    ("boston properties",           "Boston Properties"),
    ("alp 90",                      "Anchor Line Partners"),
    ("fulcrum global",              "Fulcrum Global Investors"),
    ("one mystic owner",            "Fulcrum Global Investors"),
    ("lyx/group",                   "LYX/Group"),
    ("rockwood partners",           "LYX/Group"),
    ("fpg ",                        "Federal Property Group"),
    ("fpg ds",                      "Federal Property Group"),
    ("div black falcon",            "The Davis Companies"),
    ("kic roxbury",                 "Accordia Development Group"),
    ("samuels & associates",        "Samuels and Associates"),
    ("planning office for urban affairs", "Planning Office for Urban Affairs"),
    ("mept seaport",                "Intercontinental Real Estate"),
    ("mept ",                       "Intercontinental Real Estate"),
    ("nflsre",                      "National Real Estate Advisors"),
    ("berkeley investments",        "Berkeley Investments"),
    ("stanhope hotel",              "H.N. Gorin / Masterworks Development"),
    ("h.n. gorin",                  "H.N. Gorin"),
    ("masterworks development",     "Masterworks Development"),
    ("c/o rhino capital",           "Rhino Capital Advisors"),
    ("lendlease",                   "Lendlease"),
    ("carr properties",             "Carr Properties"),
    ("sullivan square holdings",    "Sullivan Square Holdings"),
    ("new urban collaborative",     "New Urban Collaborative"),
]


_PAREN_RE = re.compile(r'\(([^)]+)\)')

def _extract_parenthetical(raw: str) -> str | None:
    """
    Many filings embed the real company in parentheses:
    '110 Canal Street LLC c/o Rhino Capital Advisors LLC'
    'ALP 90 Braintree Owner, LLC (Anchor Line Partners)'
    'Stanhope Hotel Holdings LLC (H.N. Gorin, Inc. ...)'
    Try to extract the most useful name from inside parens.
    """
    m = _PAREN_RE.search(raw)
    if not m:
        return None
    inner = m.group(1).strip()
    # Skip generic qualifiers (always skip these regardless of length)
    always_skip = ("affiliate", "affiliates", "subsidiary", "joint venture", "division")
    short_skip = ("llc", "inc.", "corp.", "a ", "an ", "the ")
    low = inner.lower()
    if any(low.startswith(s) for s in always_skip):
        return None
    if any(low.startswith(s) for s in short_skip) and len(inner) < 6:
        return None
    # If inner contains "and" or "&", take only the first entity
    for sep in (" and ", " & ", " / "):
        if sep in inner:
            inner = inner.split(sep)[0].strip()
            break
    return inner if len(inner) > 4 else None


def _rule_match(raw: str) -> str | None:
    # Check rules table first
    low = raw.lower()
    for pattern, canonical in RULES:
        if pattern in low:
            return canonical
    # Try parenthetical extraction
    paren = _extract_parenthetical(raw)
    if paren:
        # Recursively run rules on the extracted name
        inner_match = _rule_match(paren)
        if inner_match:
            return inner_match
        # Return the extracted name if it looks like a real company
        if not _ADDRESS_LLC.match(paren) and len(paren.split()) >= 2:
            return paren
    return None


def normalize(raw_name: str, session=None, client=None) -> str:
    """
    Return the canonical developer name for a raw LLC string.
    Checks cache first, then rules, then Haiku (if client provided).
    Persists new resolutions to the cache.
    """
    if not raw_name or not raw_name.strip():
        return raw_name

    raw_name = raw_name.strip()

    # 1. Cache lookup
    if session is not None:
        from db.models import DeveloperCache
        cached = session.query(DeveloperCache).filter_by(raw_name=raw_name).first()
        if cached:
            return cached.canonical_name

    # 2. Rule-based
    canonical = _rule_match(raw_name)
    if canonical:
        _store_cache(session, raw_name, canonical, "rules")
        return canonical

    # 3. Claude Haiku fallback
    if client is not None:
        canonical = _haiku_lookup(raw_name, client)
        if canonical:
            _store_cache(session, raw_name, canonical, "ai")
            return canonical

    # 4. No match — return as-is and cache it
    _store_cache(session, raw_name, raw_name, "rules")
    return raw_name


def _store_cache(session, raw_name: str, canonical: str, resolved_by: str):
    if session is None:
        return
    from db.models import DeveloperCache
    try:
        existing = session.query(DeveloperCache).filter_by(raw_name=raw_name).first()
        if not existing:
            session.add(DeveloperCache(
                raw_name=raw_name,
                canonical_name=canonical,
                resolved_by=resolved_by,
            ))
            session.commit()
    except Exception as exc:
        session.rollback()
        log.warning("Cache write failed: %s", exc)


_DISCLAIMER_PHRASES = (
    "i don't have access",
    "i cannot",
    "i can't",
    "without access",
    "without being able",
    "cannot reliably",
    "cannot identify",
    "no access to",
    "not able to",
    "unable to identify",
    "unable to determine",
    "don't have information",
)


def _haiku_lookup(raw_name: str, client) -> str | None:
    prompt = (
        f"You are identifying the real development company behind a Boston real estate LLC. "
        f"LLC name: \"{raw_name}\"\n\n"
        f"Rules:\n"
        f"- If you recognize a well-known developer behind this LLC, return only their name (e.g. \"Related Beal\").\n"
        f"- If you do not recognize the parent company, return exactly the LLC name with no other text.\n"
        f"- Never explain or add commentary. Return only a company name."
    )
    try:
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=64,
            messages=[{"role": "user", "content": prompt}],
        )
        result = resp.content[0].text.strip().strip('"').strip("'")

        # Reject disclaimers — fall back to raw name
        if any(phrase in result.lower() for phrase in _DISCLAIMER_PHRASES):
            return None
        # Reject suspiciously long responses (>80 chars is prose, not a company name)
        if len(result) > 80:
            return None

        log.info("  Haiku resolved '%s' → '%s'", raw_name[:50], result[:50])
        time.sleep(0.5)
        return result if result else None
    except Exception as exc:
        log.warning("  Haiku lookup failed for '%s': %s", raw_name[:50], exc)
        return None
