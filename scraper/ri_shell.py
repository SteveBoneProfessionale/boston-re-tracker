r"""
Single-purpose-entity detection for Rhode Island applicants.

Why this is wider than a leading street number
----------------------------------------------
A leading number plus a legal suffix ("525 Broadway LLC") is only ONE shape a
single-purpose entity takes. The obvious ones were getting through and being
written into the developer column as their own sponsor:

    Tobey Street LLC          named for the street, no number
    RLF IV Terminals SPE, LLC says "SPE" on its face
    Champlin Heights II, LLC  series marker
    WW Holdings LLC           holding company with no trading identity
    CLRI East Street LLC      named for the project's own address

An applicant that trips any rule below does NOT populate the developer column
from the registry. It routes to the web tier instead: registry first, web
second, null third, with every existing confidence rule unchanged. A shell
detected here is not discarded -- it is research that has not happened yet.

The rules
---------
1. LEADING NUMBER    name begins with a street number and carries a legal
                     suffix or street-type word (the original rule)
2. SERIES / PURPOSE  contains SPE, "Special Purpose", a roman-numeral series
                     marker (RLF IV, Champlin Heights II), or reads as a bare
                     holding company with no trading identity
3. STREET-NAMED      the name, stripped of street-type words and legal suffix,
                     is exactly a known Rhode Island street name. No
                     street-type word is required -- "Broadway" carries none,
                     which is what defeated the previous heuristic. The
                     gazetteer is the RIGIS E-911 St_Name field for the five
                     municipalities (data/ri_street_names.json, 4,051 names).
4. ADDRESS-MATCHED   the name contains the project's own street name and shows
                     no operating identity of its own
5. NEWLY FORMED      registered within 24 months of the filing with no other
                     filings -- an entity created to hold this one project

Rules 4 and 5 need the project's address and filing date, so the full verdict
is context-aware. is_shell_name() is the name-only subset (rules 1-3) used
where no project context exists, such as classifying members of an address
cluster.
"""

import re
import json
import logging
from pathlib import Path
from datetime import date

log = logging.getLogger(__name__)

ROOT = Path(__file__).parent.parent
GAZETTEER = ROOT / "data" / "ri_street_names.json"

_LEGAL_SUFFIX = re.compile(
    r"[,\s]+(LLC|L\.?L\.?C\.?|INC|INCORPORATED|CORP|CORPORATION|CO|LP|L\.?P\.?|"
    r"LTD|LLP|TRUST|COMPANY)\.?\s*$", re.I)

_STREET_TYPE = re.compile(
    r"\b(ST|STREET|AVE|AVENUE|RD|ROAD|DR|DRIVE|LN|LANE|BLVD|BOULEVARD|WAY|"
    r"PL|PLACE|CT|COURT|TER|TERRACE|HWY|HIGHWAY|PIKE|SQ|SQUARE|ROW|CIR|"
    r"CIRCLE|EXT|EXTENSION|PKWY|PARKWAY|TRAIL|PATH)\b", re.I)

_LEADING_NUM = re.compile(r"^\s*\d{1,6}[A-Za-z]?\b")

# An entity with a trading identity of its own, rather than a parcel holder.
_OPERATING_HINT = re.compile(
    r"\b(DEVELOPMENT|DEVELOPERS?|PROPERTIES|REALTY|REAL ESTATE|CAPITAL|"
    r"PARTNERS?|INVESTMENTS?|MANAGEMENT|GROUP|BUILDERS?|CONSTRUCTION|"
    r"COMPANIES|ENTERPRISES|HOMES|COMMUNITIES|ASSOCIATES|VENTURES?)\b", re.I)

# Rule 2 markers.
_SPE = re.compile(r"\bSPE\b|\bSPECIAL\s+PURPOSE\b|\bPROPCO\b|\bOPCO\b", re.I)
# Series markers. Single I/V/X are excluded: too many false positives on
# initials, and a one-project entity is not usually numbered "I".
_ROMAN = re.compile(r"\b(II|III|IV|VI|VII|VIII|IX|XI|XII|XIII)\b")
_HOLDING = re.compile(r"\bHOLDINGS?\b", re.I)


def _load_streets() -> set:
    if not GAZETTEER.exists():
        log.warning("street gazetteer missing at %s -- rule 3 disabled", GAZETTEER)
        return set()
    return {s.strip().upper() for s in json.loads(GAZETTEER.read_text(encoding="utf-8"))}


STREETS = _load_streets()


def base_name(name: str) -> str:
    """Name with its legal suffix and punctuation removed."""
    n = (name or "").strip()
    n = _LEGAL_SUFFIX.sub("", n)
    n = re.sub(r"[.,'’\-/]", " ", n)
    return re.sub(r"\s+", " ", n).strip().upper()


def _street_core(name: str) -> str:
    """Base name with street-type words removed, for gazetteer comparison."""
    return re.sub(r"\s+", " ", _STREET_TYPE.sub("", base_name(name))).strip()


def street_of(address: str) -> str | None:
    """The street name in an address, without number or street type."""
    if not address:
        return None
    a = re.sub(r"^\s*\d{1,6}(?:\s*\(\d+\))?(?:\s*[-–]\s*\d+)?\s*", "", address.strip())
    a = re.sub(r"[.,'’]", " ", a)
    a = _STREET_TYPE.sub("", a)
    a = re.sub(r"\s+", " ", a).strip().upper()
    return a or None


def has_legal_suffix(name: str) -> bool:
    return bool(_LEGAL_SUFFIX.search((name or "").strip()))


def is_shell_name(name: str) -> tuple[bool, str | None]:
    """Rules 1-3: everything decidable from the name alone."""
    raw = (name or "").strip()
    if not raw:
        return False, None
    core = _street_core(raw)

    # 1 -- leading street number
    if _LEADING_NUM.match(raw) and (has_legal_suffix(raw) or _STREET_TYPE.search(raw)):
        return True, "leading_number"

    # 2 -- explicit single-purpose or series marker
    if _SPE.search(raw):
        return True, "spe_marker"
    if _ROMAN.search(base_name(raw)):
        return True, "series_marker"
    if _HOLDING.search(raw) and not _OPERATING_HINT.search(raw):
        # "Holdings" with no trading identity of its own.
        return True, "bare_holding"

    # 3 -- the name IS a street name. No street-type word required: Broadway,
    # Moshassuck and Weeden carry none.
    if core and core in STREETS and has_legal_suffix(raw):
        return True, "street_named"

    return False, None


def shell_verdict(name: str,
                  project_address: str | None = None,
                  formation_date: str | None = None,
                  filing_date: str | None = None,
                  other_filings: int | None = None) -> tuple[bool, str | None]:
    """Full 5-rule verdict. Returns (is_shell, rule that fired)."""
    shell, rule = is_shell_name(name)
    if shell:
        return True, rule

    raw = (name or "").strip()
    if not raw:
        return False, None

    # 4 -- named for the project's own address, with no identity of its own.
    street = street_of(project_address or "")
    if street and not _OPERATING_HINT.search(raw):
        core = _street_core(raw)
        toks = [t for t in street.split() if len(t) > 2]
        if toks and all(t in core.split() for t in toks):
            return True, "address_matched"

    # 5 -- formed for this filing: registered within 24 months of it, and the
    # registry shows nothing else in its name.
    if formation_date and filing_date and other_filings in (None, 0, 1):
        try:
            f = date.fromisoformat(str(formation_date)[:10])
            d = date.fromisoformat(str(filing_date)[:10])
            months = (d.year - f.year) * 12 + (d.month - f.month)
            if 0 <= months <= 24 and other_filings in (0, 1):
                return True, "newly_formed"
        except (ValueError, TypeError):
            pass

    return False, None


RULE_NOTE = {
    "leading_number":  "begins with a street number",
    "spe_marker":      "says SPE / special purpose on its face",
    "series_marker":   "roman-numeral series marker",
    "bare_holding":    "holding company with no trading identity",
    "street_named":    "the name is a Rhode Island street name",
    "address_matched": "named for the project's own street",
    "newly_formed":    "formed within 24 months of the filing, no other filings",
}
