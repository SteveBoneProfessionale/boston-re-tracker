r"""
Extract structured development records from Rhode Island agenda text.

Deterministic, not LLM-based. These agendas are formulaic enough that regex
extraction is both accurate and free, and it stays reproducible -- the same
input always yields the same record, which an LLM pass does not guarantee.

Every field is either found in the text or left None. Nothing is inferred:
SF is never derived from unit count or acreage, a developer is never guessed
from an LLC name, and construction is never inferred from approval.

MUNICIPAL FORMATS
-----------------
Pawtucket   Review stages and vote marker on one line, then address+parcel:
                Major Land Development Project, Master Plan Review  (VOTE TAKEN)
                180 Weeden Street (AP 44 Lot 561)
                180 Weeden St LLC seeks Master Plan Approval for the adaptive
                reuse of an existing (former) mill building on a +/- 0.86-acre
                tract ... zoned Conant Thread (CT) ... twenty-nine (29)
                proposed residential units with 17 off-street parking spaces.

Providence  Numbered items with a case number and inline neighborhood:
                2. Case no 26-047MIL - 203 Douglas Ave
                   Owner: PMP Group LLC
                   ... 21 residential units ... in the C-2 zone
                   - for vote (AP 68 Lot 846, Smith Hill)

Newport     App. No. plus a semicolon-delimited parcel and zoning:
                App. No. 2025-DPR-04  0 (525) Broadway, TAP 6, Lot 1, R-10 Residential
                Application of 525 Broadway LLC, owner and applicant, to construct...

REVIEWING BODY COMES FROM THE DOCUMENT, NOT THE DASHBOARD
---------------------------------------------------------
A Pawtucket PDF contains both boards' agendas, separated by section markers:

    <<PAWTUCKET / CENTRAL FALLS JOINT PLANNING COMMISSION MEETING>>
    ...
    <<PAWTUCKET CITY PLANNING COMMISSION MEETING>>

The same PDF is served from both EntityID 2513 and 2516, so attributing items
by which dashboard supplied the file would double-count every item and assign
half of them to the wrong board. The section marker an item falls under is
what determines its reviewing body.
"""

import re
import sys
import json
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

log = logging.getLogger(__name__)

# ── Shared field patterns ───────────────────────────────────────────────

# "twenty-nine (29) proposed residential units" -> 29. The parenthesised
# numeral is authoritative; the spelled-out form is ignored.
# The noun is not always "unit": agendas write "22 apartments", "14 new
# condominium units", "23 unit apartment building". Requiring the literal
# word "units" missed roughly half the projects whose count IS stated.
# LOTS are deliberately excluded -- "44 lots for single-family residential
# use" is a subdivision count, not a unit count, and conflating the two would
# overstate the pipeline.
_UNIT_ADJ = (r"(?:new|proposed|additional|affordable|residential|rental|total|"
             r"market[- ]rate|multi-?family|one[- ]bedroom|two[- ]bedroom|"
             r"three[- ]bedroom|senior|workforce)")
_UNIT_NOUN = (r"(?:dwelling\s+units?|residential\s+units?|apartment\s+units?|"
              r"condominium\s+units?|units?|apartments?|condominiums?|"
              r"townhous(?:es|e)|dwellings?|residences?)")
_UNITS = re.compile(
    r"(?:\((\d{1,4})\)|\b(\d{1,4}))\s*(?:" + _UNIT_ADJ + r"\s+){0,3}" + _UNIT_NOUN + r"\b",
    re.I)
_PARKING = re.compile(
    r"(?:\((\d{1,4})\)|\b(\d{1,4}))\s*(?:off-?street\s+|surface\s+|structured\s+)?"
    r"parking\s+spaces?\b", re.I)
# BUILDING gross floor area, distinguished from LAND area.
#
# RI agendas state both in the same sentence and in the same units:
#   "a 23,500 +/- SF commercial building"                  <- building
#   "on a 43,560+ sq ft (1+ acre) site"                    <- land
#   "30,009 +/- sqft (0.69 +/- acre) tract of land"        <- land
#
# A bare number-plus-SF pattern cannot tell them apart, and 341 of 685 items
# with a square-footage value were actually reporting lot size. Writing land
# area into total_gsf makes the building-size column untrustworthy, so a figure
# is only accepted as GSF when something in the sentence says "building", and
# is rejected outright when the trailing context says land.
_SF_ANY = re.compile(
    r"([\d,]{3,})\s*(?:\+/-\s*)?(?:square[\s\-]*(?:feet|foot)|sq\.?\s*ft\.?|s\.?f\.?|gsf)\b",
    re.I)
_LAND_CTX = re.compile(
    r"^\W{0,12}(?:\(\s*[\d.]+\s*\+?/?-?\s*acres?\s*\)\s*)?"
    r"(?:of\s+)?(?:\+/-\s*)?(?:tract|lot|lots|site|parcel|land|acre)", re.I)
# Up to three adjectives may sit between the figure and the noun:
# "23,500 +/- SF commercial building", "9,000 sq ft two-story structure".
_BLDG_CTX_AFTER = re.compile(
    r"^\W{0,12}(?:(?!tract|lot|site|parcel|land|acre)[\w\-]+\s+){0,3}"
    r"(?:building|structure|facility|floor\s+area|addition|development|project|"
    r"(?:retail|office|commercial|industrial|residential|lab)[\w/\- ]{0,20}space)", re.I)
# "gross floor area of 128,733 square feet" -- the noun can be a few words back.
_BLDG_CTX_BEFORE = re.compile(
    r"(?:building|structure|facility|gross\s+floor\s+area|GFA|addition|footprint|construct(?:ing|ion)?)(?:\s+[\w\-]+){0,3}\W{0,12}$", re.I)


def building_sf(text: str):
    """Gross floor area of a BUILDING, or None when the figure is land area."""
    for m in _SF_ANY.finditer(text or ""):
        after = text[m.end():m.end() + 60]
        before = text[max(0, m.start() - 60):m.start()]
        if _LAND_CTX.match(after):
            continue                       # "... sq ft tract of land"
        if _BLDG_CTX_AFTER.match(after) or _BLDG_CTX_BEFORE.search(before):
            return m
    return None


_SF = _SF_ANY          # kept for callers that want any square-footage figure
_ACRES = re.compile(r"([\d.]+)\s*(?:\+/-\s*)?-?\s*acres?\b", re.I)
_STORIES = re.compile(r"(?:\((\d{1,3})\)|\b(\d{1,3}))\s*(?:-)?\s*(?:stor(?:y|ies)|floors?)\b", re.I)
_TOWNHOUSES = re.compile(r"(?:\((\d{1,4})\)|\b(\d{1,4}))\s*townhouses?\b", re.I)
_BUILDINGS = re.compile(r"(?:\((\d{1,3})\)|\b(\d{1,3}))\s*(?:new\s+)?buildings?\b", re.I)

_ADAPTIVE = re.compile(r"adaptive\s+reuse|conversion of the existing|"
                       r"redevelopment of the existing building|former mill", re.I)
# "zoned Conant Thread (CT)" / "in the C-2 zone" / "R-10 Residential"
# Zoning is stated in several shapes across the five cities:
#   "zoned Riverfront Mixed-Use (RD3)"   "in the R-10 zone"
#   "located within the Riverfront Tidewater (RTW) zoning district"
#   "TAP 6, Lot 1, R-10 Residential"     "Zoning District: B-2"   "zoned A-80"
# Only three of those shapes were matched, which is why zoning sat at 42%.
_ZONING = re.compile(
    r"zoned\s+([A-Za-z][\w\s\-/]{1,34}?\([A-Z0-9\-]{1,6}\))"
    r"|(?:located\s+)?with?in\s+the\s+([A-Za-z][\w\s\-/]{1,30}?\([A-Z0-9\-]{1,6}\))\s*zon"
    r"|in\s+the\s+([A-Z]{1,3}-?\d{0,2})\s+zone\b"
    r"|zoning\s+district\s*[:\-]?\s*([A-Z]{1,3}-?\d{1,3})\b"
    r"|,\s*([A-Z]{1,3}-\d{1,2})\s+(?:Residential|Commercial|Industrial|Business)\b"
    r"|\bzoned\s+([A-Z]{1,3}-\d{1,3})\b", re.I)

_VOTE_TAKEN = re.compile(r"\(\s*VOTE\s+TAKEN\s*\)", re.I)
_NO_VOTE = re.compile(r"\(\s*NO\s+VOTE(?:\s+TAKEN)?\s*\)|informational|discussion only", re.I)
_OUTCOME = re.compile(r"\b(CONTINUED|WITHDRAWN|TABLED|APPROVED|DENIED)\b")

# Review-stage vocabulary -> canonical label used by app/data.py::RI_STAGE_MAP
_STAGE_PATTERNS = [
    (r"pre-?application", "Pre-application Conference"),
    (r"master\s*(?:and|&|/)\s*preliminary|combined master", "Combined Master and Preliminary"),
    (r"master\s+plan", "Master Plan"),
    (r"preliminary\s+plan", "Preliminary Plan"),
    (r"final\s+plan", "Final Plan"),
    (r"development\s+plan\s+review", "Development Plan Review"),
    (r"unified\s+development\s+review", "Unified Development Review"),
    (r"special\s+use\s+permit", "Special Use Permit"),
    (r"administrative\s+(?:review|subdivision|land development)", "Administrative Review"),
    (r"rezon|zoning\s+map\s+amendment|zone\s+change", "Rezoning"),
    (r"concept", "Conceptual"),
    # Phrasings found by reading the verbatim language on projects that came
    # out with no stage. Ordered after the canonical ones so an agenda that
    # says "Preliminary Plan" still maps there rather than to the looser
    # "preliminary application" wording Warwick and Cranston use.
    (r"preliminary\s+application", "Preliminary Application"),
    (r"site\s+plan\s+review", "Site Plan Review"),
    (r"design\s+waiver", "Design Waiver"),
    (r"comprehensive\s+plan\s+amendment", "Comprehensive Plan Amendment"),
    (r"advisory\s+opinion|45-24-51", "Advisory Opinion"),
    (r"city\s+council\s+referral", "City Council Referral"),
    (r"recommendations?\s+to\s+the\s+zoning\s+board", "Zoning Board Recommendation"),
    (r"\bto\s+demolish\b|\bdemolition\b", "Demolition"),
    (r"\bto\s+merge\b|\blot\s+merger\b", "Lot Merger"),
]
# Events recorded in history that must never advance the current stage.
_NON_ADVANCING = re.compile(
    r"\bextension\b|\bextend\b|\bmodification\b|\bamend(?:ment)?\b|"
    r"\bcontinuance\b|\bwaiver\b", re.I)

_CLASSIFICATION = [
    (r"major\s+land\s+development", "Major"),
    (r"minor\s+land\s+development", "Minor"),
    (r"administrative\s+(?:land development|subdivision)", "Administrative"),
    (r"major\s+subdivision", "Major"),
    (r"minor\s+subdivision", "Minor"),
]

# Applicant: "180 Weeden St LLC seeks", "JK Equities LLC will be requesting",
# "Application of X", "Owner: X", "Applicant: X"
# Entity names may START WITH A DIGIT -- single-purpose shells are named for
# the parcel ("180 Weeden St LLC", "525 Broadway LLC"), which is precisely the
# case that matters most here. Requiring a leading capital missed all of them.
# The comma matters: "Southeastern Holding, LLC" is written with one, and a
# character class without it cannot reach the suffix.
_ENT = (r"((?:\d{1,6}\s+)?[A-Za-z][\w&'\.\-, ]{2,60}?"
        r"(?:LLC|L\.L\.C\.|Inc\.?|Corp\.?|Company|Trust|LP))")
_APPLICANT = [
    re.compile(r"(?:^|\n)\s*" + _ENT + r"\s+"
               r"(?:seeks|is seeking|requests|will be requesting|proposes)", re.M),
    re.compile(r"(?:Applicant|Owner|Petitioner|Developer)\s*(?:and\s+Owner)?\s*[:\-]\s*" + _ENT),
    re.compile(r"Application\s+of\s+" + _ENT),
]

_CASE_NO = re.compile(r"\b(?:Case\s*no\.?|App\.?\s*No\.?)\s*([\w\-]{4,20})", re.I)
# Providence appends the neighborhood: "(AP 68 Lot 846, Smith Hill)"
_PVD_NBHD = re.compile(r"\(AP\s*\d+[^)]*?,\s*([A-Z][A-Za-z' \-]{2,32})\)")

# Reviewing-body section markers inside a single Pawtucket PDF.
_BODY_MARKER = re.compile(r"<<\s*([A-Z][A-Z/ &.\-]{10,80}?)\s*(?:MEETING)?\s*>>")


def _num(m) -> int | None:
    """First non-empty group of an alternation, as int."""
    if not m:
        return None
    for g in m.groups():
        if g:
            try:
                return int(str(g).replace(",", ""))
            except ValueError:
                return None
    return None


# Self-storage is measured in "units" too, and a 605-unit storage building is
# not 605 apartments. residential_units must mean dwellings.
_STORAGE_CTX = re.compile(
    r"self[- ]?storage|storage\s+(?:facility|building|units?|complex)|"
    r"mini[- ]?storage|climate[- ]controlled", re.I)


def residential_units(text: str):
    """Dwelling count. Returns None when the units are storage units."""
    m = _UNITS.search(text or "")
    if not m:
        return None
    if _STORAGE_CTX.search(text or ""):
        # Only a dwelling word in the same phrase rescues it.
        span = text[max(0, m.start() - 40):m.end() + 40]
        if not re.search(r"dwelling|apartment|residential|condominium|"
                         r"townhous|bedroom", span, re.I):
            return None
    return m


def review_stages(text: str) -> list[str]:
    out = []
    for pat, label in _STAGE_PATTERNS:
        if re.search(pat, text, re.I) and label not in out:
            out.append(label)
    return out


def classification(text: str) -> str | None:
    for pat, label in _CLASSIFICATION:
        if re.search(pat, text, re.I):
            return label
    return None


def zoning(text: str) -> str | None:
    m = _ZONING.search(text)
    if not m:
        return None
    for g in m.groups():
        if g:
            return re.sub(r"\s+", " ", g).strip()
    return None


def applicant(text: str) -> str | None:
    for pat in _APPLICANT:
        m = pat.search(text)
        if m:
            return re.sub(r"\s+", " ", m.group(1)).strip(" ,.")
    return None


def body_sections(text: str) -> list[tuple[int, str]]:
    """(offset, body name) for each in-document reviewing-body marker."""
    return [(m.start(), re.sub(r"\s+", " ", m.group(1)).title())
            for m in _BODY_MARKER.finditer(text)]


def body_at(sections: list[tuple[int, str]], offset: int, default: str) -> str:
    """Which board heard an item at this offset."""
    current = default
    for pos, name in sections:
        if pos <= offset:
            current = name
        else:
            break
    return current


def extract_item(block: str) -> dict:
    """Pull every citable field from one agenda item's text."""
    stages = review_stages(block)
    non_adv = bool(_NON_ADVANCING.search(block))
    out = {
        "applicant_entity": applicant(block),
        "review_stages": stages,
        "review_stage_raw": ", ".join(stages) or None,
        "classification": classification(block),
        "zoning_district_raw": zoning(block),
        "residential_units": _num(residential_units(block)),
        "parking_spaces": _num(_PARKING.search(block)),
        "total_gsf": _num(building_sf(block)),
        "site_acreage": None,
        "num_stories": _num(_STORIES.search(block)),
        "townhouses": _num(_TOWNHOUSES.search(block)),
        "building_count": _num(_BUILDINGS.search(block)),
        "adaptive_reuse": bool(_ADAPTIVE.search(block)),
        "vote_taken": True if _VOTE_TAKEN.search(block) else (
            False if _NO_VOTE.search(block) else None),
        "advances_stage": not non_adv,
        "case_number": None,
        "neighborhood": None,
        "description": re.sub(r"\s+", " ", block).strip()[:1200],
    }
    am = _ACRES.search(block)
    if am:
        try:
            out["site_acreage"] = float(am.group(1))
        except ValueError:
            pass
    cm = _CASE_NO.search(block)
    if cm:
        out["case_number"] = cm.group(1).strip()
    nm = _PVD_NBHD.search(block)
    if nm:
        out["neighborhood"] = nm.group(1).strip()
    om = _OUTCOME.search(block)
    if om:
        out["outcome"] = om.group(1).title()
    return out
