r"""
Building square footage from the planning documents -- and, crucially, NOT
from anything else that is measured in square feet.

The Rhode Island agenda corpus is full of square-foot figures, and almost none
of them are building size. Lot area, parcel area, the "Area:" field on a
Warwick agenda, zoning minimum lot sizes, sign faces and patios are all
reported in SF, in the same sentence shapes, often in the same clause:

    "construct a mixed-use multifamily structure on a 97,567 +/- sqft"   LOT
    "A-20 (Residential, 20,000 sf lots)"                                 ZONING
    "install a building leasing banner ... up to 50 SF"                  SIGN
    "The new lot will measure 53,582 SF with the building providing
     70,784 SF of area"                             LOT *and* BUILDING, one clause

That last one is why this cannot be a keyword search: the wrong number and the
right number sit six words apart, and the stored value took the wrong one.

So every figure is CLASSIFIED, not merely found, and only BUILDING survives.
A figure whose class cannot be established is discarded rather than guessed --
a wrong SF is worse than a blank, because a blank is visibly missing while a
lot area reads as a reported building size.

NEVER DERIVED. Nothing here multiplies units by an assumption, reads FAR
against lot size, or infers from a zoning code. If the filing states units,
floors and a footprint but no floor area, the components are recorded and the
GSF is left blank.

    python scraper/ri_sf_extract.py --sample 30
"""

import re
import sys
import json
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import get_session
from db.models import Project
from scraper.ri_ingest_llm import load_items, collapse
from scraper.ri_identity import normalize_address

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

OUT = Path(__file__).parent.parent / "data" / "ri_sf_candidates.json"
RI = ("Providence", "Warwick", "Cranston", "Pawtucket", "Newport")

# A number followed by a square-foot unit. Decimals are captured so they can be
# rejected: "10.89 units per acre" produced a phantom 89 once already.
NUM_SF = re.compile(
    r"(?P<num>\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?)\s*"
    # A plus/minus sign in a CAD title block extracts as '?' or a private-use
    # glyph, so "5,597?SQ.FT." was not matching at all and a stated BUILDING
    # AREA went unseen.
    r"(?:\+/-|\+/\u2013|[\?\u00b1\uf0b1\u2248~])?\s*"
    r"(?P<unit>square\s*(?:feet|foot|ft)|sq\.?\s*ft\.?|sqft|s\.?f\.?|SF)\b",
    re.I)

# Immediately-surrounding words that mean this figure is a BUILDING.
BUILDING_CUE = re.compile(
    r"\b(?:gross\s+floor\s+area|GFA|floor\s+area|building\s+area|"
    r"building\s+(?:of|providing|containing|totaling|totalling|is|measuring|with|proposed|will|would|shall)|"
    r"(?:new|proposed|existing)\s+\w{0,12}\s*building|"
    r"structure\s+(?:of|containing|totaling)|"
    r"(?:restaurant|warehouse|store|showroom|clubhouse|facility|addition|"
    r"storage\s+building|commercial\s+building|industrial\s+building|"
    r"office\s+building|retail\s+building|mixed-?use\s+building|fast\s+food|drive-?thru|dwelling|apartment|hotel|lounge|garage)|"
    r"of\s+(?:retail|commercial|office|lab(?:oratory)?|residential|industrial|"
    r"warehouse|restaurant|medical)\s+(?:space|area|use)|"
    r"first\s+floor|second\s+floor|ground\s+floor|per\s+floor|footprint)\b",
    re.I)

# Words that mean it is LAND, a zoning standard, or something else entirely.
# Checked first: land vocabulary is far more common here than building
# vocabulary, so a tie must resolve to "not a building".
NOT_BUILDING = re.compile(
    r"\b(?:lot|lots|parcel|parcels|land\s+area|site\s+area|tract|acre|acres|"
    r"frontage|subdivid\w+|subdivision|merge|merging|zoned|zoning|"
    r"minimum|required|per\s+acre|density|"
    r"sign|signage|banner|billboard|awning|"
    r"patio|deck|terrace|yard|open\s+space|landscap\w+|buffer|"
    r"parking\s+(?:area|lot)|pavement|paved|impervious|driveway|"
    r"easement|right[- ]of[- ]way|abandon\w*)\b",
    re.I)

# Warwick agendas carry a literal "Area:" field. It is always land.
AREA_FIELD = re.compile(r"\b(?:land\s+)?area\s*:\s*$", re.I)

# A comparison against a zoning threshold is not a measurement of anything.
# "a Restaurant of less than 3500 sf GFA" and "a restaurant greater than 3,500
# sq.ft. GFA" both cite Table 12-1's threshold, not the building.
THRESHOLD = re.compile(
    r"\b(?:less|greater|more|fewer|larger|smaller)\s+than\s*$|"
    r"\b(?:no|not)\s+(?:more|less)\s+than\s*$|"
    r"\b(?:exceed(?:ing|s)?|in\s+excess\s+of|up\s+to|at\s+least|over|under|"
    r"minimum\s+of|maximum\s+of|threshold\s+of)\s*$", re.I)

# A change in floor area is not the floor area. "add 400 square feet of GFA"
# describes a delta against an existing building whose size is not stated.
DELTA = re.compile(
    r"\b(?:add|adds|adding|addition\s+of|additional|increase\w*\s+(?:by|of)|"
    r"expand\w*\s+by|reduce\w*\s+by|removal\s+of)\s*$", re.I)

# PRECISION FIRST. Land figures outnumber building figures 269 to 13 in this
# corpus, so 'nearest cue wins' is not a safe rule -- it accepted lot areas
# from phrases like 'reconfigure ... lots which will contain 8,000 sq. ft.'
# A figure is only taken when an EXPLICIT building phrase governs it.
STRONG_BEFORE = re.compile(
    r"(?:gross\s+floor\s+area|floor\s+area|building\s+area|\bGFA\b)[^.;]{0,40}$|"
    r"building\s+(?:proposed\s+to\s+provide|providing|will\s+provide|"
    r"contain(?:ing)?|of|totall?ing|measuring)\s*(?:approximately\s*)?$|"
    # "construct a NEW ... " reads onto the figure directly.
    r"construct(?:ing|ion\s+of)?\s+(?:a|an|the)?\s*(?:new\s+)?(?:\w+\s+){0,2}$", re.I)
# Up to two adjectives may sit between the figure and its noun:
# "5,907 sq. ft. FAST FOOD restaurant", "23,500 SF COMMERCIAL building".
STRONG_AFTER = re.compile(
    r"^\s*(?:\+/-\s*)?(?:of\s+)?(?:gross\s+)?(?:\w+[\s-]+){0,2}(?:floor\s+area|GFA|building|structure|facility|restaurant|warehouse|"
    r"store|showroom|clubhouse|addition|dwelling|garage|"
    r"of\s+new\s+space|of\s+(?:retail|commercial|office|lab)\s+space)\b", re.I)
# A land word immediately against the figure overrides building phrasing.
# These are DIRECTIONAL: the before-pattern only ever tests the text before
# the figure, and the after-pattern only the text after. Testing one string
# against both let "The new LOT will measure ... the building providing"
# veto its own building phrase.
LAND_BEFORE = re.compile(r"(?:lot|lots|parcel|land|acres?|site|tract)\b[^.;]{0,18}$", re.I)
LAND_AFTER = re.compile(r"^\s*(?:\+/-\s*)?(?:of\s+)?(?:lot|lots|parcel|of\s+land|acres?|tract|site)\b", re.I)

EXISTING_CONDITIONS = re.compile(
    r"\bN/F\s*:|now\s+or\s+formerly|existing\s+(?:building|structure|conditions)[^.;]{0,60}$|existing\s+conditions\s+plan", re.I)

WINDOW_BEFORE = 120
WINDOW_AFTER = 70


def classify(text, m):
    """BUILDING, LAND or UNKNOWN for one square-foot figure, with the reason."""
    raw = m.group("num")
    if "." in raw:
        return "UNKNOWN", "decimal figure -- a density or ratio, not an area", None
    val = int(raw.replace(",", ""))
    if val < 200:
        return "UNKNOWN", "under 200 sf -- a sign face, a patio or a fragment", val
    if val > 5_000_000:
        return "UNKNOWN", "implausibly large for a building", val

    before = text[max(0, m.start() - WINDOW_BEFORE):m.start()]
    after = text[m.end():m.end() + WINDOW_AFTER]

    if AREA_FIELD.search(before):
        return "LAND", "Warwick 'Area:' agenda field -- land area", val
    # A survey sheet states the building STANDING THERE, not the one proposed.
    # "N/F" (now or formerly) and "existing building" mark existing conditions,
    # and on plan sets those figures sit right beside the proposed ones.
    if EXISTING_CONDITIONS.search(before):
        return "EXISTING", "an existing-conditions figure, not the proposed programme", val
    # Trailing context only: these qualify the number directly.
    tail = before[-40:]
    if THRESHOLD.search(tail):
        return "UNKNOWN", "a zoning threshold being compared against, not a measurement", val
    if DELTA.search(tail):
        return "UNKNOWN", "a change in floor area, not the floor area itself", val

    # The nearest cue wins, measured by distance back from the number, because
    # "the building providing 70,784 SF" and "the new lot will measure 53,582
    # SF" appear in one sentence and the closer word is the one that governs.
    # Nearest cue wins, and the search is SYMMETRIC. The governing word sits
    # after the number as often as before it -- "a 5,907 sq. ft. fast food
    # restaurant" is a building, "on a 342,780 sqft parcel" is land -- so
    # scoring only the preceding text missed half the real cases.
    def _nearest(rx):
        d = 10_000
        for mm in rx.finditer(before):
            d = min(d, len(before) - mm.end())
        for mm in rx.finditer(after):
            d = min(d, mm.start())
        return d

    # Explicit evidence, or nothing.
    strong = bool(STRONG_BEFORE.search(before)) or bool(STRONG_AFTER.match(after))
    if strong and not (LAND_BEFORE.search(before) or LAND_AFTER.match(after)):
        return "BUILDING", "an explicit building phrase governs this figure", val

    b_near = _nearest(BUILDING_CUE)
    l_near = _nearest(NOT_BUILDING)

    if b_near == 10_000 and l_near == 10_000:
        return "UNKNOWN", "no building or land vocabulary nearby", val
    if l_near <= b_near:
        return "LAND", "land vocabulary is nearer than building vocabulary", val
    # Building vocabulary is nearer, but no explicit phrase governs the figure.
    # Reported for review rather than taken.
    return "UNKNOWN", "building-ish context but no explicit floor-area phrase", val


TEXT_DIR = Path(__file__).parent.parent / "data" / "ri_pdfs" / "text"

# The stored item descriptions are truncated at 1,200 characters -- 22% of
# items hit that ceiling exactly -- so reading them is reading a summary, not
# the document. The full text is on disk; this reads that instead.
#
# But a full agenda holds every item heard that night, so the text has to be
# SCOPED to this project's own item. Searching the whole file would attribute
# a neighbouring project's square footage to this one, which is precisely the
# error class that makes a wrong number worse than a blank.
_ITEM_BOUNDARY = re.compile(
    r"(?:Case\s+No\.?\s*[\d-]|Referral\s+(?:No\.?\s*)?\d|"
    r"Petition\s+(?:No\.?\s*)?\d|ZBR-\s*\d|"
    # Cranston and Warwick separate items with these instead, and without
    # them a single window ran across seven projects on one agenda and gave
    # them all the same figure.
    r"Applicant\s*:|Petitioner\s*:|Applicant\s+seeks|Applicant\s+is\s+proposing|"
    r"\d+\.\s+[A-Z][a-z]+\s|[A-Z]\.\s+(?:Minor|Major|Administrative|Public|New|Old)\s|"
    r"AP\s+\d+,?\s+Lot|Assessor.{0,3}s\s+Plat)", re.I)
# Tight on purpose. A figure more than a few hundred characters from the
# item's own opening is more likely to belong to the next item than to this
# one, and a wrong attribution is worse than a blank.
_SCOPE_CHARS = 1100
_text_cache = {}


def _full_text(name):
    if name not in _text_cache:
        try:
            _text_cache[name] = (TEXT_DIR / name).read_text(encoding="utf-8",
                                                            errors="replace")
        except OSError:
            _text_cache[name] = ""
    return _text_cache[name]


def _scoped(doc_text, description):
    """The slice of a full agenda that belongs to this item.

    Anchored on the opening of the stored description, which is that same text
    verbatim, then run to the next item boundary. Returns None when the anchor
    cannot be found, so the caller falls back to the truncated description
    rather than swallowing the whole meeting.
    """
    if not doc_text or not description:
        return None
    probe = " ".join(description.split())[:40]
    if len(probe) < 12:
        return None
    i = doc_text.find(probe)
    if i < 0:
        doc_text = re.sub(r"\s+", " ", doc_text)
        i = doc_text.find(probe)
        if i < 0:
            return None
    tail = doc_text[i:i + _SCOPE_CHARS]
    nxt = _ITEM_BOUNDARY.search(tail, 200)
    return tail[:nxt.start()] if nxt else tail


def project_text(p, idx, full=True):
    """Every planning document for this project.

    full=True reads the untruncated source text, scoped to this project's own
    agenda item. full=False reads the stored 1,200-character descriptions.
    """
    items = idx.get((p.city.lower(), normalize_address(p.address or "")), [])
    parts = [p.description or ""]
    for it in items:
        desc = it.get("description") or ""
        seg = None
        if full and it.get("document"):
            seg = _scoped(_full_text(it["document"]), desc)
        if seg or desc:
            parts.append(seg or desc)
    return "\n".join(dict.fromkeys(x for x in parts if x)), len(items)


def text_index():
    idx = {}
    for g in collapse(load_items()):
        muni = g["municipality"].lower()
        for it in g["items"]:
            idx.setdefault((muni, normalize_address(it.get("address") or "")), []).append(it)
    return idx


def candidates_for(text):
    """Every SF figure in the text, classified."""
    out = []
    for m in NUM_SF.finditer(text):
        cls, why, val = classify(text, m)
        i = max(0, m.start() - WINDOW_BEFORE)
        out.append({
            "value": val, "class": cls, "why": why,
            "quote": re.sub(r"\s+", " ", text[i:m.end() + WINDOW_AFTER]).strip(),
        })
    return out


def reject_shared(results):
    """Drop any figure that the same document handed to more than one project.

    Two projects on one agenda legitimately never share a building size. When
    they do, the window ran across an item boundary and the figure belongs to
    at most one of them -- and there is no way to tell which, so neither keeps
    it. This is the guard that caught seven Cranston rows all being given the
    same 20,098 sq ft.
    """
    from collections import defaultdict
    seen = defaultdict(list)
    for r in results:
        if r.get("sf"):
            seen[r["sf"]].append(r["id"])
    shared = {v for v, ids in seen.items() if len(ids) > 1}
    for r in results:
        if r.get("sf") in shared:
            r["rejected"] = ("figure also extracted for project(s) %s from the same "
                             "agenda -- the item window overlapped, so it cannot be "
                             "attributed" % ", ".join(str(i) for i in seen[r["sf"]] if i != r["id"]))
            r["sf"] = None
    return results


def building_sf(cands):
    """The building figure, or None when the evidence does not support one.

    Several distinct building figures (a first floor and a second floor, say)
    are NOT summed -- adding them would be deriving a total the filing never
    stated. The largest is proposed and the rest reported alongside it.
    """
    b = [c for c in cands if c["class"] == "BUILDING"]
    if not b:
        return None, b
    vals = sorted({c["value"] for c in b})
    return max(vals), b
