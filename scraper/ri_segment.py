r"""
Segment Rhode Island agendas into items, per municipality.

Written after reading the actual documents. The previous approach anchored on
parcel references and bounded blocks by character windows, which fails because
agenda items have no fixed length -- blocks bled into neighbours and every
downstream field was thin as a result.

Each municipality has a real, learnable item structure:

PROVIDENCE (City Plan Commission)
    A section header naming the item type, then a numbered item:

        MINOR LAND DEVELOPMENT PROJECT
        2. Case no 26-047MIL - 203 Douglas Ave
           Owner: PMP Group LLC
           The applicant is proposing to construct a mixed use building ...
           - for vote (AP 68 Lot 846, Smith Hill)

    CRITICAL: every CPC agenda ends with an "Administrative Officer's report on
    administrative approvals" -- a semicolon-run-on of lot-line actions already
    granted administratively ("26-013A - Reconfiguration of AP 95 Lots 631 and
    668 at 80 Erastus Street; ..."). These are NOT board development items and
    must not become projects. Treating them as items is what inflated
    Providence's count and left those records with no applicant, because none
    is named. They are parsed separately and marked administrative.

I-195 REDEVELOPMENT DISTRICT
    Narrative numbered items referencing District parcels, not assessor plats:

        5. Presentation by Preservation of Affordable Housing, Inc. regarding a
           proposed development on Parcel 41.

    A parcel-anchored parser misses these completely, yet they name the
    developer directly -- these are the district's actual pipeline.

PAWTUCKET
        Major Land Development Project, Master Plan Review    (VOTE TAKEN)
        180 Weeden Street (AP 44 Lot 561)
        180 Weeden St LLC seeks Master Plan Approval for ...

NEWPORT
        App. No. 2025-DPR-04
        0 (525) Broadway, TAP 6, Lot 1, R-10 Residential
        Application of 525 Broadway LLC, owner and applicant, to construct ...
"""

import re
import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

log = logging.getLogger(__name__)

# Tail sections that are not agenda items. Everything from here on is dropped
# before segmentation.
_TAIL = re.compile(
    r"(IMPORTANT INFORMATION|Any individual requiring|POSTED:)", re.I | re.M)

# Boilerplate that repeats as a PAGE FOOTER mid-document. These must be deleted
# in place, never used as a truncation point: "All Planning Commission meetings
# are open..." appears on every page of a Pawtucket agenda, so truncating at the
# first occurrence cut the agenda off before its own items and yielded zero.
_FOOTER = re.compile(
    r"^.*(All Planning Commission meetings are open|"
    r"meeting location is accessible to people with disabilities|"
    r"in need of interpreter services|not less than two \(2\) business days|"
    r"REMOTE ACCESS INFORMATION|Zoom webinar information|"
    r"Phone one-tap|Telephone: \+1|Webinar ID|International numbers|"
    r"live streamed on the City|youtube\.com).*$", re.I | re.M)

# The Providence administrative-approvals run-on, parsed separately.
# Greedy to end of text: the run-on spans many lines and a non-greedy match
# stops at the first blank line, which yielded zero entries.
_ADMIN_REPORT = re.compile(
    # The apostrophe is a curly U+2019 in the PDF text, not a straight quote.
    r"Administrative Officer['’]?s report on administrative approvals:?\s*(.+)$",
    re.I | re.S)
_ADMIN_ENTRY = re.compile(
    r"(\d{2}-\d{2,4}\s*[A-Z]{1,4})\s*[–\-]\s*([^;]+)")

# Providence section headers give the item classification.
_PVD_SECTION = re.compile(
    r"^\s*((?:MAJOR|MINOR|ADMINISTRATIVE)\s+(?:LAND DEVELOPMENT PROJECT|SUBDIVISION)"
    r"[A-Z \-–—]*|CITY COUNCIL REFERRAL|PUBLIC HEARING|INSTITUTIONAL MASTER PLAN)\s*$",
    re.M)
_PVD_ITEM = re.compile(r"^\s{0,4}(\d{1,2})\.\s+(.+?)(?=^\s{0,4}\d{1,2}\.\s|\Z)",
                       re.M | re.S)

_NUM_ITEM = re.compile(r"^\s{0,6}(\d{1,2})[\.\)]\s+(.+?)(?=^\s{0,6}\d{1,2}[\.\)]\s|\Z)",
                       re.M | re.S)

# Pawtucket: a stage line (with the vote marker) followed by address + parcel.
# The stage line may carry a parenthesised abbreviation before the vote marker
# ("Unified Development Review (DPR), Preliminary Plan (VOTE TAKEN)"). Without
# parentheses in the class the boundary lookahead cannot reach the next item's
# marker, so two projects merged into one block and 45 Division Street was
# credited with 65 East Street's applicant.
_PAW_STAGE = r"[A-Z][A-Za-z ,/&\-\.()]{6,110}?"
_PAW_ITEM = re.compile(
    rf"^[ \t]*({_PAW_STAGE})[ \t]*\((VOTE TAKEN|NO VOTE[A-Z ]*)\)[ \t]*\n"
    rf"[ \t]*(.+?)\n(.*?)(?=^[ \t]*{_PAW_STAGE}[ \t]*\((?:VOTE TAKEN|NO VOTE)|\Z)",
    re.M | re.S)

# Newport: application number then the parcel/zoning line.
_NPT_ITEM = re.compile(
    r"App\.?\s*No\.?\s*([\w\-]+)\s*(.+?)(?=App\.?\s*No\.?\s*[\w\-]+|\Z)",
    re.S | re.I)


def strip_tail(text: str) -> tuple[str, str]:
    """Item body and trailing boilerplate, with page footers removed in place.

    A tail marker only counts as the tail if it actually appears near the end.
    Warwick prints "Posted: April 2, 2026" in its letterhead, so treating the
    first match as the cut point discarded the entire agenda and returned zero
    items for the municipality.
    """
    cleaned = _FOOTER.sub("", text)
    floor = len(cleaned) // 2
    for m in _TAIL.finditer(cleaned):
        if m.start() >= floor:
            return cleaned[:m.start()], cleaned[m.start():]
    return cleaned, ""


def providence_admin_approvals(text: str) -> list[dict]:
    """The administrative-approvals run-on, as separate non-project records.

    These are lot-line actions already granted by the administrative officer.
    They are recorded so the data is not lost, but flagged administrative so
    they never inflate the development pipeline.
    """
    out = []
    m = _ADMIN_REPORT.search(text)
    if not m:
        return out
    for case, desc in _ADMIN_ENTRY.findall(m.group(1)):
        d = re.sub(r"\s+", " ", desc).strip(" ;.")
        out.append({
            "case_number": case.replace(" ", ""),
            "description": d,
            "administrative": True,
            "applicant_entity": None,
            "review_stage": "Administrative Review",
            "advances_stage": True,
        })
    return out


def providence_items(text: str) -> list[dict]:
    """Numbered CPC items, with their section header as classification."""
    body, _tail = strip_tail(text)

    # Map each item's offset to the nearest preceding section header.
    sections = [(m.start(), m.group(1).strip()) for m in _PVD_SECTION.finditer(body)]

    def section_at(pos: int) -> str | None:
        cur = None
        for p, name in sections:
            if p < pos:
                cur = name
            else:
                break
        return cur

    items = []
    for m in _PVD_ITEM.finditer(body):
        block = m.group(2)
        if len(block.strip()) < 25:
            continue
        items.append({
            "number": int(m.group(1)),
            "section": section_at(m.start()),
            "text": re.sub(r"[ \t]+", " ", block).strip(),
        })
    return items


# I-195 notices extract as ONE continuous line, so a line-anchored numbering
# pattern never fires. Split on numbering wherever it appears instead.
_INLINE_ITEM = re.compile(r"(?:(?<=\s)|^)(\d{1,2})\.\s+(.+?)(?=(?:\s\d{1,2}\.\s)|\Z)", re.S)


def i195_items(text: str) -> list[dict]:
    """I-195 District items, which reference District parcels not plats."""
    body, _ = strip_tail(text)
    items = []
    for m in _INLINE_ITEM.finditer(body):
        block = re.sub(r"\s+", " ", m.group(2)).strip()
        if not re.search(r"\bparcels?\s+\d+", block, re.I):
            continue          # only parcel-bearing items are pipeline
        if re.search(r"public comment|minutes of the|adjourn|budget|handbook|"
                     r"employee|policy regarding", block, re.I):
            continue
        items.append({"number": int(m.group(1)), "section": None, "text": block})
    return items


def pawtucket_items(text: str) -> list[dict]:
    body, _ = strip_tail(text)
    items = []
    for m in _PAW_ITEM.finditer(body):
        stage_line, vote, addr_line, desc = m.groups()
        items.append({
            "section": stage_line.strip(),
            "vote_marker": vote.strip(),
            "address_line": re.sub(r"\s+", " ", addr_line).strip(),
            "text": re.sub(r"[ \t]+", " ", f"{stage_line} | {addr_line} | {desc}").strip(),
        })
    return items


def newport_items(text: str) -> list[dict]:
    body, _ = strip_tail(text)
    items = []
    for m in _NPT_ITEM.finditer(body):
        app_no, rest = m.group(1), re.sub(r"\s+", " ", m.group(2)).strip()
        if len(rest) < 30:
            continue
        items.append({"case_number": app_no, "section": None, "text": rest[:1600]})
    return items


# Warwick items are LETTERED (A., B., C.) beneath roman-numeral sections, and
# carry a labelled block -- the richest structure of the five municipalities:
#
#     A. Major Land Development/Unified Development Review - Master Plan
#        Applicant seeking to construct 7 duplexes, totaling 14 dwelling units.
#        Specific project location:  Posnegansett Ave
#        Assessor's Plat:            300
#        Assessor's Lot:             110, 128, 247, 295, 296 & 331
#        Applicant:                  Seaview Realty, LLC.
#        Zoning:                     Residence A-10 District
#        Land Area:                  4.11 AC total
#
# A numbered-item splitter finds none of this, which is why Warwick was empty.
_WAR_ITEM = re.compile(r"^[ \t]*([A-Z])\.\s+(.+?)(?=^[ \t]*[A-Z]\.\s|^[ \t]*[IVX]{1,4}\.\s|\Z)",
                       re.M | re.S)

# Cranston items are BULLETED with U+25AA, under an all-caps section header,
# and likewise carry labelled lines (Zoning District:, Owner/Applicant:, AP..).
_CRA_SECTION = re.compile(
    r"^[ \t]*((?:PUBLIC HEARING|EXTENSION REQUEST|NEW BUSINESS|OLD BUSINESS|"
    r"ADMINISTRATIVE|MASTER PLAN|PRELIMINARY PLAN|FINAL PLAN|"
    r"DEVELOPMENT PLAN REVIEW|ZONING (?:ORDINANCE )?(?:AMENDMENT|REFERRAL)|"
    r"COMPREHENSIVE PLAN)[A-Z '\-/&]*)[ \t]*$", re.M)
_CRA_ITEM = re.compile(r"^[ \t]*[▪●•]\s*(.+?)(?=^[ \t]*[▪●•]\s|\Z)",
                       re.M | re.S)


def warwick_items(text: str) -> list[dict]:
    """Warwick lettered items with their labelled application block."""
    body, _ = strip_tail(text)
    items = []
    for m in _WAR_ITEM.finditer(body):
        block = re.sub(r"[ \t]+", " ", m.group(2)).strip()
        if len(block) < 40:
            continue
        # The header line names the review type; the labelled block follows.
        if not re.search(r"assessor|applicant|plat|land development|subdivision|"
                         r"pre-?application|development plan", block, re.I):
            continue
        items.append({"letter": m.group(1), "number": None,
                      "section": block.split("\n")[0][:120], "text": block[:2500]})
    return items


def cranston_items(text: str) -> list[dict]:
    """Cranston bulleted items, tagged with the all-caps section they sit under."""
    body, _ = strip_tail(text)
    sections = [(m.start(), m.group(1).strip()) for m in _CRA_SECTION.finditer(body)]

    def section_at(pos: int) -> str | None:
        cur = None
        for p, name in sections:
            if p < pos:
                cur = name
            else:
                break
        return cur

    items = []
    for m in _CRA_ITEM.finditer(body):
        block = re.sub(r"[ \t]+", " ", m.group(1)).strip()
        if len(block) < 40:
            continue
        # Minutes-approval bullets are just a meeting date, not a project.
        if re.match(r"^[A-Z][a-z]+ \d{1,2}, \d{4}\b", block):
            continue
        items.append({"number": None, "section": section_at(m.start()),
                      "text": block[:2500]})
    return items


def generic_items(text: str) -> list[dict]:
    """Fallback: numbered items, parcel-bearing only."""
    body, _ = strip_tail(text)
    items = []
    for m in _NUM_ITEM.finditer(body):
        block = re.sub(r"[ \t]+", " ", m.group(2)).strip()
        if len(block) < 30:
            continue
        items.append({"number": int(m.group(1)), "section": None, "text": block[:1600]})
    return items


SEGMENTERS = {
    "Providence": providence_items,
    "Pawtucket": pawtucket_items,
    "Newport": newport_items,
    "Cranston": cranston_items,
    "Warwick": warwick_items,
}


def segment(municipality: str, text: str, entity_id: int | None = None) -> list[dict]:
    if entity_id == 1531:                 # I-195 Redevelopment District
        return i195_items(text)
    fn = SEGMENTERS.get(municipality, generic_items)
    return fn(text)
