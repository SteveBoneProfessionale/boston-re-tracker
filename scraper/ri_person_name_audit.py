r"""
Audit every developer name in the tracker for the wrong KIND of name.

The failure this exists to catch: a land use attorney, an architect, an
engineer or a municipal official appearing on a filing and being recorded as
the developer. Those people appear on filings routinely and are not developers,
and the mistake is the one most likely to embarrass someone in front of a
contact. It has already happened three times in this dataset -- Warwick's
planning director Thomas Kravitz, Warwick planning staffer Daniel Geagan, and
Providence councilman and land use attorney Seth Yurdin.

Reports three tiers, worst first:

  KNOWN_PROFESSIONAL  the name matches someone recorded here as an attorney,
                      architect, engineer or official. Treat as wrong until
                      shown otherwise.
  TITLED              the name carries a professional title (Esq., AIA, PE) or
                      a courtesy title, which means it was captured from a
                      representation line.
  PERSON_SHAPED       a personal name with no company form at all. NOT
                      automatically wrong -- sole proprietors and family trusts
                      do develop -- but it needs an eye.

REPORTS ONLY. Nothing is nulled here; the list is for review.

    python scraper/ri_person_name_audit.py
"""

import re
import sys
import json
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import get_session
from db.models import Project

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

OUT = Path(__file__).parent.parent / "data" / "ri_person_name_audit.json"

# People encountered in this corpus who are NOT developers, with their role.
# Extend this as more are found -- it is the cheapest guard available.
KNOWN_PROFESSIONALS = {
    "joseph shekarchi": "attorney (also Speaker of the RI House); appears for petitioners across Warwick",
    "shekarchi": "attorney (also Speaker of the RI House)",
    "thomas kravitz": "Warwick Planning Director",
    "tom kravitz": "Warwick Planning Director",
    "daniel geagan": "Warwick Planning Department staff; reads recommendations into the record",
    "seth yurdin": "Providence city councilman (Ward 1) and land use attorney",
    "sandy resnick": "attorney; appeared for the Oakland Beach duplex petitioner",
    "john c. revens": "attorney",
    "john revens": "attorney",
    "peter friedrichs": "municipal planner (Newport City Planner)",
    "ed wojcik": "architect",
    "edward wojcik": "architect",
    "jack ryan": "architect (Matilda Overlook)",
    "david sisson": "architect (551 Chalkstone)",
    "joshua kline": "engineer (Stonefield Engineering)",
    "joshua h. kline": "engineer (Stonefield Engineering)",
    "marc greenfield": "Providence City Plan Commission chair",
    "frank picozzi": "Mayor of Warwick",
    "brett smiley": "Mayor of Providence",
    "jorge elorza": "former Mayor of Providence",
    "alexis thompson": "municipal staff",
}

# Firms that are consultants on a filing rather than the developer.
CONSULTANT_FIRMS = re.compile(
    r"\b(?:diprete|stonefield|fuss\s*&?\s*o'?neill|joe\s+casali|civil\s+design\s+group|"
    r"zds|z-ds|truth\s+box\s+architect|dbvw|elkus\s+manfredi|ayers\s+saint\s+gross|"
    r"union\s+studio|shawmut|new\s+england\s+construction|gilbane)\b", re.I)

TITLE = re.compile(r"\b(?:Esq\.?|AIA|P\.?E\.?|RLA|AICP|LEED|Attorney|Atty\.?|"
                   r"Mr\.?|Mrs\.?|Ms\.?|Dr\.?|Hon\.?)\b", re.I)

# Any token that makes a string read as a company rather than a person.
COMPANY_FORM = re.compile(
    r"\b(?:LLC|L\.L\.C\.|Inc\.?|Corp\.?|Corporation|Company|Co\.?|LP|L\.P\.|LLP|Ltd\.?|"
    r"Trust|Realty|Associates|Partners|Partnership|Properties|Group|Enterprises|"
    r"Holdings|Builders|Construction|Capital|Ventures|Communities|Homes|Development|"
    r"Developments|Authority|Agency|University|College|School|Church|Institute|"
    r"Foundation|Hospital|Bank|City\s+of|Town\s+of|State\s+of|Department|Housing|"
    r"Redevelopment|Investments?|Management|Equities|Fund|Cathedral|Academy|Centers?|"
    # Company names that are two ordinary capitalised words and would
    # otherwise read as a person: Arx Urban, Winn Companies, True Storage,
    # Just A Start, Urban Spaces, Shanti Acquisition, Avenue Concept.
    r"Companies|Operations|Acquisitions?|Urban|Storage|Residential|Rehab|"
    r"Spaces|Concept|Start|Solutions|Services|Systems|Advisors|Studio|"
    r"Design|Architects|Realty|Estate|Land|Metro|Global|National|Union)\b",
    re.I)

# A personal name: two or three capitalised words, optionally with an initial.
PERSON_SHAPE = re.compile(
    r"^[A-Z][a-z]{1,14}(?:\s+[A-Z]\.?)?\s+[A-Z][a-z'\-]{1,18}(?:\s+(?:Jr|Sr|II|III|IV)\.?)?$")
PERSON_SHAPE_CAPS = re.compile(
    r"^[A-Z][A-Z'\-]{1,14}(?:\s+[A-Z]\.?)?\s+[A-Z][A-Z'\-]{1,18}$")


def classify(name):
    if not name:
        return None, None
    n = " ".join(name.split())
    low = n.lower()

    for who, role in KNOWN_PROFESSIONALS.items():
        if who in low:
            return "KNOWN_PROFESSIONAL", role
    if CONSULTANT_FIRMS.search(n):
        return "KNOWN_PROFESSIONAL", "consultant firm (engineer/architect/builder), not the developer"
    if TITLE.search(n):
        return "TITLED", "carries a professional or courtesy title -- captured from a representation line"

    if COMPANY_FORM.search(n):
        return None, None
    # Strip a joint "and" so "David and Sharon Cutts" is still seen as people.
    parts = [x.strip() for x in re.split(r"\s+(?:and|&)\s+", n) if x.strip()]
    if parts and all(PERSON_SHAPE.match(x) or PERSON_SHAPE_CAPS.match(x) for x in parts):
        return "PERSON_SHAPED", ("personal name with no company form"
                                 + (" (joint)" if len(parts) > 1 else ""))
    return None, None


def main():
    session = get_session()
    rows = session.query(Project).all()
    RI = {"Providence", "Warwick", "Cranston", "Pawtucket", "Newport"}
    found = []
    for p in rows:
        for field in ("developer", "owner_or_agency"):
            tier, why = classify(getattr(p, field, None))
            if not tier:
                continue
            found.append({
                "id": p.id, "city": p.city, "address": p.address or "",
                "market": "Rhode Island" if p.city in RI else "Boston/Cambridge",
                "field": field, "name": getattr(p, field),
                "tier": tier, "why": why,
                "method": p.developer_resolution_method,
                "units": p.residential_units, "gsf": p.total_gsf,
            })
    session.close()

    order = {"KNOWN_PROFESSIONAL": 0, "TITLED": 1, "PERSON_SHAPED": 2}
    # Rhode Island first: the other markets' names come from a different
    # pipeline and are listed for completeness, not as part of this audit.
    found.sort(key=lambda f: (f["market"] != "Rhode Island", order[f["tier"]],
                              f["city"] or "", f["id"]))
    OUT.write_text(json.dumps(found, indent=1, ensure_ascii=False), encoding="utf-8")

    from collections import Counter
    c = Counter(f["tier"] for f in found)
    ri = [f for f in found if f["market"] == "Rhode Island"]
    log.info("Projects scanned            : %d (%d Rhode Island)",
             len(rows), sum(1 for p in rows if p.city in RI))
    log.info("  Rhode Island names to review: %d", len(ri))
    log.info("  other markets (FYI only)    : %d", len(found) - len(ri))
    log.info("Developer names needing review: %d", len(found))
    for t in ("KNOWN_PROFESSIONAL", "TITLED", "PERSON_SHAPED"):
        log.info("    %-20s %3d", t, c.get(t, 0))
    log.info("Wrote %s", OUT)


if __name__ == "__main__":
    main()
