r"""
Recover the OTHER addresses a project is filed under.

WHY. A Rhode Island case routinely spans several parcels and names them all:

    Case No. 19-051 UDR - 311 Knight Street, 321 Knight and 1077 Westminster
    Street. Applicant: K & S Development. The applicant is proposing to
    construct a five-story, 34-unit, mixed use building...

The ingest keeps the FIRST address and drops the rest, so that project is in
the tracker as "311 Knight Street" and a search for 1077 Westminster Street
returns nothing. The operator asked whether the tracker held 1077 Westminster
and the honest answer looked like no. It held it, under another door number.

This reads each project's own agenda item, pulls every street address in the
case heading, and stores the ones that are not already the primary address in
alt_addresses. Search then covers them.

GUARD. Only the case heading is read -- the text from the case number up to
the applicant clause -- not the whole item. An agenda item's body mentions
abutters, nearby streets and previous cases, and hoovering those up would
attach a neighbour's address to this project. The heading is the part that
names the site.

    python scraper/ri_alt_addresses.py
    python scraper/ri_alt_addresses.py --apply
"""

import re
import sys
import json
import logging
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from db.database import get_session
from db.models import Project
from scraper.ri_sf_extract import text_index, project_text, RI

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)
OUT = ROOT / "data" / "ri_alt_addresses.json"

STREET = (r"(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Boulevard|Blvd|Lane|Ln|Way|"
          r"Place|Pl|Court|Ct|Terrace|Ter|Highway|Hwy|Square|Sq|Parkway|Pkwy|Wharf|Pike)")

# A street address: number, one to three capitalised words, a street type.
ADDR = re.compile(
    r"\b(\d{1,5}(?:\s*[-–]\s*\d{1,5})?)\s+"
    r"((?:[A-Z][A-Za-z'\.]*\s+){0,3}?[A-Z][A-Za-z'\.]*)\s+"
    r"(" + STREET + r")\b\.?", re.I)

# The heading runs from the start of the item to whoever is applying. Beyond
# that lies the narrative, which names abutters and other sites.
HEAD_END = re.compile(
    r"\b(?:Applicant|Petitioner|Owner|Applicant/Owner|Proposal|The\s+applicant)\b", re.I)


# The meeting VENUE is not a project address. An agenda heading carries the
# city hall it was heard in, so "4332 Post Road" picked up "3275 POST ROAD",
# which is Warwick City Hall. These are the meeting places, not sites.
VENUE = {
    "3275 post rd", "3275 post road",           # Warwick City Hall
    "869 park ave", "869 park avenue",          # Cranston planning
    "25 dorrance st", "25 dorrance street",     # Providence City Hall
    "444 westminster st", "444 westminster street",
    "137 roosevelt ave", "137 roosevelt avenue",  # Pawtucket City Hall
    "43 broadway",                              # Newport City Hall
    "65 centerville rd", "65 centerville road",   # Warwick annex
}


def norm(a):
    a = re.sub(r"\s+", " ", (a or "")).strip().lower()
    a = re.sub(r"[^a-z0-9 ]", "", a)
    ab = {"street": "st", "avenue": "ave", "road": "rd", "drive": "dr",
          "boulevard": "blvd", "place": "pl", "court": "ct", "lane": "ln",
          "terrace": "ter", "highway": "hwy", "square": "sq", "parkway": "pkwy"}
    return " ".join(ab.get(w, w) for w in a.split())


def heading(text, address):
    """The case heading containing this project's address."""
    if not text:
        return ""
    key = re.escape((address or "").strip()[:14])
    m = re.search(key, text, re.I) if key else None
    start = max(0, m.start() - 130) if m else 0
    seg = text[start:start + 420]
    cut = HEAD_END.search(seg)
    return seg[:cut.start()] if cut else seg


def main(apply=False):
    idx = text_index()
    session = get_session()
    rows = [p for p in session.query(Project).filter(Project.city.in_(RI)).all()]

    found = []
    for p in rows:
        text, _ = project_text(p, idx)
        head = heading(text or p.description or "", p.address)
        if not head:
            continue
        prim = norm(p.address)
        alts = []
        for m in ADDR.finditer(head):
            cand = re.sub(r"\s+", " ", "%s %s %s" % (m.group(1), m.group(2), m.group(3))).strip()
            n = norm(cand)
            # Not the primary, not a duplicate, and not a bare plat reference.
            if (n and n != prim and n not in VENUE
                    and n not in [norm(a) for a in alts] and len(n) > 6):
                alts.append(cand)
        if alts:
            found.append({"id": p.id, "city": p.city, "primary": p.address,
                          "alts": alts, "heading": re.sub(r"\s+", " ", head)[:180]})

    log.info("\nProjects filed under more than one address: %d", len(found))
    for r in found[:30]:
        log.info("  id=%-4d %-11s %-30s  also: %s", r["id"], r["city"],
                 str(r["primary"])[:30], "; ".join(r["alts"])[:70])

    OUT.write_text(json.dumps(found, indent=1, ensure_ascii=False), encoding="utf-8")

    if apply:
        for r in found:
            p = session.get(Project, r["id"])
            p.alt_addresses = "; ".join(r["alts"])
            p.notes = ((p.notes + " | ") if p.notes else "") + (
                "Also filed as: %s. Recovered from the case heading, which names every "
                "parcel in the application; the ingest kept only the first."
                % "; ".join(r["alts"]))
        session.commit()
        log.info("\nAPPLIED to %d projects", len(found))
    else:
        log.info("\nDRY RUN -- re-run with --apply")
    session.close()


if __name__ == "__main__":
    main(apply="--apply" in sys.argv)
