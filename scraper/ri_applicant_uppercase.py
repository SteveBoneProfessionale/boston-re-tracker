r"""
Recover applicants written as UPPERCASE NAME followed by a role parenthetical.

Providence and Cranston zoning agendas name the party like this:

    IMPERIAL INVESTMENTS, INC (Applicant/Owner): 7 Mount Hope Ave ...
    JESUS R. ACOSTA (Applicant/Lessee) and R A REALTY (Owner): 221 Academy Ave
    UNISON WEST FOUNTAIN LLC (Applicant): 488 Washington St ...

The prose extractor anchors on a verb ("X seeks", "X proposes") or a labelled
"Applicant:" line, and this form has neither -- the role sits in parentheses
AFTER the name. 68 of the 148 records with no applicant carry a name in this
shape, so these were searches waiting to be paid for on data already on disk.

Runs against stored descriptions rather than requiring a full re-extract, and
writes only where the applicant is currently null.
"""

import re
import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import init_db, get_session
from db.models import Project

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

UPPER_ROLE = re.compile(
    r"([A-Z][A-Z0-9&'\.,\- ]{3,60}?)\s*"
    r"\((?:Applicant|Owner|Petitioner|Lessee|APP|OWN)[^)]{0,30}\)")

# Meeting furniture that can sit immediately before the parenthetical and is
# never a party name. "PLAN COMMISSION RECOMMENDATIONS MARC..." matched the
# shape but names a board, not an applicant.
NOT_A_PARTY = re.compile(
    r"\b(PLAN COMMISSION|ZONING BOARD|BOARD OF REVIEW|RECOMMENDATIONS?|AGENDA|"
    r"MINUTES|MOTION|MEETING|PUBLIC HEARING|COMMISSIONERS?|DEPARTMENT|"
    r"CONTINUED|WITHDRAWN|APPROVED|DENIED|VOTE|ITEM|CASE NO)\b", re.I)


def applicant_from(text: str):
    """The party named in UPPERCASE-plus-role form, or None."""
    for m in UPPER_ROLE.finditer(text or ""):
        name = re.sub(r"\s+", " ", m.group(1)).strip(" ,.;:-")
        # A conjunction picked up from the preceding clause is not part
        # of the name: "... AND CALISE DEVELOPMENT, LLC".
        name = re.sub(r"^(?:AND|OR|THE|BY|FOR|WITH)\s+", "", name).strip()
        if len(name) < 4 or NOT_A_PARTY.search(name):
            continue
        # A name that is only initials or a single short token is not usable.
        if len(name.replace(" ", "")) < 5:
            continue
        return name
    return None


def run(dry_run: bool = False) -> dict:
    init_db()
    session = get_session()
    found, skipped = 0, 0
    try:
        projects = [p for p in session.query(Project)
                    .filter(Project.bpda_url.like("manual:ri-%")).all()
                    if not (p.applicant_entity or "").strip()]
        for p in projects:
            name = applicant_from(p.description)
            if not name:
                skipped += 1
                continue
            if not dry_run:
                p.applicant_entity = name
                # Still an agenda value -- the filing did state it, the old
                # pattern just could not see it. NOT marked web-sourced.
                p.applicant_source = "agenda"
            found += 1
            if found <= 12:
                log.info("  %-11s %-28s -> %s", p.city, str(p.address)[:27], name[:44])
        if not dry_run:
            session.commit()
    finally:
        session.close()
    log.info("Applicants recovered from agenda text: %d (no match: %d)", found, skipped)
    return {"found": found, "skipped": skipped}


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    run(ap.parse_args().dry_run)
