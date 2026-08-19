r"""
Move public bodies out of the developer field.

THE STANDING CONVENTION, already agreed for this tracker: developer means the
party EXECUTING the development. A public agency, a redevelopment authority or
a passive landowner is not the developer even when it is the named applicant.
Those belong in owner_or_agency.

It had never been applied to the applicant-derived names, so "City of
Providence" was the single most active developer in Providence -- which is a
city, not a developer. Brown University, the Providence Redevelopment Agency,
the State of Rhode Island and school departments were all sitting in the same
column as Churchill & Banks.

WHAT IS AND IS NOT A PUBLIC BODY. A housing authority is. A non-profit
developer is NOT -- Crossroads Rhode Island builds and operates buildings, and
removing it would lose a genuine and active developer. So the test is on
governmental form, not on tax status: cities, towns, states, agencies,
authorities, commissions, departments, districts, public schools and public
universities. Named non-profits and private universities are left alone unless
they are unambiguously an arm of government.

The original value is preserved in owner_or_agency, never discarded.

    python scraper/clean_public_developers.py
    python scraper/clean_public_developers.py --apply
"""

import re
import sys
import json
import logging
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from db.database import get_session
from db.models import Project

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)
OUT = ROOT / "data" / "public_developer_cleanup.json"

PUBLIC = re.compile(
    r"^\s*(?:city|town)\s+of\b|"
    r"\bstate\s+of\s+rhode\s+island\b|\bcommonwealth\s+of\b|"
    r"\bredevelopment\s+(?:agency|authority)\b|"
    r"\bhousing\s+authority\b|"
    r"\bpublic\s+(?:buildings|schools?|library|works)\b|"
    r"\bschool\s+(?:department|committee|district)\b|"
    r"\b(?:department|dept)\s+of\s+\w+|"
    r"\bpublic\s+buildings\s+authority\b|"
    r"\bwater\s+supply\s+board\b|\bsewer\s+(?:authority|commission)\b|"
    r"\b(?:rhode\s+island|ri)\s+(?:department|division|commerce|housing|turnpike|airport)\b|"
    r"\bcommission\b(?!\s*,?\s*(?:llc|inc))|"
    r"\bboard\s+of\s+(?:education|trustees)\b|"
    r"\bmunicipal\b|\bcounty\s+of\b|"
    r"\bprovidence\s+(?:redevelopment|public\s+buildings|housing)\b|"
    r"\bhousing\s+trust\b",
    re.I)

# Explicitly NOT public, even though the pattern might reach for them. These
# build and operate real buildings and are among the more active developers in
# the market -- losing them would be worse than the problem being fixed.
KEEP = re.compile(
    r"crossroads\s+rhode\s+island|pennrose|preservation\s+of\s+affordable|"
    r"omni\s+development|community\s+health|wingate|pawtucket\s+central\s+falls|"
    r"federal\s+property\s+group",
    re.I)


def split_jv(name):
    """A joint venture between a private developer and a public body.

    "Leggat McCall Properties / Boston Housing Authority" is not a public
    project with no developer -- the private partner is the one executing it.
    Returns (developer_part, public_part) or (None, None).
    """
    parts = [x.strip() for x in re.split(r"\s*/\s*|\s+and\s+", name or "") if x.strip()]
    if len(parts) < 2:
        return None, None
    pub = [x for x in parts if PUBLIC.search(x) and not KEEP.search(x)]
    priv = [x for x in parts if x not in pub]
    if pub and priv:
        return " / ".join(priv), " / ".join(pub)
    return None, None


def is_public(name):
    n = (name or "").strip()
    if not n or KEEP.search(n):
        return False
    return bool(PUBLIC.search(n))


def main(apply=False):
    session = get_session()
    rows = [p for p in session.query(Project).all() if not p.excluded]
    moved, split = [], []
    for p in rows:
        for field in ("developer", "developer_canonical"):
            v = getattr(p, field, None)
            if not v:
                continue
            dev, pub = split_jv(v)
            if dev:
                split.append({"id": p.id, "city": p.city, "value": v, "keep": dev, "agency": pub})
                break
            if is_public(v):
                moved.append({"id": p.id, "city": p.city, "field": field, "value": v})
                break

    log.info("\nPUBLIC BODIES SITTING IN THE DEVELOPER FIELD: %d rows", len(moved))
    for v, n in Counter(m["value"] for m in moved).most_common(30):
        log.info("   %3d  %s", n, v[:66])
    log.info("\n   by city: %s", dict(Counter(m["city"] for m in moved)))

    log.info("\nJOINT VENTURES -- private partner kept as developer: %d", len(split))
    for m in split:
        log.info("   id=%-4d %r -> developer %r, owner_or_agency %r",
                 m["id"], m["value"][:44], m["keep"][:34], m["agency"][:30])

    if apply:
        for m in split:
            p = session.get(Project, m["id"])
            if not p.owner_or_agency:
                p.owner_or_agency = m["agency"]
            p.developer = m["keep"]
            p.notes = ((p.notes + " | ") if p.notes else "") + (
                "developer split from %r: the private partner executes the development, the "
                "public body is recorded as owner_or_agency." % m["value"][:80])
        for m in moved:
            p = session.get(Project, m["id"])
            keep = p.developer or p.developer_canonical
            if not p.owner_or_agency:
                p.owner_or_agency = keep
            p.developer = None
            p.developer_canonical = None
            p.developer_resolution_method = None
            p.notes = ((p.notes + " | ") if p.notes else "") + (
                "developer %r moved to owner_or_agency: a public body is not the party "
                "executing the development, per the standing convention. The name is kept, "
                "not discarded." % keep[:80])
        session.commit()
        log.info("\nAPPLIED -- %d rows moved to owner_or_agency", len(moved))
    else:
        log.info("\nDRY RUN -- re-run with --apply")

    OUT.write_text(json.dumps(moved, indent=1), encoding="utf-8")
    session.close()


if __name__ == "__main__":
    main(apply="--apply" in sys.argv)
