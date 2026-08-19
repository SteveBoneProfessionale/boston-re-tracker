r"""
Write the design-team readings that were extracted but never stored.

Two fields were being set on the ORM object without existing as columns --
contractor and attorney -- so 63 attorney names and 5 contractors were read,
reported as applied, and silently discarded. SQLAlchemy takes an assignment to
an unmapped name without complaint; it just never reaches the table. The
column now exists, contractor maps onto general_contractor, and this replays
the two saved extractions instead of paying for the reads again.

It also splits the values that came back as "person of firm" -- "Eric Zuena,
ZDS Architects & Interiors", "Justin Hedde of Centerbrook Architects" -- into
the two fields they belong in, and folds the ZDS spellings together.
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
from scraper import ri_design_team as DT

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

SOURCES = [(ROOT / "data" / "ri_planset_llm.json", "plan_set"),
           (ROOT / "data" / "ri_arch_corpus.json", "filing")]

# "Eric Zuena, ZDS Architects & Interiors" / "Justin Hedde of Centerbrook".
# A slash is NOT one of these. "Estes Twombly/Titrington Architects" is a
# joint venture between two practices, and splitting on it threw away half the
# name and filed the other half as a person.
SPLIT = re.compile(r"^(.{3,40}?)\s*(?:,\s*(?:of\s+)?|\s+of\s+)(.{4,60})$")
# ZDS files under several spellings, and one is an OCR slip.
FOLD = {"zds": "ZDS Architects & Interiors",
        "fzds architects": "ZDS Architects & Interiors",
        "zds, inc": "ZDS Architects & Interiors",
        "zds architects & interiors": "ZDS Architects & Interiors"}


def split_person(v):
    """(firm, person) -- a firm name with a principal welded onto the front."""
    m = SPLIT.match(v)
    if not m:
        return v, None
    a, b = m.group(1).strip(), m.group(2).strip()
    if DT.looks_like_person(a) and DT.FIRM_WORD.search(b):
        return b, a
    return v, None


def main():
    session = get_session()
    wrote = Counter()

    for path, src in SOURCES:
        if not path.exists():
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        for pid, r in data.items():
            p = session.get(Project, int(pid))
            if p is None:
                continue
            for role in ("contractor", "attorney"):
                v = DT.clean_firm(r.get(role), role)
                if not v:
                    continue
                col = DT.FIELD.get(role, role)
                if getattr(p, col, None):
                    continue
                setattr(p, col, v)
                wrote[col] += 1

    # split and fold what is already stored
    for p in session.query(Project).filter(Project.architect.isnot(None)).all():
        firm, person = split_person(p.architect)
        if person:
            p.architect = firm
            if not p.architect_person:
                p.architect_person = person
            wrote["split"] += 1
        f = FOLD.get((p.architect or "").strip().lower())
        if f and f != p.architect:
            p.architect = f
            wrote["folded"] += 1

    session.commit()
    log.info("WROTE %s", dict(wrote))
    for f in ("architect", "architect_person", "general_contractor", "attorney"):
        log.info("  %-20s %d", f,
                 session.query(Project).filter(getattr(Project, f).isnot(None)).count())
    session.close()


if __name__ == "__main__":
    main()
