r"""
Clear non-vocabulary values out of review_scale.

HOW THEY GOT THERE. scraper/ri_ingest_llm.py writes

    "review_scale": _pick(items, "classification")

straight into the column with no check against the market's declared
vocabulary. Whatever an agenda happened to carry in its classification field
became a review scale: section headings ("PUBLIC HEARING", "NEW BUSINESS",
"OLD BUSINESS"), procedural labels, and in fifteen records a sentence naming
the land use attorney who appeared -- Joseph Shekarchi, who is also Speaker of
the Rhode Island House. A person's name rendering as a statutory review scale
on a chart axis is the same error class as an attorney in the developer field.

scraper/backfill_review_scale.py already had the right guard: a value outside
the vocabulary is logged and left null. The Rhode Island ingest never got it.

TWO OUTCOMES.

  RECOVERED  the text IS the board stating a scale in its own words --
             "MINOR SUBDIVISION", "MAJOR LAND DEVELOPMENT PROJECT". Folded to
             the canonical value. This is the same evidence tier the
             classifier calls explicit_language, so it is treated the same.
  CLEARED    everything else. review_scale is nulled and the record reads as
             unclassified, which the tracker already has a meaning for.

Nothing is lost either way: review_scale_raw holds the original verbatim for
all 105 records, and is not touched.

    python scraper/ri_clean_review_scale.py
    python scraper/ri_clean_review_scale.py --apply
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
from scraper.ri_sf_extract import RI

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

OUT = ROOT / "data" / "ri_review_scale_cleanup.json"
VOCAB = {"Major", "Minor", "Administrative"}

# Only where the text is the board naming a scale. Order matters: "MINOR LAND
# DEVELOPMENT PROJECT - MAJOR CHANGE" is a MINOR project with a major change,
# so minor is tested first and the match must be anchored at the start.
RECOVER = [
    ("Minor", re.compile(r"^\s*minor\s+(?:subdivision|land\s+development)", re.I)),
    ("Major", re.compile(r"^\s*major\s+(?:subdivision|land\s+development)", re.I)),
    ("Administrative", re.compile(r"^\s*administrative\s+(?:subdivision|review)", re.I)),
]


def fold(raw):
    for label, rx in RECOVER:
        if rx.search(raw or ""):
            return label
    return None


def main(apply=False):
    session = get_session()
    rows = session.query(Project).filter(Project.city.in_(RI)).all()
    bad = [p for p in rows if p.review_scale and p.review_scale not in VOCAB]

    rec, clr = [], []
    for p in bad:
        target = fold(p.review_scale)
        entry = {"id": p.id, "city": p.city, "excluded": bool(p.excluded),
                 "raw": p.review_scale, "to": target}
        (rec if target else clr).append(entry)

    log.info("\nRI records with a non-vocabulary review_scale: %d", len(bad))
    log.info("  RECOVERED to a real scale : %d", len(rec))
    log.info("  CLEARED to unclassified   : %d", len(clr))

    log.info("\nRECOVERED")
    for v, n in Counter("%s -> %s" % (r["raw"][:52], r["to"]) for r in rec).most_common():
        log.info("  %3d  %s", n, v)

    log.info("\nCLEARED (review_scale nulled; review_scale_raw keeps the text)")
    for v, n in Counter(c["raw"][:70] for c in clr).most_common():
        log.info("  %3d  %r", n, v)

    if apply:
        for r in rec:
            p = session.get(Project, r["id"])
            p.review_scale = r["to"]
            p.review_scale_basis = "explicit_language"
            p.notes = ((p.notes + " | ") if p.notes else "") + (
                "review_scale folded to %s from the source's own wording %r, which the "
                "ingest had written into the column unvalidated." % (r["to"], r["raw"][:90]))
        for c in clr:
            p = session.get(Project, c["id"])
            p.review_scale = None
            if p.review_scale_basis not in ("not_applicable",):
                p.review_scale_basis = "unknown"
            p.notes = ((p.notes + " | ") if p.notes else "") + (
                "review_scale cleared: %r is not a statutory review scale. It was agenda "
                "text the ingest wrote into the column without checking it against the "
                "market vocabulary. Verbatim value kept in review_scale_raw."
                % c["raw"][:90])
        session.commit()
        log.info("\nAPPLIED")
    else:
        log.info("\nDRY RUN -- re-run with --apply")

    OUT.write_text(json.dumps({"recovered": rec, "cleared": clr}, indent=1,
                              ensure_ascii=False), encoding="utf-8")
    session.close()


if __name__ == "__main__":
    main(apply="--apply" in sys.argv)
