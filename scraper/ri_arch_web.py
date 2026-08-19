r"""
Write architects established by web research.

Search itself is a tool call, not scriptable, so this is the WRITER half: the
research produces data/ri_arch_web_findings.json and this applies it under the
same guards every other tier uses. Keeping the guards here rather than in the
research loop means a name found on the web has to clear exactly the same bar
as one read out of a filing.

    [{"id": 493, "architect": "Tecton Architects",
      "source_url": "https://...", "evidence": "designed by Tecton Architects",
      "role": "architect"}]

WHAT IS REFUSED, and why each rule exists:

  a person, not a practice   "Kevin Diamond" cannot be grouped or ranked. Goes
                             to architect_person instead, never to architect.
  an engineer or surveyor    DiPrete Engineering is on a large share of these
                             filings and is a civil engineer.
  a lawyer                   a filing names the attorney more often than the
                             architect, and the name drifts if there is nowhere
                             to put it.
  no evidence string         a claim with no quoted source is not a source.
  no source_url              same.
  a project already filled   an existing filing- or plan-set-sourced name is
                             stronger than a web one and is never overwritten.

Blank stays blank. A field with nothing in it beats a field with a guess.

    python scraper/ri_arch_web.py --dry-run
    python scraper/ri_arch_web.py --apply
"""

import re
import sys
import json
import logging
import argparse
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from db.database import get_session
from db.models import Project
from scraper import ri_design_team as DT

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

FINDINGS = ROOT / "data" / "ri_arch_web_findings.json"

# A source that is only a listing or a directory says a firm exists, not that
# it designed this building. Same principle as the developer work: a listing
# is not corroboration.
LISTING = re.compile(
    r"zillow|apartments\.com|loopnet|realtor\.com|trulia|redfin|yelp|"
    r"bizapedia|manta|buzzfile|dnb\.com|opencorporates|facebook\.com", re.I)


def check(f, p):
    """(field, value) to write, or (None, reason) to refuse."""
    v = (f.get("architect") or "").strip()
    if not v:
        return None, "no name"
    if not (f.get("source_url") or "").strip():
        return None, "no source_url"
    if not (f.get("evidence") or "").strip():
        return None, "no evidence quote"
    if LISTING.search(f["source_url"]):
        return None, "listing site, not a statement of authorship"
    v = DT.clean_firm(v, "architect_person" if DT.looks_like_person(v) else "architect")
    if not v:
        return None, "failed the firm guard"
    if DT.looks_like_person(v):
        return ("architect_person", v) if not p.architect_person else (None, "person already set")
    if DT.ENGINEER_NAME.search(v) and not re.search(r"architect", v, re.I):
        return None, "engineering firm"
    if DT.LAW_NAME.search(v):
        return None, "law firm"
    if p.architect:
        return None, "already has a filing- or plan-set-sourced architect"
    return "architect", v


def main(apply=False):
    if not FINDINGS.exists():
        log.error("no findings at %s -- run the research pass first", FINDINGS)
        return
    data = json.loads(FINDINGS.read_text(encoding="utf-8"))
    session = get_session()
    ok, refused = [], Counter()

    for f in data:
        p = session.get(Project, int(f["id"]))
        if p is None:
            refused["no such project"] += 1
            continue
        field, val = check(f, p)
        if field is None:
            refused[val] += 1
            continue
        ok.append((p, field, val, f))

    log.info("findings %d | writable %d", len(data), len(ok))
    for k, n in refused.most_common():
        log.info("  refused %-52s %d", k, n)
    for p, field, val, f in ok:
        log.info("  %-11s %-30s %-14s %s", p.city, (p.address or "")[:30], field, val)

    if apply:
        for p, field, val, f in ok:
            setattr(p, field, val)
            if field == "architect":
                p.architect_source = "web"
            p.notes = ((p.notes + " | ") if p.notes else "") + (
                "%s %r from web research: %s -- %s"
                % (field, val, f["source_url"], f["evidence"][:160]))
        session.commit()
        log.info("APPLIED %d", len(ok))
    else:
        log.info("NOT APPLIED -- re-run with --apply")
    session.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    main(apply=a.apply)
