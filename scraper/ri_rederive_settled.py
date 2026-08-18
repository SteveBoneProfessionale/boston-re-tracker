r"""
Re-derive every document_only name after a step-1 parser change.

The below-threshold projects are settled on the planning document alone, so
their stored name is only ever as good as the parser that produced it. Each
time the extraction rules change, those names have to be recomputed and any
drift pushed into both the verification store and the database -- otherwise a
fix silently applies to the projects still being worked and not to the ones
already settled.

Reports rather than hides the two directions of drift:
  changed  a better name is now available -> rewritten
  lost     the parser no longer finds any name -> left alone and reported,
           because losing a name is a regression to look at, not to apply.

    python scraper/ri_rederive_settled.py --dry-run
"""

import sys
import json
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import get_session
from db.models import Project
from scraper.ri_identity import normalize_address
from scraper.ri_ingest_llm import load_items, collapse
from scraper.ri_developer_candidates import candidates_from, rank

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

STORE = Path(__file__).parent.parent / "data" / "ri_developer_verification.json"


def text_index():
    idx = {}
    for g in collapse(load_items()):
        muni = g["municipality"].lower()
        for it in g["items"]:
            idx.setdefault((muni, normalize_address(it.get("address") or "")), []).append(it)
    return idx


def project_text(p, idx):
    blobs = [p.description or ""]
    if p.applicant_entity:
        blobs.append("Applicant: " + p.applicant_entity)
    for it in idx.get((p.city.lower(), normalize_address(p.address or "")), []):
        if it.get("description"):
            blobs.append(it["description"])
        if it.get("applicant_entity"):
            blobs.append("Applicant: " + it["applicant_entity"])
    return "\n".join(dict.fromkeys(b for b in blobs if b))


def main(dry_run=False):
    store = json.loads(STORE.read_text(encoding="utf-8"))
    idx = text_index()
    session = get_session()
    changed = lost = 0
    try:
        for pid, rec in store.items():
            if rec.get("outcome") != "document_only":
                continue
            # Only names the settle pass derived automatically. A name written
            # during hand research is the better one -- recomputing it turned
            # "Colbea Enterprises, LLC" back into the raw "Colbea Enterprises,
            # L" the filing happens to contain.
            if not rec.get("auto"):
                continue
            p = session.get(Project, int(pid))
            if p is None:
                continue
            cs = rank(candidates_from(project_text(p, idx)))
            new = cs[0]["name"] if cs else None
            if new == rec.get("developer"):
                continue
            if new is None:
                log.warning("  id=%-4s LOST %r -- parser now finds nothing", pid,
                            rec.get("developer"))
                lost += 1
                continue
            log.info("  id=%-4s %r -> %r", pid, rec.get("developer"), new)
            rec["developer"] = new
            if p.developer:
                p.developer = new
            changed += 1

        if dry_run:
            session.rollback()
            log.info("DRY RUN -- nothing written")
        else:
            session.commit()
            STORE.write_text(json.dumps(store, indent=1, ensure_ascii=False),
                             encoding="utf-8")
    finally:
        session.close()

    log.info("changed=%d  lost=%d", changed, lost)


if __name__ == "__main__":
    main(dry_run="--dry-run" in sys.argv)
