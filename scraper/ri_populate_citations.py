r"""
Fill the two gaps in Rhode Island's per-field source citations.

ri_citations.py built the write path and the ingest cited most extracted
fields, but two never got written:

  developer     the field this whole verification round produced. 377 names
                carry sources in projects.developer_sources as JSON, and none
                of it reached extraction_sources, so the detail view and the
                citation coverage report both showed the developer as
                uncited.
  asset_class   cited for no project at all.

Developer citations are drawn from the verification store, so the citation
records WHICH source established the name and at what confidence:

  confirmed      cite the first outside article, and keep the planning
                 document as the secondary
  document_only  cite the planning document, and say so plainly -- the value
                 rests on the filing alone
  conflicted     cite the document, and note in the value that a second name
                 is on record
  blank          no value, therefore no citation

    python scraper/ri_populate_citations.py --dry-run
"""

import sys
import json
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import get_session
from db.models import Project
from scraper.ri_citations import record_field
from scraper.ri_ingest_llm import load_items, collapse
from scraper.ri_identity import normalize_address

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

STORE = Path(__file__).parent.parent / "data" / "ri_developer_verification.json"
RI = ("Providence", "Warwick", "Cranston", "Pawtucket", "Newport")


def _pick_source(rec):
    """The source that best establishes the name, and a human label for it."""
    srcs = rec.get("sources") or []
    outcome = rec.get("outcome")
    arts = [s for s in srcs if s.get("type") in ("article", "primary", "developer_site",
                                                 "project_site", "supporting")]
    docs = [s for s in srcs if s.get("type") == "document"]

    if outcome == "confirmed" and arts:
        a = arts[0]
        label = a.get("publisher") or a.get("domain") or "outside coverage"
        return a.get("url"), f"CONFIRMED by {label}", len(arts)
    if docs:
        d = docs[0]
        detail = d.get("detail") or "planning filing"
        if outcome == "conflicted":
            return d.get("url"), f"CONFLICTED -- {detail} (a second name is on record)", 1
        return d.get("url"), f"DOCUMENT ONLY -- {detail}", 1
    if arts:
        a = arts[0]
        return a.get("url"), (a.get("publisher") or a.get("domain") or "outside coverage"), len(arts)
    return None, "no source recorded", 0


def main(dry_run=False):
    store = json.loads(STORE.read_text(encoding="utf-8")) if STORE.exists() else {}

    # asset_class comes back from the segmented items, keyed the way the
    # ingest keyed them.
    by_key = {}
    for g in collapse(load_items()):
        muni = g["municipality"].lower()
        for it in g["items"]:
            by_key.setdefault((muni, normalize_address(it.get("address") or "")), []).append(it)

    session = get_session()
    dev_n = ac_n = 0
    try:
        for p in session.query(Project).filter(Project.city.in_(RI)).all():
            rec = store.get(str(p.id))
            # Developers resolved by the EARLIER pipeline (registry_self,
            # web_corroborated, web_low_confidence) are not in the verification
            # store. Their provenance lives in the projects.developer_sources
            # column, so synthesise a record from it rather than leaving 128
            # names uncited.
            if p.developer and not rec:
                try:
                    srcs = json.loads(p.developer_sources) if p.developer_sources else []
                except (ValueError, TypeError):
                    srcs = []
                if not isinstance(srcs, list):
                    srcs = []
                rec = {
                    "outcome": p.developer_resolution_method or "unknown",
                    "sources": [{"type": "article",
                                 "publisher": x.get("publisher") or x.get("domain"),
                                 "domain": x.get("domain"),
                                 "url": x.get("url")}
                                for x in srcs if isinstance(x, dict)],
                }
                if not rec["sources"]:
                    rec["sources"] = [{"type": "document",
                                       "detail": "resolved by %s" %
                                                 (p.developer_resolution_method or "the earlier pipeline")}]
            if p.developer and rec:
                url, label, n = _pick_source(rec)
                if rec.get("outcome") in ("web_corroborated", "web_low_confidence",
                                          "registry_self") and n:
                    label = "%s (%s)" % (label, rec["outcome"])
                extra = ""
                if p.owner_or_agency:
                    extra = f"  [owner/agency: {p.owner_or_agency}]"
                if rec.get("web_developer"):
                    extra += f"  [also on record: {rec['web_developer']}]"
                if record_field(session, p.id, "developer",
                                f"{p.developer}{extra}",
                                source_url=url or "",
                                filing_name=label,
                                filing_date="") is not None:
                    dev_n += 1

            if p.asset_class:
                items = by_key.get((p.city.lower(), normalize_address(p.address or "")), [])
                it = next((i for i in items if i.get("classification")), None)
                if record_field(session, p.id, "asset_class", p.asset_class,
                                source_url=(it or {}).get("source_url", ""),
                                filing_name=(it or {}).get("reviewing_body", "") or "planning filing",
                                filing_date=(it or {}).get("meeting_date", "")) is not None:
                    ac_n += 1

        if dry_run:
            session.rollback()
            log.info("DRY RUN -- would write %d developer, %d asset_class", dev_n, ac_n)
            return
        session.commit()
    finally:
        session.close()

    log.info("developer citations   : %d", dev_n)
    log.info("asset_class citations : %d", ac_n)


if __name__ == "__main__":
    main(dry_run="--dry-run" in sys.argv)
