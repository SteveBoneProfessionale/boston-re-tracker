r"""
Tier 1.5: a second read of the Rhode Island corpus for the design team,
covering all five cities and every project still without an architect.

WHY A SECOND READ. The first pass (scraper/ri_design_team.py) took six windows
of 1,600 characters anchored on the project address. That finds a firm named
in the same breath as the address and misses one named anywhere else in the
document -- in a consultant list, a conditions-of-approval paragraph, a
signature block. The Providence plan-set pass proved the cost of that: reading
windows around the ROLE WORDS instead of around the address found sixteen
architects the first pass had walked straight past, in documents it had
already read.

So this reads the same corpus again with the plan-set window strategy, and
extends it to Warwick, Cranston, Pawtucket and Newport, which have no plan
sets of their own and so got nothing from Tier 3.

ATTRIBUTION. Widening the window widens the chance of handing one project the
architect printed under the item below it -- the exact failure
scraper/ri_attribution.py exists to stop. So the windows are cut from the
agenda BLOCK that names this project, never from the whole document, and a
firm that turns up under three or more different projects on the same agenda
is treated as boilerplate and dropped rather than written to all of them.

The prompt, the client and the name guards are ri_design_team's, unchanged.

    python scraper/ri_arch_corpus.py --dry-run
    python scraper/ri_arch_corpus.py --apply
"""

import re
import sys
import json
import logging
import argparse
from pathlib import Path
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from db.database import get_session
from db.models import Project
from scraper import ri_design_team as DT
from scraper import ri_attribution as AT
from scraper.ri_sf_extract import RI, text_index, _full_text
from scraper.ri_identity import normalize_address
from scraper.ri_planset_llm import windows

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

OUT = ROOT / "data" / "ri_arch_corpus.json"


def blocks_for(p, idx):
    """Agenda blocks that name this project, from its own documents."""
    items = idx.get((p.city.lower(), normalize_address(p.address or "")), [])
    anchors = AT.anchors_for(p)
    if not anchors:
        return []
    out, seen = [], set()
    for it in items:
        ft = _full_text(it.get("document") or "")
        if not ft:
            continue
        for b in AT.block_containing(ft, anchors):
            k = b[:120]
            if k in seen:
                continue
            seen.add(k)
            out.append(b)
    return out[:6]


def read_project(p, idx):
    chunks = []
    for b in blocks_for(p, idx):
        chunks.extend(windows(b, span=260, cap=10))
    if not chunks and p.description:
        chunks = [re.sub(r"\s+", " ", p.description)]
    if not chunks:
        return None
    payload = ("PROJECT: %s, %s RI\nExcerpts from this project's own agenda item "
               "and staff report. Identify the project team.\n\n---\n%s"
               % (p.address or p.name or "", p.city, "\n---\n".join(chunks)[:14000]))
    try:
        return DT.first_json(DT.call(payload))
    except Exception as e:                                      # noqa: BLE001
        log.info("  FAIL id=%s %s", p.id, e)
        return None


def main(apply=False, dry=False, workers=6):
    DT.KEY = DT.api_key()
    session = get_session()
    rows = [p for p in session.query(Project).filter(Project.city.in_(list(RI))).all()
            if not p.excluded and not p.architect]
    log.info("Rhode Island projects still without an architect: %d", len(rows))
    if dry:
        return
    idx = text_index()

    results = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(read_project, p, idx): p for p in rows}
        for i, (f, p) in enumerate(futs.items(), 1):
            r = f.result()
            if r:
                results[p.id] = r
            if i % 30 == 0:
                log.info("  %d/%d read | $%.2f", i, len(rows), DT._spend["usd"])
    log.info("read ok=%d  SPENT $%.2f", len(results), DT._spend["usd"])

    # A firm named under many different projects in one city is an agenda
    # fixture, not that project's architect.
    by_city = defaultdict(Counter)
    for pid, r in results.items():
        v = DT.clean_firm(r.get("architect"), "architect")
        if v:
            by_city[session.get(Project, pid).city][v.lower()] += 1
    boiler = {(c, v) for c, cnt in by_city.items() for v, n in cnt.items() if n >= 3}
    if boiler:
        log.info("dropped as agenda boilerplate: %s", sorted(boiler))

    writes, firms = [], Counter()
    for pid, r in results.items():
        p = session.get(Project, pid)
        for role in DT.ROLES:
            v = DT.clean_firm(r.get(role), role)
            if not v or getattr(p, role, None):
                continue
            if role == "architect" and (p.city, v.lower()) in boiler:
                continue
            writes.append((pid, role, v, (r.get("quote") or "")[:120]))
            if role == "architect":
                firms[v] += 1

    OUT.write_text(json.dumps({str(k): v for k, v in results.items()},
                              indent=1, ensure_ascii=False), encoding="utf-8")
    log.info("NEW VALUES  %s", dict(Counter(w[1] for w in writes)))
    for pid, role, v, q in writes:
        if role == "architect":
            p = session.get(Project, pid)
            log.info("   %-11s id=%-4d %-30s %s", p.city, pid, (p.address or "")[:30], v)

    if apply:
        for pid, role, v, q in writes:
            p = session.get(Project, pid)
            setattr(p, role, v)
            if role == "architect":
                p.architect_source = "filing"
                p.notes = ((p.notes + " | ") if p.notes else "") + (
                    "architect %r from the project's own planning filing%s"
                    % (v, (": %s" % q) if q else "."))
        session.commit()
        log.info("APPLIED %d values across %d projects",
                 len(writes), len({w[0] for w in writes}))
    else:
        log.info("NOT APPLIED -- re-run with --apply")
    session.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    main(apply=a.apply, dry=a.dry_run)
