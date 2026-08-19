r"""
Tier 3: architect firms from Providence plan sets, read by the model.

WHY NOT REGEX. The first cut of this scanner walked backwards from the word
"architect" to grab the firm name. Title blocks defeat it. A drawing set is
mostly ALL-CAPS boilerplate that uses "architect" as a common noun -- "SHALL
BE APPROVED BY THE ARCHITECT", "NURSERY STOCK AS DIRECTED BY THE LANDSCAPE
ARCHITECT", "THE AUTHORITY HAVING JURISDICTION" -- and the backwards walk
happily returns those as practice names. There is no character-level rule that
separates a firm from a general note, because the difference is meaning.

So the windows around every role word go to the model, which is asked the same
question, with the same role separation, as the agenda pass in
scraper/ri_design_team.py. That module's SYSTEM prompt, client and name guards
are imported rather than restated, so the two tiers cannot drift apart.

DiPrete Engineering is a civil engineer and is never accepted as an architect.
The guard lives in ri_design_team.clean_firm and applies here unchanged.

    python scraper/ri_planset_llm.py --dry-run
    python scraper/ri_planset_llm.py --apply
"""

import re
import sys
import json
import logging
import argparse
from pathlib import Path
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from db.database import get_session
from db.models import Project
from scraper import ri_design_team as DT
from scraper.ri_planset_architects import fetch, doc_text, keys_for, URLS

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

OUT = ROOT / "data" / "ri_planset_llm.json"

# Where a firm name plausibly sits. Cast wide -- the model does the judging.
ROLE_WORD = re.compile(
    r"\b(architect\w*|engineer\w*|surveyor|surveying|landscape\s+architect|"
    r"prepared\s+by|designed\s+by|design\s+team|applicant|attorney)\b", re.I)


def windows(text, span=220, cap=28):
    """Text around each role word, deduped and merged."""
    spans = []
    for m in ROLE_WORD.finditer(text):
        a, b = max(0, m.start() - span), min(len(text), m.end() + span)
        if spans and a <= spans[-1][1]:
            spans[-1][1] = max(spans[-1][1], b)      # merge overlapping
        else:
            spans.append([a, b])
        if len(spans) >= cap:
            break
    out, seen = [], set()
    for a, b in spans:
        chunk = re.sub(r"[ \t]+", " ", text[a:b]).strip()
        k = chunk[:80].lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(chunk)
    return out


def read_project(p, urls):
    chunks = []
    for u in urls:
        path = fetch(u)
        if not path:
            continue
        t = doc_text(path)
        if t:
            chunks.extend(windows(t))
    if not chunks:
        return None
    payload = ("PROJECT: %s, Providence RI\nExcerpts from the plan set and staff "
               "report. Identify the project team.\n\n---\n%s"
               % (p.address or p.name or "", "\n---\n".join(chunks)[:14000]))
    try:
        return DT.first_json(DT.call(payload))
    except Exception as e:                                      # noqa: BLE001
        log.info("  FAIL id=%s %s", p.id, e)
        return None


def main(apply=False, dry=False, workers=6):
    DT.KEY = DT.api_key()
    urls = [u.strip() for u in URLS.read_text(encoding="utf-8").splitlines() if u.strip()]
    session = get_session()
    rows = [p for p in session.query(Project).filter(Project.city == "Providence").all()
            if not p.excluded]

    targets = []
    for p in rows:
        ks = keys_for(p)
        if not ks:
            continue
        us = [u for u in urls
              if any(k.replace("-", "") in re.sub(r"[^A-Za-z0-9]", "", u.rsplit("/", 1)[-1]).lower()
                     for k in ks)]
        if us:
            targets.append((p, us[:4]))
    log.info("Providence projects with a plan set: %d", len(targets))
    if dry:
        return

    results = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(read_project, p, us): p for p, us in targets}
        for i, (f, p) in enumerate(futs.items(), 1):
            r = f.result()
            if r:
                results[p.id] = r
            if i % 15 == 0:
                log.info("  %d/%d read | $%.2f", i, len(targets), DT._spend["usd"])
    log.info("read ok=%d  SPENT $%.2f", len(results), DT._spend["usd"])

    writes, firms = [], Counter()
    for pid, r in results.items():
        p = session.get(Project, pid)
        for role in DT.ROLES:
            v = DT.clean_firm(r.get(role), role)
            if not v or getattr(p, role, None):
                continue
            writes.append((pid, role, v, (r.get("quote") or "")[:120]))
            if role == "architect":
                firms[v] += 1

    OUT.write_text(json.dumps(
        {str(k): v for k, v in results.items()}, indent=1, ensure_ascii=False), encoding="utf-8")

    by_role = Counter(w[1] for w in writes)
    log.info("NEW VALUES  %s", dict(by_role))
    for pid, role, v, q in writes:
        if role == "architect":
            p = session.get(Project, pid)
            log.info("   id=%-4d %-32s %s", pid, (p.address or "")[:32], v)

    if apply:
        for pid, role, v, q in writes:
            p = session.get(Project, pid)
            setattr(p, role, v)
            if role == "architect":
                p.architect_source = "plan_set"
                p.notes = ((p.notes + " | ") if p.notes else "") + (
                    "architect %r from the Providence plan set / staff report%s"
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
