r"""
Architect firms from Providence plan sets and staff reports.

The filings name a PERSON far more often than a practice -- 48 people against
5 firms in the agenda pass -- because an agenda records who stood up and
spoke. A drawing set does the opposite: its title block carries the firm, its
address and often its licence number, because that is what a stamped drawing
is for.

Providence publishes both, and the CPC materials pages give 630 document URLs
already collected in data/ri_plansets/cpc_urls.txt.

WHAT COUNTS AS AN ARCHITECT HERE. A plan set names everyone: the civil
engineer, the surveyor, the landscape architect, the traffic consultant. So a
firm is only taken when its name itself says architecture, or when the text
labels it as the architect. DiPrete Engineering appears on a large share of
these sets and is never accepted -- it is a civil engineer, and it is captured
into its own field instead.

    python scraper/ri_planset_architects.py --dry-run
    python scraper/ri_planset_architects.py --apply
"""

import re
import sys
import json
import time
import logging
import argparse
import urllib.request
from pathlib import Path
from collections import Counter, defaultdict

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

import pymupdf

from db.database import get_session
from db.models import Project
from scraper.ri_sf_extract import RI

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

URLS = ROOT / "data" / "ri_plansets" / "cpc_urls.txt"
CACHE = ROOT / "data" / "ri_plansets"
OUT = ROOT / "data" / "ri_planset_architects.json"

# A firm whose NAME declares it an architecture practice. This is deliberately
# narrower than "a firm mentioned near the word architect", because a plan set
# mentions every consultant on the job.
# Match the WORD, then walk backwards for the firm name. Matching a firm
# pattern that merely ends in "architect" pulled "URAL PLAN" out of
# "ARCHITECTURAL PLANS" -- the lazy quantifier was happy to start mid-word.
# "Architectural" is an adjective describing drawings, never a practice name,
# so it is excluded outright.
ARCH_TOKEN = re.compile(r"\b(Architects?|Architecture|Studio|Atelier)\b(?!al)", re.I)
ENG_TOKEN = re.compile(r"\b(Engineering|Engineers)\b", re.I)
# The words immediately before the token, which is where the firm name lives:
# "Kite Architects", "ZDS Architecture", "Union Studio".
# Title blocks are usually set in capitals -- "KITE ARCHITECTS INC" -- so the
# name capture has to accept an all-caps run as readily as a title-cased one.
NAME_BEFORE = re.compile(
    r"((?:[A-Z][A-Za-z0-9&'\.\-]*\s+){0,3}[A-Z][A-Za-z0-9&'\.\-]*)\s*$")
# Strip connectors off the FRONT of the captured name. "PREPARED BY KITE
# ARCHITECTS" must yield "KITE ARCHITECTS"; leaving the connector on made the
# whole string read as drawing prose and the noise filter discarded it.
STOPWORD_LEAD = re.compile(
    r"^(?:BY|FOR|OF|AND|THE|PLANS?|SHEET|PREPARED|ISSUED|DRAWN|CHECKED|"
    r"PROJECT|SITE|TITLE|CLIENT|OWNER|APPLICANT)\b\s*", re.I)
ARCH_LABEL = re.compile(
    r"\bARCHITECT\s*[:\-]\s*([A-Z][A-Za-z0-9&'\.\- ]{3,45})", re.I)

# Never an architect, whatever a title block puts next to it.
NOT_ARCH = re.compile(
    r"engineer|survey|\bDPW\b|department|commission|city of|university|"
    r"authority|realty|properties|development|holdings", re.I)
NOISE = re.compile(
    r"^(?:the|this|and|for|plan|plans|sheet|drawing|drawings|project|site|note|"
    r"notes|scale|date|rev|no|title|general|typical|detail|details|section|"
    r"prepared|issued|by|of|to|all|new|existing)\b", re.I)


def norm_firm(v):
    v = re.sub(r"\s+", " ", v or "").strip(" .,;:-")
    v = re.sub(r"\s*,?\s*(?:Inc|LLC|LLP|PC|P\.C\.|Ltd)\.?$", "", v, flags=re.I).strip()
    return v


def _named_before(text, token_match, tail):
    """The firm name ending at this token, e.g. "Kite" + "Architects"."""
    before = text[max(0, token_match.start() - 60):token_match.start()]
    before = re.sub(r"\s+", " ", before)
    m = NAME_BEFORE.search(before)
    if not m:
        return None
    lead = m.group(1).strip()
    for _ in range(4):
        t2 = STOPWORD_LEAD.sub("", lead).strip()
        if t2 == lead:
            break
        lead = t2
    # And cut anything after a sentence break the capture ran through.
    lead = re.split(r"[.;]", lead)[-1].strip()
    if not lead:
        return None
    name = ("%s %s" % (lead, tail)).strip()
    # A single stray capitalised word before the token is usually a line of
    # drawing text, not a practice. Two tokens minimum.
    if len(name.split()) < 2:
        return None
    return name


def firms_in(text):
    """(architects, engineers) named in this document."""
    arch, eng = Counter(), Counter()
    for m in ARCH_LABEL.finditer(text):
        v = norm_firm(m.group(1))
        if v and not NOT_ARCH.search(v) and not NOISE.match(v) and len(v) > 3:
            arch[v] += 3                       # a label beats a name match
    for m in ARCH_TOKEN.finditer(text):
        cand = _named_before(text, m, m.group(1))
        v = norm_firm(cand) if cand else None
        if v and not NOT_ARCH.search(v) and not NOISE.match(v) and len(v) > 5:
            arch[v] += 1
    for m in ENG_TOKEN.finditer(text):
        cand = _named_before(text, m, m.group(1))
        v = norm_firm(cand) if cand else None
        if v and not NOISE.match(v) and len(v) > 5:
            eng[v] += 1
    return arch, eng


def fetch(url):
    name = re.sub(r"[^A-Za-z0-9._-]", "_", url.rsplit("/", 1)[-1])[:120]
    dest = CACHE / name
    if dest.exists() and dest.stat().st_size > 1000:
        return dest
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=90) as r:
            data = r.read()
        if not data.startswith(b"%PDF"):
            return None
        dest.write_bytes(data)
        time.sleep(0.2)
        return dest
    except Exception:                                           # noqa: BLE001
        return None


def doc_text(path):
    try:
        d = pymupdf.open(path)
    except Exception:                                           # noqa: BLE001
        return ""
    return "\n".join(p.get_text() for p in d)


def keys_for(p):
    """Tokens that identify this project in a document filename."""
    ks = []
    c = re.sub(r"[^A-Za-z0-9]", "", (p.case_number or "")).lower()
    if len(c) >= 6:
        ks.append(c[:7])
    a = (p.address or "").lower()
    m = re.match(r"(\d+)\s+([a-z]+)", a)
    if m:
        ks.append(m.group(1) + "-" + m.group(2))
        ks.append(m.group(1) + m.group(2))
    return ks


def main(apply=False, dry=False, limit=0):
    if not URLS.exists():
        log.error("no URL list at %s", URLS)
        return
    urls = [u.strip() for u in URLS.read_text(encoding="utf-8").splitlines() if u.strip()]
    session = get_session()
    rows = [p for p in session.query(Project).filter(Project.city == "Providence").all()
            if not p.excluded]

    # match documents to projects by filename
    match = defaultdict(list)
    for p in rows:
        ks = keys_for(p)
        if not ks:
            continue
        for u in urls:
            fn = re.sub(r"[^A-Za-z0-9]", "", u.rsplit("/", 1)[-1]).lower()
            if any(k.replace("-", "") in fn for k in ks):
                match[p.id].append(u)

    targets = [(p, match[p.id][:4]) for p in rows if match.get(p.id)]
    if limit:
        targets = targets[:limit]
    log.info("Providence projects with a matching document: %d", len(targets))
    if dry:
        return

    found = []
    for i, (p, us) in enumerate(targets, 1):
        arch, eng = Counter(), Counter()
        for u in us:
            path = fetch(u)
            if not path:
                continue
            t = doc_text(path)
            if not t:
                continue
            a, e = firms_in(t)
            arch.update(a)
            eng.update(e)
        rec = {"id": p.id, "address": (p.address or "")[:40],
               "architect": arch.most_common(1)[0][0] if arch else None,
               "engineer": eng.most_common(1)[0][0] if eng else None,
               "all_arch": arch.most_common(4), "docs": len(us)}
        if rec["architect"] or rec["engineer"]:
            found.append(rec)
        if i % 15 == 0:
            log.info("  %d/%d scanned", i, len(targets))

    new_arch = [f for f in found if f["architect"] and not session.get(Project, f["id"]).architect]
    new_eng = [f for f in found if f["engineer"] and not session.get(Project, f["id"]).civil_engineer]
    log.info("\ndocuments naming a firm : %d", len(found))
    log.info("  new architect          : %d", len(new_arch))
    log.info("  new civil engineer     : %d", len(new_eng))
    for f in new_arch:
        log.info("    id=%-4d %-34s %s", f["id"], f["address"][:34], f["architect"])

    OUT.write_text(json.dumps(found, indent=1, ensure_ascii=False), encoding="utf-8")

    if apply:
        for f in new_arch:
            p = session.get(Project, f["id"])
            p.architect = f["architect"]
            p.architect_source = "plan_set"
            p.notes = ((p.notes + " | ") if p.notes else "") + (
                "architect %r from the Providence plan set / staff report title block."
                % f["architect"])
        for f in new_eng:
            p = session.get(Project, f["id"])
            if not p.civil_engineer:
                p.civil_engineer = f["engineer"]
        session.commit()
        log.info("APPLIED: %d architects, %d engineers", len(new_arch), len(new_eng))
    session.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()
    main(apply=a.apply, dry=a.dry_run, limit=a.limit)
