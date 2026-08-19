r"""
Read the design team out of the source documents.

Architect sits at 25% across Rhode Island and 0% in Cranston, because the
ingest only ever took it from a labelled field. Planning documents name the
architect in narrative far more often than in a label -- "plans prepared by",
"the architect for the project", "representing the applicant" -- which is the
same reason reading full text roughly doubled developer coverage.

THE DISCIPLINE THIS NEEDS. Rhode Island filings are thick with consultants,
and they are not interchangeable:

    DiPrete Engineering appears on a large share of filings and is a CIVIL
    ENGINEER. Put it in the architect field and it tops an architect ranking
    it has no business being in.

So every role is extracted into its own field -- architect, civil engineer,
surveyor, landscape architect, contractor, attorney -- and a firm is only
written to architect when the text actually calls it the architect or the
designer of the building. The attorney is extracted purely so it can be
recognised and kept OUT, the same reason the developer work tracked
attorneys separately.

FIRM, NOT PERSON. "Christine West, AIA of Kite Architects" stores Kite
Architects as the firm and Christine West as the person. A person's name in a
firm column cannot be grouped, ranked or matched against anything.

    python scraper/ri_design_team.py --dry-run
    python scraper/ri_design_team.py --apply
"""

import os
import re
import sys
import json
import time
import logging
import argparse
import threading
import urllib.request
import urllib.error
from pathlib import Path
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from db.database import get_session
from db.models import Project
from scraper.ri_sf_extract import text_index, _full_text, RI
from scraper.ri_identity import normalize_address

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

CACHE = ROOT / "data" / "ri_design_team_raw.jsonl"
OUT = ROOT / "data" / "ri_design_team.json"
MODEL = "claude-haiku-4-5-20251001"
PRICE_IN, PRICE_OUT = 1.00 / 1e6, 5.00 / 1e6

SYSTEM = """You read Rhode Island planning and zoning filings and identify the PROJECT TEAM. \
Return ONLY JSON.

Each role goes in its own field. These are different firms doing different work and must \
never be merged:

  architect            designed the BUILDING. Look for "architect", "architecture",
                       "designed by", "plans prepared by", "design architect".
  civil_engineer       site, drainage, utilities, survey engineering. DiPrete
                       Engineering is a CIVIL ENGINEER and appears constantly --
                       it is never the architect. Also: "engineering", "site
                       engineer", "P.E.".
  surveyor             land surveying only.
  landscape_architect  landscape design only.
  contractor           general contractor, builder, construction manager.
  attorney             the lawyer appearing for the applicant. Extract it so it
                       can be told apart from the design team. Esq., "law", "on
                       behalf of the applicant".

FIRM NAMES, NOT PEOPLE. "Christine West, AIA of Kite Architects" -> architect
"Kite Architects", architect_person "Christine West". If only a person is named and no
firm, put the person in architect_person and leave architect null.

Rules:
  * Only report a role the text actually states. Never infer from context.
  * If a firm's role is ambiguous, leave every role null rather than guessing.
  * Do not report the applicant, owner or developer as the architect.
  * Omit any key you have no value for.

Return exactly:
{"architect":"","architect_person":"","civil_engineer":"","surveyor":"",
 "landscape_architect":"","contractor":"","attorney":"","quote":"<verbatim clause naming the architect, under 20 words>"}"""


def api_key():
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        if line.startswith("ANTHROPIC_API_KEY"):
            return line.split("=", 1)[1].strip()
    return os.environ.get("ANTHROPIC_API_KEY", "")


KEY = None
_spend = {"in": 0, "out": 0, "usd": 0.0}
_lock = threading.Lock()


def call(payload, retries=4):
    body = json.dumps({"model": MODEL, "max_tokens": 700, "system": SYSTEM,
                       "messages": [{"role": "user", "content": payload}]}).encode()
    last = None
    for a in range(retries):
        try:
            req = urllib.request.Request(
                "https://api.anthropic.com/v1/messages", data=body,
                headers={"x-api-key": KEY, "anthropic-version": "2023-06-01",
                         "content-type": "application/json"})
            with urllib.request.urlopen(req, timeout=120) as r:
                d = json.load(r)
            u = d.get("usage", {})
            with _lock:
                _spend["in"] += u.get("input_tokens", 0)
                _spend["out"] += u.get("output_tokens", 0)
                _spend["usd"] = _spend["in"] * PRICE_IN + _spend["out"] * PRICE_OUT
            return d["content"][0]["text"]
        except urllib.error.HTTPError as e:
            last = "HTTP %s" % e.code
            if e.code in (429, 500, 502, 503, 529):
                time.sleep(min(2 ** a * 3, 45))
                continue
            raise RuntimeError(last)
        except Exception as e:                                  # noqa: BLE001
            last = type(e).__name__
            time.sleep(min(2 ** a * 3, 45))
    raise RuntimeError(last or "retries exhausted")


def first_json(t):
    i = t.find("{")
    if i < 0:
        raise ValueError("no json")
    return json.JSONDecoder().raw_decode(t[i:])[0]


def project_docs(p, idx, span=1600, cap=6):
    """This project's own text, anchored on its address."""
    items = idx.get((p.city.lower(), normalize_address(p.address or "")), [])
    addr = (p.address or "").strip()
    out, seen = [], set()
    if addr:
        head = re.escape(addr[:16])
        for it in items:
            ft = _full_text(it.get("document") or "")
            if not ft:
                continue
            for m in re.finditer(head, ft, re.I):
                seg = re.sub(r"\s+", " ", ft[max(0, m.start() - 200):m.start() + span]).strip()
                k = seg[:90]
                if k not in seen:
                    seen.add(k)
                    out.append(seg)
    if not out and p.description:
        out.append(re.sub(r"\s+", " ", p.description))
    return out[:cap]


# A firm whose name says engineering is not an architect, whatever a filing
# calls it in passing. This is the guard that keeps DiPrete out.
ENGINEER_NAME = re.compile(r"\bengineer(?:ing|s)?\b|\bP\.?E\.?\b|\bsurvey(?:ing|ors?)\b", re.I)
LAW_NAME = re.compile(r"\besq\b|\blaw\b|\battorney\b|\bcounsel\b", re.I)
# A firm has a firm word in it. Without this test "Tracey Donnelly" lands in
# the architect column, and a person's name cannot be grouped or ranked
# against anything -- the same failure as an individual in the developer field.
FIRM_WORD = re.compile(
    r"\b(?:architect\w*|design|studio|associates?|partners?|group|company|"
    r"co\b|inc\b|llc\b|llp\b|ltd\b|corp\w*|atelier|workshop|collaborative|"
    r"engineer\w*|consultants?|builders?|construction|contracting|surveys?)\b", re.I)


def looks_like_person(v):
    """Two or three capitalised words, no firm word, optional credentials."""
    # Generational suffixes have to come off before the shape test, not after:
    # "James Kimball, Jr" reads as three words and was written into the
    # architect column as though it were a practice.
    # This test cannot be made exact and is not meant to be. "Moody Nolan" and
    # "DiMella Shaffer" are firms with the shape of a person's name, and no
    # character rule separates them from "Kevin Diamond". It runs at write
    # time to keep the obvious cases out; it must never be run backwards over
    # values already stored, which sweeps those firms out with the people.
    t = re.sub(r",?\s*(?:AIA|PE|RA|LEED|ASLA|P\.E\.|Esq\.?)\b", "", v, flags=re.I)
    t = re.sub(r",?\s*(?:Jr|Sr|II|III|IV)\.?$", "", t, flags=re.I).strip()
    if FIRM_WORD.search(t):
        return False
    return bool(re.match(r"^[A-Z][A-Za-z'\-]+(?:\s+[A-Z]\.?)?\s+[A-Z][A-Za-z'\-]+$", t))


def clean_firm(v, role):
    v = re.sub(r"\s+", " ", str(v or "")).strip(" .,;:")
    if not v or v.lower() in ("none", "null", "n/a", "unknown", "-"):
        return None
    if len(v) < 3 or len(v) > 80:
        return None
    if role in ("architect", "civil_engineer", "surveyor", "landscape_architect",
                "contractor"):
        if looks_like_person(v):
            return None                      # a person, not a firm -- see FIRM_WORD
    if role == "architect":
        if ENGINEER_NAME.search(v) and not re.search(r"architect", v, re.I):
            return None                      # an engineering firm, not an architect
        if LAW_NAME.search(v):
            return None
    return v


def done_ids():
    if not CACHE.exists():
        return {}
    out = {}
    for line in CACHE.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            r = json.loads(line)
            out[r["id"]] = r
        except Exception:                                       # noqa: BLE001
            pass
    return out


# "contractor" is the model's general_contractor column; the prompt uses the
# shorter word, so the two are mapped rather than duplicated.
FIELD = {"contractor": "general_contractor"}
ROLES = ("architect", "architect_person", "civil_engineer", "surveyor",
         "landscape_architect", "contractor", "attorney")


def main(apply=False, dry=False, workers=6):
    global KEY
    idx = text_index()
    session = get_session()
    rows = [p for p in session.query(Project).filter(Project.city.in_(RI)).all()
            if not p.excluded]
    cached = done_ids()
    todo = [p for p in rows if p.id not in cached]
    log.info("RI projects: %d | already read: %d | to read: %d", len(rows), len(cached), len(todo))
    if dry:
        return

    KEY = api_key()
    fh = CACHE.open("a", encoding="utf-8")
    wl = threading.Lock()
    ok = err = 0

    def work(p):
        nonlocal ok, err
        docs = project_docs(p, idx)
        if not docs:
            with wl:
                fh.write(json.dumps({"id": p.id, "team": {}}) + "\n")
                fh.flush()
            return
        payload = "ADDRESS: %s\nPROJECT: %s\n\n%s" % (
            p.address, p.name or "-", "\n\n---\n\n".join(d[:1600] for d in docs))
        try:
            j = first_json(call(payload[:14000]))
        except Exception as e:                                  # noqa: BLE001
            with wl:
                err += 1
            log.warning("  FAIL id=%s %s", p.id, str(e)[:50])
            return
        with wl:
            fh.write(json.dumps({"id": p.id, "team": j}) + "\n")
            fh.flush()
            ok += 1
            if ok % 25 == 0:
                log.info("  %d read | $%.2f", ok, _spend["usd"])

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for _ in as_completed([ex.submit(work, p) for p in todo]):
            pass
    fh.close()
    log.info("read ok=%d err=%d  SPENT $%.2f", ok, err, _spend["usd"])

    # ---------- merge ----------
    got = done_ids()
    filled = Counter()
    written = []
    for p in rows:
        r = got.get(p.id)
        if not r:
            continue
        team = r.get("team") or {}
        rec = {"id": p.id, "city": p.city, "address": (p.address or "")[:40], "set": {}}
        # A person the model put in the architect field is still worth keeping,
        # just in the person column rather than the firm one.
        raw_arch = re.sub(r"\s+", " ", str(team.get("architect") or "")).strip(" .,;:")
        if raw_arch and looks_like_person(raw_arch) and not team.get("architect_person"):
            team["architect_person"] = raw_arch

        for role in ROLES:
            val = clean_firm(team.get(role), role)
            if role == "architect_person":
                val = re.sub(r"\s+", " ", str(team.get(role) or "")).strip(" .,;:") or None
                if val and (len(val) < 4 or len(val) > 60):
                    val = None
            if not val:
                continue
            col = role
            if getattr(p, col, None):
                continue                     # never overwrite an existing value
            rec["set"][col] = val
            filled[col] += 1
        if rec["set"]:
            rec["quote"] = str(team.get("quote") or "")[:150]
            written.append(rec)

    log.info("\nFIELDS THE FILINGS CAN FILL (blank ones only): %s", dict(filled))

    if apply:
        for rec in written:
            p = session.get(Project, rec["id"])
            for col, val in rec["set"].items():
                setattr(p, col, val)
            if "architect" in rec["set"]:
                p.architect_source = "filing"
            p.notes = ((p.notes + " | ") if p.notes else "") + (
                "design team from the filings: %s%s"
                % (", ".join("%s=%s" % (k, v) for k, v in rec["set"].items()),
                   (". Verbatim: %r" % rec["quote"]) if rec.get("quote") else ""))
        session.commit()
        log.info("APPLIED to %d projects", len(written))
    OUT.write_text(json.dumps(written, indent=1, ensure_ascii=False), encoding="utf-8")
    session.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--workers", type=int, default=6)
    a = ap.parse_args()
    main(apply=a.apply, dry=a.dry_run, workers=a.workers)
