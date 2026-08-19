r"""
Re-extract every Rhode Island unit count with the corrected prompt.

WHY ALL OF THEM. The wrong-noun sweep found 26 figures whose stored evidence
named storeys, buildings or lots. But that sweep could only catch cases where
the evidence string happened to reveal the error. Every unit figure in the
corpus came out of the same flawed prompt, so the only way to know which are
wrong is to ask again with the corrected one.

WHAT CHANGED IN THE ASK. The model is now told explicitly that a storey, a
building, a lot, a storage unit, a parking space, a hotel room and a bedroom
are not dwelling units, and that a filing giving only a building count must
return null rather than the nearest number to hand. Nothing may be derived --
only a stated dwelling-unit figure counts.

ONE CALL PER PROJECT, not per document. Each project's own agenda appearances
are gathered, anchored on its address so a neighbour's item cannot leak in,
and sent together with their dates. The model returns a figure per date, which
is what makes a revision visible: 311 Knight Street reads 34 in 2019 and 41 at
final plan in 2022, and only the sequence shows which is current.

THE SAFETY RULES ARE UNCHANGED, because they earned their place. A new figure
must appear in at least two documents, and a fall to under a quarter of the
existing count is refused outright. Those two rules are what stopped 580 South
Water being rewritten from 69 to 5 -- a "5" that turned out to be "5-story".

    python scraper/ri_reextract_units.py --dry-run
    python scraper/ri_reextract_units.py --apply
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
from collections import defaultdict, Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from db.database import get_session
from db.models import Project, FlaggedExtraction
from scraper.ri_sf_extract import text_index, _full_text, RI
from scraper.ri_identity import normalize_address

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

CACHE = ROOT / "data" / "ri_reextract_units_raw.jsonl"
OUT = ROOT / "data" / "ri_reextract_units.json"
MODEL = "claude-haiku-4-5-20251001"
PRICE_IN, PRICE_OUT = 1.00 / 1e6, 5.00 / 1e6

SYSTEM = """You read Rhode Island planning and zoning filings and report the number of \
DWELLING UNITS a project proposes. Return ONLY JSON.

You are given one project's appearances before a board, each with a date. Report the \
dwelling-unit figure stated at each date, so a revision between filings is visible.

COUNT THE RIGHT NOUN. These are NOT dwelling units. If the text gives one of these and \
no dwelling count, the figure for that date is null:
  storeys      "5-story residential building" is NOT 5 units
  buildings    "4 multi-family residential buildings" is NOT 4 units
  lots         "create 44 lots for single-family use" is NOT 44 units
  storage units, self-storage units
  parking spaces
  hotel rooms, guest rooms, bedrooms
  square feet, acres, plat or lot numbers, case numbers, dollar amounts

COUNT: apartments, dwelling units, residential units, condominium units, townhouse units, \
"N-unit", spelled-out counts ("sixteen (16) units" -> 16, "ten-unit" -> 10).

NEVER derive or estimate. Only a stated dwelling-unit figure counts. If a date's text \
states no dwelling-unit figure, return null for it. A null is correct and useful; a \
guess is not.

Return exactly:
{"readings":[{"date":"YYYY-MM-DD","units":<integer or null>,"quote":"<verbatim, under 20 words>"}]}"""


def api_key():
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        if line.startswith("ANTHROPIC_API_KEY"):
            return line.split("=", 1)[1].strip()
    return os.environ.get("ANTHROPIC_API_KEY", "")


KEY = None
_spend = {"in": 0, "out": 0, "usd": 0.0}
_lock = threading.Lock()


def call(payload, retries=4):
    body = json.dumps({"model": MODEL, "max_tokens": 1200, "system": SYSTEM,
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
            last = "%s" % type(e).__name__
            time.sleep(min(2 ** a * 3, 45))
    raise RuntimeError(last or "retries exhausted")


def first_json(t):
    i = t.find("{")
    if i < 0:
        raise ValueError("no json")
    return json.JSONDecoder().raw_decode(t[i:])[0]


def appearances(p, idx, span=900, cap=8):
    """This project's own agenda items, anchored on its address, newest last."""
    items = idx.get((p.city.lower(), normalize_address(p.address or "")), [])
    addr = (p.address or "").strip()
    if not addr:
        return []
    head = re.escape(addr[:16])
    out, seen = [], set()
    for it in items:
        ft = _full_text(it.get("document") or "")
        if not ft:
            continue
        d = it.get("meeting_date") or it.get("date") or ""
        for m in re.finditer(head, ft, re.I):
            seg = re.sub(r"\s+", " ", ft[m.start():m.start() + span]).strip()
            k = seg[:90]
            if k in seen:
                continue
            seen.add(k)
            out.append({"date": str(d)[:10], "text": seg})
    out.sort(key=lambda x: x["date"])
    return out[-cap:]


def done_ids():
    if not CACHE.exists():
        return {}
    got = {}
    for line in CACHE.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            r = json.loads(line)
            got[r["id"]] = r
        except Exception:                                       # noqa: BLE001
            pass
    return got


def main(apply=False, dry=False, workers=6):
    global KEY
    idx = text_index()
    session = get_session()
    rows = [p for p in session.query(Project).filter(Project.city.in_(RI)).all()
            if not p.excluded]

    cached = done_ids()
    todo = [p for p in rows if p.id not in cached]
    log.info("Rhode Island pipeline: %d projects | already read: %d | to read: %d",
             len(rows), len(cached), len(todo))
    if dry:
        log.info("DRY RUN -- nothing sent.")
        return

    KEY = api_key()
    fh = CACHE.open("a", encoding="utf-8")
    wlock = threading.Lock()
    ok = err = 0

    def work(p):
        nonlocal ok, err
        apps = appearances(p, idx)
        if not apps:
            with wlock:
                fh.write(json.dumps({"id": p.id, "readings": []}) + "\n")
                fh.flush()
            return
        payload = "ADDRESS: %s\nCASE: %s\n\n" % (p.address, p.case_number or "-")
        for a in apps:
            payload += "--- %s ---\n%s\n\n" % (a["date"] or "undated", a["text"][:900])
        try:
            j = first_json(call(payload[:14000]))
            readings = [r for r in j.get("readings", []) if isinstance(r, dict)]
        except Exception as e:                                  # noqa: BLE001
            with wlock:
                err += 1
            log.warning("  FAIL id=%s %s", p.id, str(e)[:60])
            return
        with wlock:
            fh.write(json.dumps({"id": p.id, "readings": readings}) + "\n")
            fh.flush()
            ok += 1
            if ok % 25 == 0:
                log.info("  %d read | $%.2f", ok, _spend["usd"])

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for _ in as_completed([ex.submit(work, p) for p in todo]):
            pass
    fh.close()
    log.info("read ok=%d err=%d  SPENT $%.2f", ok, err, _spend["usd"])

    # ---------- compare ----------
    got = done_ids()
    changes, refused, conflicts = [], [], []
    OUTSIDE = ("leasing_active", "news_confirmed", "human_set", "assessor_confirmed", "co_issued")

    for p in rows:
        r = got.get(p.id)
        if not r:
            continue
        vals = [x for x in r["readings"] if isinstance(x.get("units"), int) and x["units"] > 0]
        if not vals:
            continue
        vals.sort(key=lambda x: str(x.get("date") or ""))
        latest = vals[-1]
        n_same = sum(1 for x in vals if x["units"] == latest["units"])
        if not p.residential_units:
            continue
        if latest["units"] == p.residential_units:
            continue
        collapse = latest["units"] < p.residential_units * 0.25
        rec = {"id": p.id, "city": p.city, "address": (p.address or p.name or "")[:44],
               "was": p.residential_units, "now": latest["units"],
               "date": latest.get("date"), "quote": str(latest.get("quote"))[:150],
               "readings": [(x.get("date"), x["units"]) for x in vals],
               "outside_source": p.completion_basis if p.completion_basis in OUTSIDE else None}
        if n_same >= 2 and not collapse:
            changes.append(rec)
            if rec["outside_source"]:
                conflicts.append(rec)
        else:
            rec["why"] = ("a collapse to under a quarter" if collapse
                          else "stated in only one reading")
            refused.append(rec)

    log.info("\nCHANGED   %d", len(changes))
    log.info("  up      %d", sum(1 for c in changes if c["now"] > c["was"]))
    log.info("  down    %d", sum(1 for c in changes if c["now"] < c["was"]))
    log.info("  net     %+d", sum(c["now"] - c["was"] for c in changes))
    log.info("REFUSED   %d", len(refused))
    log.info("CONFLICTS WITH AN OUTSIDE-CONFIRMED FIGURE: %d", len(conflicts))

    for c in sorted(changes, key=lambda x: -abs(x["now"] - x["was"])):
        log.info("  id=%-4d %-11s %-34s %4s -> %-4s  %s", c["id"], c["city"],
                 c["address"][:34], c["was"], c["now"], str(c["quote"])[:60])
    if conflicts:
        log.info("\nTHESE DISAGREE WITH A FIGURE CONFIRMED OUTSIDE THE FILINGS:")
        for c in conflicts:
            log.info("  id=%-4d %-34s stored %s (%s) vs filings %s",
                     c["id"], c["address"][:34], c["was"], c["outside_source"], c["now"])

    OUT.write_text(json.dumps({"changed": changes, "refused": refused,
                               "conflicts": conflicts}, indent=1, ensure_ascii=False),
                   encoding="utf-8")

    if apply:
        for c in changes:
            p = session.get(Project, c["id"])
            if c["outside_source"]:
                # An outside source beats the filings. Flag, do not overwrite.
                session.add(FlaggedExtraction(
                    project_id=p.id, field_name="residential_units", status="open",
                    current_value=str(p.residential_units),
                    user_note="Re-extraction reads %s from the filings, but the stored %s was "
                              "confirmed by an outside source (%s). Stored figure kept; the two "
                              "disagree and only one can be right."
                              % (c["now"], c["was"], c["outside_source"])))
                p.units_confidence = "contradicted"
                continue
            p.residential_units = c["now"]
            p.units_confidence = "corroborated"
            p.notes = ((p.notes + " | ") if p.notes else "") + (
                "residential_units %s -> %s on re-extraction with the corrected prompt "
                "(%s). Readings: %s. Verbatim: %r"
                % (c["was"], c["now"], c["date"],
                   ", ".join("%s=%s" % (d, u) for d, u in c["readings"]), c["quote"]))
        session.commit()
        log.info("\nAPPLIED %d (%d held back as conflicts)",
                 len(changes) - len(conflicts), len(conflicts))
    else:
        log.info("\nNOT APPLIED -- re-run with --apply")
    session.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--workers", type=int, default=6)
    a = ap.parse_args()
    main(apply=a.apply, dry=a.dry_run, workers=a.workers)
