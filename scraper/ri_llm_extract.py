r"""
One LLM extraction pass over the full agenda and minutes corpus.

WHY THIS REPLACES THE PARSERS

The existing per-city regex parsers do not read the documents. They read
ri_llm_items.json, whose descriptions are truncated at exactly 1,200
characters -- 22% of items hit that ceiling. Every parser fix since has been
an attempt to recover, from a clipped summary, information that was cut off
before the parser ever saw it. Twenty fixes, each one silently discarding
real data, all downstream of one truncation.

This reads the untruncated source text instead.

It also removes the failure mode the parsers could never fully solve: an
agenda prints unrelated projects end to end, so any window-based extractor
eventually hands one project's number to its neighbour. A 210,000 sq ft
warehouse at 20 Goddard Drive was offered as the floor area of two different
Cranston schemes. The model sees the document as a document and attributes
per item.

GUARDS CARRIED OVER, ENFORCED IN THE PROMPT

  * Floor area ONLY where stated as building floor area. Never derived from
    units, acreage, footprint, storey count or a zoning code. Lot area goes
    in its own field so the two can never be confused.
  * A threshold quoted from the ordinance ("over 10,000 SF of gross floor
    area") is not a building size.
  * A comparable building in another town is not this project.
  * Attorneys, architects, engineers and municipal staff are NOT developers.
  * Every extracted value carries the sentence it came from.

Resumable: results append to a JSONL keyed by text_file, and a re-run skips
what is already done. Spend is tracked per call and the run aborts at
--max-usd rather than silently overrunning.

    python scraper/ri_llm_extract.py --dry-run       # cost estimate only
    python scraper/ri_llm_extract.py --limit 20      # small live sample
    python scraper/ri_llm_extract.py --max-usd 15
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
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

CORPUS = ROOT / "data" / "ri_agenda_corpus.json"
TEXTDIR = ROOT / "data" / "ri_pdfs" / "text"
OUT = ROOT / "data" / "ri_llm_extract.jsonl"

MODEL = "claude-haiku-4-5-20251001"
PRICE_IN, PRICE_OUT = 1.00 / 1e6, 5.00 / 1e6      # Haiku 4.5, USD per token

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

SYSTEM = """You extract development-project facts from Rhode Island municipal \
planning documents (agendas and minutes). Return ONLY JSON.

An agenda lists SEVERAL UNRELATED projects end to end. Attribute every fact to \
the item it belongs to. Never carry a figure from one item into another. If a \
document contains no development items, return {"items": []}.

For each item return an object with these keys (use null when the document does \
not state it -- NEVER guess, NEVER calculate):

  address            street address as written, or null
  plat_lot           assessor plat/lot as written (e.g. "AP 37, Lot 888")
  case_number        e.g. "23-029MA", or null
  project_name       a named project in quotes (e.g. "Champlin Heights"), or null
  applicant          the named applicant/petitioner entity
  developer          the party EXECUTING the development. A public agency, a
                     redevelopment authority or a passive landowner is NOT the
                     developer even when it is the applicant -- put those in
                     owner_or_agency. An attorney, architect, engineer or
                     municipal official is NEVER the developer.
  owner_or_agency    public agency or passive landowner, if any
  attorney           land use attorney, if named
  architect          architect firm or person, if named
  engineer           civil engineer, if named
  residential_units  integer, DWELLING units only. Read spelled-out counts
                     too ("sixteen (16) multi-unit" -> 16, "ten-unit" -> 10).
                     COUNT THE RIGHT NOUN. These are NOT dwelling units and
                     must return null for this field:
                       storeys   "5-story residential building" is not 5 units
                       buildings "4 multi-family residential buildings" is not
                                 4 units -- that scheme had 133 apartments
                       lots      "create 44 lots for single-family use"
                       storage units, parking spaces, hotel rooms, bedrooms
                     If the text gives a building or storey count and no
                     dwelling count, return null rather than the wrong noun.
  affordable_units   integer, if stated
  building_sf        integer. ONLY a figure explicitly describing the BUILDING's
                     floor area (gross floor area, GFA, building area, floor
                     area). NEVER derive it from units, acreage, footprint,
                     storey count or zoning. A threshold quoted from the
                     ordinance ("more than 10,000 sq ft of gross floor area")
                     is NOT a building size -> null. A building in another
                     town cited for comparison is NOT this project -> null.
  lot_sf             integer, land/lot/parcel area if stated
  site_acres         number, if stated
  stories            integer, if stated
  parking_spaces     integer, if stated
  asset_class        one of: multifamily, mixed-use, office, lab, industrial,
                     retail, hotel, institutional, self-storage, other, null
  review_scale       "Major", "Minor", "Administrative" or null -- ONLY if the
                     document says so for THIS item. A section heading above a
                     different item does not count.
  filing_type        e.g. "master plan", "preliminary plan", "final plan",
                     "variance", "special use permit", "rezoning", "pre-application"
  stage              the review stage reached, if stated
  outcome            one of: approved, denied, withdrawn, continued, tabled,
                     remanded, extension_granted, no_action, null
  is_withdrawn       true ONLY if the document says this project was withdrawn,
                     denied, dropped, expired or will not be pursued
  evidence           object mapping ONLY these fields, when present, to the
                     VERBATIM phrase that states them: residential_units,
                     building_sf, developer, is_withdrawn, review_scale.
                     Keep each quote under 25 words.

OUTPUT SIZE RULES, IMPORTANT:
  * OMIT every key whose value is null. Return only fields you actually found.
  * Do not repeat the address inside other fields.
  * No commentary, no markdown fences. JSON only.

Return: {"items": [ ... ]}"""


class Spend:
    def __init__(self):
        self.lock = threading.Lock()
        self.usd = 0.0
        self.tin = 0
        self.tout = 0
        self.calls = 0

    def add(self, u):
        with self.lock:
            self.tin += u.get("input_tokens", 0)
            self.tout += u.get("output_tokens", 0)
            self.usd += u.get("input_tokens", 0) * PRICE_IN + \
                u.get("output_tokens", 0) * PRICE_OUT
            self.calls += 1
            return self.usd


def api_key():
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        if line.startswith("ANTHROPIC_API_KEY"):
            return line.split("=", 1)[1].strip()
    return os.environ.get("ANTHROPIC_API_KEY", "")


KEY = None


def call(text, retries=4):
    body = json.dumps({
        "model": MODEL,
        "max_tokens": 8000,
        "system": SYSTEM,
        "messages": [{"role": "user", "content": text}],
    }).encode("utf-8")
    last = None
    for a in range(retries):
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages", data=body,
            headers={"x-api-key": KEY, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=180) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            last = "HTTP %s %s" % (e.code, e.read().decode()[:200])
            if e.code in (429, 500, 502, 503, 529):
                time.sleep(min(2 ** a * 2, 40))
                continue
            raise RuntimeError(last)
        except Exception as e:                                  # noqa: BLE001
            last = "%s: %s" % (type(e).__name__, e)
            time.sleep(min(2 ** a * 2, 40))
    raise RuntimeError(last or "exhausted retries")


def parse_json(s):
    s = s.strip()
    m = re.search(r"```(?:json)?\s*(.*?)```", s, re.S)
    if m:
        s = m.group(1).strip()
    i, j = s.find("{"), s.rfind("}")
    if i >= 0 and j > i:
        s = s[i:j + 1]
    return json.loads(s)


def salvage(txt):
    """Complete item objects from a response truncated mid-array."""
    i = txt.find("[")
    if i < 0:
        return []
    out, depth, start = [], 0, None
    for j in range(i, len(txt)):
        c = txt[j]
        if c == "{":
            if depth == 0:
                start = j
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0 and start is not None:
                try:
                    out.append(json.loads(txt[start:j + 1]))
                except Exception:                               # noqa: BLE001
                    pass
                start = None
    return out


def load_docs():
    corpus = json.loads(CORPUS.read_text(encoding="utf-8"))
    docs = []
    for mid, m in corpus.items():
        for d in m.get("documents", []):
            tf = d.get("text_file")
            if not tf or not (d.get("chars") or 0) > 200:
                continue
            docs.append({"text_file": tf, "municipality": m.get("municipality"),
                         "board": m.get("board"), "date": m.get("date"),
                         "kind": d.get("kind"), "meeting_id": mid,
                         "chars": d.get("chars") or 0, "url": d.get("url")})
    # largest first: the big minutes carry the most, and any budget stop then
    # lands on the least valuable documents rather than the most.
    docs.sort(key=lambda d: -d["chars"])
    return docs


def done_keys():
    if not OUT.exists():
        return set()
    ks = set()
    for line in OUT.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            ks.add(json.loads(line)["text_file"])
        except Exception:                                       # noqa: BLE001
            pass
    return ks


def main():
    global KEY
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--max-usd", type=float, default=15.0)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--docs-file", default="",
                    help="newline-separated text_file names to process first")
    a = ap.parse_args()

    docs = load_docs()
    have = done_keys()
    todo = [d for d in docs if d["text_file"] not in have]
    if a.docs_file:
        want = {x.strip() for x in Path(a.docs_file).read_text().splitlines() if x.strip()}
        todo = [d for d in todo if d["text_file"] in want]
    if a.limit:
        todo = todo[:a.limit]

    chars = sum(d["chars"] for d in todo)
    est_in = chars / 3.6 + 700 * len(todo)
    est = est_in * PRICE_IN + 450 * len(todo) * PRICE_OUT
    log.info("documents total %d | already done %d | to do %d",
             len(docs), len(have), len(todo))
    log.info("chars %s | est input tokens %s | ESTIMATE $%.2f",
             f"{chars:,}", f"{int(est_in):,}", est)
    if a.dry_run:
        log.info("DRY RUN -- nothing sent.")
        return

    KEY = api_key()
    if not KEY:
        log.error("no API key")
        return

    spend = Spend()
    stop = threading.Event()
    wlock = threading.Lock()
    fh = OUT.open("a", encoding="utf-8")
    ok = err = 0

    def work(d):
        nonlocal ok, err
        if stop.is_set():
            return
        p = TEXTDIR / d["text_file"]
        if not p.exists():
            return
        text = p.read_text(encoding="utf-8", errors="replace")
        head = ("Municipality: %s\nBoard: %s\nMeeting date: %s\nDocument type: %s\n\n"
                % (d["municipality"], d["board"], d["date"], d["kind"]))
        try:
            resp = call(head + text)
        except Exception as e:                                  # noqa: BLE001
            err += 1
            log.warning("  FAIL %s: %s", d["text_file"], str(e)[:120])
            return
        total = spend.add(resp.get("usage", {}))
        try:
            data = parse_json(resp["content"][0]["text"])
            items = data.get("items", [])
        except json.JSONDecodeError:
            # A response cut off at max_tokens still holds complete items
            # before the break. Losing 30 good items because the 31st was
            # truncated would be the same silent discard the parsers made.
            items = salvage(resp["content"][0]["text"])
            if not items:
                err += 1
                log.warning("  BADJSON %s (unsalvageable)", d["text_file"])
                return
            log.info("  salvaged %d item(s) from truncated %s", len(items),
                     d["text_file"])
        except Exception as e:                                  # noqa: BLE001
            err += 1
            log.warning("  BADJSON %s: %s", d["text_file"], str(e)[:90])
            return
        rec = dict(d)
        rec["items"] = items
        with wlock:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            fh.flush()
            ok += 1
            if ok % 25 == 0:
                log.info("  %d done | %d items | $%.2f spent", ok,
                         sum(1 for _ in items), total)
        if total >= a.max_usd:
            log.warning("BUDGET CAP $%.2f reached -- stopping.", a.max_usd)
            stop.set()

    with ThreadPoolExecutor(max_workers=a.workers) as ex:
        futs = [ex.submit(work, d) for d in todo]
        for _ in as_completed(futs):
            pass
    fh.close()
    log.info("DONE  ok=%d err=%d  calls=%d  in=%s out=%s  SPENT $%.2f",
             ok, err, spend.calls, f"{spend.tin:,}", f"{spend.tout:,}", spend.usd)


if __name__ == "__main__":
    main()
