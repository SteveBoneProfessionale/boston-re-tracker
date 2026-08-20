r"""Record one researched completion date, or one researched null.

State lives in data/ri_delivery_findings.json, one record per project, so the
sweep can be run in passes and a later pass never re-researches a project an
earlier one settled. Reading is separate from writing to the database on
purpose: ri_delivery_apply.py does that, and only for records that pass the
sourcing rules here.

The rules, applied at record time so a bad record cannot be written:

  web_corroborated    two independent sources, neither an aggregator, both
                      naming this project unambiguously
  web_low_confidence  one such source
  document_confirmed  a primary document with the passage quoted
  registry_confirmed  a permit, certificate-of-occupancy or assessor record

Two sources from the same outlet are one source. A source that names the
address but not the building, or names the building at a different phase, is
not a source for this project -- record the null and say so.

    python scraper/ri_delivery_record.py < record.json
    echo '[{...}]' | python scraper/ri_delivery_record.py
"""

import json
import sys
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).parent.parent))

from scraper.delivery_dates import parse_date_phrase, format_date
from scraper.ri_delivery_queue import BLOCKED, STATE

VALID_FIELDS = ("delivered_date", "target_date")
VALID_OUTCOMES = ("resolved", "null")
VALID_TIERS = ("document_confirmed", "registry_confirmed",
               "web_corroborated", "web_low_confidence")


def _host(url: str) -> str:
    try:
        return (urlparse(url).netloc or "").lower().replace("www.", "")
    except Exception:
        return ""


def validate(rec: dict) -> list[str]:
    errs = []
    if rec.get("field") not in VALID_FIELDS:
        errs.append(f"field must be one of {VALID_FIELDS}")
    if rec.get("outcome") not in VALID_OUTCOMES:
        errs.append(f"outcome must be one of {VALID_OUTCOMES}")

    srcs = rec.get("sources") or []
    for s in srcs:
        h = _host(s.get("url", ""))
        if any(b in h for b in BLOCKED):
            errs.append(f"aggregator source not allowed: {h}")
        if not (s.get("passage") or "").strip():
            errs.append(f"source {h} has no supporting passage")

    if rec.get("outcome") == "resolved":
        if not rec.get("date"):
            errs.append("resolved record needs a date")
        if rec.get("tier") not in VALID_TIERS:
            errs.append(f"tier must be one of {VALID_TIERS}")
        # The tier is a claim about how many independent sources there are, so
        # it is checked against them rather than trusted.
        hosts = {_host(s.get("url", "")) for s in srcs}
        if rec.get("tier") == "web_corroborated" and len(hosts) < 2:
            errs.append(f"web_corroborated needs two independent hosts, got {hosts}")
        if rec.get("tier") == "web_low_confidence" and len(hosts) < 1:
            errs.append("web_low_confidence needs a source")
        if rec.get("precision") not in ("day", "month", "quarter", "year"):
            errs.append("precision must be day|month|quarter|year")
        if rec["field"] == "target_date" and not rec.get("stated_on"):
            errs.append("a target needs the date the forecast was made "
                        "(stated_on); a forecast without a vintage is not usable")
    else:
        if not (rec.get("reason") or "").strip():
            errs.append("a null needs a reason -- what was searched and why "
                        "nothing was usable")
    return errs


def main():
    payload = json.loads(sys.stdin.read())
    if isinstance(payload, dict):
        payload = [payload]
    state = json.loads(STATE.read_text(encoding="utf-8")) if STATE.exists() else {}

    ok = bad = 0
    for rec in payload:
        errs = validate(rec)
        pid = str(rec.get("id"))
        if errs:
            bad += 1
            print(f"[{pid}] REJECTED")
            for e in errs:
                print(f"    - {e}")
            continue
        if rec.get("outcome") == "resolved":
            parsed = parse_date_phrase(rec["date"])
            shown = format_date(parsed[0], rec["precision"]) if parsed else rec["date"]
            rec["display"] = shown
            print(f'[{pid}] {rec["field"]:<14} {shown:<12} '
                  f'{rec["tier"]:<18} {len(rec.get("sources") or [])} source(s)')
        else:
            print(f'[{pid}] {rec["field"]:<14} null         '
                  f'{(rec.get("reason") or "")[:60]}')
        state[pid] = rec
        ok += 1

    STATE.write_text(json.dumps(state, indent=1, ensure_ascii=False), encoding="utf-8")
    print(f"\n{ok} recorded, {bad} rejected, {len(state)} total in state")


if __name__ == "__main__":
    main()
