r"""Record and apply field fills for the top 100 Boston and Cambridge projects.

State lives in data/top100_findings.json, one record per (project, field), so
the sweep survives being run in pieces and nothing is researched twice.

Three things this enforces, because each was a way to get it wrong:

WHOSE NUMBER IS IT. An Article 80 filing is full of other projects' figures --
cumulative-impact tables, neighbouring buildings, master-plan totals. A regex
pass over the held PDFs produced ten unit counts and eight were somebody else's:
66 Cambridge Street picked up The Graphic across the street, 17 Bradston picked
up 771 Harrison Avenue out of a traffic-capacity list, and Seaport Square Block
G picked up the 3,200 homes of the entire Seaport Square master plan. Every
value carries the passage so this is checkable rather than trusted.

WHOSE PHASE IS IT. Suffolk Downs, Seaport Square, Cambridge Crossing, Kendall
Common and the MXD Infill buildings are phases. A firm named for the master plan
is not the firm for a phase unless the source says the phase. Where only the
master plan is named, that is what gets recorded, and the phase stays null.

WHETHER A CONTRACTOR CAN EXIST YET. A developer does not appoint a general
contractor years before groundbreaking. Anything not under construction or
holding a building permit records not_yet_selected without a search being spent
on it -- which is a different fact from "we looked and found nobody", and the
screener already renders the two differently.

    python scraper/top100_fill.py --apply
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from db.database import engine
from scraper.backfill_delivery_dates import _prov
from scraper.ri_delivery_queue import BLOCKED

STATE = Path("data/top100_findings.json")
logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

FIELD_COLUMN = {
    "architect": "architect",
    "civil_engineer": "civil_engineer",
    "general_contractor": "general_contractor",
    "residential_units": "residential_units",
}
VALID_TIERS = ("document_confirmed", "registry_confirmed",
               "web_corroborated", "web_low_confidence")


def _host(url: str) -> str:
    try:
        return (urlparse(url).netloc or "").lower().replace("www.", "")
    except Exception:
        return ""


def validate(rec: dict) -> list[str]:
    errs = []
    if rec.get("field") not in FIELD_COLUMN:
        errs.append(f"field must be one of {tuple(FIELD_COLUMN)}")
    if rec.get("outcome") not in ("resolved", "null", "not_yet_selected"):
        errs.append("outcome must be resolved | null | not_yet_selected")
    srcs = rec.get("sources") or []
    for s in srcs:
        h = _host(s.get("url", ""))
        if any(b in h for b in BLOCKED):
            errs.append(f"aggregator source not allowed: {h}")
        if not (s.get("passage") or "").strip():
            errs.append(f"source {h} carries no passage")
    if rec.get("outcome") == "resolved":
        if rec.get("value") in (None, "", 0):
            errs.append("a resolved record needs a value")
        if rec.get("tier") not in VALID_TIERS:
            errs.append(f"tier must be one of {VALID_TIERS}")
        hosts = {_host(s.get("url", "")) for s in srcs}
        if rec.get("tier") == "web_corroborated" and len(hosts) < 2:
            errs.append(f"web_corroborated needs two independent hosts, got {hosts}")
        if rec.get("tier") in ("web_low_confidence", "document_confirmed",
                               "registry_confirmed") and not srcs:
            errs.append("a resolved record needs a source")
    elif not (rec.get("reason") or "").strip():
        errs.append("a null or not_yet_selected needs a reason")
    return errs


def record(payload):
    state = json.loads(STATE.read_text(encoding="utf-8")) if STATE.exists() else {}
    ok = bad = 0
    for rec in payload:
        errs = validate(rec)
        key = f"{rec.get('id')}:{rec.get('field')}"
        if errs:
            bad += 1
            print(f"[{key}] REJECTED")
            for e in errs:
                print(f"    - {e}")
            continue
        state[key] = rec
        ok += 1
    STATE.write_text(json.dumps(state, indent=1, ensure_ascii=False), encoding="utf-8")
    print(f"{ok} recorded, {bad} rejected, {len(state)} in state")


def apply(dry_run: bool = False):
    state = json.loads(STATE.read_text(encoding="utf-8"))
    conn = engine.connect()
    counts = {"resolved": 0, "null": 0, "not_yet_selected": 0, "rejected": 0}
    for key, rec in sorted(state.items()):
        if validate(rec):
            counts["rejected"] += 1
            continue
        pid, field = int(rec["id"]), rec["field"]
        col = FIELD_COLUMN[field]
        srcs = rec.get("sources") or []
        primary = srcs[0] if srcs else {}
        passage = "\n\n".join(
            f'{s.get("name") or s.get("url","")}: "{s.get("passage","")}"' for s in srcs)[:2000]

        if rec["outcome"] == "resolved":
            counts["resolved"] += 1
            if not dry_run:
                conn.execute(text(f"update projects set {col} = :v where id = :id"),
                             {"v": rec["value"], "id": pid})
                _prov(conn, pid, field, value=str(rec["value"]), outcome="resolved",
                      tier=rec["tier"], source_type=rec.get("source_type") or "web",
                      source_url=primary.get("url"), source_name=primary.get("name") or "",
                      source_date=primary.get("date") or "", passage=passage,
                      reason=rec.get("reason"), step=6)
        elif rec["outcome"] == "not_yet_selected":
            counts["not_yet_selected"] += 1
            if not dry_run:
                conn.execute(text(
                    "update projects set general_contractor = 'not_yet_selected' "
                    "where id = :id and coalesce(general_contractor,'') = ''"), {"id": pid})
                _prov(conn, pid, field, value=None, outcome="not_yet_selected",
                      tier=None, source_type="stage_rule", source_url=None,
                      source_name="project stage", source_date="", passage=None,
                      reason=rec["reason"], step=6)
        else:
            counts["null"] += 1
            if not dry_run:
                _prov(conn, pid, field, value=None, outcome="null", tier=None,
                      source_type=rec.get("source_type") or "web", source_url=None,
                      source_name="web search", source_date="", passage=None,
                      reason=rec["reason"], step=6)
    if not dry_run:
        conn.commit()
    conn.close()
    log.info("%s", counts)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    if a.apply:
        apply(a.dry_run)
    else:
        record(json.loads(sys.stdin.read()))
