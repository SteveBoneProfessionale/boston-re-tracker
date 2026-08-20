r"""Write the researched completion dates from the findings file to the database.

Separate from ri_delivery_record.py so that recording a finding and trusting it
are two decisions. This re-validates every record before writing -- a findings
file edited by hand does not get to skip the sourcing rules -- and enforces the
one invariant the two columns exist to protect:

    a project never holds a DELIVERED and a TARGET at the same time.

When a delivery lands on a project that had a forecast, the forecast is retired
into field_provenance rather than deleted, so "they said Q3 2024 and finished in
2026" stays on the record.

    python scraper/ri_delivery_apply.py --dry-run
"""

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from db.database import engine
from scraper.backfill_delivery_dates import _prov
from scraper.delivery_dates import parse_date_phrase, format_date
from scraper.ri_delivery_queue import STATE
from scraper.ri_delivery_record import validate

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)


def main(dry_run: bool = False):
    state = json.loads(STATE.read_text(encoding="utf-8"))
    conn = engine.connect()
    applied = {"delivered": 0, "target": 0, "null": 0, "rejected": 0,
               "retired": 0}

    for pid, rec in sorted(state.items(), key=lambda kv: int(kv[0])):
        errs = validate(rec)
        if errs:
            applied["rejected"] += 1
            log.warning("[%s] rejected: %s", pid, "; ".join(errs))
            continue
        pid_i = int(pid)
        field = rec["field"]
        col = "delivered" if field == "delivered_date" else "target"

        if rec["outcome"] == "null":
            applied["null"] += 1
            if not dry_run:
                _prov(conn, pid_i, field, value=None, outcome="null", tier=None,
                      source_type="web", source_url=None,
                      source_name="web search", source_date="", passage=None,
                      reason=rec["reason"], step=4)
            continue

        parsed = parse_date_phrase(rec["date"])
        if not parsed:
            applied["rejected"] += 1
            log.warning("[%s] rejected: date %r does not parse", pid, rec["date"])
            continue
        d = parsed[0]
        prec = rec["precision"]
        shown = format_date(d, prec)
        srcs = rec.get("sources") or []
        primary = srcs[0] if srcs else {}
        passage = "\n\n".join(
            f'{s.get("name") or s.get("url","")}: "{s.get("passage","")}"'
            for s in srcs)[:2000]

        applied[col] += 1
        log.info("[%s] %-9s %-12s %-18s %s", pid, col.upper(), shown,
                 rec["tier"], (primary.get("name") or primary.get("url", ""))[:46])
        if dry_run:
            continue

        if col == "delivered":
            # A delivery retires whatever forecast was on the row.
            prior = conn.execute(text(
                "select target_date, target_precision, target_stated_on, "
                "       target_stated_by from projects where id = :id"),
                {"id": pid_i}).mappings().first()
            if prior and prior["target_date"]:
                applied["retired"] += 1
                _prov(conn, pid_i, "target_date", live=False,
                      value=format_date(parse_date_phrase(str(prior["target_date"]))[0],
                                        prior["target_precision"] or "year"),
                      outcome="retired", tier=None, source_type="",
                      source_url=None, source_name=prior["target_stated_by"] or "",
                      source_date=str(prior["target_stated_on"] or ""), passage=None,
                      reason=f"forecast retired: the project delivered {shown}")
            conn.execute(text(
                "update projects set delivered_date = :d, delivered_precision = :p,"
                " target_date = null, target_precision = null,"
                " target_stated_on = null, target_stated_by = null where id = :id"),
                {"d": d.isoformat(), "p": prec, "id": pid_i})
        else:
            conn.execute(text(
                "update projects set target_date = :d, target_precision = :p,"
                " target_stated_on = :on, target_stated_by = :by where id = :id"),
                {"d": d.isoformat(), "p": prec, "on": rec.get("stated_on"),
                 "by": rec.get("stated_by") or "", "id": pid_i})

        _prov(conn, pid_i, field, value=shown, outcome="resolved",
              tier=rec["tier"], source_type=rec.get("source_type") or "web",
              source_url=primary.get("url"), source_name=primary.get("name") or "",
              source_date=primary.get("date") or "", passage=passage,
              reason=rec.get("reason") or (parsed[2] or None), step=4)

    if not dry_run:
        conn.commit()
    conn.close()
    log.info("\n%s", applied)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    main(**vars(ap.parse_args()))
