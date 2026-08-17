"""
Fold non-canonical asset_class values onto the canonical set.

`asset_class` may only hold a value from app/data.py::ASSET_CLASSES. This
script applies ASSET_CLASS_FOLDS to existing records and backfills
`asset_class_raw` with the verbatim source classification for every row, so
the pre-fold distinction stays recoverable and the detail view can still show
what the filing actually said.

Prints a before/after diff of every affected chart value -- per-class record
counts and per-class GSF on the Gross SF by Asset Class chart, scoped the same
way the chart scopes them (has_financials and not conditional_alternative).

Idempotent: `asset_class_raw` is only written when empty, so re-running never
overwrites a preserved original with an already-folded value.

    python scraper/fold_asset_class.py --dry-run
    python scraper/fold_asset_class.py
"""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import init_db, get_session
from db.models import Project
from app.data import ASSET_CLASSES, ASSET_CLASS_FOLDS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def _chart_scope(p: Project) -> bool:
    """Mirrors the Gross SF by Asset Class chart's row filter."""
    has_financials = (
        p.extraction_timestamp is not None
        or (p.requires_extraction is not None and not p.requires_extraction)
    )
    return has_financials and not bool(p.conditional_alternative)


def snapshot(session) -> dict:
    """Per-asset-class record count and GSF, as the chart computes them."""
    out: dict[str, dict] = {}
    for p in session.query(Project).all():
        if not p.asset_class or not _chart_scope(p):
            continue
        gsf = p.total_gsf if p.total_gsf is not None else p.bpda_gsf
        row = out.setdefault(p.asset_class, {"n": 0, "gsf": 0})
        row["n"] += 1
        row["gsf"] += gsf or 0
    return out


def _print_diff(before: dict, after: dict):
    keys = sorted(set(before) | set(after))
    log.info("")
    log.info("=== Gross SF by Asset Class — before -> after ===")
    for k in keys:
        b = before.get(k, {"n": 0, "gsf": 0})
        a = after.get(k, {"n": 0, "gsf": 0})
        mark = "   " if (b == a) else "->"
        log.info(
            "  %-16s n %4d -> %-4d   gsf %14s -> %-14s %s",
            k, b["n"], a["n"], f'{b["gsf"]:,}', f'{a["gsf"]:,}', mark,
        )
    tb = sum(v["gsf"] for v in before.values())
    ta = sum(v["gsf"] for v in after.values())
    nb = sum(v["n"] for v in before.values())
    na = sum(v["n"] for v in after.values())
    log.info("  %-16s n %4d -> %-4d   gsf %14s -> %-14s", "TOTAL", nb, na, f"{tb:,}", f"{ta:,}")
    if tb != ta or nb != na:
        log.error("TOTALS MOVED — a fold must redistribute between classes, never change the total.")
    else:
        log.info("  Totals unchanged, as a fold requires.")


def run(dry_run: bool = False) -> dict:
    init_db()
    session = get_session()
    try:
        before = snapshot(session)

        raw_written = folded = 0
        fold_detail: list[tuple[int, str, str, str]] = []
        deferred: dict[str, int] = {}

        for p in session.query(Project).all():
            if not p.asset_class:
                continue

            # Preserve the verbatim source value once, before any rewrite.
            if not p.asset_class_raw:
                if not dry_run:
                    p.asset_class_raw = p.asset_class
                raw_written += 1

            target = ASSET_CLASS_FOLDS.get(p.asset_class)
            if target:
                fold_detail.append((p.id, p.name or "?", p.asset_class, target))
                if not dry_run:
                    p.asset_class = target
                folded += 1
            elif p.asset_class not in ASSET_CLASSES:
                # Left as-is and counted, not silently rewritten or nulled.
                # Summarized once below rather than warned per row -- these are
                # the known deferred values, and 21 identical warnings would
                # bury the two folds that actually happened.
                deferred[p.asset_class] = deferred.get(p.asset_class, 0) + 1

        if not dry_run:
            session.commit()

        log.info("")
        log.info("=== Folded records ===")
        for pid, name, src, dst in fold_detail:
            log.info("  %-5d %-44s %-16s -> %s", pid, name[:44], src, dst)

        after = snapshot(session) if not dry_run else _simulated(before)
        _print_diff(before, after)

        n_deferred = sum(deferred.values())
        log.info("")
        log.info(
            "=== asset_class fold %s ===\n"
            "  asset_class_raw written: %d\n"
            "  Records folded:          %d\n"
            "  Non-canonical remaining: %d (deferred to the separate merge task)",
            "(DRY RUN)" if dry_run else "complete",
            raw_written, folded, n_deferred,
        )
        for label, n in sorted(deferred.items(), key=lambda kv: -kv[1]):
            log.info("      %-18s %d", label, n)
        log.info("")
        return {"raw_written": raw_written, "folded": folded, "deferred": deferred}
    finally:
        session.close()


def _simulated(before: dict) -> dict:
    """Apply the folds to a snapshot in memory, for --dry-run."""
    after: dict[str, dict] = {}
    for k, v in before.items():
        tgt = ASSET_CLASS_FOLDS.get(k, k)
        row = after.setdefault(tgt, {"n": 0, "gsf": 0})
        row["n"] += v["n"]
        row["gsf"] += v["gsf"]
    return after


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
