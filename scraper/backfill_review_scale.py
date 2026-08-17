"""
Backfill the normalized `review_scale` column from each market's native field.

`review_scale` is the single column the Review Scale chart reads, so that the
chart never branches on a city name. Each market declares its permitted values
in app/data.py::MARKETS["review_scale_vocab"]; a market whose statute has no
scale classification declares None and its rows stay null, which the UI renders
as "not applicable" rather than as an empty chart.

Boston:    project_scale ("Large Project" / "Small Project", from Article 80
           tags scraped off the BPDA index) copies straight across. The label
           is preserved verbatim rather than remapped onto a cross-market tier
           vocabulary -- Article 80's 50,000 SF threshold and RIGL 45-23's
           major/minor tests are not the same test, so a shared label would
           imply a comparability that does not exist.
Cambridge: stays null. The Development Log has no scale classification.
Others:    manual MA entries carry no scale; stay null.

Idempotent. Run after db.init_db() has added the columns:
    python scraper/backfill_review_scale.py
"""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import init_db, get_session
from db.models import Project
from app.data import MARKETS, _DEFAULT_MARKET

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def native_scale(project: Project) -> str | None:
    """The market-native scale value for a row, before normalization.

    Only Boston has one today. Rhode Island writes review_scale at ingestion
    time rather than through this backfill, since its classification comes off
    the agenda text and has no pre-existing column to copy from.
    """
    city = project.city or "Boston"
    if city == "Boston":
        return project.project_scale
    return None


def run(dry_run: bool = False) -> dict:
    init_db()
    session = get_session()
    try:
        counts: dict[str, int] = {}
        written = skipped_na = skipped_null = 0

        for p in session.query(Project).all():
            city = p.city or "Boston"
            vocab = MARKETS.get(city, _DEFAULT_MARKET)["review_scale_vocab"]

            if vocab is None:
                # Market has no scale concept -- null is the correct value, and
                # is distinct from "we have not populated this yet".
                skipped_na += 1
                continue

            raw = native_scale(p)
            if not raw:
                skipped_null += 1
                continue

            if raw not in vocab:
                # Loud rather than silent: a value outside the declared
                # vocabulary means the registry and the source have drifted.
                log.warning(
                    "Project %d (%s, %s): native scale %r is not in that "
                    "market's declared vocabulary %s — left null",
                    p.id, (p.name or "?")[:40], city, raw, vocab,
                )
                continue

            if not dry_run:
                p.review_scale = raw
                p.review_scale_raw = raw
            counts[raw] = counts.get(raw, 0) + 1
            written += 1

        if not dry_run:
            session.commit()

        log.info(
            "\n=== review_scale backfill %s ===\n"
            "  Written:            %d\n"
            "  Null (market N/A):  %d\n"
            "  Null (no value):    %d\n",
            "(DRY RUN)" if dry_run else "complete",
            written, skipped_na, skipped_null,
        )
        for label, n in sorted(counts.items(), key=lambda kv: -kv[1]):
            log.info("  %-20s %d", label, n)

        return {"written": written, "na": skipped_na, "null": skipped_null, "counts": counts}
    finally:
        session.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
