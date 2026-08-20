r"""Move the existing delivery data into the two new dated columns.

Nothing here goes looking for anything new. It reads what the tracker already
holds -- completion_date, which is a real day from a certificate of occupancy
or an assessor record, and expected_delivery, which is free text a filing
stated -- and lands each in the column it belongs in, with its precision and
its provenance.

Two rules decide which column a value lands in:

  a project that has delivered has a DELIVERED date and no TARGET. The forecast
  is not deleted, it is retired into field_provenance with a reason, because
  "they said 2024 and finished in 2026" is worth keeping and worth not
  counting as a forecast.

  a project that has not delivered has a TARGET and no DELIVERED, whatever the
  filing status says.

Tiers are assigned by what the source actually is, not by how much is known:
a certificate of occupancy is a registry record, a filing-extracted forecast
whose passage was never retained is unverified_prior -- the earlier extraction
did not keep the sentence, so nobody can check it without reopening the PDF.

    python scraper/backfill_delivery_dates.py --dry-run
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from db.database import get_session, engine
from scraper.delivery_dates import parse_date_phrase, format_date

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

# What the completion evidence is, in the vocabulary the rest of the tracker
# already uses for evidence strength. A certificate of occupancy and an
# assessor record are registry records; a press report of an opening is one
# web source until a second one is found.
BASIS_TIER = {
    "co_issued":          ("registry_confirmed", "certificate of occupancy"),
    "permit_final":       ("registry_confirmed", "building permit closed out"),
    "assessor_confirmed": ("registry_confirmed", "assessor record"),
    "news_confirmed":     ("web_low_confidence", "press report of the opening"),
    "human_set":          ("unverified_prior",   "set by hand"),
}


def _prov(conn, project_id, field, *, value, outcome, tier, source_type,
          source_url, source_name, source_date, passage, reason, step=5,
          live=True):
    """Write one provenance row, superseding any earlier live one.

    live=False stores a row that is history rather than the current answer --
    a forecast a delivery has overtaken. It stays readable and stays out of
    every count.
    """
    conn.execute(text(
        "update field_provenance set superseded = 1 "
        "where project_id = :p and field = :f and superseded = 0"),
        {"p": project_id, "f": field})
    conn.execute(text("""
        insert into field_provenance
            (project_id, field, value, outcome, tier, source_type, source_url,
             source_name, source_date, firm_sentence, reason, resolution_step,
             superseded, retracted)
        values (:p, :f, :v, :o, :t, :st, :url, :sn, :sd, :q, :r, :step, :sup, 0)"""),
        {"p": project_id, "f": field, "v": value, "o": outcome, "t": tier,
         "st": source_type, "url": source_url, "sn": source_name,
         "sd": source_date, "q": passage, "r": reason, "step": step,
         "sup": 0 if live else 1})


def main(dry_run: bool = False):
    session = get_session()
    conn = engine.connect()
    rows = conn.execute(text("""
        select p.id, p.name, p.address, p.city, p.completion_stage,
               p.completion_basis, p.completion_date, p.completion_source_url,
               p.completion_evidence, p.expected_delivery,
               p.processed_filing_url, p.processed_filing_name,
               -- The vintage of a forecast is the date of the document the
               -- forecast was read out of, NOT the project's latest filing.
               -- Taking the newest filing dated a 2022 target to 2026, which
               -- is precisely the confusion the vintage exists to prevent.
               (select f.date from project_filings f
                 where f.project_id = p.id and coalesce(f.is_processed,0) = 1
                   and coalesce(f.date,'') <> '' limit 1) as filing_date,
               (select f.name from project_filings f
                 where f.project_id = p.id and coalesce(f.is_processed,0) = 1
                 limit 1) as processed_name,
               coalesce(nullif(p.developer_canonical,''), nullif(p.developer,''),
                        nullif(p.applicant_entity,''), nullif(p.owner_or_agency,''))
                        as proponent
          from projects p
         where coalesce(p.excluded, 0) = 0
    """)).mappings().all()

    stats = {"delivered": 0, "delivered_unparsed": 0, "delivered_range_rejected": 0,
             "target": 0, "target_retired": 0, "target_unparsed": 0,
             "no_date_complete": 0}

    for r in rows:
        pid = r["id"]
        delivered = None          # (date, precision, tier, source bits)
        is_complete = (r["completion_stage"] or "") == "Complete"

        if is_complete:
            parsed = parse_date_phrase(r["completion_date"])
            # A RANGE is not a completion date. The assessor rows say
            # "between 2019 and 2025 tax rolls" -- an assessment that jumped
            # somewhere inside a six-year window. Storing the start of that
            # window as a year-precision delivery would assert the building
            # finished in 2019, which the evidence does not say and which no
            # reader could tell apart from a real 2019 completion.
            #
            # A target may legitimately come from a range, because there the
            # range IS the forecast. An actual may not: we either know when it
            # finished or we do not.
            if parsed and "range" in (parsed[2] or ""):
                stats["delivered_range_rejected"] += 1
                if not dry_run:
                    _prov(conn, pid, "delivered_date", value=None, outcome="null",
                          tier=None, source_type=r["completion_basis"] or "",
                          source_url=r["completion_source_url"], source_name="",
                          source_date="", passage=(r["completion_evidence"] or "")[:1000],
                          reason=f'the record states "{r["completion_date"]}", which '
                                 f"is a window rather than a date -- it establishes "
                                 f"that the building was finished, not when")
                parsed = None
                range_rejected = True
            else:
                range_rejected = False
            if parsed:
                d, prec, note = parsed
                tier, label = BASIS_TIER.get(
                    r["completion_basis"] or "", ("unverified_prior", "unrecorded basis"))
                delivered = (d, prec, tier, label, note)
            elif not range_rejected:
                # Complete, but nothing states WHEN. The stage is not in doubt;
                # the date is unknown, and an unknown date must stay null
                # rather than borrow the date the stage was established.
                stats["no_date_complete"] += 1
                if not dry_run:
                    _prov(conn, pid, "delivered_date", value=None, outcome="null",
                          tier=None, source_type=r["completion_basis"] or "",
                          source_url=r["completion_source_url"], source_name="",
                          source_date="", passage=(r["completion_evidence"] or "")[:1000],
                          reason=f"established complete by {r['completion_basis']}, "
                                 f"which evidences the stage but states no completion date")

        target = None
        parsed_t = parse_date_phrase(r["expected_delivery"])
        if r["expected_delivery"] and not parsed_t:
            stats["target_unparsed"] += 1
        if parsed_t and delivered is None:
            target = parsed_t

        if not dry_run:
            conn.execute(text("""
                update projects set
                    delivered_date = :dd, delivered_precision = :dp,
                    target_date = :td, target_precision = :tp,
                    target_stated_on = :ts, target_stated_by = :tb
                 where id = :id"""), {
                "id": pid,
                "dd": delivered[0].isoformat() if delivered else None,
                "dp": delivered[1] if delivered else None,
                "td": target[0].isoformat() if target else None,
                "tp": target[1] if target else None,
                "ts": (r["filing_date"] or None) if target else None,
                # WHO made the forecast: the party that filed it. The
                # document it appears in is provenance, not the claimant --
                # a PNF does not forecast anything, its proponent does.
                "tb": ((r["proponent"] or r["processed_name"]
                        or r["processed_filing_name"] or "the filing's proponent")
                       if target else None),
            })

        if delivered:
            stats["delivered"] += 1
            if not dry_run:
                _prov(conn, pid, "delivered_date",
                      value=format_date(delivered[0], delivered[1]),
                      outcome="resolved", tier=delivered[2],
                      source_type=r["completion_basis"] or "",
                      source_url=r["completion_source_url"],
                      source_name=delivered[3], source_date=r["completion_date"],
                      passage=(r["completion_evidence"] or "")[:1000],
                      reason=delivered[4] or None)
            if parsed_t:
                # The forecast is retired, not discarded: it stays readable as
                # a superseded provenance row so the miss stays visible.
                stats["target_retired"] += 1
                if not dry_run:
                    _prov(conn, pid, "target_date", live=False,
                          value=format_date(parsed_t[0], parsed_t[1]),
                          outcome="retired", tier=None,
                          source_type="article80_pdf",
                          source_url=r["processed_filing_url"], source_name="",
                          source_date=r["filing_date"], passage=None,
                          reason=f'forecast "{r["expected_delivery"]}" retired: the '
                                 f"project delivered "
                                 f"{format_date(delivered[0], delivered[1])}")
        elif target:
            stats["target"] += 1
            if not dry_run:
                _prov(conn, pid, "target_date",
                      value=format_date(target[0], target[1]), outcome="resolved",
                      tier="unverified_prior", source_type="article80_pdf",
                      source_url=r["processed_filing_url"],
                      source_name=r["processed_name"] or r["processed_filing_name"] or "",
                      source_date=r["filing_date"], passage=None,
                      reason=(target[2] or "") + ("; " if target[2] else "")
                             + "carried from the original filing extraction, which "
                               "did not retain the sentence stating it")

    if not dry_run:
        conn.commit()
    conn.close()
    session.close()
    for k, v in stats.items():
        log.info("%-20s %d", k, v)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    main(**vars(ap.parse_args()))
