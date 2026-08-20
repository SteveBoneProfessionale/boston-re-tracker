r"""Record what the Rhode Island minutes actually establish about completion.

ri_completion_scan.py mined the 19 MB of held minutes for completion claims
near each project's address. It surfaced 41 passages across 19 projects. Every
one was read in full context, and all but one turned out to be about something
other than the project's building:

  22 Rye Street        "we anticipate an early spring 2022 project delivery
                       date" -- the Roger Williams Park Gateway Project, three
                       items later in the same executive director's report
  220 Broad Street     "Completion is anticipated later this fall" -- the PRA's
                       five-year strategic plan
  50 Sims Avenue       "completion date of 2019" -- the Farm Fresh food hub
                       easement item; same address, different filing, and the
                       identity with PRA referral 3487 is not established
  50 Sims Avenue       "projects are to be completed by 2026" -- the ARPA
                       obligation deadline for the housing trust round
  327 Elmwood Avenue   "a path to completion in 2026 or sooner" -- a scoring
                       criterion for a funding round
  233-261 Richmond     "Work was completed without DDRC review" -- the window
   / 186 Fountain      alterations at Trinity Brewhouse, the item above it
  322 Washington       "due diligence has been completed"
  74 Rolfe Square      "have been completed" -- the 2010 CVS at 1776 Broad St

That is the finding, and it is worth writing down: an address appearing in a
meeting is not the same as that meeting discussing the project's schedule.
Boards minute what they decide, and completion is not something they decide.

Twenty-two further passages are board CONDITIONS -- "shall be started and
substantially completed within twelve (12) months of the date of decision".
Those are deadlines imposed on an applicant, not forecasts made by one, and
turning one into a TARGET would put the board's words in the developer's
mouth. They are recorded as the reason a target is null, never as the target.

    python scraper/ri_apply_completion_docs.py --dry-run
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from db.database import engine
from scraper.backfill_delivery_dates import _prov

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

_MIN_195 = ("https://opengov.sos.ri.gov/Common/DownloadMeetingFiles"
            "?FilePath=\\Minutes\\5943\\2025\\538662.pdf")

# The one claim that survived reading. Two later meetings corroborate it: the
# 19 November 2025 minutes say the ribbon cutting "was held in early October",
# and the 2 February 2026 minutes repeat that the Ocean State Labs incubator
# at 150 Richmond received its certificate of occupancy.
CONFIRMED = [
    dict(
        project_id=510,
        field="delivered_date",
        date="2025-01-01",
        precision="year",
        display="2025",
        tier="document_confirmed",
        source_type="minutes",
        source_url=_MIN_195,
        source_name="I-195 Redevelopment District Commission minutes, "
                    "17 September 2025",
        source_date="2025-09-17",
        passage="She stated 150 Richmond had received its Certificate of "
                "Occupancy and that Rhode Island State Health Lab had started "
                "to move in; she also stated planning for a building ribbon "
                "cutting event was underway.",
        reason="the minutes establish the certificate of occupancy had been "
               "received by the 17 September 2025 meeting but do not date it, "
               "so the precision is the year and not the month -- September is "
               "when the commission was told, not when the building finished. "
               "Corroborated by the 19 November 2025 minutes (ribbon cutting "
               "'held in early October') and the 2 February 2026 minutes.",
    ),
]

# Where the documents were read and came back with nothing. Recorded rather
# than left blank, because a null with a reason is a result and a bare null is
# an open question.
NULLED = [
    (716, "target_date", "the only dated completion phrase near this address "
                         "is the Roger Williams Park Gateway Project, a "
                         "different item in the same PRA report"),
    (679, "target_date", "'Completion is anticipated later this fall' is the "
                         "PRA's five-year strategic plan, not the building"),
    (717, "target_date", "the 2019 completion date belongs to the Farm Fresh "
                         "food hub easement item at the same address; identity "
                         "with PRA referral 3487 is not established"),
    (604, "target_date", "'a path to completion in 2026 or sooner' is a "
                         "scoring criterion for a housing trust funding round"),
    (627, "target_date", "the minutes state completion 'within 18 months from "
                         "PRA funds closing' -- a relative term with no anchor "
                         "date in the record, so no date can be stored"),
    (769, "delivered_date", "'Work was completed without DDRC review' is the "
                            "Trinity Brewhouse window alteration at 186 "
                            "Fountain Street, the item above this one"),
    (771, "delivered_date", "the completed work is a window and garage door "
                            "alteration, not the delivery of a development"),
    (492, "delivered_date", "'due diligence has been completed' is not a "
                            "building completion"),
    (869, "delivered_date", "the completed improvements are the 2010 CVS at "
                            "1776 Broad Street, a different project"),
]


def main(dry_run: bool = False):
    conn = engine.connect()
    for c in CONFIRMED:
        col = "delivered" if c["field"] == "delivered_date" else "target"
        log.info("%s  project %s  <- %s (%s)", col.upper(), c["project_id"],
                 c["display"], c["precision"])
        if dry_run:
            continue
        conn.execute(text(
            f"update projects set {col}_date = :d, {col}_precision = :p "
            f"where id = :id"),
            {"d": c["date"], "p": c["precision"], "id": c["project_id"]})
        if col == "delivered":
            # A delivery retires the forecast. Nothing to retire here -- this
            # project never had one -- but the invariant is enforced anyway,
            # so it holds no matter what order these scripts run in.
            conn.execute(text(
                "update projects set target_date = null, target_precision = null,"
                " target_stated_on = null, target_stated_by = null where id = :id"),
                {"id": c["project_id"]})
        _prov(conn, c["project_id"], c["field"], value=c["display"],
              outcome="resolved", tier=c["tier"], source_type=c["source_type"],
              source_url=c["source_url"], source_name=c["source_name"],
              source_date=c["source_date"], passage=c["passage"],
              reason=c["reason"], step=2)

    for pid, field, reason in NULLED:
        log.info("null      project %-4s %-15s %s", pid, field, reason[:60])
        if dry_run:
            continue
        _prov(conn, pid, field, value=None, outcome="null", tier=None,
              source_type="minutes", source_url=None,
              source_name="Rhode Island board minutes corpus", source_date="",
              passage=None, reason=reason, step=2)

    if not dry_run:
        conn.commit()
    conn.close()
    log.info("\n%d confirmed, %d recorded null", len(CONFIRMED), len(NULLED))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    main(**vars(ap.parse_args()))
