"""
Report meeting activity for a Rhode Island Open Meetings Portal board.

Confirms two things an EntityID alone cannot: that the board name renders as
expected on its own dashboard, and that the body is actually still meeting.
A body can exist in the portal's directory for years after it stopped filing.

Dates are parsed from the 14-digit numeric sort key, never from the display
text -- the key is unambiguous and locale-independent. The key is not a literal
prefix of the display string as it appears in rendered text: it lives in its own
hidden element, and the human-readable date follows as a sibling node:

    <span onclick="ViewMeetingDetails(1089966)">
      <div id="hdnDateSeq" style="display:none;">20260616000000</div>Jun 16, 2026
    </span>

so it is read out of div#hdnDateSeq rather than by regex against surrounding
text. MeetingIDs come from the same span's ViewMeetingDetails(...) argument.

    python scraper/ri_board_activity.py 2516 382 2506
"""

import re
import sys
import time
import logging
from pathlib import Path
from datetime import datetime

import httpx
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format="%(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

DASH = ("https://opengov.sos.ri.gov/OpenMeetingsPublic/OpenMeetingDashboard"
        "?subtopmenuId=201&EntityID={eid}&MeetingID=0")
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    )
}

MEETING_ID = re.compile(r"ViewMeetingDetails\((\d+)\)")

# Dashboard tables, by the ids the portal assigns them
TABLES = {
    "NextDayEntitiesSummary":     "upcoming",
    "YesterDayEntitiesSummary":   "past",
    "CancelledMeetingSummary":    "cancelled",
    "RecentlyFiledMeetingMinutes": "minutes",
}


def _dates(node) -> list[datetime]:
    """Meeting dates from a table, read out of the hidden sort-key divs."""
    out = []
    for div in node.find_all("div", id="hdnDateSeq"):
        raw = div.get_text(strip=True)
        try:
            out.append(datetime.strptime(raw, "%Y%m%d%H%M%S"))
        except ValueError:
            continue
    return out


def meeting_ids(node) -> list[str]:
    return MEETING_ID.findall(str(node))


def activity(client: httpx.Client, eid: int) -> dict:
    r = client.get(DASH.format(eid=eid), timeout=60)
    s = BeautifulSoup(r.text, "html.parser")
    h1 = s.find("h1")
    name = h1.get_text(" ", strip=True) if h1 else ""

    per_table, all_dates = {}, []
    for tid, label in TABLES.items():
        tb = s.find("table", id=tid)
        if not tb:
            per_table[label] = (0, None)
            continue
        ds = _dates(tb)
        rows = max(0, len(tb.find_all("tr")) - 1)   # minus header
        per_table[label] = (rows, max(ds) if ds else None)
        all_dates.extend(ds)

    meeting_dates = [d for d in all_dates if d.year >= 2000]
    return {
        "entity_id": eid,
        "name": name,
        "tables": per_table,
        "latest": max(meeting_dates) if meeting_dates else None,
        "earliest": min(meeting_dates) if meeting_dates else None,
        "n_dates": len(meeting_dates),
    }


def main(ids: list[int]):
    today = datetime(2026, 8, 17)
    with httpx.Client(headers=HEADERS, follow_redirects=True) as client:
        for eid in ids:
            try:
                a = activity(client, eid)
            except Exception as exc:
                log.info("%-6d  ERROR %s: %s", eid, type(exc).__name__, exc)
                continue

            latest = a["latest"]
            if latest is None:
                verdict = "NO MEETINGS FOUND"
            else:
                days = (today - latest).days
                verdict = (
                    "ACTIVE" if days <= 120 else
                    "STALE (>4mo)" if days <= 730 else
                    "DORMANT (>2yr)"
                )
            log.info("")
            log.info("EntityID %-6d %s", eid, a["name"] or "<no name — entity does not exist>")
            log.info("  status:   %s", verdict)
            if latest:
                log.info("  latest:   %s   (%d days ago)", latest.date(), (today - latest).days)
                log.info("  earliest: %s", a["earliest"].date())
            for label, (rows, mx) in a["tables"].items():
                log.info("  %-10s rows=%-4d latest=%s", label, rows, mx.date() if mx else "—")
            time.sleep(0.5)


if __name__ == "__main__":
    args = [int(x) for x in sys.argv[1:]]
    main(args or [2516])
