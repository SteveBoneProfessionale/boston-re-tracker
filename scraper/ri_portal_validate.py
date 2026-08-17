r"""
Validation harness for Rhode Island Open Meetings Portal parsing.

Guards the failure mode that matters most here: a parser that silently returns
zero rows because the portal changed its markup. That failure is invisible --
ingestion "succeeds", finds nothing, and the pipeline quietly reports no new
projects. It has already happened twice during development:

  1. Meeting dates were assumed to be a text prefix ("20260812000000Aug 12,
     2026"). They are not: the sort key sits in its own hidden
     <div id="hdnDateSeq"> and the display date is a sibling node. A regex
     with a lookahead for the display text matched nothing, and eight active
     boards reported "NO MEETINGS FOUND" despite 100 rows each.
  2. Document links were assumed to be <a href>. They are not: the path is the
     argument of an onclick handler, so an href scan returned zero documents.

Two layers, because either alone is insufficient:

  FIXTURE tests run offline against frozen markup. They fail if someone
  refactors the parser and breaks it. They CANNOT detect the portal changing,
  because the fixture never changes.

  LIVE canaries fetch a known-active board and assert non-empty results. They
  fail if the portal changes its markup. This is the layer that catches the
  silent-zero scrape.

Exits non-zero on any failure, loudly, in the style of cambridge_validate.py.

    python scraper/ri_portal_validate.py
    python scraper/ri_portal_validate.py --offline    # fixtures only
"""

import re
import sys
import logging
from pathlib import Path
from datetime import datetime

from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent.parent))

from scraper.ri_board_activity import _dates, meeting_ids, activity, HEADERS
from scraper.ri_meeting_docs import meeting_documents, ONCLICK_PATH

logging.basicConfig(level=logging.INFO, format="%(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

# ── Frozen markup, copied verbatim from live pages on 2026-08-17 ────────

# One row of table#YesterDayEntitiesSummary on a board dashboard.
FIXTURE_MEETING_ROW = """
<table id="YesterDayEntitiesSummary">
<tr class="grid-header"><th scope="col">Date</th><th scope="col">Time</th></tr>
<tr>
<td class="center"><a><span id="MeetingOnclick" onclick="ViewMeetingDetails(1089966)">
<div id="hdnDateSeq" style="display:none;">20260616000000</div>Jun 16, 2026</span></a>
</td>
<td class="center">
<a onclick="ViewMeetingDetails(1089966)" style="cursor:pointer;color:#428bca;"><span> 06:00PM</span></a>
</td>
</tr>
</table>
"""

# The agenda/minutes links on a MeetingInformation page.
FIXTURE_DOC_LINKS = r"""
<td>
<a onclick="DownloadMeetingFiles('\\Notices\\4009\\2026\\562476.pdf')"
   style="cursor:pointer;color: #428bca;">Agenda filed on Jul 24 2026, 11:37AM by Carl Johnson</a>
</td>
<td>
<a onclick="DownloadMeetingFiles('\\Minutes\\4009\\2026\\563606.pdf')"
   style="cursor:pointer;color: #428bca;">Minutes filed on Aug 12 2026, 07:53AM by AnaMaria Salum</a>
</td>
"""

# Boards used as live canaries: chosen because they meet monthly and have long
# filing histories, so an empty result means the parser broke, not that the
# board went quiet. EntityID -> expected board name.
CANARIES = {
    2516: "Pawtucket City Planning Commission",
    2767: "Providence City Plan Commission",
    748:  "Cranston Zoning Board of Review",
}

# A meeting known to carry both an agenda and minutes.
CANARY_MEETING = 1091758
CANARY_MEETING_BOARD = "Pawtucket Board of Appeals"

_failures: list[str] = []


def check(label: str, condition: bool, detail: str = ""):
    if condition:
        log.info("  PASS  %s", label)
    else:
        log.error("  FAIL  %s%s", label, f"  — {detail}" if detail else "")
        _failures.append(label)


# ── Fixture tests ───────────────────────────────────────────────────────

def test_fixtures():
    log.info("\n=== FIXTURE: meeting date parsing ===")
    soup = BeautifulSoup(FIXTURE_MEETING_ROW, "html.parser")
    table = soup.find("table", id="YesterDayEntitiesSummary")

    dates = _dates(table)
    check("hdnDateSeq yields exactly one date", len(dates) == 1, f"got {len(dates)}")
    check(
        "date parses to 2026-06-16",
        dates and dates[0] == datetime(2026, 6, 16),
        f"got {dates[0] if dates else None}",
    )

    ids = meeting_ids(table)
    check("ViewMeetingDetails yields a MeetingID", "1089966" in ids, f"got {ids}")

    # The specific regression, stated precisely. In EXTRACTED TEXT the sort key
    # does read as a prefix of the display date ("20260616000000Jun 16, 2026"),
    # which is why that description looks correct. In MARKUP it is not: the key
    # is closed off by </div> before the display date begins. A regex run over
    # raw HTML expecting the display text to follow the digits therefore matches
    # nothing. Both halves are asserted so the distinction can't be lost.
    naive_html = re.search(r"\d{14}(?=[A-Z][a-z]{2}\s)", FIXTURE_MEETING_ROW)
    check(
        "naive prefix regex finds nothing in raw HTML (documents the bug)",
        naive_html is None,
        "markup changed — digits now adjacent to display date in HTML; re-check parser",
    )
    check(
        "same regex WOULD match extracted text (explains the wrong assumption)",
        re.search(r"\d{14}(?=[A-Z][a-z]{2}\s)", table.get_text("", strip=True)) is not None,
        "text-level adjacency changed too — the whole date layout moved",
    )
    check(
        "sort key is isolated inside div#hdnDateSeq",
        table.find("div", id="hdnDateSeq") is not None
        and table.find("div", id="hdnDateSeq").get_text(strip=True) == "20260616000000",
        "hdnDateSeq div missing or renamed — this is the parser's only anchor",
    )

    log.info("\n=== FIXTURE: document link parsing ===")
    dsoup = BeautifulSoup(FIXTURE_DOC_LINKS, "html.parser")
    found = [ONCLICK_PATH.search(a["onclick"]).group(1)
             for a in dsoup.find_all("a", onclick=True)
             if ONCLICK_PATH.search(a["onclick"])]
    check("two document paths extracted from onclick", len(found) == 2, f"got {len(found)}")
    check(
        "agenda path recognized under \\Notices",
        any("Notices" in p for p in found),
        f"got {found}",
    )
    check(
        "minutes path recognized under \\Minutes",
        any("Minutes" in p for p in found),
        f"got {found}",
    )
    # Folder is not the EntityID -- 2506 files under 4009. Guards against anyone
    # "simplifying" the parser into constructing the path from an EntityID.
    check(
        "folder number is not the EntityID (4009 != 2506)",
        any("4009" in p for p in found),
        f"got {found}",
    )


# ── Live canaries ───────────────────────────────────────────────────────

def test_live():
    import httpx

    log.info("\n=== LIVE: board dashboards return non-zero meetings ===")
    with httpx.Client(headers=HEADERS, follow_redirects=True) as client:
        for eid, expected in CANARIES.items():
            try:
                a = activity(client, eid)
            except Exception as exc:
                check(f"EntityID {eid} reachable", False, f"{type(exc).__name__}: {exc}")
                continue

            check(f"EntityID {eid} name == {expected!r}", a["name"] == expected,
                  f"got {a['name']!r}")
            # The silent-zero guard.
            check(f"EntityID {eid} parsed >0 meeting dates", a["n_dates"] > 0,
                  "zero dates parsed — hdnDateSeq structure likely changed")
            past_rows, past_latest = a["tables"]["past"]
            check(f"EntityID {eid} past table has rows", past_rows > 0, f"rows={past_rows}")
            # Rows present but no dates is exactly the failure that hid before.
            check(
                f"EntityID {eid} rows and dates agree",
                not (past_rows > 0 and past_latest is None),
                f"{past_rows} rows but no parseable date — parser broke, not the board",
            )

        log.info("\n=== LIVE: meeting page returns document links ===")
        try:
            info = meeting_documents(client, CANARY_MEETING)
            check("meeting board name resolves",
                  info["board"] == CANARY_MEETING_BOARD, f"got {info['board']!r}")
            check("meeting yields >0 documents", len(info["documents"]) > 0,
                  "zero documents — onclick structure likely changed")
            kinds = {d["kind"] for d in info["documents"]}
            check("both agenda and minutes present", {"agenda", "minutes"} <= kinds,
                  f"got {kinds}")
            for d in info["documents"]:
                check(f"document url carries FilePath ({d['kind']})",
                      "FilePath=" in d["url"], d["url"][:80])
        except Exception as exc:
            check("meeting page reachable", False, f"{type(exc).__name__}: {exc}")


def main(offline: bool = False):
    test_fixtures()
    if offline:
        log.info("\n(--offline: skipping live canaries)")
    else:
        test_live()

    log.info("")
    if _failures:
        log.error("=== %d CHECK(S) FAILED ===", len(_failures))
        for f in _failures:
            log.error("  - %s", f)
        log.error("\nDo NOT trust an ingestion run until these pass.")
        return 1
    log.info("=== ALL CHECKS PASSED ===")
    return 0


if __name__ == "__main__":
    sys.exit(main(offline="--offline" in sys.argv))
