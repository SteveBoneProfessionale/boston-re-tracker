r"""
Harvest agenda/minutes PDFs for Rhode Island Tier 1 boards into a local corpus.

Read-only analysis input. Nothing here writes to the projects table -- this
exists so review-stage vocabulary, square-footage availability and field
coverage can be measured against real filings before any ingestion is designed.

Every document is cached permanently under data/ri_pdfs/ keyed by meeting id,
and extracted text is cached alongside, so a document is never fetched or
re-parsed twice.

    python scraper/ri_harvest_agendas.py --meetings 15
    python scraper/ri_harvest_agendas.py --tier 1 --meetings 20
"""

import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime

import httpx
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent.parent))

from scraper.ri_sources import BOARDS
from scraper.ri_meeting_docs import (
    meeting_documents, fetch_pdf, pdf_text, polite_get, HEADERS, CACHE_DIR,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

DASH = ("https://opengov.sos.ri.gov/OpenMeetingsPublic/OpenMeetingDashboard"
        "?subtopmenuId=201&EntityID={eid}&MeetingID=0")
CORPUS = Path(__file__).parent.parent / "data" / "ri_agenda_corpus.json"
TEXT_DIR = CACHE_DIR / "text"

MEETING_ID = re.compile(r"ViewMeetingDetails\((\d+)\)")

# Past Meetings and Recently Filed Minutes both list real meetings. Cancelled is
# excluded deliberately: it carries a year typo'd as 2118 and future-dated rows.
MEETING_TABLES = ("YesterDayEntitiesSummary", "RecentlyFiledMeetingMinutes")


def board_meetings(client: httpx.Client, eid: int, limit: int) -> list[dict]:
    """Most recent (date, meeting_id) pairs for a board, newest first."""
    r = polite_get(client, DASH.format(eid=eid))
    r.raise_for_status()
    s = BeautifulSoup(r.text, "html.parser")

    seen, out = set(), []
    for tid in MEETING_TABLES:
        tb = s.find("table", id=tid)
        if not tb:
            continue
        for tr in tb.find_all("tr")[1:]:
            div = tr.find("div", id="hdnDateSeq")
            m = MEETING_ID.search(str(tr))
            if not (div and m):
                continue
            mid = m.group(1)
            if mid in seen:
                continue
            try:
                dt = datetime.strptime(div.get_text(strip=True), "%Y%m%d%H%M%S")
            except ValueError:
                continue
            # Reject implausible years rather than trust the source; Cranston
            # Planning Commission has a meeting stamped 2118.
            if not (2000 <= dt.year <= 2030):
                log.warning("  EntityID %d: rejecting implausible date %s", eid, dt.date())
                continue
            seen.add(mid)
            out.append({"meeting_id": mid, "date": dt.strftime("%Y-%m-%d")})

    out.sort(key=lambda x: x["date"], reverse=True)
    return out[:limit]


def cached_text(path: Path, meeting_id: str, idx: int) -> str:
    TEXT_DIR.mkdir(parents=True, exist_ok=True)
    tp = TEXT_DIR / f"{meeting_id}_{idx}.txt"
    if tp.exists():
        return tp.read_text(encoding="utf-8", errors="replace")
    try:
        txt = pdf_text(path, max_pages=40)
    except Exception as exc:
        log.warning("  pdf parse failed (%s): %s", path.name, exc)
        txt = ""
    tp.write_text(txt, encoding="utf-8", errors="replace")
    return txt


def harvest(tier: int, per_board: int) -> dict:
    boards = [b for b in BOARDS if b["tier"] == tier]
    corpus = json.loads(CORPUS.read_text(encoding="utf-8")) if CORPUS.exists() else {}

    with httpx.Client(headers=HEADERS, follow_redirects=True) as client:
        for b in boards:
            eid = b["entity_id"]
            log.info("=== %s (%d) ===", b["name"], eid)
            try:
                meetings = board_meetings(client, eid, per_board)
            except Exception as exc:
                log.warning("  dashboard failed: %s", exc)
                continue
            log.info("  %d meetings", len(meetings))

            for mtg in meetings:
                mid = mtg["meeting_id"]
                key = f"{eid}:{mid}"
                if key in corpus:
                    continue
                try:
                    info = meeting_documents(client, int(mid))
                except Exception as exc:
                    log.warning("  meeting %s failed: %s", mid, exc)
                    continue

                docs = []
                for i, d in enumerate(info["documents"]):
                    p = fetch_pdf(client, d["url"], int(mid), i)
                    if not p:
                        continue
                    txt = cached_text(p, mid, i)
                    docs.append({
                        "kind": d["kind"], "label": d["label"], "url": d["url"],
                        "path": d["path"], "chars": len(txt), "text_file": f"{mid}_{i}.txt",
                    })
                    time.sleep(1.2)

                corpus[key] = {
                    "entity_id": eid, "board": b["name"], "municipality": b["municipality"],
                    "tier": b["tier"], "meeting_id": mid, "date": mtg["date"],
                    "documents": docs,
                }
                CORPUS.write_text(json.dumps(corpus, indent=1), encoding="utf-8")
                time.sleep(1.5)

    n_docs = sum(len(v["documents"]) for v in corpus.values())
    log.info("\nCorpus: %d meetings, %d documents", len(corpus), n_docs)
    return corpus


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--tier", type=int, default=1)
    parser.add_argument("--meetings", type=int, default=15)
    args = parser.parse_args()
    harvest(args.tier, args.meetings)
