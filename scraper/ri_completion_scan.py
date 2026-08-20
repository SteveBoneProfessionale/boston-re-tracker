r"""Do the Rhode Island minutes we already hold state a completion date?

Before spending a single web search, mine the 19 MB of board minutes and staff
reports already extracted to text. A date found here is document-confirmed,
carries a quotable passage, and carries a vintage for free -- every text file
is one meeting, and the agenda corpus knows that meeting's date and board.

The scan is address-first, the same way ri_minutes_index.py works: find the
project's street number and street name in a meeting, window around the hit,
and read only that window. A completion sentence four pages away from the only
mention of the address is not about this project.

What comes back is sorted into four kinds, because they are four different
claims and only two of them are dates this tracker can store:

  actual     "the building was completed in June 2023" -> DELIVERED
  target     "the applicant anticipates completion in spring 2025" -> TARGET
  condition  "shall be substantially completed within 12 months of the date of
             decision" -- a zoning condition, not a forecast. It is a DEADLINE
             imposed by a board, and turning it into a target would put the
             board's words in the developer's mouth. Counted, never stored.
  start      "construction to start in 2024" -- a start date is not a
             completion date. Counted, never stored.

    python scraper/ri_completion_scan.py            # writes data/ri_completion_scan.json
"""

import json
import re
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

from addr_norm import address_keys
from scraper.delivery_dates import parse_date_phrase

TEXT = Path("data/ri_pdfs/text")
CORPUS = Path("data/ri_agenda_corpus.json")
OUT = Path("data/ri_completion_scan.json")
RI_CITIES = ("Providence", "Cranston", "Warwick", "Pawtucket", "Newport")
WINDOW = 1500

# A completion CLAIM, in the four shapes the minutes actually use. Ordered:
# the first pattern that matches decides the kind, so the conditional deadline
# is tested before the bare "completed" that appears inside it.
KINDS = [
    ("condition", re.compile(
        r"(?:shall|must|is\s+to)\s+be\s+(?:started\s+and\s+)?(?:substantially\s+)?"
        r"complet\w+\s+within\s+[\w\-()]+\s+(?:months?|years?)"
        r"|within\s+[\w\-()]+\s+(?:months?|years?)\s+of\s+the\s+date\s+of\s+(?:the\s+)?decision",
        re.I)),
    ("actual", re.compile(
        r"(?:was|were|has\s+been|have\s+been)\s+(?:substantially\s+)?complet\w+"
        r"|construction\s+(?:was|is)\s+complete"
        r"|certificate\s+of\s+occupancy\s+(?:was\s+)?(?:issued|received|granted)"
        r"|(?:building|project|it)\s+opened"
        r"|received\s+(?:its\s+)?certificate\s+of\s+occupancy",
        re.I)),
    ("target", re.compile(
        r"(?:anticipat\w+|expect\w+|projected|estimated|scheduled|slated|targeted?)"
        r"[^.]{0,60}\b(?:completion|complete|deliver\w*|occupancy|open\w*)"
        r"|completion\s+(?:is\s+)?(?:anticipated|expected|projected|estimated|scheduled)"
        r"|(?:complet\w+|deliver\w+|open\w+)\s+(?:by|in)\s+(?:the\s+)?"
        r"(?:spring|summer|fall|autumn|winter|early|mid|late|Q[1-4]|\w+\s+of\s+)?\s*(?:19|20)\d{2}"
        r"|build[- ]?out\s+(?:is\s+)?(?:anticipated|expected|projected)",
        re.I)),
    ("start", re.compile(
        r"construction\s+(?:will|shall|is\s+expected\s+to|to)\s+(?:begin|commence|start)"
        r"|(?:break|breaking|broke)\s+ground|groundbreaking",
        re.I)),
]

_YEAR = re.compile(r"\b(?:19|20)\d{2}\b")


def _meeting_index() -> dict:
    """meeting_id -> {date, board, municipality}. Every text file is a meeting,
    so this is where a document-sourced date gets its vintage."""
    corpus = json.loads(CORPUS.read_text(encoding="utf-8"))
    out = {}
    for v in corpus.values():
        out[str(v.get("meeting_id"))] = {
            "date": v.get("date") or "",
            "board": v.get("board") or "",
            "municipality": v.get("municipality") or "",
        }
    return out


def _sentence_around(text: str, start: int, end: int) -> str:
    left = max(text.rfind(".", 0, start), text.rfind("\n", 0, start))
    right = text.find(".", end)
    left = 0 if left < 0 else left + 1
    right = len(text) if right < 0 else right + 1
    return re.sub(r"\s+", " ", text[left:right]).strip()


def main():
    conn = sqlite3.connect("data/boston_re.db")
    conn.row_factory = sqlite3.Row
    projects = conn.execute(
        f"select id, name, address, alt_addresses, city, status, stage_confirmed, "
        f"       completion_stage, delivered_date, target_date "
        f"  from projects "
        f" where coalesce(excluded,0)=0 and city in {RI_CITIES}").fetchall()

    pats, meta = {}, {}
    for p in projects:
        addrs = [p["address"] or ""]
        if p["alt_addresses"]:
            addrs += [a.strip() for a in str(p["alt_addresses"]).split("|") if a.strip()]
        keys = set()
        for a in addrs:
            keys |= address_keys(a)
        alts = []
        for (n, sn) in keys:
            first = sn.split()[0] if sn else ""
            if not first or len(first) < 3:
                continue
            alts.append(rf"\b{n}\b[^\n]{{0,40}}?\b{re.escape(first)}")
        if alts:
            pats[p["id"]] = re.compile("|".join(alts), re.I)
        meta[p["id"]] = dict(p)

    print(f"{len(projects)} live Rhode Island projects, "
          f"{len(pats)} with an address usable for matching")

    meetings = _meeting_index()
    findings = defaultdict(list)
    kinds = Counter()
    files = sorted(TEXT.glob("*.txt"))
    any_claim = re.compile("|".join(k[1].pattern for k in KINDS), re.I)

    for i, f in enumerate(files, 1):
        if i % 400 == 0:
            print(f"  {i}/{len(files)} files")
        try:
            t = f.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        if not any_claim.search(t):
            continue                      # no completion language anywhere in it
        mid = f.stem.split("_")[0]
        mm = meetings.get(mid, {})
        for pid, pat in pats.items():
            for am in pat.finditer(t):
                lo, hi = max(0, am.start() - WINDOW), am.end() + WINDOW
                win = t[lo:hi]
                for kind, kp in KINDS:
                    for cm in kp.finditer(win):
                        sent = _sentence_around(win, cm.start(), cm.end())
                        parsed = parse_date_phrase(sent) if _YEAR.search(sent) else None
                        kinds[kind] += 1
                        findings[pid].append({
                            "kind": kind,
                            "file": f.name,
                            "meeting_id": mid,
                            "meeting_date": mm.get("date", ""),
                            "board": mm.get("board", ""),
                            "municipality": mm.get("municipality", ""),
                            "matched": re.sub(r"\s+", " ", cm.group(0))[:160],
                            "sentence": sent[:700],
                            "address_hit": re.sub(r"\s+", " ", am.group(0))[:80],
                            "date": parsed[0].isoformat() if parsed else None,
                            "precision": parsed[1] if parsed else None,
                            "date_note": parsed[2] if parsed else "",
                        })
                        break             # one claim per pattern per window
                break                     # one window per address hit is enough

    # Dedupe: the same sentence often appears in both an agenda and its minutes.
    for pid, rows in findings.items():
        seen, keep = set(), []
        for r in rows:
            k = (r["kind"], r["sentence"][:200])
            if k in seen:
                continue
            seen.add(k)
            keep.append(r)
        findings[pid] = keep

    with_dates = {k: Counter() for k in ("actual", "target", "condition", "start")}
    projects_by_kind = defaultdict(set)
    for pid, rows in findings.items():
        for r in rows:
            projects_by_kind[r["kind"]].add(pid)
            with_dates[r["kind"]]["total"] += 1
            if r["date"]:
                with_dates[r["kind"]]["dated"] += 1

    print("\n== what the minutes say, by kind ==")
    for k in ("actual", "target", "condition", "start"):
        print(f"  {k:<10} {with_dates[k]['total']:>5} passages "
              f"({with_dates[k]['dated']:>4} carry a parseable date) "
              f"across {len(projects_by_kind[k]):>4} projects")
    storable = projects_by_kind["actual"] | projects_by_kind["target"]
    dated = {pid for pid, rows in findings.items()
             if any(r["date"] and r["kind"] in ("actual", "target") for r in rows)}
    print(f"\n  projects with a storable claim of any kind: {len(storable)}")
    print(f"  projects where that claim carries a date:    {len(dated)}")

    OUT.write_text(json.dumps(
        {"findings": {str(k): v for k, v in findings.items()},
         "meta": {str(k): {kk: (str(vv) if vv is not None else None)
                           for kk, vv in v.items()} for k, v in meta.items()}},
        indent=1, ensure_ascii=False), encoding="utf-8")
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
