"""Record the team facts read out of the cached Rhode Island minutes.

The project is already known -- it is encoded in the request id, because the
passages were selected by an address match in code. The model's only job was
to confirm the passage really concerns that property and to quote the role.
Anything it flags as not-about-this-property is dropped.

Citations resolve back to the real meeting document: the text filename maps
through the agenda corpus to the board, the meeting date and the URL the
document was downloaded from.
"""
import json
import re
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from provenance import connect, record

FIELDS = ("architect", "civil_engineer", "general_contractor")


def parse(t):
    if not t:
        return None
    t = t.strip()
    if t.startswith("```"):
        t = re.sub(r"^```(json)?\s*", "", t)
        t = re.sub(r"\s*```$", "", t)
    try:
        return json.loads(t)
    except Exception:
        m = re.search(r"\{.*\}", t, re.S)
        if m:
            try:
                return json.loads(m.group(0))
            except Exception:
                return None
    return None


def build_citation_index():
    """text_file -> (board, date, url, label)"""
    corpus = json.loads(Path("data/ri_agenda_corpus.json").read_text(encoding="utf-8"))
    idx = {}
    for mtg in corpus.values():
        for d in mtg.get("documents", []):
            tf = d.get("text_file")
            if tf:
                idx[tf] = (mtg.get("board"), mtg.get("date"), d.get("url"),
                           d.get("kind"))
    return idx


def main():
    results = json.loads(Path("data/ri_minutes_results.json").read_text())
    hits = json.loads(Path("data/ri_minutes_hits.json").read_text())
    cite = build_citation_index()
    c = connect()

    stats = Counter()
    for r in results:
        pid = int(r["custom_id"][2:])
        d = parse(r.get("text"))
        if not d:
            stats["unparseable"] += 1
            continue
        stats["parsed"] += 1
        if not d.get("about_this_property"):
            stats["rejected_wrong_property"] += 1
            continue
        stats["accepted"] += 1

        h = hits.get(str(pid), {})
        files = [p["file"] for p in h.get("passages", [])]
        board = date = url = None
        if files:
            board, date, url, kind = cite.get(files[0], (None, None, None, None))
        src = (f"{board} minutes/staff report, {date}" if board
               else f"RI board document {files[0] if files else ''}")

        for f in FIELDS:
            got = d.get(f) or {}
            firm = (got.get("firm") or "").strip() or None
            person = (got.get("person") or "").strip() or None
            label = (got.get("role_label") or "").strip() or None
            quote = got.get("quote")
            if not label or not (firm or person):
                continue
            value = firm or person
            reason = None
            if not firm:
                reason = ("minutes name an individual and no firm; stored as "
                          "stated and not expanded to an employer")
            record(c, pid, f, value=value, outcome="resolved",
                   tier="document_confirmed", source_type="minutes",
                   source_url=url, source_name=src, source_date=date,
                   page_ref=", ".join(files[:2]),
                   firm_sentence=quote,
                   address_sentence=f"matched to {h.get('address')} , "
                                    f"{h.get('city')} by street number and name; "
                                    f"model confirmed the passage concerns it",
                   resolution_step=2, reason=reason)
            stats[f"resolved_{f}"] += 1
            if f == "architect" and person and not firm:
                c.execute("update projects set architect_person=? where id=?",
                          (person, pid))
    c.commit()
    for k, v in sorted(stats.items()):
        print(f"  {k:28} {v}")


if __name__ == "__main__":
    main()
