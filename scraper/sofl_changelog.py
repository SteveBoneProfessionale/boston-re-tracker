"""An append-only record of every field this review changed.

The review asked for a change log of project, field, old value, new value and
source URL. Keeping it as a side effect of the scripts that do the work -- each
one calling `record()` as it writes -- means the log cannot drift from the
database, which a hand-written table would within an hour.

Entries accumulate in data/sofl/change_log.json and `render()` turns them into
the markdown table appended to south_florida_log.md. The JSON is the record and
the markdown is a view of it, so the table can be regenerated and the log
rebuilt without re-running any research.

`record()` is idempotent on (project_id, field, new value): re-running a script
updates the existing entry instead of adding a second one, so a corrected run
does not leave two contradictory lines in the log.
"""

import datetime as dt
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

STORE = os.path.join(ROOT, "data", "sofl", "change_log.json")
LOG = os.path.join(ROOT, "south_florida_log.md")

HEADING = "## Change log — City of Miami review (5 November 2026)"


def load() -> list[dict]:
    if not os.path.exists(STORE):
        return []
    with open(STORE, encoding="utf-8") as fh:
        return json.load(fh)


def save(entries: list[dict]) -> None:
    os.makedirs(os.path.dirname(STORE), exist_ok=True)
    with open(STORE, "w", encoding="utf-8") as fh:
        json.dump(entries, fh, indent=1, ensure_ascii=False)
        fh.write("\n")


def record(entries, project_id, name, field, old, new, source_url="", why=""):
    """Add or replace one change. Mutates and returns `entries`."""
    if _norm(old) == _norm(new):
        return entries
    row = {"id": project_id, "project": name, "field": field,
           "old": _show(old), "new": _show(new), "source": source_url,
           "why": why, "at": dt.date.today().isoformat()}
    for i, e in enumerate(entries):
        if e["id"] == project_id and e["field"] == field:
            # A later pass supersedes an earlier one on the same field, but the
            # ORIGINAL old value is what the log should show -- otherwise a
            # two-step correction reads as if the first step never happened.
            row["old"] = e["old"]
            entries[i] = row
            return entries
    entries.append(row)
    return entries


def _norm(v):
    if v is None or v == "":
        return None
    if isinstance(v, float) and v.is_integer():
        return int(v)
    return v


def _show(v):
    if v is None or v == "":
        return "—"
    if isinstance(v, float) and v.is_integer():
        return f"{int(v):,}"
    if isinstance(v, (int, float)):
        return f"{v:,}"
    return " ".join(str(v).split())


def counts(entries=None) -> dict:
    entries = load() if entries is None else entries
    out = {}
    for e in entries:
        out[e["field"]] = out.get(e["field"], 0) + 1
    return dict(sorted(out.items(), key=lambda kv: -kv[1]))


def render(entries=None) -> str:
    """The markdown table, grouped by field so a reader can scan one at a time."""
    entries = load() if entries is None else entries
    if not entries:
        return ""
    by_field = {}
    for e in entries:
        by_field.setdefault(e["field"], []).append(e)

    out = [HEADING, "",
           f"{len(entries):,} field changes across "
           f"{len({e['id'] for e in entries}):,} projects. Grouped by field; within a "
           "field, ordered by project id. A source of “—” means the change was "
           "derived from a record already cited on the row (the master building "
           "permit, or the permit's own work items) rather than from a new document, "
           "and the row's existing FIELD SOURCES entry still applies.", ""]

    tally = counts(entries)
    out += ["| Field | Changes |", "| --- | --: |"]
    out += [f"| {f} | {n:,} |" for f, n in tally.items()]
    out += ["", ""]

    for field, rows in sorted(by_field.items(), key=lambda kv: -len(kv[1])):
        out += [f"### {field} — {len(rows):,} change(s)", "",
                "| id | Project | Old | New | Source |",
                "| --: | --- | --- | --- | --- |"]
        for e in sorted(rows, key=lambda r: r["id"] if isinstance(r["id"], int) else 0):
            src = e["source"]
            cell = f"[link]({src})" if src else "—"
            proj = e["project"].replace("|", "/")
            out.append(f"| {e['id']} | {proj} | {e['old']} | {e['new']} | {cell} |")
        out.append("")
        # The reasons, where they were given, kept out of the table so the table
        # stays readable and the reasoning stays available.
        reasons = [e for e in sorted(rows, key=lambda r: r["id"] if isinstance(r["id"], int) else 0)
                   if e.get("why")]
        if reasons:
            seen = set()
            out.append("Reasons:")
            out.append("")
            for e in reasons:
                key = e["why"]
                if key in seen:
                    continue
                seen.add(key)
                ids = [str(x["id"]) for x in rows if x.get("why") == key]
                head = (f"{len(ids)} rows" if len(ids) > 3
                        else ", ".join(f"id {i}" for i in ids))
                out.append(f"- **{head}** — {key}")
            out.append("")
    return "\n".join(out).rstrip() + "\n"


def write_log(entries=None, verbose=True):
    """Replace the change-log section of south_florida_log.md with the table."""
    table = render(entries)
    if not table:
        if verbose:
            print("no changes recorded; log not touched")
        return
    with open(LOG, encoding="utf-8") as fh:
        text = fh.read()
    if HEADING in text:
        # Replace in place so re-running does not stack copies. The section
        # runs to the next level-2 heading, or to the end.
        i = text.index(HEADING)
        rest = text[i + len(HEADING):]
        j = rest.find("\n## ")
        text = text[:i] + table + (rest[j + 1:] if j >= 0 else "")
    else:
        text = text.rstrip() + "\n\n" + table
    with open(LOG, "w", encoding="utf-8") as fh:
        fh.write(text)
    if verbose:
        n = len(load() if entries is None else entries)
        print(f"wrote {n:,} change(s) into {os.path.basename(LOG)}")


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    for field, n in counts().items():
        print(f"  {field:<24}{n:>5}")
    print(f"  {'TOTAL':<24}{len(load()):>5}")
