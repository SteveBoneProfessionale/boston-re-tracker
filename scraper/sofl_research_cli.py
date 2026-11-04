"""Command-line research helper for South Florida projects.

Fetches the named trade press for one project and prints the sentences that
could establish a field, each with its article, date and URL. It decides
nothing: the reader decides, which is the point -- an article about one tower
routinely names the one next door, and a list of firms on a "development team"
does not say which is the architect.

    python -m scraper.sofl_research_cli --name "Kelly Tractor" --city Doral
    python -m scraper.sofl_research_cli --id 1208
    python -m scraper.sofl_research_cli --city Doral --missing developer --limit 20

Output is grouped by article and marked with how the article was matched:

    TITLE   the article's headline names the project or its address  (strongest)
    BODY    the address pattern appears in the article text
    NAME    the search matched on the project name only              (weakest)

A BODY or NAME match is a candidate, not an answer. Check that the sentence is
about THIS project before believing it.
"""

import argparse
import os
import re
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scraper import sofl_address_match as A   # noqa: E402
from scraper import sofl_common as sc         # noqa: E402
from scraper import sofl_press as P           # noqa: E402

DB = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                  "data", "boston_re.db")

FIELDS = ["developer", "architect", "contractor", "civil_engineer",
          "lender", "units", "height", "status"]


def projects(city=None, missing=None, limit=None, pid=None, name=None):
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    sql = ("select id, name, address, city, owner_or_agency, total_gsf, status, "
           "developer, architect, general_contractor, lender, residential_units, "
           "building_height_ft, num_stories from projects where 1=1")
    args = []
    if pid:
        sql += " and id = ?"
        args.append(pid)
    if name:
        sql += " and name like ?"
        args.append("%" + name + "%")
    if city:
        sql += " and city = ?"
        args.append(city)
    if missing:
        col = {"developer": "developer", "architect": "architect",
               "contractor": "general_contractor", "lender": "lender",
               "units": "residential_units", "height": "building_height_ft"}[missing]
        sql += f" and ({col} is null or {col} = '')"
    sql += " order by coalesce(total_gsf, 0) desc"
    if limit:
        sql += f" limit {int(limit)}"
    rows = con.execute(sql, args).fetchall()
    con.close()
    return rows


def queries_for(row):
    """Search strings, most likely to least. Name first: a project the press
    has named is found by its name, and the City's address of record is often
    not the address the press prints."""
    out = []
    nm = (row["name"] or "").strip()
    addr = (row["address"] or "").strip()
    city = row["city"]
    if nm and nm.upper() != addr.upper():
        out.append(f"{nm} {city}")
        out.append(nm)
    out += [f"{v} {city}" for v in A.variants(addr)[:2]]
    for term in A.entity_terms(row["owner_or_agency"])[:2]:
        out.append(f"{term} {city}")
    seen, uniq = set(), []
    for q in out:
        if q and q.lower() not in seen:
            seen.add(q.lower())
            uniq.append(q)
    return uniq[:5]


def research(row, per_page=6, max_articles=4, sess=None):
    s = sess or sc.session()
    pat = A.pattern(row["address"] or "")
    nm = (row["name"] or "").strip()
    name_pat = None
    if nm and nm.upper() != (row["address"] or "").upper():
        core = re.split(r"[—(]", nm)[0].strip()
        if len(core) > 3:
            name_pat = re.compile(re.escape(core), re.I)

    found = {}
    for q in queries_for(row):
        for post in P.fy_search(q, per_page=per_page, sess=s):
            tier = None
            if (pat and pat.search(post["title"])) or (name_pat and name_pat.search(post["title"])):
                tier = "TITLE"
            elif pat and pat.search(post["text"]):
                tier = "BODY"
            elif name_pat and name_pat.search(post["text"]):
                tier = "NAME"
            if tier:
                prior = found.get(post["url"])
                order = {"TITLE": 0, "BODY": 1, "NAME": 2}
                if prior is None or order[tier] < order[prior[0]]:
                    found[post["url"]] = (tier, post)
    ranked = sorted(found.values(),
                    key=lambda tp: ({"TITLE": 0, "BODY": 1, "NAME": 2}[tp[0]],
                                    -int(tp[1]["date"].replace("-", ""))))
    return ranked[:max_articles]


def show(row, sess=None):
    have = [f for f in ("developer", "architect", "general_contractor", "lender")
            if row[f]]
    print("=" * 78)
    print(f"[{row['id']}] {row['name']}   ({row['city']})")
    print(f"    address: {row['address']}    owner of record: {row['owner_or_agency'] or '-'}")
    print(f"    sq ft: {row['total_gsf'] or '-'}   status: {row['status']}")
    print(f"    already filled: {', '.join(have) or 'nothing'}")
    results = research(row, sess=sess)
    if not results:
        print("    NO COVERAGE FOUND in Florida YIMBY for any query tried:")
        for q in queries_for(row):
            print(f"      - {q}")
        return
    for tier, post in results:
        print(f"\n    [{tier}] {post['date']}  {post['title'][:100]}")
        print(f"    {post['url']}")
        cues = P.cue_sentences(post["text"], fields=FIELDS)
        for f in FIELDS:
            for x in (cues.get(f) or [])[:2]:
                print(f"      ({f}) {x['sentence'][:250]}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--city")
    ap.add_argument("--name")
    ap.add_argument("--id", type=int)
    ap.add_argument("--missing", choices=["developer", "architect", "contractor",
                                          "lender", "units", "height"])
    ap.add_argument("--limit", type=int)
    args = ap.parse_args()
    rows = projects(city=args.city, missing=args.missing, limit=args.limit,
                    pid=args.id, name=args.name)
    if not rows:
        print("no projects matched")
        return
    sess = sc.session()
    for row in rows:
        show(row, sess=sess)
        print()


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    main()
