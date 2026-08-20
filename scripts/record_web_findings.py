"""Record Step 4 web findings, applying the corroboration rule mechanically.

Two independent non-aggregator sources naming the same firm in the same role
earns web_corroborated; one earns web_low_confidence. Independence is judged
by registrable domain, so two pages on the same site count once. Aggregators
are rejected outright and never counted.

Input: a JSON list of evidence items, each
  {project_id, field, value, url, title, date, firm_sentence, address_sentence}
"""
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).parent.parent / "scraper"))
from provenance import add_evidence, connect, record

AGGREGATORS = {
    "bldup.com", "loopnet.com", "redfin.com", "zoominfo.com",
    "buildzoom.com", "citizenportal.ai", "livabl.com", "constructionjournal.com",
    "cobbl.es", "crunchbase.com", "dnb.com", "manta.com", "yelp.com",
}


def domain(url):
    try:
        h = (urlparse(url).hostname or "").lower()
    except Exception:
        return ""
    h = re.sub(r"^www\.", "", h)
    parts = h.split(".")
    if len(parts) > 2 and parts[-2] in ("co", "com", "org", "gov", "ac"):
        return ".".join(parts[-3:])
    return ".".join(parts[-2:]) if len(parts) >= 2 else h


def main(path):
    items = json.loads(Path(path).read_text(encoding="utf-8"))
    c = connect()
    claims = defaultdict(list)
    rejected = 0
    for it in items:
        d = domain(it["url"])
        agg = d in AGGREGATORS
        if agg:
            rejected += 1
        add_evidence(c, it["project_id"], it["field"], it["value"],
                     source_url=it["url"], source_domain=d,
                     source_title=it.get("title"), source_date=it.get("date"),
                     firm_sentence=it.get("firm_sentence"),
                     address_sentence=it.get("address_sentence"),
                     is_aggregator=1 if agg else 0)
        if not agg:
            claims[(it["project_id"], it["field"], it["value"].strip())].append((d, it))

    applied = 0
    for (pid, field, value), evs in claims.items():
        doms = {d for d, _ in evs}
        tier = "web_corroborated" if len(doms) >= 2 else "web_low_confidence"
        first = evs[0][1]
        srcs = "; ".join(sorted(doms))
        record(c, pid, field, value=value, outcome="resolved", tier=tier,
               source_type="web", source_url=first["url"],
               source_name=first.get("title"), source_date=first.get("date"),
               page_ref=None,
               firm_sentence=" || ".join(e[1].get("firm_sentence") or "" for e in evs),
               address_sentence=" || ".join(e[1].get("address_sentence") or "" for e in evs),
               resolution_step=4,
               reason=f"independent sources: {len(doms)} ({srcs})")
        applied += 1
    c.commit()
    print(f"evidence rows: {len(items)}  aggregator-rejected: {rejected}")
    print(f"claims applied: {applied}")
    for (pid, f, v), evs in sorted(claims.items()):
        doms = {d for d, _ in evs}
        print(f"  {pid:4} {f:20} {v[:34]:34} {len(doms)} src "
              f"{'CORROBORATED' if len(doms)>=2 else 'low'}")


if __name__ == "__main__":
    main(sys.argv[1])
