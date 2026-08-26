r"""Parse The Real Reporter's article archive for Boston/Cambridge sales.

TRR is the most Boston-specific of the trade publications and the one most
likely to carry a mid-market deal the nationals skip, so it is the real test of
where press coverage stops. Its listing carries headline, date and a lead
paragraph, which is enough to triage before fetching bodies.
"""
import html, json, re
from pathlib import Path

ITEM = re.compile(
    r'<a href="(?P<url>https://therealreporter\.com/(?:article|briefs)/[^"]+)">\s*'
    r'<h3[^>]*>(?P<title>.*?)</h3>\s*</a>\s*'
    r'<h5>(?P<date>[A-Z][a-z]+ \d{1,2}, \d{4})[^<]*</h5>\s*'
    r'<p>(?P<excerpt>.*?)</p>', re.S)
MONTHS = {m: i+1 for i, m in enumerate(
    ["January","February","March","April","May","June","July","August",
     "September","October","November","December"])}
SALE = re.compile(r"\b(sell\w*|sold|sale|trade[sd]?|trading|acquir\w*|buys?|"
                  r"bought|purchase\w*|recapitaliz\w*|stake|joint venture|"
                  r"pays?|paid|fetch\w*|nabs?)\b", re.I)
CITY = re.compile(r"\b(BOSTON|CAMBRIDGE|EAST CAMBRIDGE|SEAPORT|BACK BAY|FENWAY|"
                  r"KENDALL|SOUTH BOSTON|EAST BOSTON|DORCHESTER|CHARLESTOWN|"
                  r"ALLSTON|BRIGHTON|ROXBURY|JAMAICA PLAIN|SOUTH END|BEACON HILL|"
                  r"FORT POINT|DOWNTOWN CROSSING|FINANCIAL DISTRICT)\b")

rows, seen = [], set()
for p in sorted(Path("data/trr").glob("p*.html"),
                key=lambda x: int(re.search(r"\d+", x.name).group())):
    t = p.read_text(encoding="utf-8", errors="ignore")
    for m in ITEM.finditer(t):
        if m.group("url") in seen:
            continue
        seen.add(m.group("url"))
        mon, day, yr = re.match(r"([A-Z][a-z]+) (\d{1,2}), (\d{4})", m.group("date")).groups()
        rows.append({
            "date": f"{yr}-{MONTHS[mon]:02d}-{int(day):02d}",
            "title": html.unescape(re.sub(r"<[^>]+>", "", m.group("title"))).strip(),
            "excerpt": html.unescape(re.sub(r"<[^>]+>", "", m.group("excerpt"))).strip(),
            "url": m.group("url"),
        })

rows.sort(key=lambda r: r["date"], reverse=True)
y26 = [r for r in rows if r["date"] >= "2026-01-01"]
Path("data/trr_index.json").write_text(json.dumps(y26, indent=1), encoding="utf-8")
print(f"{len(rows)} articles parsed, {len(y26)} dated 2026\n")

# TRR datelines the CITY in caps at the head of the lead paragraph, which is a
# far more reliable city signal than a name appearing anywhere in the prose.
hits = [r for r in y26
        if CITY.match(r["excerpt"].upper().replace("—", " ").strip())
        and SALE.search(r["title"] + " " + r["excerpt"])]
print(f"{len(hits)} Boston/Cambridge-datelined sale stories:\n")
for h in hits:
    print(f"{h['date']}  {h['title'][:88]}")
