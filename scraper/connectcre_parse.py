r"""Parse the downloaded Connect CRE Metro Boston archive pages.

"Metro Boston" is the MSA. Quincy, Malden, Medford, Needham, Reading and
Worcester all appear in it, so the section name admits nothing on its own --
city has to be read off the item.
"""
import html, json, re, sys
from pathlib import Path

ITEM = re.compile(
    r'href="(?P<url>https://www\.connectcre\.com/stories/[^"]+)"[^>]*>\s*'
    r'<h3 class="insight-main-heading">(?P<title>.*?)</h3>.*?'
    r'\|\s*(?P<date>[A-Z][a-z]+ \d{1,2}, \d{4})\s*</span>'
    r'<span class="insight-content">(?P<excerpt>.*?)</span>', re.S)
MONTHS = {m: i+1 for i, m in enumerate(
    ["January","February","March","April","May","June","July","August",
     "September","October","November","December"])}
SALE = re.compile(r"\b(sell|sells|sold|sale|trade[sd]?|acquir\w*|buys?|bought|"
                  r"purchase\w*|recapitaliz\w*|stake|joint venture)\b", re.I)
CITY = re.compile(r"\b(Boston|Cambridge|Seaport|Back Bay|Fenway|Kendall|"
                  r"Downtown Crossing|Financial District|South Boston|East Boston|"
                  r"Dorchester|Charlestown|Allston|Brighton|Roxbury|Jamaica Plain|"
                  r"South End|Beacon Hill|Fort Point|Theater District|Newbury)\b", re.I)
# Suburbs that often co-occur with "Boston" in an excerpt ("...outside Boston").
SUBURB = re.compile(r"\b(Quincy|Malden|Medford|Needham|Reading|Woburn|Waltham|"
                    r"Watertown|Somerville|Brookline|Newton|Danvers|Andover|Lynn|"
                    r"Revere|Everett|Chelsea|Braintree|Burlington|Marlborough|"
                    r"Worcester|Framingham|Peabody|Wakefield|Taunton|Lowell|"
                    r"Lawrence|Stoughton|Norwood|Canton|Hingham|Weymouth|Salem|"
                    r"Beverly|Bedford|Lexington|Concord|Westford|Shrewsbury)\b", re.I)

rows, seen = [], set()
for p in sorted(Path("data/connectcre").glob("page*.html"),
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
Path("data/connectcre_boston_index.json").write_text(
    json.dumps(y26, indent=1), encoding="utf-8")
print(f"{len(rows)} articles parsed, oldest {rows[-1]['date']}, {len(y26)} in 2026")

hits = []
for r in y26:
    blob = r["title"] + " " + r["excerpt"]
    if SALE.search(blob) and CITY.search(blob) and not SUBURB.search(blob):
        hits.append(r)
print(f"\n{len(hits)} candidate Boston/Cambridge sales:\n")
for h in hits:
    print(f"{h['date']}  {h['title'][:92]}")
    print(f"            {h['excerpt'][:150]}")
