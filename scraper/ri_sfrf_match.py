r"""Propose matches between RIHousing SFRF developments and tracker rows.

Proposes. It does not decide. RIHousing names a development ("The Avenue",
"Potters Tigrai") where the tracker holds a street address, so the join is
fuzzy by nature and a confident-looking wrong match is the expensive failure
here -- attaching another building's completion quarter to a row is worse than
leaving the row blank.

So this scores candidates on the three things that can be checked against each
other -- municipality, developer, unit count -- and prints them for a human to
read, with the numbers that would make a match wrong sitting right next to the
ones that would make it right.

    python scraper/ri_sfrf_match.py
"""

import json
import re
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from scraper.normalize_developer import suffix_stripped

SFRF = Path("data/ri_rihousing_sfrf.json")
RI = ("Providence", "Cranston", "Warwick", "Pawtucket", "Newport")

_STOP = {"the", "at", "of", "and", "street", "st", "avenue", "ave", "road", "rd",
         "apartments", "development", "developments", "phase", "i", "ii", "iii",
         "place", "commons", "court", "lofts", "house", "homes", "providence",
         "pawtucket", "cranston", "warwick", "newport"}


def _tokens(s: str) -> set:
    return {w for w in re.findall(r"[a-z0-9]+", (s or "").lower())
            if w not in _STOP and len(w) > 2}


def _units(s) -> int | None:
    m = re.search(r"(\d[\d,]*)", str(s or ""))
    return int(m.group(1).replace(",", "")) if m else None


def _devkey(s: str) -> str:
    k = suffix_stripped(s or "")
    return re.sub(r"[^a-z0-9]", "", str(k).lower())


def main():
    devs = json.loads(SFRF.read_text(encoding="utf-8"))
    c = sqlite3.connect("data/boston_re.db")
    c.row_factory = sqlite3.Row
    rows = c.execute(
        f"select id, name, address, city, developer_canonical, developer, "
        f"       applicant_entity, residential_units, delivered_date, target_date "
        f"  from projects where coalesce(excluded,0)=0 and city in {RI}").fetchall()

    for d in devs:
        muni = (d["name"].split(",")[-1] or "").strip()
        if muni not in RI:
            continue                       # outside the tracked municipalities
        dname = d["name"].rsplit(",", 1)[0].strip()
        dtok = _tokens(dname)
        ddev = _devkey(d.get("developer") or "")
        dunits = _units(d.get("units_total"))

        scored = []
        for r in rows:
            if (r["city"] or "") != muni:
                continue
            score, why = 0, []
            rdev = _devkey(r["developer_canonical"] or r["developer"]
                           or r["applicant_entity"] or "")
            if ddev and rdev and (ddev in rdev or rdev in ddev):
                score += 3
                why.append("developer")
            overlap = dtok & (_tokens(r["name"]) | _tokens(r["address"]))
            if overlap:
                score += 2 * len(overlap)
                why.append("name:" + ",".join(sorted(overlap)))
            if dunits and r["residential_units"]:
                diff = abs(dunits - r["residential_units"])
                if diff == 0:
                    score += 3
                    why.append("units exact")
                elif diff <= 3:
                    score += 1
                    why.append(f"units +-{diff}")
                else:
                    score -= 1
                    why.append(f"units {r['residential_units']} vs {dunits}")
            if score > 0:
                scored.append((score, r, why))
        scored.sort(key=lambda x: -x[0])

        print(f"\n=== {d['name']}")
        print(f"    completion={d.get('completion')}  status={d.get('status')}  "
              f"units={d.get('units_total')}  dev={d.get('developer')}  "
              f"updated={d.get('page_updated')}")
        if not scored:
            print("    no candidate row")
        for score, r, why in scored[:3]:
            have = ("DELIVERED" if r["delivered_date"] else
                    ("TARGET" if r["target_date"] else "-"))
            print(f"    {score:>3}  [{r['id']}] {r['address'][:34]:<36} "
                  f"{str(r['residential_units'] or ''):>4}u  {have:<10} "
                  f"{(r['developer_canonical'] or r['developer'] or '')[:26]:<28} "
                  f"{'; '.join(why)[:70]}")


if __name__ == "__main__":
    main()
