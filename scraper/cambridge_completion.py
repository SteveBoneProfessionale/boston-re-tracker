r"""Find Cambridge completion dates in the city's own registries.

Cambridge is the opposite of Rhode Island. There, completion dates existed only
in press coverage and had to be searched for one project at a time. Here the
city publishes three datasets that answer the question directly, and all three
join to what the tracker already stores:

  qwvv-deed   Certificates of Occupancy -- 1,535 records with an issue_date and
              the building permit number the CO was issued against. The tracker
              already holds Cambridge building permit numbers in
              cambridge_building_permits, so this is a key join rather than a
              fuzzy one, and a CO issue date is a DELIVERED date at day
              precision from the authority that issued it.

  a5ud-8kjv   Development Log Historical Projects 1997-2025 -- carries
              yrcomplete. A project that finishes moves out of the current
              edition into this one, so a current-edition row that also appears
              here has completed since the tracker last ingested.

  5432-hmix   Historical Additional Details -- year_complete for a further 176.

Nothing is written to the database here. This harvests and proposes; the join
is exact on permit numbers and fuzzy on address, and a fuzzy match still has to
be read before it is believed.

    python scraper/cambridge_completion.py
"""

import json
import re
import sqlite3
from pathlib import Path

import httpx

OUT = Path("data/cambridge_completion.json")
UA = {"User-Agent": "Mozilla/5.0 (compatible; boston-re-tracker/1.0)"}
BASE = "https://data.cambridgema.gov/resource/{}.json"


def fetch_all(dataset: str, limit: int = 50000) -> list:
    rows, offset = [], 0
    with httpx.Client(headers=UA, timeout=60) as client:
        while True:
            r = client.get(BASE.format(dataset),
                           params={"$limit": 5000, "$offset": offset})
            r.raise_for_status()
            page = r.json()
            rows.extend(page)
            if len(page) < 5000 or len(rows) >= limit:
                break
            offset += 5000
    return rows


def _norm_permit(p) -> str:
    """Permit numbers appear as '196010', 'BLD-196010-2019', '707078-030893'."""
    return re.sub(r"\D", "", str(p or ""))


def _addr_key(a) -> str:
    a = re.sub(r"\s+", " ", str(a or "").lower())
    a = re.sub(r",.*$", "", a)
    a = re.sub(r"\b(street|st|avenue|ave|road|rd|drive|dr|place|pl|boulevard|blvd|square|sq)\b\.?", "", a)
    return re.sub(r"[^a-z0-9 ]", "", a).strip()


def main():
    print("fetching Cambridge certificates of occupancy...")
    cos = fetch_all("qwvv-deed")
    print(f"  {len(cos)} CO records")
    print("fetching historical development log...")
    hist = fetch_all("a5ud-8kjv")
    print(f"  {len(hist)} historical projects")
    detail = fetch_all("5432-hmix")
    print(f"  {len(detail)} historical additional-detail rows")

    # index the registries
    co_by_permit, co_by_addr = {}, {}
    for c in cos:
        k = _norm_permit(c.get("bldg_permit_number"))
        if k:
            co_by_permit.setdefault(k, []).append(c)
        a = _addr_key(c.get("full_address"))
        if a:
            co_by_addr.setdefault(a, []).append(c)

    hist_by_sp, hist_by_permit = {}, {}
    for h in hist + detail:
        sp = (h.get("pb_special_permit") or "").strip().upper()
        if sp:
            hist_by_sp.setdefault(sp, []).append(h)
        for part in re.split(r"[;,/]", str(h.get("building_permit") or "")):
            k = _norm_permit(part)
            if k:
                hist_by_permit.setdefault(k, []).append(h)

    conn = sqlite3.connect("data/boston_re.db")
    conn.row_factory = sqlite3.Row
    projects = conn.execute("""
        select p.id, p.name, p.address, p.status, p.total_gsf, p.residential_units,
               p.developer_canonical, p.special_permit_raw, p.building_permit_raw,
               p.delivered_date, p.target_date
          from projects p
         where p.city = 'Cambridge' and coalesce(p.excluded,0) = 0""").fetchall()

    out = []
    for p in projects:
        permits = [r["permit_number"] for r in conn.execute(
            "select permit_number from cambridge_building_permits where project_id = ?",
            (p["id"],))]
        permits += re.split(r"[;,/]", str(p["building_permit_raw"] or ""))
        keys = {_norm_permit(x) for x in permits if _norm_permit(x)}

        rec = {"id": p["id"], "name": p["name"], "address": p["address"],
               "status": p["status"], "gfa": p["total_gsf"],
               "units": p["residential_units"], "permits": sorted(keys),
               "co_matches": [], "hist_matches": []}

        for k in keys:
            for c in co_by_permit.get(k, []):
                rec["co_matches"].append({
                    "how": f"building permit {k}",
                    "issue_date": (c.get("issue_date") or "")[:10],
                    "address": c.get("full_address"),
                    "type": c.get("type_cert_occ"),
                    "units": c.get("num_residential_units"),
                    "permit": c.get("bldg_permit_number"),
                })
        if not rec["co_matches"]:
            for c in co_by_addr.get(_addr_key(p["address"]), []):
                rec["co_matches"].append({
                    "how": "address",
                    "issue_date": (c.get("issue_date") or "")[:10],
                    "address": c.get("full_address"),
                    "type": c.get("type_cert_occ"),
                    "units": c.get("num_residential_units"),
                    "permit": c.get("bldg_permit_number"),
                })

        sp = (p["special_permit_raw"] or "").strip().upper()
        for h in hist_by_sp.get(sp, []) if sp else []:
            rec["hist_matches"].append({
                "how": f"special permit {sp}",
                "year": h.get("yrcomplete") or h.get("year_complete"),
                "name": h.get("project_name"), "gfa": h.get("total_gfa"),
                "status": h.get("status"),
            })
        for k in keys:
            for h in hist_by_permit.get(k, []):
                rec["hist_matches"].append({
                    "how": f"building permit {k}",
                    "year": h.get("yrcomplete") or h.get("year_complete"),
                    "name": h.get("project_name"), "gfa": h.get("total_gfa"),
                    "status": h.get("status"),
                })
        out.append(rec)

    OUT.write_text(json.dumps(out, indent=1, ensure_ascii=False), encoding="utf-8")
    withco = [r for r in out if r["co_matches"]]
    withhist = [r for r in out if r["hist_matches"]]
    print(f"\n{len(projects)} Cambridge projects")
    print(f"  {len(withco)} match a certificate of occupancy")
    print(f"  {len(withhist)} appear in the historical log with a completion year")
    print(f"wrote {OUT}")

    for r in withco:
        best = sorted(r["co_matches"], key=lambda m: m["issue_date"])[-1]
        print(f"\n  [{r['id']}] {r['name'][:52]}")
        print(f"        {r['status']}  |  {len(r['co_matches'])} CO(s), latest "
              f"{best['issue_date']} via {best['how']}  ({best['type']})")
    for r in withhist:
        print(f"\n  [{r['id']}] {r['name'][:52]} -> historical: "
              f"{[m['year'] for m in r['hist_matches']]} via "
              f"{r['hist_matches'][0]['how']}")


if __name__ == "__main__":
    main()
