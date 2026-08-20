r"""Build and drive the Rhode Island completion-date research queue.

The minutes gave one date across 256 projects (see ri_apply_completion_docs),
so everything else has to come off the web. This holds the worklist and the
state, so the sweep survives being run in several passes instead of one.

Two queues, because they are different questions asked of different sources:

  DELIVERED  projects already established complete. Look for the certificate
             of occupancy or final permit record first, then the developer's
             or architect's project page, then coverage of the opening.
  TARGET     everything still in the pipeline. Look for the developer's
             project page, construction and groundbreaking announcements,
             preleasing announcements, then local press.

Ordered by consequence: the delivered set first, then the pipeline by size,
so that a pass that stops early has still answered the projects that matter
most. Everything is in the queue either way -- size orders the work, it does
not scope it.

    python scraper/ri_delivery_queue.py build     # (re)build data/ri_delivery_queue.json
    python scraper/ri_delivery_queue.py next 20   # print the next 20 to research
    python scraper/ri_delivery_queue.py status
"""

import json
import sqlite3
import sys
from pathlib import Path

QUEUE = Path("data/ri_delivery_queue.json")
STATE = Path("data/ri_delivery_findings.json")
RI = ("Providence", "Cranston", "Warwick", "Pawtucket", "Newport")

# Aggregators and listing sites. None of these corroborate anything: they
# republish, they carry no byline, and a date on one of them is a date from
# somewhere else with the somewhere-else removed. Same list the developer
# research used, plus the property-listing sites an address search always
# drags in.
BLOCKED = (
    "bldup", "loopnet", "redfin", "zillow", "realtor.com", "homes.com",
    "rentcafe", "cityfeet", "crexi", "trulia", "apartments.com", "neighborwho",
    "spokeo", "yelp", "coldwellbanker", "compass.com", "point2homes",
    "propertyshark", "rentals.com", "zumper", "hotpads", "movoto", "har.com",
    "commercialsearch", "42floors", "showcase.com", "buildout.com",
)


def build():
    c = sqlite3.connect("data/boston_re.db")
    c.row_factory = sqlite3.Row
    rows = c.execute(f"""
        select id, name, address, alt_addresses, city, status, case_number,
               developer_canonical, developer, applicant_entity, owner_or_agency,
               architect, asset_class, residential_units, total_gsf,
               completion_stage, completion_basis, delivered_date, target_date,
               description
          from projects
         where coalesce(excluded,0) = 0 and city in {RI}
    """).fetchall()

    delivered, pipeline = [], []
    for r in rows:
        item = {
            "id": r["id"],
            "name": r["name"] or "",
            "address": r["address"] or "",
            "alt_addresses": r["alt_addresses"] or "",
            "city": r["city"] or "",
            "status": r["status"] or "",
            "case_number": r["case_number"] or "",
            "developer": (r["developer_canonical"] or r["developer"]
                          or r["applicant_entity"] or r["owner_or_agency"] or ""),
            "architect": r["architect"] or "",
            "asset_class": r["asset_class"] or "",
            "units": r["residential_units"],
            "gsf": r["total_gsf"],
            "has_delivered": r["delivered_date"] is not None,
            "has_target": r["target_date"] is not None,
            "want": None,
            "description": (r["description"] or "")[:400],
        }
        if (r["completion_stage"] or "") == "Complete":
            item["want"] = "delivered"
            delivered.append(item)
        else:
            item["want"] = "target"
            pipeline.append(item)

    # Size orders the pipeline. A project with no stated size is not small,
    # it is unmeasured, so it sorts with the middle rather than the bottom.
    def size_key(i):
        u, g = i["units"] or 0, i["gsf"] or 0
        return -(max(u * 1000, g) if (u or g) else 20_000)

    pipeline.sort(key=size_key)
    delivered.sort(key=size_key)
    QUEUE.write_text(json.dumps(
        {"delivered": delivered, "pipeline": pipeline}, indent=1, ensure_ascii=False),
        encoding="utf-8")
    print(f"delivered queue: {len(delivered)}   pipeline queue: {len(pipeline)}")
    print(f"wrote {QUEUE}")


def _state():
    return json.loads(STATE.read_text(encoding="utf-8")) if STATE.exists() else {}


def nxt(n=20):
    q = json.loads(QUEUE.read_text(encoding="utf-8"))
    done = _state()
    out = []
    for bucket in ("delivered", "pipeline"):
        for item in q[bucket]:
            if str(item["id"]) in done:
                continue
            out.append(item)
            if len(out) >= n:
                break
        if len(out) >= n:
            break
    for i in out:
        size = (f'{i["units"]}u' if i["units"] else "") + \
               (f' {i["gsf"]:,}sf' if i["gsf"] else "")
        print(f'[{i["id"]}] want={i["want"]:<9} {i["address"]}, {i["city"]}'
              f' | {i["name"][:44]} | dev={i["developer"][:34]} | {size}'
              f' | {i["asset_class"]}')
    print(f"\n{len(out)} shown, {len(done)} already researched")


def status():
    q = json.loads(QUEUE.read_text(encoding="utf-8"))
    done = _state()
    tot = len(q["delivered"]) + len(q["pipeline"])
    from collections import Counter
    c = Counter(v.get("outcome", "?") for v in done.values())
    t = Counter(v.get("tier") or "-" for v in done.values() if v.get("outcome") == "resolved")
    print(f"researched {len(done)}/{tot}")
    print("  outcomes:", dict(c))
    print("  tiers   :", dict(t))
    print(f"  searches spent: {sum(v.get('searches', 0) for v in done.values())}")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"
    if cmd == "build":
        build()
    elif cmd == "next":
        nxt(int(sys.argv[2]) if len(sys.argv) > 2 else 20)
    else:
        status()
