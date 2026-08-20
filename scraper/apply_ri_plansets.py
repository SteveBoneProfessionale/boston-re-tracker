"""Match plan-set extractions back to RI projects and record them.

The model read the site address off the drawing; the match to a project is
made here, in code, against the normalised address. A drawing that does not
carry an address it can be tied to is dropped rather than guessed at.
"""
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from addr_norm import norm_address, street_name, street_numbers
from provenance import connect, looks_like_person, record

RI = {"Providence", "Cranston", "Warwick", "Pawtucket", "Newport"}
BASE = "https://www.providenceri.gov/planning/"


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


def main():
    results = json.loads(Path("data/ri_planset_results.json").read_text())
    idmap = json.loads(Path("data/ri_planset_idmap.json").read_text())
    c = connect()
    projects = c.execute(
        "select id,name,address,city,alt_addresses from projects "
        "where coalesce(excluded,0)=0 and city in "
        "('Providence','Cranston','Warwick','Pawtucket','Newport')").fetchall()

    by_norm, by_street = defaultdict(list), defaultdict(list)
    for p in projects:
        addrs = [p["address"] or ""]
        if p["alt_addresses"]:
            addrs += [a.strip() for a in str(p["alt_addresses"]).split("|") if a.strip()]
        for a in addrs:
            if norm_address(a):
                by_norm[norm_address(a)].append(p)
            sn = street_name(a)
            for num in street_numbers(a):
                if sn:
                    by_street[(num, sn)].append(p)

    stats = Counter()
    for r in results:
        d = parse(r.get("text"))
        if not d:
            stats["unparseable"] += 1
            continue
        stats["parsed"] += 1
        addr = (d.get("site_address") or "").strip()
        if not addr:
            stats["no_address"] += 1
            continue
        cands = by_norm.get(norm_address(addr)) or []
        how = "normalised_address"
        if not cands:
            sn = street_name(addr)
            for num in street_numbers(addr):
                if (num, sn) in by_street:
                    cands = by_street[(num, sn)]
                    how = "street_number_and_name"
                    break
        if not cands:
            stats["no_project_match"] += 1
            continue
        if len({p["id"] for p in cands}) > 1:
            stats["ambiguous_match"] += 1
            continue
        p = cands[0]
        stats["matched"] += 1
        fname = idmap.get(r["custom_id"], r["custom_id"])
        for f in ("architect", "civil_engineer", "general_contractor"):
            got = d.get(f) or {}
            firm = (got.get("firm") or "").strip() or None
            label = (got.get("role_label") or "").strip() or None
            quote = got.get("quote")
            if not (firm and label):
                continue
            record(c, p["id"], f, value=firm, outcome="resolved",
                   tier="document_confirmed", source_type="planset",
                   source_url=None, source_name=f"RI plan set: {fname}",
                   page_ref="title block",
                   firm_sentence=quote,
                   address_sentence=d.get("address_quote"),
                   resolution_step=2,
                   reason=("value is an individual, stored as stated"
                           if looks_like_person(firm) else None))
            stats[f"resolved_{f}"] += 1
    c.commit()
    for k, v in sorted(stats.items()):
        print(f"  {k:26} {v}")


if __name__ == "__main__":
    main()
