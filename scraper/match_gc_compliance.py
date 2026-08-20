"""Match Boston projects to Jobs Policy compliance records on name + address.

Reports the match rate before and after normalisation so the lift from the
normaliser is visible rather than asserted.
"""
import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from addr_norm import norm_address, norm_firm, street_name, street_numbers

NO_GC = "no general contractor"


def load_compliance():
    recs = json.loads(Path("data/boston_gc_compliance.json").read_text())
    out = []
    for r in recs:
        gc = (r.get("general_contractor_name") or "").strip()
        out.append({
            "project": (r.get("compliance_project_name") or "").strip(),
            "address": (r.get("project_address") or "").strip(),
            "developer": (r.get("developer") or "").strip(),
            "gc": "" if gc.lower() == NO_GC else gc,
            "explicit_no_gc": gc.lower() == NO_GC,
            "first": r.get("first_period"),
            "last": r.get("last_period"),
            "rows": int(r.get("rows") or 0),
        })
    return out


def main():
    c = sqlite3.connect("data/boston_re.db")
    c.row_factory = sqlite3.Row
    projects = c.execute(
        "select id, name, address, alt_addresses, completion_stage, status "
        "from projects where coalesce(excluded,0)=0 and city='Boston'").fetchall()
    comp = load_compliance()

    # ---------- before: exact lowercased string equality on address ----------
    naive_idx = defaultdict(list)
    for r in comp:
        naive_idx[r["address"].lower().strip()].append(r)
    naive_hits = {p["id"] for p in projects
                  if naive_idx.get((p["address"] or "").lower().strip())}

    # ---------- after: normalised address, street number+name, project name ----------
    by_norm = defaultdict(list)
    by_street = defaultdict(list)
    by_name = defaultdict(list)
    for r in comp:
        na = norm_address(r["address"])
        if na:
            by_norm[na].append(r)
        sn = street_name(r["address"])
        for num in street_numbers(r["address"]):
            if sn:
                by_street[(num, sn)].append(r)
        pn = norm_firm(r["project"])
        if pn and len(pn) > 4:
            by_name[pn].append(r)

    matches = {}
    for p in projects:
        addrs = [p["address"] or ""]
        if p["alt_addresses"]:
            addrs += [a.strip() for a in str(p["alt_addresses"]).split("|") if a.strip()]

        hit, how = [], None
        for a in addrs:
            if norm_address(a) in by_norm:
                hit, how = by_norm[norm_address(a)], "normalised_address"
                break
        if not hit:
            for a in addrs:
                sn = street_name(a)
                for num in street_numbers(a):
                    if (num, sn) in by_street:
                        hit, how = by_street[(num, sn)], "street_number_and_name"
                        break
                if hit:
                    break
        if not hit:
            pn = norm_firm(p["name"] or "")
            if pn and len(pn) > 4 and pn in by_name:
                hit, how = by_name[pn], "project_name"
        if hit:
            matches[p["id"]] = {"how": how, "records": hit}

    started = {p["id"] for p in projects
               if (p["completion_stage"] in ("Under Construction", "Complete")
                   or p["status"] in ("Under Construction", "Complete",
                                      "Building Permit Granted"))}
    n = len(projects)
    print(f"Boston non-excluded projects: {n}")
    print(f"  construction started (GC should exist): {len(started)}")
    print()
    print(f"BEFORE normalisation (exact address string): {len(naive_hits)}/{n} "
          f"({100*len(naive_hits)/n:.1f}%)")
    print(f"AFTER  normalisation (addr + street + name): {len(matches)}/{n} "
          f"({100*len(matches)/n:.1f}%)")
    print()
    ms = len(set(matches) & started)
    print(f"  on construction-started projects: {ms}/{len(started)} "
          f"({100*ms/max(1,len(started)):.1f}%)")
    print(f"  naive on started: {len(naive_hits & started)}/{len(started)}")
    print()
    by_how = defaultdict(int)
    for m in matches.values():
        by_how[m["how"]] += 1
    for k, v in sorted(by_how.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v}")

    named = {pid: m for pid, m in matches.items()
             if any(r["gc"] for r in m["records"])}
    only_no = {pid: m for pid, m in matches.items()
               if not any(r["gc"] for r in m["records"])
               and any(r["explicit_no_gc"] for r in m["records"])}
    print()
    print(f"  matched with at least one named GC: {len(named)}")
    print(f'  matched but only "No General Contractor": {len(only_no)}')

    Path("data/gc_compliance_matches.json").write_text(json.dumps(
        {str(k): v for k, v in matches.items()}, indent=1))
    print("\nwrote data/gc_compliance_matches.json")


if __name__ == "__main__":
    main()
