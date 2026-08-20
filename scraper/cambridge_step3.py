"""Step 3 for Cambridge: the permit architect field.

`architect_firm` is a labelled column on the New Construction permit dataset,
so a match is registry_confirmed. `engineer_name` is empty across all 362
rows, so Cambridge civil engineers cannot come from here at all.
`licensed_name` is the construction supervisor -- a person, not a firm, and
is recorded as a GC candidate only, never merged to a company.
"""
import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from addr_norm import norm_address, street_name, street_numbers

PERMITS = Path("data/cambridge_newcon_permits.json")
SRC_URL = "https://data.cambridgema.gov/resource/9qm7-wbdc.json"


def main():
    permits = json.loads(PERMITS.read_text())
    by_norm, by_street = defaultdict(list), defaultdict(list)
    for p in permits:
        a = p.get("full_address") or ""
        if norm_address(a):
            by_norm[norm_address(a)].append(p)
        sn = street_name(a)
        for num in street_numbers(a):
            if sn:
                by_street[(num, sn)].append(p)

    c = sqlite3.connect("data/boston_re.db")
    c.row_factory = sqlite3.Row
    projects = c.execute(
        "select id,name,address,alt_addresses,status from projects "
        "where city='Cambridge' and coalesce(excluded,0)=0 order by id").fetchall()

    naive = 0
    out = {}
    for p in projects:
        addrs = [p["address"] or ""]
        if p["alt_addresses"]:
            addrs += [a.strip() for a in str(p["alt_addresses"]).split("|") if a.strip()]
        if any((p["address"] or "").lower().strip()
               == (q.get("full_address") or "").lower().strip() for q in permits):
            naive += 1

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
        if hit:
            out[p["id"]] = {"how": how, "permits": hit}

    n = len(projects)
    print(f"Cambridge non-excluded: {n}")
    print(f"BEFORE normalisation (exact address): {naive}/{n} ({100*naive/n:.1f}%)")
    print(f"AFTER  normalisation:                 {len(out)}/{n} ({100*len(out)/n:.1f}%)")
    witharch = {k: v for k, v in out.items()
                if any(q.get("architect_firm") for q in v["permits"])}
    print(f"  matched permits carrying architect_firm: {len(witharch)}")
    print()
    for pid, v in sorted(witharch.items()):
        pr = next(x for x in projects if x["id"] == pid)
        firms = sorted({q["architect_firm"].strip() for q in v["permits"]
                        if q.get("architect_firm")})
        print(f"  {pid:4} {pr['address'][:38]:38} -> {'; '.join(firms)[:60]}")

    Path("data/cambridge_step3_matches.json").write_text(
        json.dumps({str(k): v for k, v in out.items()}, indent=1))
    print(f"\nwrote data/cambridge_step3_matches.json")


if __name__ == "__main__":
    main()
