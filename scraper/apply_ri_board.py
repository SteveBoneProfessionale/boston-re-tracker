"""Match the Rhode Island board-document extractions back to projects.

The model read the site address off each document; the match to a project is
made here, in code, against the normalised address. A document that cannot be
tied to exactly one project is dropped rather than guessed at.

Where a document names a person and no firm, the person is recorded as the
value exactly as stated and flagged as an individual. That follows the rule
already applied to the permit datasets: a sole practitioner really can be the
architect of record, and the person is never expanded into an employer.
"""
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from addr_norm import address_keys, norm_address, street_name, street_numbers
from provenance import connect, record

RI = ("Providence", "Cranston", "Warwick", "Pawtucket", "Newport")
FIELDS = ("architect", "civil_engineer", "general_contractor")


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
    results = json.loads(Path("data/ri_board_results.json").read_text())
    idmap = json.loads(Path("data/ri_board_idmap.json").read_text())
    c = connect()
    projects = c.execute(
        f"select id,name,address,city,alt_addresses from projects "
        f"where coalesce(excluded,0)=0 and city in {RI}").fetchall()

    by_norm, by_street = defaultdict(list), defaultdict(list)
    for p in projects:
        addrs = [p["address"] or ""]
        if p["alt_addresses"]:
            addrs += [a.strip() for a in str(p["alt_addresses"]).split("|") if a.strip()]
        for a in addrs:
            if norm_address(a):
                by_norm[norm_address(a)].append(p)
            for key in address_keys(a):
                by_street[key].append(p)

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
            for key in address_keys(addr):
                if key in by_street:
                    cands = by_street[key]
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

        for f in FIELDS:
            got = d.get(f) or {}
            firm = (got.get("firm") or "").strip() or None
            person = (got.get("person") or "").strip() or None
            label = (got.get("role_label") or "").strip() or None
            quote = got.get("quote")
            if not label or not (firm or person):
                continue
            value = firm or person
            reason = None
            if not firm:
                reason = ("document names an individual and no firm; stored as "
                          "stated and not expanded to an employer")
            record(c, p["id"], f, value=value, outcome="resolved",
                   tier="document_confirmed", source_type="board_document",
                   source_url=None,
                   source_name=f"RI board document: {fname}",
                   page_ref="see quoted passage",
                   firm_sentence=quote, address_sentence=d.get("address_quote"),
                   resolution_step=2, reason=reason)
            stats[f"resolved_{f}"] += 1
            if f == "architect" and person:
                c.execute("update projects set architect_person=? where id=?",
                          (person, p["id"]))
    c.commit()
    for k, v in sorted(stats.items()):
        print(f"  {k:28} {v}")


if __name__ == "__main__":
    main()
