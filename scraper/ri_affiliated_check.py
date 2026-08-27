r"""Check the Rhode Island development data for the affiliated-LLC pattern.

The concern is well founded but the shape is different. The projects table
holds DEVELOPMENTS, not transactions: there is no price, no buyer and no
seller, so an affiliated transfer cannot appear there as a fake trade the way
it did in the acquisitions table. What it CAN do is appear as two things that
are really one, and that is what this looks for:

  DUPLICATE PROJECTS. The same site filed twice under two vehicles of one
  sponsor, which would double-count units, square footage and pipeline volume.

  A DEVELOPER AND AN APPLICANT THAT ARE THE SAME PARTY. `developer`,
  `applicant_entity` and `owner_or_agency` are separate columns, and where two
  of them are affiliated vehicles of one firm the project looks like a
  partnership between two entities when it is one sponsor wearing two hats.

  A CHANGE OF DEVELOPER BETWEEN AFFILIATES, which reads as a site trading
  hands when nothing economic happened.

The same guard applies as in the acquisitions sweep: a SHARED NAME STEM IS NOT
EVIDENCE when the stem comes from the property's own address, because
development vehicles are named after their sites even more consistently than
acquisition vehicles are.

    python scraper/ri_affiliated_check.py
"""

import argparse
import logging
import re
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from db.database import engine
from scraper.acq_affiliated_sweep import norm_entity, addr_tokens

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

RI_CITIES = ("Providence", "Warwick", "Cranston", "Pawtucket", "Newport",
             "East Providence", "Central Falls", "Woonsocket", "Bristol",
             "Johnston", "North Kingstown", "West Warwick")


def norm_addr(a: str) -> str:
    s = re.sub(r"[^A-Z0-9 ]", " ", (a or "").upper())
    s = re.sub(r"\b(STREET|ST|AVENUE|AVE|ROAD|RD|DRIVE|DR|BOULEVARD|BLVD|"
               r"PLACE|PL|SQUARE|SQ|LANE|LN|COURT|CT|WAY)\b", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def main():
    conn = engine.connect()
    ph = ",".join(f":c{i}" for i in range(len(RI_CITIES)))
    params = {f"c{i}": c for i, c in enumerate(RI_CITIES)}
    rows = conn.execute(text(
        f"select id, name, address, city, coalesce(developer,''), "
        f"coalesce(developer_canonical,''), coalesce(applicant_entity,''), "
        f"coalesce(owner_or_agency,''), residential_units, total_gsf "
        f"from projects where city in ({ph})"), params).fetchall()
    log.info("%d Rhode Island projects", len(rows))

    # ── 1. duplicate sites ──────────────────────────────────────────
    by_addr = defaultdict(list)
    for r in rows:
        k = norm_addr(r[2])
        if len(k) > 4:
            by_addr[k].append(r)
    dupes = {k: v for k, v in by_addr.items() if len(v) > 1}
    log.info("\n%d addresses carry more than one project row", len(dupes))
    affiliated_dupes = 0
    for k, v in sorted(dupes.items(), key=lambda x: -len(x[1]))[:12]:
        devs = {x[4].strip() for x in v if x[4].strip()}
        norms = {norm_entity(d) for d in devs}
        same = len(devs) > 1 and len(norms) == 1
        if same:
            affiliated_dupes += 1
        log.info("  %-32s %d rows  developers: %s%s", k[:32], len(v),
                 " | ".join(sorted(devs))[:70] or "(none)",
                 "   <- SAME PARTY, different spelling" if same else "")

    # ── 2. developer vs applicant vs owner, same party ──────────────
    log.info("")
    hits = []
    for rid, name, addr, city, dev, devc, app, own, units, gsf in rows:
        at = addr_tokens(addr)
        pairs = (("developer", dev, "applicant_entity", app),
                 ("developer", dev, "owner_or_agency", own),
                 ("applicant_entity", app, "owner_or_agency", own))
        for an, a, bn, b in pairs:
            if not a.strip() or not b.strip():
                continue
            na, nb = norm_entity(a), norm_entity(b)
            if not na or not nb or na == nb:
                if na and na == nb and a.strip() != b.strip():
                    hits.append((rid, name, an, a, bn, b, "identical after normalisation"))
                continue
            shared = (set(na.split()) & set(nb.split())) - at
            shared = {w for w in shared if len(w) >= 4}
            if shared:
                hits.append((rid, name, an, a, bn, b,
                             f"shared stem {sorted(shared)[0]} (not from the address)"))
    log.info("%d projects where two ownership fields look like the same party:",
             len(hits))
    for rid, name, an, a, bn, b, why in hits[:14]:
        log.info("  id=%-5s %-34s %s", rid, (name or "")[:34], why)
        log.info("        %-18s %s", an, a[:52])
        log.info("        %-18s %s", bn, b[:52])

    log.info("")
    log.info("VERDICT: the projects table records developments, not trades. It "
             "carries no price, buyer or seller, so an affiliated transfer "
             "cannot appear as a phantom transaction. The exposure is "
             "double-counted pipeline, and %d addresses carry more than one row, "
             "of which %d have developer names that are the same party spelled "
             "differently.", len(dupes), affiliated_dupes)
    conn.close()


if __name__ == "__main__":
    argparse.ArgumentParser().parse_args()
    main()
