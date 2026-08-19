r"""
Tier 0: structural errors that need no research at all.

Three classes, all detectable by query:

  DUPLICATES     the same project on more than one row. Exact address matching
                 misses most of them, because the ways a row duplicates are
                 messy: "580 South" is a TRUNCATION of "580 South Water
                 Street"; "6-42 Somerset Street" is "16-42 Somerset Street"
                 with the leading digit lost to OCR; and 145/165 Dartmouth vs
                 171 Dartmouth are the same BXP scheme filed under two
                 addresses. So candidates are generated four ways and scored,
                 never merged automatically.

  PHANTOMS       a row with no stage, no developer, no plat and no source
                 document. Nothing anchors it to a real filing.

  SCOPE MISMATCH a row named "Phase N" carrying the whole master plan's units
                 or floor area. Faneuil Gardens Phase 1 shows 441 units when
                 the phase is 114; On the Dot writes one 1.4M sq ft total onto
                 three separate address rows.

CRITICAL DISTINCTION. Several addresses legitimately carry more than one row:
Suffolk Downs has five phases at 525 McClellan Highway, Allston Yards has
Buildings C and D at 60 Everett Street. Those are not duplicates. A group is
only a duplicate candidate when the rows do not describe distinct phases or
buildings, so phase and building markers are checked before anything is
flagged.

Report only. Nothing is written.

    python scraper/t0_structural_audit.py
"""

import re
import sys
import json
import logging
from pathlib import Path
from collections import defaultdict, Counter

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from db.database import get_session
from db.models import Project, ProjectFiling, ProjectStageEvent

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)
OUT = ROOT / "data" / "t0_structural_audit.json"

ABBR = {"street": "st", "avenue": "ave", "av": "ave", "road": "rd", "drive": "dr",
        "boulevard": "blvd", "blv": "blvd", "place": "pl", "court": "ct",
        "lane": "ln", "terrace": "ter", "parkway": "pkwy", "square": "sq",
        "highway": "hwy"}

# A row that names a phase or a lettered building is a DISTINCT scope, not a
# duplicate of its neighbour at the same address.
PHASE_MARK = re.compile(
    r"\bphase\s*([0-9ivx]+|[a-z])\b|\bbuilding\s+([a-z0-9]+)\b|\bparcel\s+([a-z0-9]+)\b|"
    r"\bblock\s+([a-z0-9]+)\b|\btower\s+([a-z0-9]+)\b", re.I)


def naddr(a):
    a = (a or "").lower().split(",")[0]
    a = re.sub(r"\(.*?\)", " ", a)
    a = re.sub(r"[^a-z0-9 \-]", " ", a)
    a = re.sub(r"\s+", " ", a).strip()
    return " ".join(ABBR.get(w, w) for w in a.split())


def street_only(a):
    t = [w for w in naddr(a).split() if not re.match(r"^\d+[a-z]?(-\d+[a-z]?)?$", w)]
    return " ".join(t[:2])


def leading_nums(a):
    m = re.match(r"^(\d+)\s*-\s*(\d+)", naddr(a))
    if m:
        return {int(m.group(1)), int(m.group(2))}
    m = re.match(r"^(\d+)", naddr(a))
    return {int(m.group(1))} if m else set()


def phase_key(p):
    m = PHASE_MARK.search((p.name or "") + " " + (p.description or "")[:120])
    return (m.group(0).lower().strip() if m else None)


def dev_key(d):
    d = re.sub(r"[^a-z ]", " ", (d or "").lower())
    d = re.sub(r"\b(llc|inc|lp|corp|corporation|company|co|properties|development|"
               r"developments|group|partners|realty|trust|the)\b", " ", d)
    return re.sub(r"\s+", " ", d).strip()


def main():
    session = get_session()
    allp = [p for p in session.query(Project).all() if not p.excluded]
    have_doc = {r.project_id for r in session.query(ProjectFiling).all()}
    have_doc |= {r.project_id for r in session.query(ProjectStageEvent).all()}

    # ---------- duplicates ----------
    cand = defaultdict(set)
    by_city = defaultdict(list)
    for p in allp:
        by_city[p.city].append(p)

    for city, rows in by_city.items():
        for i, a in enumerate(rows):
            for b in rows[i + 1:]:
                na, nb = naddr(a.address), naddr(b.address)
                if not na or not nb:
                    continue
                score, why = 0, []
                if na == nb:
                    score += 3; why.append("identical address")
                elif len(na) > 6 and (na.startswith(nb) or nb.startswith(na)):
                    score += 3; why.append("one address is a truncation of the other")
                elif street_only(a.address) and street_only(a.address) == street_only(b.address):
                    if leading_nums(a.address) & leading_nums(b.address):
                        score += 3; why.append("same street, overlapping number")
                    else:
                        # "6-42" vs "16-42": OCR dropped a leading digit
                        sa = "".join(str(x) for x in sorted(leading_nums(a.address)))
                        sb = "".join(str(x) for x in sorted(leading_nums(b.address)))
                        if sa and sb and (sa.endswith(sb) or sb.endswith(sa)):
                            score += 3; why.append("same street, number differs by a lost leading digit")
                        else:
                            score += 1; why.append("same street")
                if score == 0:
                    continue
                dk = dev_key(a.developer)
                if dk and dk == dev_key(b.developer):
                    score += 2; why.append("same developer")
                if a.residential_units and a.residential_units == b.residential_units:
                    score += 2; why.append("same unit count")
                pa, pb = phase_key(a), phase_key(b)
                if pa and pb and pa != pb:
                    score -= 4; why.append("DISTINCT phases/buildings -- not a duplicate")
                if score >= 4:
                    cand[tuple(sorted((a.id, b.id)))] = (score, why)

    # ---------- phantoms ----------
    phantom = [p for p in allp
               if not (p.stage_heard or p.status)
               and not p.developer and not p.applicant_entity
               and not p.plat_lots_raw and p.id not in have_doc]

    # ---------- scope mismatch ----------
    scope = []
    for p in allp:
        pk = phase_key(p)
        if not pk or not re.search(r"phase", pk, re.I):
            continue
        peers = [q for q in allp if q.id != p.id and q.city == p.city
                 and street_only(q.address) and street_only(q.address) == street_only(p.address)]
        same_units = [q for q in peers if q.residential_units and q.residential_units == p.residential_units]
        same_sf = [q for q in peers if q.total_gsf and q.total_gsf == p.total_gsf]
        if same_units or same_sf:
            scope.append({"id": p.id, "name": p.name, "city": p.city, "phase": pk,
                          "units": p.residential_units, "sf": p.total_gsf,
                          "shares_with": [q.id for q in (same_units or same_sf)]})

    log.info("\nTIER 0 -- STRUCTURAL AUDIT (report only, nothing written)\n")
    log.info("Active rows examined: %d", len(allp))
    log.info("\nDUPLICATE CANDIDATES (score >= 4): %d pairs", len(cand))
    got = session.get
    for (i, j), (sc, why) in sorted(cand.items(), key=lambda kv: -kv[1][0]):
        a, b = got(Project, i), got(Project, j)
        log.info("  [%d] %-11s %s", sc, a.city, "; ".join(dict.fromkeys(why)))
        for x in (a, b):
            log.info("        id=%-4d units=%-5s sf=%-9s %-34s %s", x.id, x.residential_units,
                     x.total_gsf, (x.address or "")[:34], (x.developer or "")[:26])

    log.info("\nPHANTOM ROWS (no stage, developer, plat or source document): %d", len(phantom))
    for p in phantom:
        log.info("  id=%-4d %-11s units=%-5s sf=%-9s %s", p.id, p.city, p.residential_units,
                 p.total_gsf, (p.address or p.name or "")[:44])

    log.info("\nSCOPE MISMATCH -- a phase row sharing a figure with a peer: %d", len(scope))
    for r in scope:
        log.info("  id=%-4d %-11s %-38s units=%-5s sf=%-9s shares with %s",
                 r["id"], r["city"], str(r["name"])[:38], r["units"], r["sf"], r["shares_with"])

    OUT.write_text(json.dumps({
        "duplicates": [{"ids": list(k), "score": v[0], "why": v[1]} for k, v in cand.items()],
        "phantoms": [p.id for p in phantom], "scope": scope}, indent=1), encoding="utf-8")
    session.close()


if __name__ == "__main__":
    main()
