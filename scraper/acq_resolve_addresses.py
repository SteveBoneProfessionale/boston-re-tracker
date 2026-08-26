r"""Layer 3 of entity resolution: cluster entities by principal office address.

Pattern matching resolves the sponsors who brand their vehicles. It cannot touch
"100 SUMMER OWNER LLC" or "245 STATE STREET LLC", which name the asset and say
nothing about who is behind them -- and those are the majority.

What they do carry is a mailing address. Both assessment sources publish where
the owner's tax bill goes: MassGIS has OWN_ADDR/OWN_CITY/OWN_STATE and
Cambridge has owner_address/owner_city. Entities sharing a principal office are
usually one sponsor, so a cluster is a candidate sponsor family.

THE CLUSTER IS NOT THE ANSWER, IT IS THE QUESTION. Two things sit at shared
addresses and only one of them is a sponsor:

  a sponsor's own office, where its SPEs genuinely live, and
  a REGISTERED AGENT, LAW FIRM or FUND ADMINISTRATOR, which houses hundreds of
  unrelated entities and would merge every client into one fictitious mega-buyer.

CT Corporation, Corporation Service Company, and the big Boston firms all show
up this way. So no cluster is ever resolved on its own evidence. A cluster only
resolves when it contains at least one member ALREADY resolved by pattern, and
then the known sponsor propagates to its siblings. That single rule does the
work of a registered-agent blacklist without needing one: an agent's address
will contain entities from many different sponsors, so it either has no resolved
member to propagate from, or it has several conflicting ones and is rejected on
the conflict.

Clusters that are large, unresolved, or internally contradictory are printed as
research candidates rather than guessed at.

    python scraper/acq_resolve_addresses.py            # verify
    python scraper/acq_resolve_addresses.py --apply
"""

import argparse
import logging
import re
import sys
import time
from collections import defaultdict
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from db.database import engine

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

UA = {"User-Agent": "boston-re-tracker/1.0 (13silonergan@gmail.com)"}
L3 = ("https://arcgisserver.digital.mass.gov/arcgisserver/rest/services/AGOL/"
      "L3_Parcels_FeatureService_4326/FeatureServer/1/query")
CAMB = "https://data.cambridgema.gov/resource/waa7-ibdu.json"

# Above this many DISTINCT sponsors at one address, it is a service address and
# nothing propagates from it.
CONFLICT_LIMIT = 1
# Above this many entities at one address with no resolved member, it is
# reported as an agent-like cluster rather than a sponsor candidate.
AGENT_SIZE = 12

# An entity with its own corporate identity is never absorbed into a sponsor on
# address evidence alone. The first verification run produced exactly this
# error twice: MASSDEVELOPMENT/NECCO was about to be labelled Alexandria and SUN
# LIFE ASSURANCE CO OF CANADA was about to be labelled MEPT, because each shares
# a management or servicing address with one of that sponsor's vehicles. Both
# are independent institutions. Address co-location is evidence about
# single-purpose vehicles, which have no identity of their own; it is not
# evidence about a company that plainly does.
INDEPENDENT = re.compile(
    r"\b(ASSURANCE|INSURANCE|LIFE|BANK|BANC|UNIVERSITY|COLLEGE|HOSPITAL|"
    r"CHURCH|MASSDEVELOPMENT|MASSPORT|AUTHORITY|COMMONWEALTH OF|CITY OF|"
    r"UNITED STATES|MBTA|HOUSING AUTHORITY|ARCHDIOCESE|FOUNDATION|"
    r"MUTUAL|PENSION|RETIREMENT SYSTEM)")


def norm_addr(street, city=None, state=None) -> str:
    """Normalise a mailing address into a cluster key.

    The street part is cleaned separately from city and state, because an
    earlier version applied a "strip everything after SUITE" rule to the joined
    string and so deleted the city and state along with the suite number.

    A key with no digit in it is returned empty. "SUMMER BOSTON MA" is not an
    address -- it is every address on Summer Street at once, and clustering on
    it merged unrelated owners.
    """
    st = re.sub(r"[^A-Z0-9 ]", " ", str(street or "").upper())
    st = re.sub(r"\b(SUITE|STE|FLOOR|FL|UNIT|APT|ATTN|C O|CO)\b.*$", "", st)
    st = re.sub(r"\b(STREET|ST|AVENUE|AVE|ROAD|RD|DRIVE|DR|BOULEVARD|BLVD|"
                r"PLACE|PL|SQUARE|SQ|LANE|LN|COURT|CT)\b", " ", st)
    st = re.sub(r"\s+", " ", st).strip()
    if not any(ch.isdigit() for ch in st):
        return ""
    tail = re.sub(r"[^A-Z0-9 ]", " ",
                  f"{city or ''} {state or ''}".upper())
    return re.sub(r"\s+", " ", f"{st} {tail}").strip()


def fetch_boston() -> dict:
    """OWNER1 -> normalised mailing address."""
    out, offset = {}, 0
    where = ("CITY='BOSTON' AND LS_PRICE >= 2000000 AND "
             "USE_CODE >= '300' AND USE_CODE < '500'")
    with httpx.Client(headers=UA, timeout=120) as c:
        while True:
            r = c.get(L3, params={
                "where": where,
                "outFields": "OWNER1,OWN_ADDR,OWN_CITY,OWN_STATE,OWN_ZIP",
                "returnGeometry": "false", "resultOffset": offset,
                "resultRecordCount": 2000, "f": "json"})
            r.raise_for_status()
            d = r.json()
            feats = d.get("features", [])
            for f in feats:
                a = f["attributes"]
                own = (a.get("OWNER1") or "").strip().upper()
                addr = norm_addr(a.get("OWN_ADDR"), a.get("OWN_CITY"),
                                 a.get("OWN_STATE"))
                if own and len(addr) > 6:
                    out[own] = addr
            if not d.get("exceededTransferLimit") or not feats:
                return out
            offset += len(feats)
            time.sleep(0.4)


def fetch_cambridge() -> dict:
    out, offset = {}, 0
    with httpx.Client(headers=UA, timeout=90) as c:
        while True:
            r = c.get(CAMB, params={
                "$select": "owner_name,owner_address,owner_city,owner_state",
                "$where": "stateclasscode >= '300' AND stateclasscode <= '499'",
                "$limit": 5000, "$offset": offset})
            r.raise_for_status()
            page = r.json()
            for x in page:
                own = (x.get("owner_name") or "").strip().upper()
                addr = norm_addr(x.get("owner_address"), x.get("owner_city"),
                                 x.get("owner_state"))
                if own and len(addr) > 6:
                    out[own] = addr
            if len(page) < 5000:
                return out
            offset += 5000
            time.sleep(0.3)


def main(dry_run: bool):
    addrs = fetch_boston()
    addrs.update(fetch_cambridge())
    log.info("mailing address for %d distinct owner names", len(addrs))

    conn = engine.connect()
    # entity -> its already-resolved sponsor, from the pattern layer
    known = {}
    rows = []
    for side in ("buyer", "seller"):
        # A row whose resolution basis is already set has been decided --
        # including rows deliberately locked as unresolvable after a
        # record-versus-press conflict. Never silently re-resolve those.
        for rid, name, canon in conn.execute(text(
                f"select id, {side}, {side}_canonical from transactions "
                f"where coalesce({side},'') <> '' "
                f"and coalesce({side}_resolution_basis,'') "
                f"not like 'conflict%'")):
            key = name.strip().upper()
            rows.append((rid, side, key, canon))
            if canon:
                known[key] = canon

    # cluster entities by address
    cluster = defaultdict(set)
    for _rid, _side, key, _canon in rows:
        a = addrs.get(key)
        if a:
            cluster[a].add(key)

    log.info("%d distinct mailing addresses across the entities", len(cluster))

    resolved_by_addr, agentish, conflicted = {}, [], []
    rejected_independent = []
    for addr, members in cluster.items():
        sponsors = {known[m] for m in members if m in known}
        if len(sponsors) > CONFLICT_LIMIT:
            conflicted.append((addr, members, sponsors))
            continue
        if not sponsors:
            if len(members) >= AGENT_SIZE:
                agentish.append((addr, members))
            continue
        sponsor = next(iter(sponsors))
        if len(members) >= AGENT_SIZE:
            # One known sponsor but an implausible number of entities: this is a
            # service address that happens to house one recognisable name.
            agentish.append((addr, members))
            continue
        for m in members:
            if m in known:
                continue
            if INDEPENDENT.search(m):
                rejected_independent.append((m, sponsor))
                continue
            resolved_by_addr[m] = (sponsor, addr)

    log.info("\n%d entities newly resolvable by shared address", len(resolved_by_addr))
    by_sponsor = defaultdict(list)
    for ent, (sp, a) in resolved_by_addr.items():
        by_sponsor[sp].append(ent)
    for sp in sorted(by_sponsor, key=lambda s: -len(by_sponsor[s])):
        log.info("  %-34s +%d", sp, len(by_sponsor[sp]))
        for e in sorted(by_sponsor[sp])[:3]:
            log.info("        %s", e[:66])

    if rejected_independent:
        log.info("%d entities REJECTED as independent institutions "
                 "despite a shared address:", len(rejected_independent))
        for m, sp in rejected_independent:
            log.info("  %-46s would have become %s", m[:46], sp)

    if conflicted:
        log.info("\n%d addresses REJECTED for holding multiple sponsors "
                 "(service addresses):", len(conflicted))
        for a, m, s in sorted(conflicted, key=lambda x: -len(x[1]))[:6]:
            log.info("  %-30s %2d entities, sponsors: %s", a[:30], len(m),
                     ", ".join(sorted(s))[:60])
    if agentish:
        log.info("\n%d addresses REJECTED as agent-like (>= %d entities):",
                 len(agentish), AGENT_SIZE)
        for a, m in sorted(agentish, key=lambda x: -len(x[1]))[:6]:
            log.info("  %-30s %d entities", a[:30], len(m))

    if not dry_run:
        n = 0
        for rid, side, key, canon in rows:
            if canon or key not in resolved_by_addr:
                continue
            sp, a = resolved_by_addr[key]
            conn.execute(text(
                f"update transactions set {side}_canonical = :s, "
                f"{side}_confidence = 'pattern_matched', "
                f"{side}_resolution_basis = :b where id = :id"),
                {"s": sp, "b": f"address_cluster:{a[:40]}", "id": rid})
            n += 1
        conn.commit()
        log.info("\n%d rows updated", n)
        tot = conn.execute(text("select count(*) from transactions")).scalar()
        for side in ("buyer", "seller"):
            v = conn.execute(text(
                f"select count(*) from transactions "
                f"where coalesce({side}_canonical,'') <> ''")).scalar()
            log.info("%s_canonical: %d of %d (%.0f%%)", side, v, tot, v / tot * 100)
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
