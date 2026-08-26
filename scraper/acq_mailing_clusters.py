r"""Sponsor resolution through owner mailing addresses, harvested per parcel.

The first address-clustering pass yielded 8 rows because it clustered on
whatever mailing addresses happened to be lying around. This does it properly:
for every spine row the parcel is known, and both assessment files publish the
owner's MAILING address per parcel. Where the buyer still owns the parcel -- and
on a spine row it does by construction, since the row records that parcel's most
recent sale -- the current mailing address IS that buyer's mailing address.

That yields a mailing address for hundreds of entities with no external lookup
and no blocked portal.

Clustering then uses the guards already built, because the guards are the whole
difference between this working and it inventing a mega-buyer:

  A CLUSTER ONLY RESOLVES FROM A KNOWN MEMBER. At least one entity at the
  address must already be resolved by pattern; that sponsor propagates to its
  siblings. An agent's address houses many sponsors and so either has none to
  propagate from or several in conflict.

  CONFLICTING CLUSTERS ARE REJECTED outright.

  ENTITIES WITH THEIR OWN CORPORATE IDENTITY ARE NEVER ABSORBED. Sun Life
  Assurance and MassDevelopment were each about to be labelled with a sponsor
  whose vehicle merely shares their servicing address.

  A KEY WITH NO STREET NUMBER IS NOT AN ADDRESS. "SUMMER BOSTON MA" is every
  address on Summer Street at once, and it pulled an unrelated owner into
  Fidelity before the guard existed.

    python scraper/acq_mailing_clusters.py            # verify
    python scraper/acq_mailing_clusters.py --apply
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
from scraper.acq_resolve_addresses import INDEPENDENT, norm_addr

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

UA = {"User-Agent": "boston-re-tracker/1.0 (13silonergan@gmail.com)"}
L3 = ("https://arcgisserver.digital.mass.gov/arcgisserver/rest/services/AGOL/"
      "L3_Parcels_FeatureService_4326/FeatureServer/1/query")
CAMB = "https://data.cambridgema.gov/resource/waa7-ibdu.json"

CONFLICT_LIMIT = 1
AGENT_SIZE = 12


def boston_mail() -> dict:
    """LOC_ID -> (owner, normalised mailing address). Every commercial parcel."""
    out, offset = {}, 0
    where = "CITY='BOSTON' AND USE_CODE >= '300' AND USE_CODE < '500'"
    with httpx.Client(headers=UA, timeout=120) as c:
        while True:
            r = c.get(L3, params={
                "where": where,
                "outFields": "LOC_ID,OWNER1,OWN_ADDR,OWN_CITY,OWN_STATE",
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
                if a.get("LOC_ID") and own and addr:
                    out[a["LOC_ID"]] = (own, addr)
            if not d.get("exceededTransferLimit") or not feats:
                return out
            offset += len(feats)
            time.sleep(0.4)


def cambridge_mail() -> dict:
    out, offset = {}, 0
    with httpx.Client(headers=UA, timeout=90) as c:
        while True:
            r = c.get(CAMB, params={
                "$select": ("map_lot,gisid,address,owner_name,owner_address,"
                            "owner_city,owner_state"),
                "$where": "stateclasscode >= '300' AND stateclasscode <= '499'",
                "$limit": 5000, "$offset": offset})
            r.raise_for_status()
            page = r.json()
            for x in page:
                own = (x.get("owner_name") or "").strip().upper()
                addr = norm_addr(x.get("owner_address"), x.get("owner_city"),
                                 x.get("owner_state"))
                if not own or not addr:
                    continue
                for k in (x.get("map_lot"), x.get("gisid"), x.get("address")):
                    if k:
                        out[str(k).strip()] = (own, addr)
            if len(page) < 5000:
                return out
            offset += 5000
            time.sleep(0.3)


def main(dry_run: bool):
    mail = boston_mail()
    log.info("Boston: mailing address for %d commercial parcels", len(mail))
    cmail = cambridge_mail()
    log.info("Cambridge: mailing address for %d parcel keys", len(cmail))

    conn = engine.connect()
    rows = []
    for side in ("buyer", "seller"):
        for rid, name, canon, parcel, addr, basis in conn.execute(text(
                f"select id, {side}, coalesce({side}_canonical,''), parcel_id, "
                f"address, coalesce({side}_resolution_basis,'') from transactions "
                f"where coalesce({side},'') <> ''")):
            rows.append((rid, side, name.strip().upper(), canon, parcel, addr, basis))

    # entity -> mailing address, harvested per parcel
    ent_addr = {}
    for _rid, _side, ent, _c, parcel, addr, _b in rows:
        hit = mail.get(str(parcel or "")) or cmail.get(str(parcel or "")) \
            or cmail.get(str(addr or "").strip())
        if hit and hit[0] == ent:
            ent_addr[ent] = hit[1]
    log.info("mailing address recovered for %d distinct entities", len(ent_addr))

    known = {e: c for _r, _s, e, c, _p, _a, _b in rows if c}
    cluster = defaultdict(set)
    for ent, a in ent_addr.items():
        cluster[a].add(ent)
    multi = {a: m for a, m in cluster.items() if len(m) > 1}
    log.info("%d distinct mailing addresses, %d shared by 2+ entities",
             len(cluster), len(multi))

    resolved, conflicted, agentish, independent = {}, [], [], []
    for a, members in multi.items():
        sponsors = {known[m] for m in members if m in known}
        if len(sponsors) > CONFLICT_LIMIT:
            conflicted.append((a, members, sponsors))
            continue
        if not sponsors:
            continue
        if len(members) >= AGENT_SIZE:
            agentish.append((a, members))
            continue
        sponsor = next(iter(sponsors))
        for m in members:
            if m in known:
                continue
            if INDEPENDENT.search(m):
                independent.append((m, sponsor))
                continue
            resolved[m] = (sponsor, a)

    log.info("\n%d entities newly resolvable", len(resolved))
    by = defaultdict(list)
    for e, (sp, a) in resolved.items():
        by[sp].append(e)
    for sp in sorted(by, key=lambda s: -len(by[s]))[:14]:
        log.info("  %-38s +%d   e.g. %s", sp, len(by[sp]), sorted(by[sp])[0][:40])
    if conflicted:
        log.info("\n%d addresses rejected for holding multiple sponsors", len(conflicted))
    if agentish:
        log.info("%d addresses rejected as agent-like (>=%d entities)",
                 len(agentish), AGENT_SIZE)
    if independent:
        log.info("%d entities rejected as independent institutions", len(independent))

    if not dry_run:
        n = 0
        for rid, side, ent, canon, _p, _a, basis in rows:
            if canon or basis.startswith("conflict") or ent not in resolved:
                continue
            sp, a = resolved[ent]
            conn.execute(text(
                f"update transactions set {side}_canonical = :s, "
                f"{side}_confidence = 'pattern_matched', "
                f"{side}_resolution_basis = :b where id = :id"),
                {"s": sp, "b": f"mailing_cluster:{a[:40]}", "id": rid})
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
