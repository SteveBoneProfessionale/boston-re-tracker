r"""Resolve sponsors from the "c/o" line in the assessment roll's mailing address.

This is the Torrington Properties trick, found on a Cambridge zoning filing --
"UEP JBBP LLC ... c/o TORRINGTON PROPERTIES INC." -- applied at scale to a
source already in hand. Boston's parcel layer carries an owner mailing address
per parcel, and 794 of its 8,130 commercial parcels put a care-of name in it.
A single-purpose vehicle that mails care of a firm IS that firm's vehicle.

WHY THIS BEATS CLUSTERING. Address clustering needs a cluster: at least two
entities at one address AND one of them already resolved. Most SPEs mail alone,
so it found 8 rows and then 4. A care-of line needs neither -- it names the
sponsor directly on a single row, so it resolves entities that cluster with
nothing.

WHAT GETS REJECTED, because a care-of line is not always a sponsor:

  BARE PLACEHOLDERS. "c/o" followed by nothing, or by the entity's own name.
  AGENTS AND SERVICE FIRMS. A care-of naming a law firm, registered agent,
  bank trust department or property manager identifies who handles the mail,
  not who owns the asset. Names carrying those markers are dropped.
  OVER-USED NAMES. A care-of appearing across an implausible number of
  otherwise unrelated entities is an agent by behaviour even when its name
  gives nothing away, and is reported rather than applied.
  INDEPENDENT INSTITUTIONS keep the guard they already have.

    python scraper/acq_care_of.py            # verify
    python scraper/acq_care_of.py --apply
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
from scraper.acq_resolve_addresses import INDEPENDENT

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

UA = {"User-Agent": "boston-re-tracker/1.0 (13silonergan@gmail.com)"}
L3 = ("https://arcgisserver.digital.mass.gov/arcgisserver/rest/services/AGOL/"
      "L3_Parcels_FeatureService_4326/FeatureServer/1/query")
CAMB = "https://data.cambridgema.gov/resource/waa7-ibdu.json"

CO = re.compile(r"\bC\s*[/.]?\s*O\b[:\s]*(.+)$", re.I)
# Whoever handles the mail is not necessarily whoever owns the building.
AGENTLIKE = re.compile(
    r"\b(LLP|ATTORNEY|ATTY|LAW|ESQ|COUNSEL|CT CORPORATION|CORPORATION SERVICE|"
    r"REGISTERED AGENT|ACCOUNTING|CPA|BOOKKEEP|"
    r"TRUSTEE SERVICES|PROPERTY MANAGEMENT|MGMT CO|MANAGEMENT CO|"
    r"BANK|TRUST DEPT|SERVICING|ESCROW|TITLE|"
    # Any tax function: a "TAX DIVISION" or "TAX DEPT" line routes the bill,
    # it does not identify an owner.
    r"TAX|"
    # Property-tax consultancies, which appear as care-of on hundreds of
    # unrelated owners nationally.
    r"RYAN LLC|\bSLK\b|ALTUS GROUP|DUFF & PHELPS|MARVIN F POER|PARADIGM TAX|"
    r"MIRICK|OCONNELL|O'CONNELL)\b", re.I)

# Tenant billing addresses on net-leased property. The care-of is the OCCUPIER
# handling the tax bill, and the owner is a separate net-lease landlord --
# "BRICKS AND MORTAR HOLDINGS LLC c/o WALGREENS CO" is a landlord whose tenant
# pays the taxes, not a Walgreens subsidiary. There is no general rule that
# catches these without a tenant list, so the observed ones are named.
TENANT_BILLING = {
    "WALGREENS CO", "WALGREEN CO", "ORIENTAL FURNITURE", "CVS", "CVS HEALTH",
    "RITE AID", "DOLLAR TREE", "AUTOZONE", "OREILLY AUTO PARTS",
    "SUNOCO", "SPEEDWAY", "7-ELEVEN", "MCDONALDS CORPORATION",
}
TRAILING = re.compile(r"\b(\d{1,6}[\w-]*\s+\w+|SUITE|STE|FLOOR|FL|PO BOX|P O BOX).*$", re.I)
MAX_ENTITIES_PER_CO = 25


def clean_co(s: str) -> str:
    s = re.sub(r"[^A-Za-z0-9&'., -]", " ", s or "").strip()
    s = TRAILING.sub("", s).strip(" ,.-")
    return re.sub(r"\s+", " ", s).upper()


def fetch() -> dict:
    """entity -> care-of name, from both cities' mailing addresses."""
    out = {}
    off = 0
    with httpx.Client(headers=UA, timeout=120) as c:
        while True:
            r = c.get(L3, params={
                "where": "CITY='BOSTON' AND USE_CODE >= '300' AND USE_CODE < '500'",
                "outFields": "OWNER1,OWN_ADDR", "returnGeometry": "false",
                "resultOffset": off, "resultRecordCount": 2000, "f": "json"})
            r.raise_for_status()
            d = r.json()
            feats = d.get("features", [])
            for f in feats:
                a = f["attributes"]
                own = (a.get("OWNER1") or "").strip().upper()
                m = CO.search(str(a.get("OWN_ADDR") or ""))
                if own and m:
                    co = clean_co(m.group(1))
                    if len(co) > 3:
                        out[own] = co
            if not d.get("exceededTransferLimit") or not feats:
                break
            off += len(feats)
            time.sleep(0.3)
        off = 0
        while True:
            r = c.get(CAMB, params={
                "$select": "owner_name,owner_address",
                "$where": "stateclasscode >= '300' AND stateclasscode <= '499'",
                "$limit": 5000, "$offset": off})
            r.raise_for_status()
            page = r.json()
            for x in page:
                own = (x.get("owner_name") or "").strip().upper()
                m = CO.search(str(x.get("owner_address") or ""))
                if own and m:
                    co = clean_co(m.group(1))
                    if len(co) > 3:
                        out.setdefault(own, co)
            if len(page) < 5000:
                break
            off += 5000
            time.sleep(0.3)
    return out


def main(dry_run: bool):
    care = fetch()
    log.info("%d entities carry a care-of name in their mailing address", len(care))

    freq = defaultdict(set)
    for ent, co in care.items():
        freq[co].add(ent)

    conn = engine.connect()
    rows = []
    for side in ("buyer", "seller"):
        for rid, name, canon, basis in conn.execute(text(
                f"select id, {side}, coalesce({side}_canonical,''), "
                f"coalesce({side}_resolution_basis,'') from transactions "
                f"where coalesce({side},'') <> ''")):
            rows.append((rid, side, name.strip().upper(), canon, basis))

    applied, rejected = {}, defaultdict(list)
    for _rid, _side, ent, canon, _b in rows:
        if canon or ent not in care:
            continue
        co = care[ent]
        if co == ent or ent.startswith(co) or co in ent:
            rejected["care-of repeats the entity's own name"].append((ent, co))
            continue
        if AGENTLIKE.search(co):
            rejected["care-of is a law firm, tax agent, bank or manager"].append((ent, co))
            continue
        if co.upper().rstrip(".") in TENANT_BILLING:
            rejected["care-of is a tenant billing address, not the owner"].append((ent, co))
            continue
        if INDEPENDENT.search(ent):
            rejected["entity is an independent institution"].append((ent, co))
            continue
        if len(freq[co]) > MAX_ENTITIES_PER_CO:
            rejected[f"care-of used by >{MAX_ENTITIES_PER_CO} entities"].append((ent, co))
            continue
        applied[ent] = co.title()

    log.info("\n%d entities resolvable from a care-of line", len(applied))
    by = defaultdict(list)
    for e, co in applied.items():
        by[co].append(e)
    for co in sorted(by, key=lambda c: -len(by[c]))[:18]:
        log.info("  %-40s %d  e.g. %s", co[:40], len(by[co]), sorted(by[co])[0][:38])
    log.info("")
    for why, items in rejected.items():
        log.info("  rejected: %-46s %d   e.g. %s -> %s", why, len(items),
                 items[0][0][:26], items[0][1][:26])

    if not dry_run:
        n = 0
        for rid, side, ent, canon, basis in rows:
            if canon or basis.startswith("conflict") or ent not in applied:
                continue
            conn.execute(text(
                f"update transactions set {side}_canonical = :s, "
                f"{side}_confidence = 'registry_confirmed', "
                f"{side}_resolution_basis = 'care_of' where id = :id"),
                {"s": applied[ent], "id": rid})
            n += 1
        conn.commit()
        log.info("\n%d rows updated", n)
        tot = conn.execute(text("select count(*) from transactions")).scalar()
        for side in ("buyer", "seller"):
            v, d = conn.execute(text(
                f"select count(*), sum(coalesce(price,0)) from transactions "
                f"where coalesce({side}_canonical,'') <> ''")).first()
            log.info("%s_canonical: %d of %d (%.0f%%), $%.2fB", side, v, tot,
                     v / tot * 100, (d or 0) / 1e9)
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
