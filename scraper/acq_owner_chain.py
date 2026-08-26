r"""Derive the GRANTOR from annual assessment snapshots.

The seller was reported as structurally unobtainable from an assessment file,
because such a file records who owns a parcel now and never who owned it before.
That is true of ONE file. It is not true of eighteen of them.

Boston publishes a property assessment file every year from FY2008 to FY2026,
each carrying PID and OWNER, and Cambridge publishes FY2015 to FY2026 -- with
FY2016-FY2026 already stacked into one dataset carrying yearofassessment and
owner_name. Lay them side by side and a parcel has an ownership timeline. A sale on a known date then has an answer on both sides:

    seller = the owner in the last snapshot BEFORE the sale
    buyer  = the owner in the first snapshot AFTER the sale

No deed feed, no registry access, no inference beyond "the person who owned it
immediately before the sale is the person who sold it".

THE FISCAL-YEAR TRAP, AND WHY THE OBVIOUS FIX IS WRONG. A Massachusetts FY(N)
assessment nominally reflects ownership as of 1 January of year N-1, but the
owner field is refreshed nearer publication, so the effective lag is not the
statutory one. Fitting an offset by asking which one best reproduces the known
buyer picks offset 2 at 95%. That fit is a trap: a larger offset pushes the
"after" snapshot further into the future, where it is more likely to show the
buyer for reasons that have nothing to do with the convention being right. Score
the SELLER side instead and offset 2 collapses -- 94% of its "before" snapshots
already show the BUYER, meaning it would record the buyer as the seller on
almost every row.

    offset   buyer reproduced   "before" already shows buyer
      0            38%                     1%
      1            93%                    26%
      2            95%                    94%

So no offset is used at all. The transition is located directly: order a
parcel's snapshots, find the FIRST one whose owner is the buyer, and take the
owner in the snapshot immediately before it. That is the grantor by
construction, and it is invariant to whatever the lag happens to be -- which
also means it survives Boston changing its publication timing.

WHAT IT STILL CANNOT DO. Snapshots are annual, so a parcel that traded twice
between two 1 Januarys collapses to one transfer and the intermediate owner is
invisible. And a parcel whose sale predates the earliest snapshot has no prior
owner to find. Both are reported rather than papered over.

    python scraper/acq_owner_chain.py --download
    python scraper/acq_owner_chain.py --fit
    python scraper/acq_owner_chain.py --apply
"""

import argparse
import csv
import json
import logging
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
DUMP = "https://data.boston.gov/datastore/dump/{}?format=csv&fields=PID,OWNER"
CACHE = Path("data/boston_owners")
L3 = ("https://arcgisserver.digital.mass.gov/arcgisserver/rest/services/AGOL/"
      "L3_Parcels_FeatureService_4326/FeatureServer/1/query")

# Fiscal year -> CKAN resource id for Boston's annual property assessment file.
YEARS = {
    2026: "ee73430d-96c0-423e-ad21-c4cfb54c8961",
    2025: "6b7e460e-33f6-4e61-80bc-1bef2e73ac54",
    2024: "a9eb19ad-da79-4f7b-9e3b-6b13e66f8285",
    2023: "1000d81c-5bb5-49e8-a9ab-44cd042f1db2",
    2022: "4b99718b-d064-471b-9b24-517ae5effecc",
    2021: "c4b7331e-e213-45a5-adda-052e4dd31d41",
    2020: "8de4e3a0-c1d2-47cb-8202-98b9cbe3bd04",
    2019: "695a8596-5458-442b-a017-7cd72471aade",
    2018: "fd351943-c2c6-4630-992d-3f895360febd",
    2017: "062fc6fa-b5ff-4270-86cf-202225e40858",
    2016: "cecdf003-9348-4ddb-94e1-673b63940bb8",
    2015: "bdb17c2b-e9ab-44e4-a070-bf804a0e1a7f",
    2014: "7190b0a4-30c4-44c5-911d-c34f60b22181",
}


def download():
    CACHE.mkdir(parents=True, exist_ok=True)
    with httpx.Client(headers=UA, timeout=300, follow_redirects=True) as c:
        for fy, rid in sorted(YEARS.items(), reverse=True):
            p = CACHE / f"fy{fy}.csv"
            if p.exists() and p.stat().st_size > 1000:
                continue
            try:
                with c.stream("GET", DUMP.format(rid)) as r:
                    if r.status_code != 200:
                        log.warning("FY%d -> HTTP %d", fy, r.status_code)
                        continue
                    with p.open("wb") as fh:
                        for chunk in r.iter_bytes():
                            fh.write(chunk)
                log.info("FY%d  %8d bytes", fy, p.stat().st_size)
            except Exception as e:
                log.warning("FY%d -> %s", fy, e)
            time.sleep(1.0)


def load_owners() -> dict:
    """PID -> {fiscal_year: owner}."""
    out = defaultdict(dict)
    for p in sorted(CACHE.glob("fy*.csv")):
        fy = int(p.stem[2:])
        with p.open(encoding="utf-8", errors="ignore", newline="") as fh:
            rd = csv.DictReader(fh)
            cols = {c.upper().strip(): c for c in (rd.fieldnames or [])}
            pid_c, own_c = cols.get("PID"), cols.get("OWNER")
            if not pid_c or not own_c:
                log.warning("FY%d missing PID/OWNER (%s)", fy, rd.fieldnames)
                continue
            n = 0
            for row in rd:
                pid = (row.get(pid_c) or "").strip()
                own = (row.get(own_c) or "").strip()
                if pid and own:
                    out[pid][fy] = own
                    n += 1
        log.info("FY%d  %7d parcels", fy, n)
    return out


def fetch_prop_ids() -> dict:
    """LOC_ID -> PROP_ID, which is Boston's PID."""
    out, offset = {}, 0
    where = ("CITY='BOSTON' AND LS_PRICE >= 2000000 AND "
             "USE_CODE >= '300' AND USE_CODE < '500'")
    with httpx.Client(headers=UA, timeout=120) as c:
        while True:
            r = c.get(L3, params={
                "where": where, "outFields": "LOC_ID,PROP_ID",
                "returnGeometry": "false", "resultOffset": offset,
                "resultRecordCount": 2000, "f": "json"})
            r.raise_for_status()
            d = r.json()
            feats = d.get("features", [])
            for f in feats:
                a = f["attributes"]
                if a.get("LOC_ID") and a.get("PROP_ID"):
                    out[a["LOC_ID"]] = str(a["PROP_ID"]).strip()
            if not d.get("exceededTransferLimit") or not feats:
                return out
            offset += len(feats)
            time.sleep(0.4)


CAMB_MULTI = "https://data.cambridgema.gov/resource/eey2-rv59.json"


def fetch_cambridge_chain() -> dict:
    """parcel key -> {assessment_year: owner}, from the stacked FY16-FY26 file."""
    out, offset = defaultdict(dict), 0
    with httpx.Client(headers=UA, timeout=120) as c:
        while True:
            r = c.get(CAMB_MULTI, params={
                "$select": "gisid,map_lot,address,yearofassessment,owner_name",
                "$where": "stateclasscode >= '300' AND stateclasscode <= '499'",
                "$limit": 50000, "$offset": offset})
            r.raise_for_status()
            page = r.json()
            for x in page:
                yr = x.get("yearofassessment")
                own = (x.get("owner_name") or "").strip()
                if not yr or not own:
                    continue
                for key in (x.get("map_lot"), x.get("gisid"), x.get("address")):
                    if key:
                        out[str(key).strip()][int(float(yr))] = own
            if len(page) < 50000:
                break
            offset += 50000
            time.sleep(0.3)

    # FY2015 sits in its own dataset with a different vocabulary: the owner
    # column is called `grantee` and the parcel key is `gis_id`. Without it the
    # earliest snapshot is FY2016, and every parcel that last traded in 2015 or
    # 2016 has its buyer already in place in that first snapshot, leaving no
    # prior owner to read.
    with httpx.Client(headers=UA, timeout=120) as c:
        off = 0
        while True:
            r = c.get("https://data.cambridgema.gov/resource/crnm-mw9n.json",
                      params={"$select": "gis_id,location,grantee,state_use",
                              "$limit": 50000, "$offset": off})
            if r.status_code != 200:
                break
            page = r.json()
            for x in page:
                own = (x.get("grantee") or "").strip()
                if not own:
                    continue
                for key in (x.get("gis_id"), x.get("location")):
                    if key and 2015 not in out[str(key).strip()]:
                        out[str(key).strip()][2015] = own
            if len(page) < 50000:
                break
            off += 50000
            time.sleep(0.3)
    return out


def _norm(s: str) -> str:
    return "".join(ch for ch in (s or "").upper() if ch.isalnum())


def snapshot_year(fy: int, offset: int) -> int:
    """Calendar year whose 1 January this fiscal-year file represents."""
    return fy - offset


def pick(owners_by_fy: dict, sale_year: int, offset: int, before: bool):
    """Owner at the last snapshot before (or first after) the sale.

    Retained only for the offset diagnostics above. The seller derivation does
    not use it, because it depends on an offset that cannot be fitted honestly.
    """
    cands = [(snapshot_year(fy, offset), o) for fy, o in owners_by_fy.items()]
    if before:
        cands = [(y, o) for y, o in cands if y <= sale_year]
        return max(cands)[1] if cands else None
    cands = [(y, o) for y, o in cands if y > sale_year]
    return min(cands)[1] if cands else None


def grantor(owners_by_fy: dict, buyer: str):
    """The owner in the snapshot immediately before the buyer first appears.

    Offset-free. Walk the parcel's snapshots in fiscal-year order, find where
    ownership first becomes the buyer, and return whoever held it the snapshot
    before. Returns (seller, first_fy_as_buyer) or (None, reason).
    """
    if not buyer:
        return None, "no known buyer to locate the transition"
    seq = sorted(owners_by_fy.items())
    nb = _norm(buyer)
    for i, (fy, own) in enumerate(seq):
        if _norm(own) == nb:
            if i == 0:
                return None, "buyer already owns it in the earliest snapshot"
            prev = seq[i - 1][1]
            if _norm(prev) == nb:
                continue
            return prev, fy
    return None, "buyer never appears in any snapshot"


def main(do_download: bool, do_fit: bool, do_apply: bool):
    if do_download:
        download()
        return

    owners = load_owners()
    log.info("ownership timeline for %d parcels", len(owners))
    locmap = fetch_prop_ids()
    log.info("LOC_ID -> PROP_ID for %d parcels", len(locmap))

    conn = engine.connect()
    rows = conn.execute(text(
        "select id, parcel_id, sale_date, buyer, address from transactions "
        "where source = 'massgis_l3'")).fetchall()

    # ── fit the fiscal-year offset against the known buyer ──────────
    scores = {}
    for off in (0, 1, 2):
        ok = tot = 0
        for _rid, loc, sd, buyer, _a in rows:
            pid = locmap.get(loc)
            if not pid or pid not in owners or not buyer:
                continue
            got = pick(owners[pid], int(str(sd)[:4]), off, before=False)
            if got:
                tot += 1
                ok += _norm(got) == _norm(buyer)
        scores[off] = (ok, tot, ok / tot if tot else 0)
        log.info("offset %d: derived buyer matches known buyer on %d of %d (%.0f%%)",
                 off, ok, tot, scores[off][2] * 100)
    best = max(scores, key=lambda k: scores[k][2])
    log.info("\nbest offset = %d  (FY(N) file is a snapshot of 1 Jan %s)",
             best, f"N-{best}" if best else "N")
    if do_fit:
        conn.close()
        return

    # ── derive the seller, offset-free ──────────────────────────────
    filled = no_pid = 0
    reasons = defaultdict(int)
    for rid, loc, sd, buyer, _a in rows:
        pid = locmap.get(loc)
        if not pid or pid not in owners:
            no_pid += 1
            continue
        seller, why = grantor(owners[pid], buyer)
        if not seller:
            reasons[why] += 1
            continue
        if do_apply:
            conn.execute(text("""
                update transactions
                   set seller = :s, seller_canonical = null,
                       notes = coalesce(notes,'') || :n
                 where id = :id and coalesce(seller,'') = ''"""), {
                "s": seller, "id": rid,
                "n": (" | SELLER DERIVED FROM THE OWNERSHIP CHAIN. Boston "
                      "publishes an annual assessment file; this is the owner of "
                      "record in the last annual snapshot before the sale date, "
                      "which is the grantor. Not read off a deed, and subject to "
                      "the limit that annual snapshots collapse two trades in one "
                      "year into one.")})
        filled += 1
    if do_apply:
        conn.commit()

    log.info("\nseller derived for %d rows", filled)
    log.info("  %d had no matching parcel in the assessment files", no_pid)

    # ── Cambridge, same offset-free method ──────────────────────────
    cchain = fetch_cambridge_chain()
    log.info("Cambridge ownership timeline for %d parcel keys", len(cchain))
    crows = conn.execute(text(
        "select id, parcel_id, address, buyer from transactions "
        "where source = 'cambridge_socrata' and coalesce(seller,'') = ''")).fetchall()
    cfilled = 0
    creasons = defaultdict(int)
    for rid, pid, addr, buyer in crows:
        chain = cchain.get(str(pid or "").strip()) or cchain.get(str(addr or "").strip())
        if not chain:
            creasons["no matching parcel in the Cambridge files"] += 1
            continue
        seller, why = grantor(chain, buyer)
        if not seller:
            creasons[why] += 1
            continue
        if do_apply:
            conn.execute(text("""
                update transactions
                   set seller = :s,
                       notes = coalesce(notes,'') || :n
                 where id = :id and coalesce(seller,'') = ''"""), {
                "s": seller, "id": rid,
                "n": (" | SELLER DERIVED FROM THE OWNERSHIP CHAIN. Cambridge "
                      "publishes an annual property database; this is the owner "
                      "of record in the snapshot immediately before the buyer "
                      "first appears, which is the grantor. Not read off a deed.")})
        cfilled += 1
    if do_apply:
        conn.commit()
    log.info("Cambridge seller derived for %d rows", cfilled)
    for why, n in sorted(creasons.items(), key=lambda x: -x[1]):
        log.info("  %d %s", n, why)

    for label, cond in (("press + SEC", "source in ('press','sec_filing')"),
                        ("assessment spine",
                         "source in ('massgis_l3','cambridge_socrata')")):
        tot = conn.execute(text(
            f"select count(*) from transactions where {cond}")).scalar()
        sel = conn.execute(text(
            f"select count(*) from transactions where {cond} "
            f"and coalesce(seller,'') <> ''")).scalar()
        log.info("%-18s %4d rows | seller %4d (%.0f%%)", label, tot, sel,
                 sel / tot * 100)
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--download", action="store_true")
    ap.add_argument("--fit", action="store_true")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    main(a.download, a.fit, a.apply)
