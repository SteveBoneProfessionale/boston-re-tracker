r"""Backfill the record grantee, and separate Torrens registered land from deeds.

TWO GAPS FOUND BY ASKING HOW MANY ROWS ARE ACTUALLY USABLE, not how many exist.

FIRST, 846 of 885 rows had no party at all. Both spine loaders pulled price,
date, area and book/page and dropped the owner name on the floor -- MassGIS
carries OWNER1 and Cambridge's assessment file carries owner_name, and neither
was being written. The row count looked like coverage; the table could not
answer "who bought it" for 96% of itself.

WHAT THIS DOES AND DOES NOT GIVE YOU. The owner of record as of the assessment
year, on a parcel whose last recorded sale is the one stored, IS the grantee of
that sale. So this is the buyer. But it is the RECORD ENTITY, and in
Massachusetts the record entity is usually a single-purpose LLC or a nominee
trust: "STONEGATE 2 NEWBURY STREET 2022 LLC", "LMDE8 LLC", "YARD 5 MA OWNER
LLC". Those are legally the buyer and practically not an answer -- you cannot
rank sponsors by them, and two entities of the same sponsor look like two
buyers.

So it lands in `buyer`, with `buyer_confidence` = registry_confirmed because it
is what the record says, and `buyer_canonical` left NULL because resolving the
entity to the firm behind it needs the Secretary of the Commonwealth corporate
database and has not been done. The distinction is the whole point of having two
columns: press-sourced rows name the sponsor, spine rows name the entity, and
conflating them would produce buyer rankings that are confidently wrong.

NO SELLER IS AVAILABLE. An assessment file records who owns a parcel now, not
who owned it before. The grantor exists only on the deed itself, so seller stays
null on every spine row -- a licensed feed's most valuable single column.

SECOND, 49 Boston rows are not deeds at all. Recorded land always has a book AND
a page. These have a six-digit number beginning with 9 and NO page, which is the
signature of Torrens REGISTERED LAND: a Certificate of Title with a document
number, held in a separate registry section. That is ~8% of the Boston spine,
and the schema has carried is_registered_land, certificate_number and
document_number since it was written precisely so this could not hide inside
deed_book. This moves them into the right column and sets the flag.

    python scraper/acq_backfill_parties.py --apply
"""

import argparse
import logging
import sys
import time
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from db.database import engine

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

L3 = ("https://arcgisserver.digital.mass.gov/arcgisserver/rest/services/AGOL/"
      "L3_Parcels_FeatureService_4326/FeatureServer/1/query")
CAMB = "https://data.cambridgema.gov/resource/waa7-ibdu.json"
UA = {"User-Agent": "boston-re-tracker/1.0 (13silonergan@gmail.com)"}

ENTITY_NOTE = (
    "BUYER IS THE RECORD ENTITY, NOT THE SPONSOR. Taken from the assessment "
    "roll's owner of record, which on a parcel whose last recorded sale is this "
    "one is the grantee. Massachusetts buyers hold through single-purpose LLCs "
    "and nominee trusts, so this name is legally correct and analytically thin: "
    "buyer_canonical is deliberately left null until the entity is resolved "
    "through the Secretary of the Commonwealth. SELLER IS UNOBTAINABLE from an "
    "assessment file, which records current ownership and not the grantor."
)


def fetch_l3_owners(city: str) -> dict:
    """LOC_ID -> OWNER1 for parcels with a priced sale."""
    out, offset = {}, 0
    where = (f"CITY='{city}' AND LS_PRICE >= 2000000 AND "
             f"USE_CODE >= '300' AND USE_CODE < '500'")
    with httpx.Client(headers=UA, timeout=120) as c:
        while True:
            r = c.get(L3, params={
                "where": where, "outFields": "LOC_ID,OWNER1,LS_DATE,LS_PRICE",
                "returnGeometry": "false", "resultOffset": offset,
                "resultRecordCount": 2000, "f": "json"})
            r.raise_for_status()
            d = r.json()
            feats = d.get("features", [])
            for f in feats:
                a = f["attributes"]
                if a.get("LOC_ID") and a.get("OWNER1"):
                    out[a["LOC_ID"]] = a["OWNER1"].strip()
            if not d.get("exceededTransferLimit") or not feats:
                return out
            offset += len(feats)
            time.sleep(0.4)


def fetch_cambridge_owners() -> dict:
    """(address, saledate) -> owner_name."""
    out, offset = {}, 0
    with httpx.Client(headers=UA, timeout=90) as c:
        while True:
            r = c.get(CAMB, params={
                "$where": ("stateclasscode >= '300' AND stateclasscode <= '499' "
                           "AND saleprice > 0"),
                "$select": "address,saledate,saleprice,owner_name",
                "$limit": 5000, "$offset": offset})
            r.raise_for_status()
            page = r.json()
            for x in page:
                if x.get("address") and x.get("owner_name"):
                    out[(x["address"], (x.get("saledate") or "")[:10])] = \
                        x["owner_name"].strip()
            if len(page) < 5000:
                return out
            offset += 5000
            time.sleep(0.3)


def main(dry_run: bool):
    conn = engine.connect()

    # ── 1. Torrens: a document number is not a book ─────────────────
    tor = conn.execute(text(
        "select id, deed_book from transactions "
        "where source = 'massgis_l3' and length(coalesce(deed_book,'')) = 6 "
        "and coalesce(deed_page,'') = ''")).fetchall()
    log.info("registered land (Torrens): %d rows", len(tor))
    if not dry_run:
        for rid, book in tor:
            conn.execute(text("""
                update transactions
                   set is_registered_land = 1, document_number = :doc,
                       deed_book = null, doc_type = 'Certificate of Title',
                       notes = coalesce(notes,'') || :n
                 where id = :id"""), {
                "doc": book, "id": rid,
                "n": (" | REGISTERED LAND (Torrens). Six-digit document number "
                      "with no page, which recorded land never has. Moved from "
                      "deed_book to document_number so a book/page join against a "
                      "future registry feed does not silently miss it.")})

    # ── 2. Boston grantee from MassGIS OWNER1 ───────────────────────
    owners = fetch_l3_owners("BOSTON")
    log.info("fetched %d Boston owner names", len(owners))
    rows = conn.execute(text(
        "select id, parcel_id from transactions "
        "where source = 'massgis_l3' and coalesce(buyer,'') = ''")).fetchall()
    hit = 0
    for rid, pid in rows:
        name = owners.get(pid)
        if not name:
            continue
        hit += 1
        if not dry_run:
            conn.execute(text("""
                update transactions
                   set buyer = :b, buyer_confidence = 'registry_confirmed',
                       notes = coalesce(notes,'') || :n
                 where id = :id"""),
                {"b": name, "id": rid, "n": " | " + ENTITY_NOTE})
    log.info("Boston: %d of %d rows matched an owner", hit, len(rows))

    # ── 3. Cambridge grantee from the assessment file ───────────────
    cowners = fetch_cambridge_owners()
    log.info("fetched %d Cambridge owner names", len(cowners))
    crows = conn.execute(text(
        "select id, address, sale_date from transactions "
        "where source = 'cambridge_socrata' and coalesce(buyer,'') = ''")).fetchall()
    chit = 0
    for rid, addr, sd in crows:
        name = cowners.get((addr, str(sd)[:10]))
        if not name:
            continue
        chit += 1
        if not dry_run:
            conn.execute(text("""
                update transactions
                   set buyer = :b, buyer_confidence = 'registry_confirmed',
                       notes = coalesce(notes,'') || :n
                 where id = :id"""),
                {"b": name, "id": rid, "n": " | " + ENTITY_NOTE})
    log.info("Cambridge: %d of %d rows matched an owner", chit, len(crows))

    if not dry_run:
        conn.commit()

    tot = conn.execute(text("select count(*) from transactions")).scalar()
    for label, cond in (("with a buyer", "coalesce(buyer,'') <> ''"),
                        ("with a seller", "coalesce(seller,'') <> ''"),
                        ("registered land", "is_registered_land = 1")):
        n = conn.execute(text(
            f"select count(*) from transactions where {cond}")).scalar()
        log.info("%-18s %4d of %d  (%.0f%%)", label, n, tot, n / tot * 100)
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
