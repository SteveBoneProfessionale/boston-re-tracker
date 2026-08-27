r"""Quarantine affiliated-party transfers: a company conveying to itself.

An intra-sponsor conveyance is not an acquisition. It moves an asset between
vehicles of one owner, usually for financing or restructuring, and counting it
inflates both the volume and the buyer rankings. LA3 exposed the pattern on
twelve rows worth $2.7B; this sweeps the whole table on six signals.

SIGNALS, any one of which is sufficient:

  same_sponsor         both sides resolve to the same firm
  same_mailing         both entities mail to the same address
  same_care_of         both entities mail care of the same firm
  nal_code             the source's own non-arm's-length code, conclusive on
                       its own, the way LA3 code 'U' was on the Alexandria row

TWO SIGNALS WERE TRIED AND REJECTED, and the reasons matter more than the rule.

SHARED NAME STEM DOES NOT WORK. Single-purpose vehicles are named after the
ASSET, so on any given deal the two sides routinely share a stem without being
related. It flagged 63 rows and the sample was overwhelmingly false:

    GAZIT HORIZONS (MARKETPLACE) LLC  <- MARKETPLACE CENTER ASSOC LLC
    DAVENPORT OWNER (DE) LLC          <- JAMESTOWN PREMIER DAVENPORT LLC
    T-C FORT POINT CREATIVE           <- AG/ND FORT POINT LLC

Those share a building or a neighbourhood, not an owner. Requiring the stem to
appear at three or more distinct properties rejects Marketplace and Davenport
but still admits FORT and POINT, which appear at eight and ten. And the one true
case it caught, ARE-MA, is already caught by same_sponsor. The signal adds
nothing and imports errors, so it is not used.

IDENTICAL ENTITY NAMES TURNED OUT TO BE A BUG IN MY OWN CODE, not affiliation.
All 32 such rows have a seller derived from the ownership chain, and in every
one the "seller" is the buyer's name minus its legal suffix:

    B  CSHV 50 POST OFFICE SQUARE  LLC     S  CSHV 50 POST OFFICE SQUARE
    B  ICONIC COPLEY PLAZA HOTEL  LLC      S  ICONIC COPLEY PLAZA HOTEL

The assessment roll is inconsistent about suffixes between years, and the
chain's comparison did not strip them, so a formatting change read as a change
of ownership. Those rows record a REAL sale with a SPURIOUS grantor. The seller
is nulled and the transaction stays live -- quarantining them would delete
genuine transactions, including a $285M trade, to hide a normalisation bug.
The root cause is fixed in acq_owner_chain._norm.

QUARANTINE, NOT DELETE. Rows are flagged and drop out of counts, volumes and
rankings, but stay in the table with the reason recorded and remain visible in
review -- the same treatment every other exclusion in this project gets.

    python scraper/acq_affiliated_sweep.py            # report
    python scraper/acq_affiliated_sweep.py --apply
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

LEGAL = re.compile(
    r"\b(LLC|L\.?L\.?C|INC|CORP|CORPORATION|LP|L\.?P|LLP|LTD|CO|COMPANY|TRUST|"
    r"TR|TRS|TRUSTEE|TRUSTEES|REALTY|PROPERTIES|PROPERTY|HOLDINGS|HOLDING|"
    r"OWNER|PROPCO|FEE|DE|MASS|NOMINEE|ASSOCIATES|PARTNERS|LESSEE|LESSOR)\b",
    re.I)
NUMWORD = re.compile(
    r"\b(NO|NUMBER|ONE|TWO|THREE|FOUR|FIVE|SIX|SEVEN|EIGHT|NINE|TEN|ELEVEN|"
    r"TWELVE|HUNDRED|THIRTY|FORTY|FIFTY|SIXTY|I|II|III|IV|V|VI|VII|VIII|IX|X)\b",
    re.I)


def norm_entity(s: str) -> str:
    t = re.sub(r"[^A-Za-z0-9 &-]", " ", (s or "").upper())
    t = LEGAL.sub(" ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def stem_tokens(s: str) -> list:
    """Distinctive tokens: no legal forms, no bare numbers, no number-words."""
    t = norm_entity(s)
    t = NUMWORD.sub(" ", t)
    return [w for w in t.split() if len(w) >= 3 and not w.isdigit()]


def addr_tokens(a: str) -> set:
    t = re.sub(r"[^A-Za-z0-9 ]", " ", (a or "").upper())
    return {w for w in t.split() if len(w) >= 3 and not w.isdigit()}


def fetch_mail() -> tuple:
    """entity -> mailing address, and entity -> care-of name."""
    mail, care = {}, {}
    CO = re.compile(r"\bC\s*[/.]?\s*O\b[:\s]*(.+)$", re.I)

    def norm(s):
        s = re.sub(r"[^A-Z0-9 ]", " ", (s or "").upper())
        return re.sub(r"\s+", " ", s).strip()

    off = 0
    with httpx.Client(headers=UA, timeout=120) as c:
        while True:
            r = c.get(L3, params={
                "where": "CITY='BOSTON' AND USE_CODE >= '300' AND USE_CODE < '500'",
                "outFields": "OWNER1,OWN_ADDR,OWN_CITY", "returnGeometry": "false",
                "resultOffset": off, "resultRecordCount": 2000, "f": "json"})
            r.raise_for_status()
            d = r.json()
            feats = d.get("features", [])
            for f in feats:
                a = f["attributes"]
                own = (a.get("OWNER1") or "").strip().upper()
                raw = str(a.get("OWN_ADDR") or "")
                if not own or not raw:
                    continue
                m = CO.search(raw)
                if m:
                    care[own] = norm(m.group(1))[:40]
                key = norm(raw + " " + str(a.get("OWN_CITY") or ""))
                if any(ch.isdigit() for ch in key):
                    mail[own] = key
            if not d.get("exceededTransferLimit") or not feats:
                break
            off += len(feats)
            time.sleep(0.3)
        off = 0
        while True:
            r = c.get(CAMB, params={
                "$select": "owner_name,owner_address,owner_city",
                "$where": "stateclasscode >= '300' AND stateclasscode <= '499'",
                "$limit": 5000, "$offset": off})
            r.raise_for_status()
            page = r.json()
            for x in page:
                own = (x.get("owner_name") or "").strip().upper()
                raw = str(x.get("owner_address") or "")
                if not own or not raw:
                    continue
                m = CO.search(raw)
                if m:
                    care.setdefault(own, norm(m.group(1))[:40])
                key = norm(raw + " " + str(x.get("owner_city") or ""))
                if any(ch.isdigit() for ch in key):
                    mail.setdefault(own, key)
            if len(page) < 5000:
                break
            off += 5000
            time.sleep(0.3)
    return mail, care


def main(dry_run: bool):
    mail, care = fetch_mail()
    log.info("mailing address for %d entities, care-of for %d", len(mail), len(care))

    conn = engine.connect()
    for col, typ in (("quarantined", "INTEGER"), ("quarantine_reason", "TEXT")):
        try:
            conn.execute(text(f"select {col} from transactions limit 1"))
        except Exception:
            conn.execute(text(f"alter table transactions add column {col} {typ}"))
            conn.commit()

    rows = conn.execute(text("""
        select id, address, sale_date, price, coalesce(buyer,''),
               coalesce(seller,''), coalesce(buyer_canonical,''),
               coalesce(seller_canonical,''), coalesce(non_arms_length_reason,''),
               source
          from transactions
         where coalesce(buyer,'') <> '' and coalesce(seller,'') <> ''""")).fetchall()

    hits, by_signal, by_sponsor = [], defaultdict(int), defaultdict(lambda: [0, 0])
    spurious = []
    for (rid, addr, sd, price, b, s, bc, sc, nal, src) in rows:
        B, S = b.strip().upper(), s.strip().upper()
        sig = None

        if norm_entity(B) and norm_entity(B) == norm_entity(S):
            spurious.append((rid, b, s))
            continue
        if bc and bc == sc:
            sig = "same_sponsor"
        elif re.search(r"NAL code '([A-Z])'", nal or ""):
            code = re.search(r"NAL code '([A-Z])'", nal).group(1)
            # Only codes the source itself marks unusable. A blank or an
            # arm's-length code is not evidence of affiliation.
            if code in ("U",):
                sig = f"nal_code_{code}"
        if not sig and B in mail and S in mail and mail[B] == mail[S]:
            sig = "same_mailing"
        if not sig and B in care and S in care and care[B] == care[S]:
            sig = "same_care_of"
        if sig:
            hits.append((rid, addr, str(sd)[:10], price or 0, b, s, bc or sc, sig))
            by_signal[sig.split(":")[0]] += 1
            who = bc or sc or "(unresolved)"
            by_sponsor[who][0] += 1
            by_sponsor[who][1] += price or 0

    tot_v = sum(h[3] for h in hits)
    log.info("\n%d affiliated-party transfers, $%s", len(hits), f"{tot_v:,}")
    log.info("\nby signal:")
    for k, v in sorted(by_signal.items(), key=lambda x: -x[1]):
        log.info("   %-22s %d", k, v)
    log.info("\nby sponsor:")
    for who, (n, v) in sorted(by_sponsor.items(), key=lambda x: -x[1][1])[:12]:
        log.info("   %-42s %2d  $%s", who[:42], n, f"{v:,}")
    log.info("\nlargest:")
    for rid, addr, sd, price, b, s, who, sig in sorted(hits, key=lambda h: -h[3])[:12]:
        log.info("   $%-14s %s %-26s %s", f"{price:,}", sd, addr[:26], sig)
        log.info("        B %-42s S %s", b[:42], s[:42])

    log.info("")
    log.info("%d rows carry a SPURIOUS chain-derived seller (the buyer's own "
             "name minus its legal suffix); their seller is nulled, not "
             "quarantined", len(spurious))

    if not dry_run:
        for rid, b, s in spurious:
            conn.execute(text("""
                update transactions
                   set seller = null, seller_canonical = null,
                       seller_confidence = null, seller_resolution_basis = null,
                       notes = coalesce(notes,'') || :n
                 where id = :id"""), {
                "id": rid,
                "n": (f" | SPURIOUS SELLER REMOVED. The chain-derived grantor "
                      f"'{s[:50]}' is the buyer '{b[:50]}' minus its legal "
                      f"suffix. The assessment roll is inconsistent about "
                      f"suffixes between annual snapshots and the chain's "
                      f"comparison did not strip them, so a formatting change "
                      f"read as a change of ownership. The SALE is real; the "
                      f"grantor was not. Root cause fixed in "
                      f"acq_owner_chain._norm.")})
        for rid, addr, sd, price, b, s, who, sig in hits:
            conn.execute(text("""
                update transactions
                   set quarantined = 1, quarantine_reason = :r,
                       arms_length = 0,
                       non_arms_length_reason = coalesce(non_arms_length_reason,
                                                         'affiliated_parties'),
                       notes = coalesce(notes,'') || :n
                 where id = :id and coalesce(quarantined,0) = 0"""), {
                "r": f"affiliated_party_transfer:{sig}", "id": rid,
                "n": (f" | QUARANTINED AS AN AFFILIATED-PARTY TRANSFER, signal "
                      f"'{sig}'. Buyer '{b[:60]}' and seller '{s[:60]}' are the "
                      f"same ownership, so this conveyance is a restructuring and "
                      f"not an acquisition. It is excluded from every count, "
                      f"volume and ranking, and kept in the table for review "
                      f"rather than deleted.")})
        conn.commit()
        n = conn.execute(text(
            "select count(*) from transactions where quarantined = 1")).scalar()
        v = conn.execute(text("select sum(coalesce(price,0)) from transactions "
                              "where quarantined = 1")).scalar()
        ln = conn.execute(text("select count(*), sum(coalesce(price,0)) "
                               "from transactions "
                               "where coalesce(quarantined,0) = 0")).first()
        log.info("\nquarantined %d rows, $%s", n, f"{int(v or 0):,}")
        log.info("live table:  %d rows, $%s", ln[0], f"{int(ln[1] or 0):,}")
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
