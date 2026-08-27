r"""Typo-tolerant sweep for a company selling to itself, and the rule it produced.

The earlier affiliated-party sweep compared entity names EXACTLY after
normalisation. That misses a transposition. This one normalises and then
compares with a similarity ratio, and the very first thing it found was

    buyer   GWL DIRECT 1 BULFINCH PLACE LLC
    seller  GWL DIRECT 1 BULFINCH PALCE LLC

-- PLACE and PALCE, one transposed letter, $39,000,000. No exact-match rule
would ever have caught that.

Nine transactions came back. THE INTERESTING PART IS THAT SEVEN OF THEM ARE NOT
AFFILIATED TRANSFERS, and two of those seven are proven not to be by press this
project already gathered:

    23 White St    TA PORTER SQUARE LLC <- PORTER SQUARE LLC        $112,476,000
                   Cambridge Day and The Real Reporter: TA Realty bought Porter
                   Square Shopping Center from GRAVESTAR, one of nine sites in a
                   $390M portfolio. Two unrelated firms, each of which named its
                   vehicle after the shopping centre.
    165 Second St  AMERICAN TWINE OWNER LLC <- AMERICAN TWINE LP    $ 87,000,000
                   Connect CRE and Bisnow: New England Development bought the
                   American Twine Office Park from TRANSATLANTIC INVESTMENT
                   MANAGEMENT. Identical after normalisation, completely
                   unrelated parties.

That is ground truth, not judgement, and it is why the standing instruction to
reject a shared name stem as a quarantine signal was right. But it is not the
whole story, because GWL DIRECT and CREFIII are not shared stems in the same
sense. So this pass proposes a sharper rule:

    QUARANTINE when the shared element is a SPONSOR CODE -- an arbitrary prefix
    that identifies a firm or a fund and appears on BOTH sides.
    DO NOT QUARANTINE when the shared element is an ADDRESS OR PLACE NAME, no
    matter how identical, because vehicles are named after buildings and both
    parties to an ordinary sale name theirs after the same building.

Applying it:

    QUARANTINE  1177  GWL DIRECT 1 BULFINCH PLACE <- GWL DIRECT 1 BULFINCH PALCE
                      "GWL DIRECT" is a sponsor code, identical on both sides,
                      and the rest differs only by a typo. $39,000,000.
    QUARANTINE  1431  CREFIII RIVERWOOD OWNER <- CREFIII FIN RIVERWOOD OWNER
                      "CREFIII" is a fund code, identical on both sides; the only
                      difference is the word FIN, which reads as a financing
                      vehicle conveying to the owning vehicle of the same fund.
                      $16,000,000.
    KEEP        1589, 1660  proven arm's-length by press, above.
    KEEP        1684, 929, 1655, 1644, 1110  address or place collisions, flagged
                      on the row but not quarantined.

1110 IS THE CLOSE CALL AND IT IS DELIBERATELY KEPT. P-12 PROPERTY LLC bought
408 Newbury Street from P-12 LLC for $30,504,385. "P-12" looks like a sponsor
code, which would quarantine it -- but it also reads as a PARCEL designation,
and Boston development parcels are numbered exactly like that. If it is a parcel
number then it is an address collision and quarantining it would delete a real
$30M transaction. Flagged, not removed, with the reasoning on the row.

    python scraper/acq_near_identical_sweep.py --apply
"""

import argparse
import difflib
import logging
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from db.database import engine

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

SUFFIX = (r"\b(LLC|LLP|LP|INC|CORP|CORPORATION|CO|COMPANY|LTD|TRUST|TR|TRS|"
          r"TRUSTEE|TRUSTEES|REALTY|THE|OWNER|HOLDING|HOLDINGS|PROPERTY|"
          r"PROPERTIES|ASSOCIATES|LIMITED|PARTNERSHIP)\b")

QUARANTINE = {
    1177: ("QUARANTINED AS AN AFFILIATED-PARTY TRANSFER. The buyer of record is "
           "GWL DIRECT 1 BULFINCH PLACE LLC and the seller of record is GWL "
           "DIRECT 1 BULFINCH PALCE LLC -- the SAME NAME with PLACE transposed to "
           "PALCE, $39,000,000. GWL DIRECT is a sponsor code, not an address, and "
           "it is identical on both sides; the asset name is identical too. One "
           "sponsor conveying between its own vehicles is a restructuring, not an "
           "acquisition. Found only because this sweep compares entity names with "
           "a similarity ratio rather than exactly: the earlier exact-match sweep "
           "read PLACE and PALCE as different strings and passed it through."),
    1431: ("QUARANTINED AS AN AFFILIATED-PARTY TRANSFER. The buyer of record is "
           "CREFIII RIVERWOOD OWNER LLC and the seller of record is CREFIII FIN "
           "RIVERWOOD OWNER LLC, $16,000,000. CREFIII is a fund code, identical "
           "on both sides, and the asset name is identical; the only difference "
           "is the word FIN, which reads as a financing vehicle conveying to the "
           "owning vehicle of the same fund. That is a restructuring within one "
           "sponsor, not a sale."),
}

FLAG_KEEP = {
    1110: ("NEAR-IDENTICAL ENTITY ON BOTH SIDES, DELIBERATELY NOT QUARANTINED. "
           "P-12 PROPERTY LLC bought 408 Newbury Street from P-12 LLC for "
           "$30,504,385 -- identical after normalisation. This is the closest "
           "call in the sweep. If P-12 is a SPONSOR CODE then this is one firm "
           "conveying to itself and belongs in quarantine. But P-12 also reads as "
           "a PARCEL designation, and Boston development parcels are numbered "
           "exactly that way, in which case it is an address collision and two "
           "unrelated parties simply named their vehicles after the same parcel. "
           "Quarantining it on the wrong reading would delete a real $30 million "
           "transaction, and the two press-proven cases in this sweep -- Porter "
           "Square and American Twine -- show how often identical names are "
           "innocent. Flagged for a licensed feed to settle."),
    1684: ("NEAR-IDENTICAL ENTITY ON BOTH SIDES, NOT QUARANTINED. AP BRATTLE "
           "SQUARE LP bought 8 Brattle Street from BRATTLE SQUARE PROPERTIES LLC. "
           "The shared element is BRATTLE SQUARE, which is a PLACE in Harvard "
           "Square, not a sponsor code; the buyer carries its own distinct AP "
           "prefix. Two parties naming vehicles after the same square is exactly "
           "the pattern proven innocent at Porter Square, where press confirms TA "
           "Realty bought from the unrelated Gravestar."),
    929: ("NEAR-IDENTICAL ENTITY ON BOTH SIDES, NOT QUARANTINED. CP LOWER MILLS "
          "LLC bought 18-20 Richmond Street from LOWER MILLS REALTY TRUST. Lower "
          "Mills is a Dorchester NEIGHBOURHOOD, not a sponsor code, and the buyer "
          "carries its own CP prefix."),
    1655: ("NEAR-IDENTICAL ENTITY ON BOTH SIDES, NOT QUARANTINED. 57 SMITH PLACE "
           "LLC bought 57 Smith Place from SMITH PLACE LLC. The shared element is "
           "the STREET, and the buyer's vehicle is simply the street plus the "
           "house number. Smith Place in West Cambridge is also where Healthpeak "
           "assembled its Alewife position, so multiple unrelated parties "
           "demonstrably held vehicles named for it."),
    1644: ("NEAR-IDENTICAL ENTITY ON BOTH SIDES, NOT QUARANTINED, BUT WEAKER THAN "
           "THE OTHERS. 512 MASS AVE LLC bought 512 Massachusetts Avenue from 512 "
           "MASS AVE PROPERTIES CORP. Both are the bare address with nothing else "
           "to tell them apart, so unlike Brattle Square or Lower Mills there is "
           "no distinguishing prefix on the buyer -- and a CORP conveying to an "
           "LLC at the same address can be a simple re-domiciling. It is kept "
           "because the shared element is still an address and the burden of "
           "proof for deleting a transaction sits with the sweep, not the row."),
    1589: ("NEAR-IDENTICAL ENTITY ON BOTH SIDES, AND PROVEN ARM'S-LENGTH. TA "
           "PORTER SQUARE LLC bought from PORTER SQUARE LLC, which any name-based "
           "affiliation rule would flag. It is not affiliated: Cambridge Day and "
           "The Real Reporter both report TA Realty buying Porter Square Shopping "
           "Center from GRAVESTAR, one of nine sites in a $390 million portfolio. "
           "This row is the ground truth for why a shared name stem is rejected "
           "as a quarantine signal."),
    1660: ("NEAR-IDENTICAL ENTITY ON BOTH SIDES, AND PROVEN ARM'S-LENGTH. AMERICAN "
           "TWINE OWNER LLC bought from AMERICAN TWINE LIMITED PARTNERSHIP -- "
           "identical after normalisation. It is not affiliated: Connect CRE and "
           "Bisnow both report New England Development buying the American Twine "
           "Office Park from TRANSATLANTIC INVESTMENT MANAGEMENT for $87 million, "
           "deed registered 3 June 2019. Second ground-truth negative in this "
           "sweep."),
}


def norm(t):
    t = re.sub(r"[^A-Z0-9 ]", " ", (t or "").upper())
    t = re.sub(SUFFIX, " ", t)
    return re.sub(r"\s+", "", t)


def main(dry_run: bool):
    conn = engine.connect()
    rows = conn.execute(text("""
        select id, buyer, seller, price, address, substr(sale_date,1,10)
          from transactions
         where coalesce(quarantined,0) = 0
           and coalesce(buyer,'') <> '' and coalesce(seller,'') <> ''""")).fetchall()

    found = []
    for rid, b, s, price, addr, dt in rows:
        nb, ns = norm(b), norm(s)
        if not nb or not ns:
            continue
        if nb == ns:
            found.append((rid, price, addr, dt, b, s, 1.0))
        else:
            r = difflib.SequenceMatcher(None, nb, ns).ratio()
            if r >= 0.90 and abs(len(nb) - len(ns)) <= 3:
                found.append((rid, price, addr, dt, b, s, r))
    found.sort(key=lambda x: -x[1])

    log.info("%d transactions where buyer and seller are the same or near-same "
             "entity\n", len(found))
    qn = kn = 0
    for rid, price, addr, dt, b, s, r in found:
        verdict = ("QUARANTINE" if rid in QUARANTINE else
                   "keep" if rid in FLAG_KEEP else "UNCLASSIFIED")
        log.info("id=%-5s %s $%-13s %-26s %3.0f%%  %s", rid, dt, f"{price:,}",
                 addr[:26], r * 100, verdict)
        log.info("      B: %s", b)
        log.info("      S: %s", s)
        if dry_run:
            continue
        if rid in QUARANTINE:
            conn.execute(text("""
                update transactions
                   set quarantined = 1,
                       quarantine_reason = 'affiliated_party_transfer:same_sponsor_code',
                       arms_length = 0,
                       non_arms_length_reason = coalesce(non_arms_length_reason,
                                                         'affiliated_parties'),
                       notes = coalesce(notes,'') || :n
                 where id = :id"""), {"n": " | " + QUARANTINE[rid], "id": rid})
            qn += 1
        elif rid in FLAG_KEEP:
            conn.execute(text(
                "update transactions set notes = coalesce(notes,'') || :n "
                "where id = :id"), {"n": " | " + FLAG_KEEP[rid], "id": rid})
            kn += 1

    if not dry_run:
        conn.commit()
    log.info("\n%d quarantined, %d flagged and kept", qn, kn)

    tot, vol = conn.execute(text(
        "select count(*), sum(coalesce(price,0)) from transactions "
        "where coalesce(quarantined,0)=0")).first()
    q, qv = conn.execute(text(
        "select count(*), sum(coalesce(price,0)) from transactions "
        "where coalesce(quarantined,0)=1")).first()
    log.info("table: %d transactions, $%.2fB", tot, (vol or 0) / 1e9)
    log.info("quarantined: %d rows, $%s", q, f"{int(qv or 0):,}")
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
