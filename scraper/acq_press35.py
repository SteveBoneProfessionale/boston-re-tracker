r"""Thirty-fifth pass. A third refusal vindicated, this time about the ASSET.

25-27 LAND BOULEVARD WAS NOT THE ROYAL SONESTA. An earlier pass refused it in
these words: "The entities are CAMBRIDGE LLC and CAMBRIDGE HOTEL LLC, which
point at the Royal Sonesta. The Royal Sonesta is at 40 EDWIN LAND BOULEVARD, a
different address on the same road, and nothing found reports it selling in
2018. Two entities that say nothing but 'Cambridge' plus a nearby famous hotel
is not evidence."

Correct, and correct about which building too. The asset is the HOTEL MARLOWE:
BLDUP, "Hotel Marlowe in East Cambridge sold to Hong Kong investor for $81.75
million" -- this row to the dollar, February 2018. JUNSON CAPITAL of Hong Kong
bought the 236-key hotel from BARINGS, the MassMutual subsidiary, inside an $800
million portfolio acquisition of US hotels. Kimpton managed it.

    Had the Royal Sonesta guess been written, the row would now name the wrong
    hotel, the wrong buyer and the wrong seller. That is the third refusal in
    this project vindicated by later evidence, after KHP/THI VI at 90 Tremont
    and CCF at Smith Place -- and the first where the refusal protected the
    ASSET IDENTITY rather than just the party.

BARINGS NOW SELLS TWO BOSTON-AREA HOTELS IN THIS TABLE, eighteen months apart:
the Hotel Marlowe to Junson in February 2018 and the Copley Square Hotel to
Hawkins Way in November 2019. A disposition programme, visible only once both
sellers resolve.

211 CONGRESS STREET, IDENTIFIED THROUGH ITS SELLER ENTITY. The seller of record
is 211-10 CONGRESS OWNER LLC, and multiple outlets state that Drucker purchased
211 Congress Street from WESTBROOK in 2018 -- an 80,000-90,000 SF Financial
District office building on which Drucker then ran a $5 million capital
improvement programme. This row is addressed 209-217 Congress Street in June
2018, which brackets 211.

    NO SOURCE PUBLISHES THE PRICE, so the $55,000,000 here is corroborated by
    nothing but the registry. The parties are written because the deal is
    identified by address, year and seller entity together; the price is not
    treated as confirmed.

    python scraper/acq_press35.py --apply
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from db.database import engine

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

RESOLVE = [
    (1681, "buyer", "Junson Capital",
     'REVERSES AN EARLIER REFUSAL, WHICH WAS RIGHT ABOUT THE BUILDING AS WELL AS '
     'THE PARTY. BLDUP, "Hotel Marlowe in East Cambridge sold to Hong Kong '
     'investor for $81.75 million" -- this row to the dollar, February 2018. '
     'Junson Capital of Hong Kong acquired the 236-key Hotel Marlowe, managed by '
     'IHG\'s Kimpton, as part of an $800 million portfolio acquisition of US '
     'hotels. THE PREVIOUS NOTE ON THIS ROW declined to read CAMBRIDGE LLC and '
     'CAMBRIDGE HOTEL LLC as the Royal Sonesta, on the ground that the Sonesta is '
     'at 40 Edwin Land Boulevard and nothing reported it selling in 2018. Had '
     'that guess been written, this row would now carry the wrong hotel, the '
     'wrong buyer and the wrong seller.'),
    (1681, "seller", "Barings (MassMutual)",
     'Same reporting: the seller was BARINGS, the MassMutual subsidiary, disposing '
     'of a US hotel portfolio reported at about $800 million. PORTFOLIO '
     'ALLOCATION: $81,750,000 is the Marlowe\'s share, not the deal. Barings now '
     'sells two Boston-area hotels in this table eighteen months apart -- the '
     'Marlowe to Junson in February 2018 and the Copley Square Hotel to Hawkins '
     'Way Capital in November 2019 -- which reads as a programme rather than two '
     'unrelated trades.'),

    (1298, "buyer", "Drucker Associates",
     'IDENTIFIED THROUGH THE SELLER ENTITY. The seller of record is 211-10 '
     'CONGRESS OWNER LLC, and NEREJ, Banker & Tradesman ("Hot Property: 211 '
     'Congress St."), Connect CRE and REBusinessOnline all state that DRUCKER '
     'PURCHASED THE ASSET FROM WESTBROOK IN 2018 -- an 80,000-90,000 SF Financial '
     'District office building on which Drucker then ran a $5 million capital '
     'improvement programme, with Newmark completing more than 30,000 SF of '
     'leasing across seven tenants. This row is addressed 209-217 Congress Street '
     'in June 2018, a range that brackets 211. PRICE CAVEAT: no source publishes '
     'a price for this deal, so the $55,000,000 recorded here rests on the '
     'registry alone. The PARTIES are established by address, year and seller '
     'entity together; the price is not corroborated.'),
    (1298, "seller", "Westbrook Partners",
     'Same reporting names Westbrook as the party Drucker bought from, and the '
     'seller entity 211-10 CONGRESS OWNER LLC is the address.'),

    (1092, "seller", "Centerbridge Partners",
     'Bisnow, "Alexandria Pays $168M To Acquire South Boston Development Sites", '
     'and the Boston Globe\'s later coverage: the storage facility at 380 E Street '
     'was owned by CONROY DEVELOPMENT GROUP, and investment firm CENTERBRIDGE '
     'PARTNERS purchased it and packaged the deal for Alexandria, which closed in '
     'November 2020. Centerbridge is written as the seller because the reporting '
     'has it buying and then assembling the transaction Alexandria completed. '
     'AMBIGUITY RECORDED: the phrasing leaves room for Conroy having conveyed '
     'directly to Alexandria with Centerbridge acting only as assembler, and the '
     'record entity 920 STORAGE LLC matches neither name. The asset traded on as '
     'Boston Seaport Self Storage, and Alexandria sold both E Street properties '
     'in December 2023 for about $87 million after abandoning the lab scheme.'),
]


def main(dry_run: bool):
    conn = engine.connect()
    n = 0
    for rid, side, sponsor, why in RESOLVE:
        cur = conn.execute(text(
            f"select {side}, coalesce({side}_canonical,'') from transactions "
            f"where id = :id"), {"id": rid}).first()
        if not cur:
            log.warning("id %s missing", rid)
            continue
        if cur[1]:
            log.info("id=%-5s %-6s already %s, skipped", rid, side, cur[1])
            continue
        log.info("id=%-5s %-6s %-34s -> %s", rid, side, (cur[0] or "")[:34], sponsor)
        if not dry_run:
            conn.execute(text(f"""
                update transactions
                   set {side}_canonical = :s, {side}_confidence = 'web_corroborated',
                       {side}_resolution_basis = 'web',
                       notes = coalesce(notes,'') || :n
                 where id = :id"""), {
                "s": sponsor, "id": rid,
                "n": f" | {side.upper()} RESOLVED. " + why})
            n += 1

    if not dry_run:
        conn.execute(text(
            "update transactions "
            "set price_caveat = coalesce(price_caveat || ' ', '') || :c "
            "where id = 1298"), {
            "c": ("Parties are established from press; the price is not. No "
                  "source publishes a figure for Drucker's 2018 purchase of 211 "
                  "Congress Street, so $55,000,000 rests on the registry alone.")})
        conn.commit()

    log.info("\n%d sides written", n)
    tot = conn.execute(text(
        "select count(*) from transactions where coalesce(quarantined,0)=0")).scalar()
    for side in ("buyer", "seller"):
        v = conn.execute(text(
            f"select count(*) from transactions where coalesce(quarantined,0)=0 "
            f"and coalesce({side}_canonical,'') <> ''")).scalar()
        log.info("%s_canonical: %d of %d (%.0f%%)", side, v, tot, v / tot * 100)
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
