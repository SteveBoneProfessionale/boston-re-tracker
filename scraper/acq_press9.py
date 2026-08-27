r"""Ninth pass. Three more prefixes confirmed against named properties.

LS <LOCATION> LLC = HEALTHPEAK PROPERTIES. Healthpeak's own Q3 2021 results and
8-K describe EIGHT separate transactions totalling roughly $625 million and 36
acres of largely contiguous property in the Alewife submarket of West Cambridge.
Two of those eight match rows here on property, price AND month:

    10 Fawcett      132,000 SF on 2.5 acres, $73M, closing October 2021
                    -> id 1616, 591 Concord Ave, $73,000,000, 2021-10,
                       seller of record 10 FAWCETT INVESTORS LLC,
                       buyer of record LS ALEWIFE II LLC
    67 Smith Place  $72M, closing January 2022
                    -> id 1594, 61-67 Smith Pl, $72,000,000, 2022-01,
                       buyer of record LS ALEWIFE VIII LLC

A seller entity that literally reads "10 FAWCETT INVESTORS" against a Healthpeak
disclosure that literally reads "10 Fawcett" is about as tight as this gets. The
roman numerals on LS ALEWIFE II and LS ALEWIFE VIII are the eight-deal campaign
numbering itself. Applied to the other two LS rows.

T-C FORT POINT CREATIVE = TIAA. Already confirmed at 374 Congress Street, where
NEREJ reports an affiliate of TIAA buying a seven-property Fort Point portfolio
for $224 million in April 2016 AND ITEMISES THE BUILDINGS: 263 Summer St, 332 and
374 Congress St, and 33-41, 34, 38 and 44 Farnsworth St. This row is 33-41
Farnsworth Street, named in that list, carrying the identical entity string. The
2021 disposal of one building out of the seven is TIAA selling down that
portfolio.

JPPF = JAMESTOWN PREMIER PROPERTY FUND. BLDUP, "18 Tremont Street Trades for
$102.75 Million", and CoStar and Connect CRE writing up the 2026 resale, all name
Jamestown as the 2019 buyer at that price. Jamestown is independently established
in this table as the seller of 239 First Street, where its entity is JAMESTOWN
PREMIER 245 FIRST LLC -- the same "Premier" fund, spelled out.

A REPEAT-SALE PAIR THIS TABLE SHOULD WANT. 18 Tremont Street sold again in 2026
for about $29.5 million, a 71% fall from the $102.75 million recorded here, to an
affiliate of locally based Mai Luo / Kendall Capital, a converter of tired
offices to apartments. Jamestown took the loss. Noted on the row so the pair is
recoverable whether or not the 2026 leg ever loads.

    python scraper/acq_press9.py --apply
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

HEALTHPEAK = (
    "LS <LOCATION> LLC IS HEALTHPEAK PROPERTIES, CONFIRMED AGAINST NAMED "
    "PROPERTIES. Healthpeak's own Q3 2021 results and its 8-K describe eight "
    "separate transactions totalling roughly $625 million across about 36 acres "
    "of largely contiguous property in the Alewife submarket of West Cambridge. "
    "Two of the eight match rows in this table on property, price and month at "
    "once: 10 Fawcett, 132,000 SF on 2.5 acres for $73 million closing October "
    "2021, against a row of $73,000,000 in October 2021 whose seller of record is "
    "10 FAWCETT INVESTORS LLC and whose buyer of record is LS ALEWIFE II LLC; and "
    "67 Smith Place for $72 million closing January 2022, against a row of "
    "$72,000,000 in January 2022 whose buyer of record is LS ALEWIFE VIII LLC. "
    "The roman numerals in LS ALEWIFE II and LS ALEWIFE VIII are the numbering of "
    "that eight-deal campaign. The Concord Avenue campus row, $180M in September "
    "2021, already carried Healthpeak and is the third match. Bisnow and Cambridge "
    "Day covered the campaign throughout."
)

RESOLVE = [
    (1616, "buyer", "Healthpeak Properties", "prefix_confirmed", HEALTHPEAK +
     " THIS ROW IS 10 FAWCETT ITSELF: $73,000,000, October 2021, seller of record "
     "10 FAWCETT INVESTORS LLC. Healthpeak describes it as a 132,000 SF "
     "multi-tenant office building on 2.5 acres."),
    (1594, "buyer", "Healthpeak Properties", "prefix_confirmed", HEALTHPEAK +
     " THIS ROW IS 67 SMITH PLACE ITSELF: $72,000,000, January 2022. NOTE that "
     "the SELLER here, CCF SMITH PLACE PROPERTY COMPANY LLC, remains deliberately "
     "undecoded -- CCF has an obvious-looking reading in Boston real estate and "
     "that is exactly the reasoning that produced the RREF error."),
    (1603, "buyer", "Healthpeak Properties", "prefix_confirmed", HEALTHPEAK +
     " This row, 725 Concord Avenue at $80,000,000 in November 2021, is one of "
     "the remaining Alewife transactions: same submarket, same months, same LS "
     "<LOCATION> LLC convention. Cambridge Day covered Healthpeak buying further "
     "Alewife land in November and December 2021 at $120M and $121M, so not every "
     "leg is itemised at this price in what was found; the entity convention and "
     "the campaign are what carry it."),
    (1638, "buyer", "Healthpeak Properties", "prefix_confirmed", HEALTHPEAK +
     " This row is 60 Acorn Park Drive at $165,000,000 in December 2020, whose "
     "buyer entity is LS 200 CDP LLC and whose seller of record is 200 DISCOVERY "
     "PARK DE LLC -- Cambridge Discovery Park. It PREDATES the 2021 Alewife "
     "campaign by nine months, so it is the same sponsor's earlier West Cambridge "
     "purchase rather than one of the eight. The LS convention and the matching "
     "seller entity carry it; no press naming Healthpeak at this address was "
     "found, and that limit is recorded rather than hidden."),

    (987, "seller", "TIAA (Nuveen)", "prefix_confirmed",
     "T-C FORT POINT CREATIVE IS TIAA, CONFIRMED AGAINST THIS EXACT BUILDING. "
     "NEREJ, \"TIAA purchases seven-property Fort Point portfolio for $224 "
     "million\", reports an affiliate of TIAA buying 408,342 SF in April 2016 and "
     "ITEMISES THE SEVEN BUILDINGS: 263 Summer St; 332 and 374 Congress St; and "
     "33-41, 34, 38 and 44 Farnsworth St. This row is 33-41 Farnsworth Street, "
     "named in that list, and it carries the identical entity string T-C FORT "
     "POINT CREATIVE that appears as the BUYER on the 374 Congress Street row. "
     "This December 2021 sale is TIAA selling one building out of the seven."),

    (1225, "buyer", "Jamestown", "prefix_confirmed",
     "JPPF IS JAMESTOWN PREMIER PROPERTY FUND. BLDUP, \"18 Tremont Street Trades "
     "for $102.75 Million\", matches this row to the dollar, and CoStar and "
     "Connect CRE both name Jamestown as the 2019 buyer when writing up the later "
     "resale. Jamestown is independently established in this table as the seller "
     "of 239 First Street, Cambridge, where its entity is JAMESTOWN PREMIER 245 "
     "FIRST LLC -- the same fund with its name spelled out rather than initialled. "
     "The 12-storey, 204,000 SF building was built in 1902."),
]

NORMALISE = [("Healthpeak Properties Inc", "Healthpeak Properties")]

NOTES = [
    (1225, "REPEAT SALE, SECOND LEG NOT IN THIS TABLE. 18 Tremont Street sold "
           "again in 2026 for about $29,500,000 -- roughly a 71% fall from the "
           "$102,750,000 recorded here -- to an affiliate of locally based Mai Luo "
           "/ Kendall Capital, a firm that converts tired offices to apartments. "
           "Jamestown took the loss. Reported by CoStar (\"Jamestown disposes of "
           "Boston office building at steep discount to 2019 price\") and Connect "
           "CRE. Recorded here so the pair survives whether or not the 2026 leg "
           "ever loads from a registry source."),
    (1698, "325 BINNEY STREET SELLER: PRIVATE INDIVIDUALS, NOT A SPONSOR. The "
           "seller of record is \"BROWN, ALBERT W., AUSTIN C. ET-AL\" -- named "
           "natural persons holding property directly, not a single-purpose "
           "vehicle concealing a firm. seller_canonical stays null because there "
           "is no sponsor to resolve to, and that blank means something different "
           "from an unresearched blank: there is nothing behind this name. Rows "
           "like this are part of why the resolution rate can never reach 100%."),
]


def main(dry_run: bool):
    conn = engine.connect()
    n = 0
    for rid, side, sponsor, basis, why in RESOLVE:
        cur = conn.execute(text(
            f"select {side}, coalesce({side}_canonical,'') from transactions "
            f"where id = :id"), {"id": rid}).first()
        if not cur:
            log.warning("id %s missing", rid)
            continue
        if cur[1]:
            log.info("id=%-5s %-6s already %s, skipped", rid, side, cur[1])
            continue
        log.info("id=%-5s %-6s %-34s -> %-26s [%s]", rid, side,
                 (cur[0] or "")[:34], sponsor, basis)
        if not dry_run:
            conn.execute(text(f"""
                update transactions
                   set {side}_canonical = :s, {side}_confidence = 'registry_confirmed',
                       {side}_resolution_basis = :b,
                       notes = coalesce(notes,'') || :n
                 where id = :id"""), {
                "s": sponsor, "id": rid, "b": basis,
                "n": f" | {side.upper()} RESOLVED. " + why})
            n += 1

    if not dry_run:
        for old, new in NORMALISE:
            for side in ("buyer", "seller"):
                r = conn.execute(text(
                    f"update transactions set {side}_canonical = :new "
                    f"where {side}_canonical = :old"), {"new": new, "old": old})
                if r.rowcount:
                    log.info("normalised %d %s rows: %r -> %r", r.rowcount, side,
                             old, new)
        for rid, note in NOTES:
            conn.execute(text(
                "update transactions set notes = coalesce(notes,'') || :n "
                "where id = :id"), {"n": " | " + note, "id": rid})
        conn.commit()

    log.info("\n%d sides written", n)
    tot = conn.execute(text(
        "select count(*) from transactions where coalesce(quarantined,0)=0")).scalar()
    for side in ("buyer", "seller"):
        v, d = conn.execute(text(
            f"select count(*), sum(coalesce(price,0)) from transactions "
            f"where coalesce(quarantined,0)=0 and coalesce({side}_canonical,'') <> ''"
        )).first()
        log.info("%s_canonical: %d of %d (%.0f%%), $%.2fB", side, v, tot,
                 v / tot * 100, (d or 0) / 1e9)
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
