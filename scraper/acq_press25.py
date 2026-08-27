r"""Twenty-fifth pass. P&G's third Fort Point disposal, and the last free names.

PROCTER & GAMBLE SELLS FORT POINT LAND THREE TIMES IN THIS TABLE, and only now
does that read as a programme rather than three unrelated rows:

    2016-12   6 Necco Court          $ 57,400,000   -> MassDevelopment
    2019-05   244-284 A Street       $218,000,000   -> Related Beal
    2021-09   232 A Street           $ 80,000,000   -> Breakthrough Properties

MassDevelopment's own release is titled "Procter & Gamble sells Fort Point land
for GE project": MassDevelopment bought more than an acre including two vacant
brick buildings on Necco Court for $57.4 million, closing 15 December 2016, as
part of a 2.5-acre purchase made jointly with GE. THE SPLIT HAS A REASON WORTH
KEEPING: MassDevelopment used state grant money, and the grant programme could
only pay out to a public agency, so the acquisition was divided between GE and
the agency rather than made by one buyer. That is why a quasi-public body
appears as the buyer of a corporate headquarters site.

    And the far end of it is already in this table: GE sold its Fort Point
    property for $252 million in 2019, and the former GE headquarters site
    later sold for $57 million as the lab market turned.

ONE LEAD LEFT UNWRITTEN AT 1345 BOYLSTON. Target Corporation names itself as the
buyer of the CityTarget unit in the Fenway. Samuels & Associates developed the
$315 million mixed-use scheme it sits in, which makes Samuels the obvious
seller -- but the seller of record on this row is BLANK, no source names a party
to this conveyance, and "the developer of the building probably sold the
condominium in it" is an inference, not a source.

    python scraper/acq_press25.py --apply
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
    (1428, "seller", "Procter & Gamble (Gillette)", "web",
     'MassDevelopment\'s own release, "Procter & Gamble sells Fort Point land for '
     'GE project", with the Boston Globe and GE\'s release "GE, Gillette and '
     'MassDevelopment Complete the Sale of Property for Future GE Headquarters in '
     'Boston": P&G, which owned the land through its Gillette subsidiary, sold '
     'more than an acre including two vacant brick buildings on Necco Court to '
     'MassDevelopment for $57.4 million, closing 15 December 2016 -- this row '
     'exactly -- within a 2.5-acre joint purchase with GE. WHY A PUBLIC AGENCY IS '
     'THE BUYER: MassDevelopment used state grant money and the programme could '
     'only disburse to a public agency, so the site was split rather than bought '
     'by one party. This is P&G\'s THIRD Fort Point disposal in this table, after '
     '244-284 A Street to Related Beal for $218M in 2019 and 232 A Street to '
     'Breakthrough Properties for $80M in 2021.'),

    (1242, "seller", "Danker & Donohue Garage Co.", "self_identifying",
     'The record entity is DANKER AND DONOHUE, the long-standing Boston garage '
     'company, which names its own owner. No decoding step is involved. The buyer '
     'side already read Gazit Horizons, which is separately confirmed by press '
     'buying the 200 State Street retail condominium in April 2019.'),
]

NOTES = [
    (1540, "1345 BOYLSTON STREET SELLER, $59,000,000, March 2015: A LEAD, NOT A "
           "RESOLUTION, AND THE SELLER OF RECORD IS BLANK. The buyer is certain -- "
           "the entity is TARGET CORPORATION, the retailer buying its own "
           "three-level CityTarget unit, which opened to the public on 22 July "
           "2015. SAMUELS & ASSOCIATES developed the $315 million mixed-use "
           "scheme the store sits in, which makes it the obvious seller of the "
           "condominium unit, and Samuels already appears elsewhere in this table. "
           "But no source names a party to THIS conveyance, the seller field is "
           "empty rather than merely undecoded, and \"the developer of the "
           "building probably sold the unit in it\" is an inference. NOTE: this "
           "row's building_sf was the 1 SF placeholder and its $/SF has been "
           "nulled."),
    (1698, "325 BINNEY STREET SELLER, $80,250,000, March 2017: NOT A FIRM, AND "
           "THAT IS THE ANSWER RATHER THAN A GAP. The seller of record is "
           "\"BROWN, ALBERT W., AUSTIN C. ET-AL\" -- named natural persons "
           "holding property directly, not a single-purpose vehicle concealing a "
           "sponsor. seller_canonical stays null because THERE IS NO SPONSOR TO "
           "RESOLVE TO, and that blank means something different from an "
           "unresearched blank. The buyer side reads Alexandria Real Estate "
           "Equities. Rows like this are a structural part of why the resolution "
           "rate cannot reach 100%: some owners are people."),
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
        log.info("id=%-5s %-6s %-34s -> %s", rid, side, (cur[0] or "")[:34], sponsor)
        if not dry_run:
            conf = ("web_corroborated" if basis == "web" else "registry_confirmed")
            conn.execute(text(f"""
                update transactions
                   set {side}_canonical = :s, {side}_confidence = :c,
                       {side}_resolution_basis = :b,
                       notes = coalesce(notes,'') || :n
                 where id = :id"""), {
                "s": sponsor, "id": rid, "c": conf, "b": basis,
                "n": f" | {side.upper()} RESOLVED. " + why})
            n += 1

    if not dry_run:
        for rid, note in NOTES:
            conn.execute(text(
                "update transactions set notes = coalesce(notes,'') || :n "
                "where id = :id"), {"n": " | " + note, "id": rid})
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
