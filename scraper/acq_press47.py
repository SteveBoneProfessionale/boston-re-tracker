r"""Forty-seventh pass. Ten sides, and the $25-50M band opens up.

The $50M+ rows still carrying gaps are four and five searches deep and mostly
structural. The $25-50M band had never been worked systematically, and it turns
out to hold both fresh press AND the other halves of deals already resolved
above the line.

TWO ROWS ARE THE MISSING HALVES OF DEALS THIS TABLE ALREADY HAD.

    101 Summer St   99 Chauncy and 101 Summer sold TOGETHER for $70,351,000 --
                    159,781 SF, 100% let to 19 tenants -- from Nuveen to
                    Alduwaliya on 7 October 2019. The Chauncy row was resolved
                    in the twenty-ninth pass. This is the Summer Street leg,
                    carrying the identical TRPF 99/101 BOSTON OFFICE seller
                    entity, and it resolves without a single new search.
    500-528 Comm Av The buyer entity is KENMORE SQUARE HOTEL LLC -- byte for
                    byte the entity resolved to Ohana Real Estate Investors on
                    the other Hotel Commonwealth parcel. Same buyer, same
                    building, two parcels.

BOP IS BROOKFIELD OFFICE PROPERTIES, confirmed against the property. NEREJ and
CPE: JLL arranged the $46.1 million sale of 15 Broad Street -- 77,678 SF of
office and street-level retail, the 1910 Marshall Building by Clarence Blackall
-- from BROOKFIELD to TA Realty in late 2019. The seller entity is BOP 15 BROAD
LLC. Brookfield was already in this table under BRREP; this is a second, unlike
convention for the same firm, which is why it had to be confirmed separately
rather than assumed.

TWO SELLERS WHO ARE PEOPLE, AND BOTH ARE WRITEABLE. This table leaves natural
persons null where they are simply owners of record -- Albert and Austin Brown
at 325 Binney, Robert Posner at 399 Washington. These two are different: the
press names each as the PRINCIPAL BEHIND A NAMED HOLDING COMPANY, which is a
sponsor identification rather than a name on a deed.

    2 Charlesgate W  Scape bought the Trans National Building site for $39
                     million in 2019 FROM STEVEN BELKIN, who had owned it.
    265 Purchase St  FoxRock bought its first downtown building for $43.5M from
                     "a holding company controlled by SAMER KHANACHET, the chief
                     operating officer of Kuwait Projects Co."

    python scraper/acq_press47.py --apply
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
    (1163, "buyer", "Alduwaliya Asset Management", "web",
     'THE MISSING HALF OF A DEAL THIS TABLE ALREADY HELD. BLDUP, "Alduwaliya '
     'Acquires 99 Chauncy for $70.351 Million", and Boston Real Estate Times, '
     '"Alduwaliya Purchases TWO Boston Office and Retail Properties": 99 Chauncy '
     'Street and 101 Summer Street sold together on 7 October 2019, 159,781 SF of '
     'office and ground-floor retail, 100% let to 19 tenants on a 7.23-year '
     'weighted average term. The Chauncy leg is already resolved in this table; '
     'this is the Summer Street leg, same buyer, same day.'),
    (1163, "seller", "Nuveen Real Estate", "prefix_confirmed",
     'Same reporting names Nuveen Real Estate as the seller with CBRE acting for '
     'it. TRPF -- the TIAA Real Property Fund, which Nuveen manages -- is confirmed '
     'against this exact pairing: the seller entity here is TRPF 99/101 BOSTON '
     'OFFICE, and 99/101 names both addresses in the deal.'),

    (1145, "buyer", "Ohana Real Estate Investors", "prefix_confirmed",
     'The buyer entity is KENMORE SQUARE HOTEL LLC, byte for byte the entity '
     'already resolved to Ohana Real Estate Investors on the other Hotel '
     'Commonwealth parcel in this table, where Xenia\'s own release names Ohana as '
     'the acquirer of the 245-room hotel. Hotel Commonwealth spans several Kenmore '
     'Square parcels and this table carries them separately; the buyer is the same '
     'across them. DATE NOTE, ALREADY ON THE SIBLING ROW: this parcel is dated '
     'December 2019 and Xenia announced the disposition in November 2020, so the '
     'parcels did not convey together.'),

    (1453, "buyer", "Home Depot", "self_identifying",
     'The record entity is HOME DEPOT USA INC, the retailer buying its own store '
     'site on VFW Parkway in West Roxbury. No decoding step, so nothing to get '
     'wrong.'),
    (1453, "seller", "National Amusements", "self_identifying",
     'The record entity is NAI ENTERTAINMENT HOLDINGS, and NAI is National '
     'Amusements Inc, the Redstone family cinema company headquartered in Norwood. '
     'CONFIRMED AT THIS ADDRESS rather than decoded from initials: coverage of the '
     'Redstone drive-in empire records the VFW PARKWAY LOCATION IN WEST ROXBURY as '
     'one of its Massachusetts theatres, so the firm and the site are named '
     'together. A cinema operator selling a drive-in site to Home Depot is exactly '
     'the shape of the deal the record shows.'),

    (1217, "buyer", "Scape", "web",
     'The Boston Globe, Universal Hub and BLDUP all cover Scape at 2 Charlesgate '
     'West: the British student-housing developer bought the parcel for $39 '
     'million in 2019 -- this row exactly -- with a $30 million mortgage from '
     'CMTG, initially planning a private dormitory tower, then revising to 220 '
     'affordable homes and eventually winning BPDA approval in July 2024 for a '
     '28-storey, 400-unit tower. HOW IT ENDED: CMTG foreclosed when Scape could '
     'not service the mortgage, and Samuels & Associates bought the parcel at a '
     'foreclosure sale on 16 October 2025 for $28.1 million.'),
    (1217, "seller", "Steven Belkin (Trans National Group)", "web",
     'Same reporting: "Scape had purchased the parcel for $39 million in 2019 from '
     'STEVEN BELKIN, who had owned the property previously." The site carried the '
     'Trans National Building, Belkin\'s company. WHY THIS PERSON IS WRITTEN WHEN '
     'OTHERS ARE NOT: this table leaves natural persons null where they are simply '
     'owners of record -- Albert and Austin Brown at 325 Binney Street, Robert '
     'Posner at 399 Washington Street. Here the press names Belkin as the '
     'PRINCIPAL BEHIND the selling ownership, which is a sponsor identification '
     'rather than a name on a deed.'),

    (1148, "seller", "Brookfield Properties", "prefix_confirmed",
     'BOP IS BROOKFIELD OFFICE PROPERTIES, confirmed against the property. NEREJ, '
     '"JLL Capital Markets arranges $46.1 million sale of 15 Broad St., Boston -- '
     'a 77,678 s/f office and street level retail building", with CPE and BLDUP '
     'reporting the same: Brookfield sold to TA Realty in late 2019, JLL acting '
     'for the seller and procuring the buyer. The seller entity is BOP 15 BROAD '
     'LLC. NOTE THAT THIS IS A SECOND BROOKFIELD CONVENTION: the firm is already '
     'in this table as BRREP 51 SLEEPER STREET LLC, confirmed separately, and one '
     'confirmed convention does not license reading another -- which is why BOP '
     'was left unwritten until now. The building is the 1910 Marshall Building by '
     'Clarence Blackall; Broder reacquired it for $13.5 million later, a fifth '
     'round trip in this table.'),

    (1019, "seller", "Samer Khanachet", "web",
     'Bisnow, "FoxRock Pays $43.5M For Its First Downtown Building In Boston\'s '
     'Financial District": FoxRock bought the 77,000 SF, 121-year-old building '
     'from "a holding company controlled by SAMER KHANACHET, the chief operating '
     'officer of Kuwait Projects Co", 80% occupied with tenants including Rich May '
     'and Capstone Partners. As at 2 Charlesgate West, the press names the '
     'PRINCIPAL BEHIND a holding company rather than merely an owner of record, so '
     'the person is written. ADDRESS NOTE: this row is addressed 265 Purchase '
     'Street and the asset is 176 FEDERAL STREET, which the seller entity FEDERAL '
     'ST 176 HOLDINGS LLC confirms -- another parcel-versus-asset split. DO NOT '
     'CONFUSE IT WITH 175 FEDERAL STREET, a different building elsewhere in this '
     'table whose seller is an unresolved Boston Properties lead.'),

    (1454, "seller", "Investec / GLL Real Estate Partners", "prefix_confirmed",
     'The record entity is INVESTEC GLL SGO REF -- the Investec GLL Special Global '
     'Opportunities Real Estate Fund, a venture naming both managers on its face. '
     'GLL Real Estate Partners is independently confirmed twice in this table: as '
     'the seller of 200 State Street to Carr Properties, from NEREJ and Newmark\'s '
     'release, and as the buyer of the Paddock Building at 101 Tremont Street from '
     'CPE. Recorded as the venture, not one partner.'),
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
        log.info("id=%-5s %-6s %-34s -> %-38s [%s]", rid, side,
                 (cur[0] or "")[:34], sponsor, basis)
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

    # BOP is now confirmed; sweep any other row carrying it.
    for side in ("buyer", "seller"):
        for rid, ent in conn.execute(text(f"""
                select id, {side} from transactions
                 where coalesce(quarantined,0) = 0
                   and upper(coalesce({side},'')) like 'BOP %'
                   and coalesce({side}_canonical,'') = ''""")):
            log.info("id=%-5s %-6s %-34s -> Brookfield Properties [family sweep]",
                     rid, side, (ent or "")[:34])
            if not dry_run:
                conn.execute(text(f"""
                    update transactions
                       set {side}_canonical = 'Brookfield Properties',
                           {side}_confidence = 'registry_confirmed',
                           {side}_resolution_basis = 'prefix_confirmed',
                           notes = coalesce(notes,'') || :n
                     where id = :id"""), {
                    "id": rid,
                    "n": (f" | {side.upper()} RESOLVED. BOP IS BROOKFIELD OFFICE "
                          "PROPERTIES, confirmed against a named property: NEREJ "
                          "and CPE record Brookfield selling 15 Broad Street to TA "
                          "Realty for $46.1 million in late 2019, and that row's "
                          "seller entity is BOP 15 BROAD LLC.")})
                n += 1

    if not dry_run:
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
