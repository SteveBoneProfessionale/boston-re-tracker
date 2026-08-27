r"""Eleventh pass. Hotels, a portfolio allocation, and four self-identifying names.

350 STUART STREET CLOSES A LOOP ALREADY IN THE TABLE. The buyer side already
read Electra America Hospitality Group, from the entity EAHG BOSTON LLC. The
press confirms the deal and the seller: the Loews Hotel at 350 Stuart Street sold
for $116,650,000 -- this row to the dollar -- with plans to rebrand the 172,000
SF hotel as Hotel AKA Back Bay. Electra America is the same firm that took 90
Tremont Street and rebranded it Hotel AKA Boston Common, which is a second Boston
AKA conversion by the same sponsor and a useful independent check on both rows.
(90 Tremont's own 2016 row stays unresolved: the coverage found is of Electra
America's LATER purchase, not the 2016 conveyance.)

159-175 DEVONSHIRE IS ONE LEG OF A $410 MILLION PORTFOLIO. Blackstone acquired
the Club Quarters hotel portfolio from Masterworks Development for $410 million
in 2016; the Boston asset is the 178-key Club Quarters Hotel at 161 Devonshire
Street, in the nationally registered Compton Building. This row is the Boston
allocation at $75,420,000. The entity BRE QUAD MA OWNER LLC carries Blackstone
Real Estate's BRE convention -- independently present in this table as BRE-BMR
215 FIRST STREET LLC for BioMed Realty -- but it is the press naming Blackstone
at this portfolio and this building that establishes it, not the initials.

WHAT HAPPENED TO IT AFTERWARDS IS WORTH RECORDING. Blackstone faced foreclosure
on the Club Quarters portfolio in 2024, the Boston hotel went to the CMBS trust
at a February foreclosure sale, and Arch & Devonshire LLC bought it for about
$75 million -- almost exactly what Blackstone's leg cost in 2016, eight years of
nothing.

FOUR NAMES THAT NAME THEIR OWN OWNER, and one that looks like it does and is
refused. ASB SUMMER STREET VENTURE, L&B CIP 625 MASS AVE, P6/SARACEN 2 OLIVER
and NB GUEST STREET ASSOCIATES all carry a real firm on their face, and three of
those firms appear elsewhere in this table under independent evidence. WELLS
OPERATING PARTNERSHIP LP does not get the same treatment: see the note on it.

    python scraper/acq_press11.py --apply
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
    (928, "seller", "Loews Hotels & Co.", "web",
     'BLDUP, "Back Bay Hotel Acquired for $116.6 Million", and contemporaneous '
     'coverage: the Loews Hotel at 350 Stuart Street sold for $116,650,000 -- '
     'this row to the dollar -- with the 172,000 SF hotel to be rebranded Hotel '
     'AKA Back Bay. The record entity is LBV HOTEL LLC. The buyer side already '
     'read Electra America Hospitality Group from the entity EAHG BOSTON LLC, and '
     'Electra America is separately reported buying 90 Tremont Street and '
     'rebranding it Hotel AKA Boston Common, so the sponsor, the brand and the '
     'strategy all line up across two Boston rows.'),

    (1479, "buyer", "Blackstone Real Estate", "web",
     'Blackstone acquired the Club Quarters hotel portfolio from Masterworks '
     'Development for $410 million in 2016; the Boston asset is the 178-key Club '
     'Quarters Hotel at 161 Devonshire Street, in the nationally registered '
     'Compton Building at 159, 161-175 Devonshire. This row is the Boston leg at '
     '$75,420,000 in February 2016. PORTFOLIO ALLOCATION, not a standalone trade. '
     'The entity BRE QUAD MA OWNER LLC does carry Blackstone Real Estate\'s BRE '
     'convention, present independently in this table as BRE-BMR 215 FIRST STREET '
     'LLC, but the press naming Blackstone at this portfolio is what establishes '
     'it. AFTERWARDS: Blackstone faced foreclosure on the Club Quarters portfolio '
     'in 2024, the Boston hotel passed to the CMBS trust at a February foreclosure '
     'sale, and Arch & Devonshire LLC bought it for about $75 million -- '
     'essentially the 2016 price, eight years later.'),
    (1479, "seller", "Masterworks Development", "web",
     'Same reporting names Masterworks Development as the seller of the Club '
     'Quarters portfolio to Blackstone in 2016.'),

    (1098, "seller", "NB Development Group (New Balance)", "prefix_confirmed",
     'The record entity is NB GUEST STREET ASSOCIATES LLC. NEREJ names NB '
     'Development Group at Guest Street directly -- "Transwestern arranges $76 '
     'million financing for NB Development Group for 40 Guest Street - Part of '
     'Boston Landing" -- and Boston Landing is New Balance\'s Brighton development, '
     'of which Guest Street is the spine. ADDRESS CAVEAT: the source names 40 '
     'Guest Street and this row is 20 Guest Street East. They are the same '
     'development by the same sponsor under the same entity convention, but the '
     'article is not about this building.'),

    (1480, "buyer", "ASB Real Estate Investments", "self_identifying",
     'The record entity is ASB SUMMER STREET VENTURE LLC, which names its own '
     'owner. ASB Real Estate Investments is independently established in this '
     'table as half of the venture that sold Two Financial Center at 60 South '
     'Street to Nan Fung in 2021, confirmed there by Institutional Real Estate, so '
     'this is not a bare name match. The seller, W2005 BWH II REALTY LLC, is NOT '
     'decoded: the W2005 series has an obvious-looking reading and no source found '
     'names a firm at this address.'),

    (1529, "seller", "P6 / Saracen Properties", "self_identifying",
     'The record entity is P6/SARACEN 2 OLIVER REALTY, which names Saracen '
     'Properties on its face as one side of a joint venture. Under the rule that '
     'a joint venture resolves to all partners rather than the most visible one, '
     'the P6 partner is carried through as the record renders it rather than '
     'dropped -- and it is NOT expanded, because no source found says what it '
     'stands for. Recording "Saracen Properties" alone would have quietly deleted '
     'a partner, which is the failure the rule exists to prevent.'),

    (1688, "seller", "L&B Realty Advisors", "self_identifying",
     'The record entity is L&B CIP 625 MASS AVE LLC, which names L&B Realty '
     'Advisors on its face; CIP is its Core Income Partners series. No decoding '
     'step is required for the firm itself, which is what makes a self-identifying '
     'name safe.'),
]

NOTES = [
    (1577, "1416 MASSACHUSETTS AVENUE SELLER: WELLS OPERATING PARTNERSHIP IS NOT "
           "WRITTEN, AND THE REASON IS A DATE. The entity names a real firm on its "
           "face, so the self-identifying rule would normally apply. But Wells "
           "Operating Partnership LP is the operating partnership of Wells Real "
           "Estate Investment Trust, which became PIEDMONT OFFICE REALTY TRUST in "
           "2005 -- and this row is dated December 2022, seventeen years later. "
           "Either the entity is a survivor of that rename holding title under a "
           "dead brand, in which case the sponsor is Piedmont and not Wells, or it "
           "belongs to Wells Real Estate Funds, a separate and still-trading firm. "
           "That is exactly the FelCor/RLJ situation, where the entity kept its "
           "pre-merger name and the sponsor had changed, and there the press "
           "settled it. Here nothing found does."),
    (1443, "90 TREMONT STREET: THE LATER BUYER IS KNOWN, THIS ROW'S IS NOT. Traded "
           "records Electra America Hospitality Group acquiring 90 Tremont Street "
           "for $82,630,000 around 2021, and the hotel now trades as Hotel AKA "
           "Boston Common -- the same sponsor and brand as 350 Stuart Street in "
           "this table. That is a LATER conveyance. This row is September 2016 at "
           "$85,100,300, and neither KHP BOSTON HOTEL LLC nor THI VI BOSTON LLC is "
           "confirmed against this address. Knowing who owns a building now says "
           "nothing about who bought it eight years ago; that assumption is the "
           "condominium-fault error in another form."),
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
        log.info("id=%-5s %-6s %-34s -> %-36s [%s]", rid, side,
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
