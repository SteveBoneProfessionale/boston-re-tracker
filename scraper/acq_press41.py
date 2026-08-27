r"""Forty-first pass. One resolution, and two leads that have now survived four
attempts each -- which is itself worth recording.

100 CAMBRIDGEPARK DRIVE'S SELLER WAS A LEAD AND IS NOW SOURCED. The previous
note refused Roseview/PMRG because the only evidence was that they had BOUGHT
the building in December 2014 for $41.5 million -- the earlier-buyer inference.
NEREJ now supplies the sale itself: "Roseview-PMRG Fund I sells 135,615 s/f 100
CambridgePark Dr.; Griffin, Maher, Pullen and Hallowell of NKF oversee deal",
with Boston Real Estate Times covering it as "Roseview/PMRG Sell Class A office
building in Cambridge, MA". The fund sold "in conjunction with operating partner
LONGFELLOW REAL ESTATE PARTNERS", so all three are recorded.

    NEREJ says the buyer was undisclosed and no price was given. This row's
    buyer, Morgan Stanley Prime Property Fund, comes from the confirmed PPF
    prefix, and MSREI separately bought 125/150 CambridgePark Drive for $210
    million -- a different, larger complex on the same street, which is exactly
    the kind of near-miss this table has to keep apart.

TWO LEADS THAT HAVE NOW SURVIVED FOUR SEARCHES EACH. Recording the failure count
matters: it is the difference between "not yet looked for" and "looked for hard
and not there", and only the second tells you anything about the source
landscape.

    175 Federal St     Boston Properties. The entity BP-175 FEDERAL STREET LLC
                       matches the BP <ASSET> LLC convention confirmed from BXP's
                       own SEC filing at 415 Main Street; the buyer Deka is
                       confirmed by NEREJ and the BBJ; nothing contradicts it;
                       and two separate retrievals ASSERT it. But no article
                       quotes BXP as seller, its Q2 2016 earnings release could
                       not be reached, and SEC EDGAR refuses automated fetches.
                       An assertion inside a search summary is not a source.
    1000 Mass Ave      Intercontinental Real Estate. Its own property page
                       carries "1000 Mass Ave" at 105,062 SF between Harvard and
                       Kendall, and it is confirmed in this table twice
                       elsewhere. Still no acquisition date anywhere.

    python scraper/acq_press41.py --apply
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
    (1693, "seller", "Roseview / PMRG / Longfellow Real Estate Partners",
     'UPGRADED FROM LEAD TO RESOLUTION. The earlier note refused this because the '
     'only evidence was that Roseview-PMRG Fund I had BOUGHT 100 CambridgePark '
     'Drive in December 2014 for $41.5 million from Transatlantic Investment '
     'Management -- the earlier-buyer inference this table declines on principle. '
     'NEREJ now covers the SALE: "Roseview-PMRG Fund I sells 135,615 s/f 100 '
     'CambridgePark Dr.; Griffin, Maher, Pullen and Hallowell of NKF oversee '
     'deal", and Boston Real Estate Times reports it as "Roseview/PMRG Sell Class '
     'A office building in Cambridge, MA". The fund sold IN CONJUNCTION WITH '
     'OPERATING PARTNER LONGFELLOW REAL ESTATE PARTNERS, so all three parties are '
     'recorded under the rule that a venture resolves to every partner. CAVEATS '
     'KEPT ON THE ROW: NEREJ reports the buyer as undisclosed and gives no price, '
     'so this row\'s $60,200,000 and its Morgan Stanley Prime Property Fund buyer '
     'rest on the registry and the confirmed PPF prefix rather than on that '
     'article. The entity KT CAMBRIDGE PARK LLC matches none of the three names, '
     'which is the derived-seller lag documented at 327-333 Summer, 6-10 Oliver, '
     '374 Congress, 36-44 Broad and 350 Washington. AND A NEAR MISS TO KEEP '
     'APART: Morgan Stanley Real Estate Investing separately bought 125/150 '
     'CambridgePark Drive, a 470,258 SF complex, for $210 million -- same street, '
     'different asset, different deal.'),
]

NOTES = [
    (1464, "175 FEDERAL STREET SELLER: BOSTON PROPERTIES REMAINS A LEAD AFTER FOUR "
           "SEARCHES. Recording the count deliberately -- \"looked for hard and not "
           "found\" is a different statement from \"not yet looked for\". "
           "EVERYTHING POINTS ONE WAY: the entity BP-175 FEDERAL STREET LLC "
           "matches the BP <ASSET> LLC convention confirmed from Boston "
           "Properties' OWN SEC FILING at 415 Main Street; the buyer is "
           "independently confirmed as Deka Immobilien by NEREJ and the BBJ at "
           "$139 million in May 2016, Deka's first Boston purchase within about "
           "$250m of Boston deals that year; nothing anywhere contradicts BXP; and "
           "two separate search retrievals ASSERT that this was Boston Properties' "
           "disposition. WHY IT IS STILL NOT WRITTEN: no article QUOTES Boston "
           "Properties as the seller. The assertion appears only in summarised "
           "search output, BXP's Q2 2016 earnings release could not be retrieved, "
           "and SEC EDGAR returns 403 to automated fetches. A restatement inside a "
           "search summary is not a source, and this table's rule is a source "
           "naming the firm alongside the property. If a licensed feed or the "
           "Q2 2016 supplemental ever confirms it, this becomes a one-line fix."),
    (1708, "1000 MASSACHUSETTS AVENUE BUYER: INTERCONTINENTAL REMAINS A LEAD AFTER "
           "FOUR SEARCHES. Intercontinental Real Estate Corporation carries \"1000 "
           "Mass Ave\" on its own property page -- a 105,062 SF office building "
           "between Harvard and Kendall Square -- and is confirmed in this table "
           "twice elsewhere, buying the Canal Park complex in 2016 and the Yard 5 "
           "industrial portfolio in 2022. The buyer entity 1000 MASSACHUSETTS AVE "
           "MA LLC even resembles its [NUMBER] CANAL PARK MASSACHUSETTS LLC "
           "convention. BUT ITS PAGE STATES NO ACQUISITION DATE, and no source "
           "found reports the December 2016 conveyance. Current ownership does not "
           "establish who bought nine years ago -- the standard that kept "
           "Breakthrough Properties a lead at 1 Canal Park until a source supplied "
           "the year, and then promoted it. The seller, Cambridge College, is "
           "solid: it names itself and was selling its own campus building."),
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
