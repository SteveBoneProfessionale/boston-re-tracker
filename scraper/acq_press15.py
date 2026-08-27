r"""Fifteenth pass. The $25-50M band, and one lead refused for consistency's sake.

Five sides in the band the previous fourteen passes barely touched, four of them
from names that identify their own owner and one from press that also settles an
older loose end.

147 MILK STREET CLOSES THE BENTALLGREENOAK QUESTION. Traded records "LCI 147
Milk Street LLC Acquires Medical/Dental Facility In Boston For $47.95M" and
identifies LCI 147 Milk Street LLC as a subsidiary of KanAm Grund America LP;
Bisnow's "German Firm Enters Boston Market With $48M Medical Office Buy" and The
Real Reporter's "BentallGreenOak Boston MOB Brings $47M+ as German Buyer Lands
Asset Via Newmark" are the same deal from the other side. The building is ten
storeys and 52,200 SF, built 1920, let to Atrius Health. Earlier in this project
a BentallGreenOak claim was dismissed after checking only one outlet, when a
supporting Real Reporter lead was already in the cached index; this is that same
Real Reporter brief, and it holds up.

REALTY ASSOCIATES FUND XII IS TA REALTY. The buyer entity is REALTY ASSOCIATES
FUND XII PORTFOLIO LLC. TA Realty was TA Associates Realty and its funds are the
Realty Associates Fund series; the firm is separately confirmed in this table at
Porter Square, where Cambridge Day and The Real Reporter name it, and at 131
Dartmouth Street from the ownership record. The fund's own name is on the face
of the entity.

AND ONE LEAD THAT IS REFUSED, on purpose. 315 Kendall Street's buyer entity is
BMR-THIRD LLC. BMR is BioMed Realty's standard convention, BioMed is confirmed
in this table at 321 Harrison Avenue and appears as BRE-BMR 215 FIRST STREET
LLC, and BioMed demonstrably owns 585 Third Street and several Kendall Square
labs. Every arrow points one way. But no source found names BioMed Realty at 315
KENDALL STREET, and 175 Federal Street was left as a lead in an earlier pass on
exactly this reasoning. Applying the rule when it is inconvenient is the only
thing that makes it a rule.

    python scraper/acq_press15.py --apply
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

FOXROCK = ("The record entity names FoxRock on its face. FoxRock Properties is "
           "the Quincy-based owner-operator, and no decoding step is involved, "
           "which is what makes a self-identifying name safe. It appears twice in "
           "this table, buying 265 Purchase Street in 2021 and selling in 2022.")

RESOLVE = [
    (1064, "buyer", "KanAm Grund", "web",
     'Traded, "LCI 147 Milk Street LLC Acquires Medical/Dental Facility In Boston '
     'For $47.95M", identifies the buyer entity as a subsidiary of KanAm Grund '
     'America LP -- this row to the dollar, March 2021. Bisnow reports it as '
     '"German Firm Enters Boston Market With $48M Medical Office Buy" and The Real '
     'Reporter as "BentallGreenOak Boston MOB Brings $47M+ as German Buyer Lands '
     'Asset Via Newmark". The asset is a ten-storey, 52,200 SF 1920 building let '
     'to Atrius Health. NOTE: this is the Real Reporter brief that an earlier pass '
     'in this project overlooked when dismissing a BentallGreenOak claim after '
     'checking a single outlet. It corroborates rather than contradicts the seller '
     'already recorded here.'),

    (1148, "buyer", "TA Realty", "self_identifying",
     'The record entity is REALTY ASSOCIATES FUND XII PORTFOLIO LLC. TA Realty was '
     'TA Associates Realty and its fund series is the Realty Associates Fund, so '
     'the entity carries the fund\'s own name. TA Realty is separately confirmed '
     'in this table at Porter Square, where Cambridge Day and The Real Reporter '
     'name it alongside the property, and at 131 Dartmouth Street from the '
     'ownership record. THE SELLER IS NOT WRITTEN: BOP 15 BROAD LLC suggests '
     'Brookfield Office Properties, but Brookfield is confirmed in this table '
     'under a different convention entirely (BRREP), and reading a second, unlike '
     'convention as the same firm is a guess.'),

    (1019, "buyer", "FoxRock Properties", "self_identifying", FOXROCK),
    (933, "seller", "FoxRock Properties", "self_identifying", FOXROCK),

    (1073, "buyer", "The Centurion Foundation", "self_identifying",
     'The record entity is CENTURION FOUNDATION OF MASSACHUSETTS, which names its '
     'own owner. The Centurion Foundation is a non-profit that acquires and '
     'finances healthcare and senior-living real estate, which fits a '
     'retail/wholesale/service parcel being taken for institutional use. No '
     'decoding step is involved.'),
]

NOTES = [
    (1572, "315 KENDALL STREET BUYER: BIOMED REALTY IS A LEAD, NOT THE BUYER, AND "
           "IT IS REFUSED FOR CONSISTENCY. The record entity is BMR-THIRD LLC. "
           "BMR-<ASSET> LLC is BioMed Realty's standard convention; BioMed Realty "
           "is confirmed in this table at 321 Harrison Avenue and 1000 Washington "
           "Street by Banker & Tradesman, appears here as BRE-BMR 215 FIRST STREET "
           "LLC buying from Alexandria in 2024, owns more than 6 million SF in "
           "Greater Boston, and demonstrably holds 585 Third Street and several "
           "Kendall Square labs -- so THIRD in the entity has an obvious referent. "
           "Every arrow points one way. But NO SOURCE FOUND NAMES BIOMED REALTY AT "
           "315 KENDALL STREET, and 175 Federal Street was left as a lead in an "
           "earlier pass on exactly this reasoning even though its entity matched "
           "a confirmed convention too. A rule that is only applied when it is "
           "convenient is not a rule."),
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
        log.info("id=%-5s %-6s %-38s -> %-26s [%s]", rid, side,
                 (cur[0] or "")[:38], sponsor, basis)
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
