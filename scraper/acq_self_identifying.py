r"""Resolve entities whose own name IS the firm. No research, no inference.

A large slice of the unresolved set was never a single-purpose vehicle at all.
"STEWARD CARNEY HOSPITAL INC", "JOHN HANCOCK MUTUAL LIFE", "PRESIDENT & FELLOWS
OF HARVARD COLLEGE" name their owner on the face of the record; they sat
unresolved only because no pattern happened to match them. Writing these costs
nothing and risks nothing, because there is no decoding step to get wrong.

WHAT IS DELIBERATELY NOT IN THIS LIST, and why the exclusions matter more than
the inclusions. A scan for corporate-sounding words returned 23 candidates and 8
of them are not sponsors:

    PUTNAM CIRCLE ASSOCIATES     Putnam Circle is a STREET. Not Putnam
                                 Investments.
    ONE 48 STATE STREET LPS      an address.
    WINHALL LIBERTY LLC          an address.
    US BANK TRUST NATIONAL ASSOC a securitisation trustee holding title for
                                 bondholders, not an owner.
    CAMBRIDGE TRUST COMPANY TR OF likewise a trustee, "TR OF" being the tell.
    CCF SMITH PLACE PROPERTY CO  CCF looks like Cabot, Cabot & Forbes. That is
                                 exactly the reasoning that produced the RREF /
                                 Rialto error, and it is not written.
    S-BANK CAMBRIDGE LLC         undecoded.
    UNIVERSITY COMMON REAL ESTATE ambiguous.

AND ONE OF THEM IS AN AFFILIATED TRANSFER. Atrius MSO LLC bought 133 Brookline
Avenue from Harvard Vanguard Medical Associates for $164.5M. Harvard Vanguard IS
Atrius Health -- it is the group's original practice name. That is one
organisation conveying to itself and it is quarantined, not resolved.

    python scraper/acq_self_identifying.py --apply
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

# (LIKE pattern on the entity string, canonical sponsor, note)
SELF = [
    ("GILLETTE COMPANY", "Procter & Gamble (Gillette)",
     "The Gillette Company is Procter & Gamble's razor business, acquired by P&G "
     "in 2005; its South Boston campus is held in its own name."),
    ("STEWARD CARNEY HOSPITAL", "Steward Health Care",
     "Carney Hospital is a Steward Health Care facility."),
    ("STEWARD ST ELIZABETH", "Steward Health Care",
     "St Elizabeth's Medical Center is a Steward Health Care facility."),
    ("JOHN HANCOCK MUTUAL LIFE", "John Hancock (Manulife)",
     "John Hancock Mutual Life Insurance, the Boston insurer, now part of "
     "Manulife."),
    ("PRESIDENT & FELLOWS OF HARVARD", "Harvard University",
     "The President and Fellows of Harvard College is the university's "
     "corporate name."),
    ("FELCOR COPLEY PLAZA", "FelCor Lodging Trust",
     "FelCor Lodging Trust, the hotel REIT, named on its own vehicle."),
    ("INVESCO IF IV", "Invesco Real Estate",
     "An Invesco fund vehicle naming the manager on its face."),
    ("STERLING SUFFOLK RACECOURSE", "Sterling Suffolk Racecourse",
     "The operating company of Suffolk Downs, which owned the racecourse land."),
    ("BOSTON REDEVELOPMENT AUTHORITY", "Boston Planning & Development Agency",
     "The BRA, now trading as the Boston Planning & Development Agency."),
    ("CAMBRIDGE COLLEGE", "Cambridge College",
     "The college itself, owning its own campus."),
    ("BENJAMIN BANNEKER CHARTER", "Benjamin Banneker Charter Public School",
     "The school itself."),
    ("ARCHDIOCESE CENTRAL HIGH SCHOOL", "Roman Catholic Archdiocese of Boston",
     "An Archdiocese of Boston school corporation."),
    ("SUN LIFE ASSURANCE", "Sun Life Financial",
     "Sun Life Assurance Company of Canada, the insurer. Note this entity was "
     "twice nearly absorbed into MEPT by address clustering, because it shares "
     "a servicing address with a MEPT vehicle; it is an independent institution "
     "and resolves to itself."),
]

# 133 Brookline Avenue: Atrius MSO LLC <- Harvard Vanguard Medical Associates.
# Harvard Vanguard is Atrius Health's founding practice, so this is one
# organisation conveying to itself.
AFFILIATED_ID = 950
AFFILIATED_NOTE = (
    "QUARANTINED AS AN AFFILIATED-PARTY TRANSFER. Atrius MSO LLC acquired 133 "
    "Brookline Avenue from HARVARD VANGUARD MEDICAL ASSOCIATES for $164,518,074. "
    "Harvard Vanguard is not a counterparty to Atrius Health -- it is Atrius "
    "Health's original and largest medical group, operating under that name. One "
    "organisation conveying between its own entities is a restructuring, not an "
    "acquisition, so this is excluded from counts, volumes and rankings and kept "
    "for review. It was found while resolving self-identifying names, which is "
    "the only reason it surfaced: neither side is a single-purpose vehicle, so "
    "no entity-pattern rule would ever have flagged it."
)


def main(dry_run: bool):
    conn = engine.connect()
    n = 0
    for key, sponsor, why in SELF:
        for side in ("buyer", "seller"):
            rows = conn.execute(text(
                f"select id, {side} from transactions "
                f"where upper(coalesce({side},'')) like :k "
                f"and coalesce({side}_canonical,'') = '' "
                f"and coalesce({side}_resolution_basis,'') not like 'conflict%'"),
                {"k": f"%{key}%"}).fetchall()
            for rid, ent in rows:
                log.info("id=%-5s %-6s %-40s -> %s", rid, side, (ent or "")[:40],
                         sponsor)
                if not dry_run:
                    conn.execute(text(f"""
                        update transactions
                           set {side}_canonical = :s,
                               {side}_confidence = 'registry_confirmed',
                               {side}_resolution_basis = 'self_identifying',
                               notes = coalesce(notes,'') || :n
                         where id = :id"""), {
                        "s": sponsor, "id": rid,
                        "n": (f" | {side.upper()} RESOLVED: THE ENTITY NAMES ITS "
                              f"OWN OWNER. No decoding step, so nothing to get "
                              f"wrong. {why}")})
                n += 1

    if not dry_run:
        conn.execute(text("""
            update transactions
               set quarantined = 1,
                   quarantine_reason = 'affiliated_party_transfer:same_organisation',
                   arms_length = 0,
                   non_arms_length_reason = coalesce(non_arms_length_reason,
                                                     'affiliated_parties'),
                   notes = coalesce(notes,'') || :n
             where id = :id and coalesce(quarantined,0) = 0"""),
            {"n": " | " + AFFILIATED_NOTE, "id": AFFILIATED_ID})
        conn.commit()

    tot = conn.execute(text(
        "select count(*) from transactions where coalesce(quarantined,0)=0")).scalar()
    log.info("\n%d sides resolved", n)
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
