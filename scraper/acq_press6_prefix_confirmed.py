r"""Sixth pass. Two prefixes CONFIRMED against a named property, then applied.

The standing rule is that a prefix is never decoded from what it looks like it
stands for -- the rule that exists because RREF was read as Rialto and was
Related. This pass does the confirming step properly for two prefixes and then
applies each only across its own naming convention.

BCSP = BEACON CAPITAL PARTNERS. Confirmed, not guessed. The Globe and NEREJ
report that Intercontinental Real Estate Corp bought the three-building Canal
Park complex in Cambridge in May 2016 for $304 million, and that BEACON CAPITAL
was the seller. Two of those three buildings are rows in this table, and both
carry a seller entity of the form BCSP CAMBRIDGE <NUMBER> PROPERTY LLC. That is
a source naming the firm alongside the property, which is what the rule
requires. PitchBook independently records Beacon Capital Strategic Partners
funds I-9 as vehicles of Beacon Capital Partners of Boston.

NFLSRE = NAN FUNG LIFE SCIENCES REAL ESTATE. Confirmed twice already in this
table by press that names the firm alongside the address: Bisnow at 470 Atlantic
Avenue and Institutional Real Estate at 60 South Street. Extending it to 51
Sleeper Street, which follows the same NFLSRE <ADDRESS> LLC convention, is
applying a confirmed decode rather than making a new one.

<NUMBER> CANAL PARK MASSACHUSETTS LLC = INTERCONTINENTAL REAL ESTATE CORP. The
same 2016 reporting establishes this from the other side: the buyer entities on
the two 2016 rows are TWO CANAL PARK MASSACHUSETTS LLC and TEN CANAL PK
MASSACHUSETTS LLC, and the press names Intercontinental as the buyer. The 2021
seller of One Canal Park is ONE CANAL PARK MASSACHUSETTS LLC, the same
convention and the same portfolio.

THE PORTFOLIO ARITHMETIC CHECKS OUT, which is worth stating because it is a
genuine cross-validation rather than another citation:

    Two Canal Park   $154,371,200   in this table
    Ten Canal Park   $ 77,368,000   in this table
    One Canal Park   $ 72,000,000   NOT in this table; BLDUP states One Canal
                                    "last traded in 2016 for $72M"
                     ------------
                     $303,739,200   against the reported $304 million

So the three per-building prices in the registry are the allocation of one
portfolio deal, and the row-level prices are correct as recorded.

ONE CANAL PARK'S 2021 BUYER IS A LEAD, NOT A RESOLUTION. BLDUP reports that "a
life science developer" acquired it on 7 July 2021 for $131,000,000 and does not
name the firm. Breakthrough Properties -- the Tishman Speyer and Bellco Capital
venture -- carries "One Canal By Breakthrough" on its own portfolio page. Those
two facts converge but they do not touch: Breakthrough's page gives no
acquisition date, and this table's 2023-2025 coverage hole means an intervening
trade would not show here. Stored as a lead.

    python scraper/acq_press6_prefix_confirmed.py --apply
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

BEACON = (
    "BCSP IS BEACON CAPITAL PARTNERS, CONFIRMED AGAINST A NAMED PROPERTY. The "
    "Boston Globe (\"Intercontinental Real Estate acquires Cambridge office "
    "complex for $304m\") and NEREJ (\"Intercontinental Real Estate acquires "
    "425,730 s/f Canal Park\") report that Beacon Capital sold the three-building "
    "Canal Park complex in East Cambridge in May 2016, and the seller entities on "
    "those rows are BCSP CAMBRIDGE TWO PROPERTY LLC and BCSP CAMBRIDGE TEN "
    "PROPERTY LLC. PitchBook independently records the Beacon Capital Strategic "
    "Partners fund series as vehicles of Beacon Capital Partners of Boston, and "
    "Beacon is separately established in this table at 160 Federal Street and 135 "
    "Morrissey Boulevard. The prefix was verified before being applied, not read "
    "off its initials."
)

INTERCONTINENTAL = (
    "<NUMBER> CANAL PARK MASSACHUSETTS LLC IS INTERCONTINENTAL REAL ESTATE CORP, "
    "CONFIRMED AGAINST A NAMED PROPERTY. The same May 2016 reporting names "
    "Intercontinental Real Estate Corp as the buyer of the whole Canal Park "
    "complex for $304 million, and the buyer entities on those rows are TWO CANAL "
    "PARK MASSACHUSETTS LLC and TEN CANAL PK MASSACHUSETTS LLC. Intercontinental "
    "carries the Canal Park Portfolio on its own site."
)

NANFUNG = (
    "NFLSRE IS NAN FUNG LIFE SCIENCES REAL ESTATE, ALREADY CONFIRMED TWICE IN "
    "THIS TABLE against a named property: Bisnow names Nan Fung at 470 Atlantic "
    "Avenue and Institutional Real Estate names it at 60 South Street, both "
    "December 2021 / August 2021 rows whose entities are NFLSRE 470 ATLANTIC LLC "
    "and NFLSRE 2 FINANCIAL LLC. This row follows the identical NFLSRE <ADDRESS> "
    "LLC convention, so this is a confirmed decode being applied, not a new one "
    "being made."
)

PORTFOLIO = (
    "PORTFOLIO ALLOCATION, NOT A STANDALONE TRADE. This price is one building's "
    "share of a single May 2016 conveyance of the three-building, 425,730 SF "
    "Canal Park complex from Beacon Capital to Intercontinental Real Estate Corp, "
    "reported at $304 million. The legs reconcile: Two Canal Park $154,371,200 "
    "plus Ten Canal Park $77,368,000 plus One Canal Park $72,000,000 (per BLDUP, "
    "and not present in this table) equals $303,739,200. The recorded per-building "
    "prices are correct; they are simply not three separate deals."
)

RESOLVE = [
    (1727, "seller", "Beacon Capital Partners", BEACON + " " + PORTFOLIO),
    (1727, "buyer", "Intercontinental Real Estate Corp.",
     INTERCONTINENTAL + " " + PORTFOLIO),
    (1728, "seller", "Beacon Capital Partners", BEACON + " " + PORTFOLIO),
    (1728, "buyer", "Intercontinental Real Estate Corp.",
     INTERCONTINENTAL + " " + PORTFOLIO),
    (1195, "seller", "Beacon Capital Partners", BEACON +
     " This row's entity is BCSP 2 MORRISSEY PROPERTY LLC, the same BCSP "
     "<LOCATION> PROPERTY LLC convention. Beacon's presence on Morrissey "
     "Boulevard is separately corroborated: the Globe records Beacon Capital "
     "Partners buying 135 Morrissey, the former Boston Globe headquarters, for "
     "$362.5M in 2021."),
    (1580, "buyer", "Beacon Capital Partners", BEACON +
     " This row's entity is BCSP 9 OBS PROPERTY LLC, naming the ninth Beacon "
     "Capital Strategic Partners fund. Written even though the row is small, "
     "because a confirmed decode costs nothing to apply."),
    (1628, "seller", "Intercontinental Real Estate Corp.", INTERCONTINENTAL +
     " Intercontinental bought One Canal Park in that 2016 portfolio deal and "
     "sold it in July 2021 for $131,000,000, against the $72,000,000 allocated to "
     "it in 2016 -- BLDUP notes the property \"last traded in 2016 for $72M\"."),
    (1137, "buyer", "Nan Fung Life Sciences Real Estate", NANFUNG),
]

# Cosmetic but it matters: one row carried the legal suffix into the sponsor
# name, which splits the firm into two rows in every ranking.
NORMALISE = [("Nan Fung Life Sciences Real Estate Llc",
              "Nan Fung Life Sciences Real Estate")]

LEADS = [
    (1628, "ONE CANAL PARK 2021 BUYER: BREAKTHROUGH PROPERTIES IS A LEAD, NOT "
           "THE BUYER. BLDUP reports that \"a life science developer\" acquired 1 "
           "Canal Park on 7 July 2021 for $131,000,000 -- this row's exact price "
           "and effectively its date -- and does not name the firm. Breakthrough "
           "Properties, the Tishman Speyer and Bellco Capital venture, carries "
           "\"One Canal By Breakthrough\" on its own portfolio page and describes "
           "converting the building to up to 112,000 SF of R&D space, which is "
           "exactly the plan BLDUP attributes to the 2021 buyer. The two facts "
           "converge but do not touch: Breakthrough's page states no acquisition "
           "date, and this table's known 2023-2025 coverage hole means an "
           "intervening trade would not appear here. Not written."),
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
        log.info("id=%-5s %-6s %-36s -> %s", rid, side, (cur[0] or "")[:36], sponsor)
        if not dry_run:
            conn.execute(text(f"""
                update transactions
                   set {side}_canonical = :s, {side}_confidence = 'registry_confirmed',
                       {side}_resolution_basis = 'prefix_confirmed',
                       notes = coalesce(notes,'') || :n
                 where id = :id"""), {
                "s": sponsor, "id": rid,
                "n": f" | {side.upper()} RESOLVED. " + why})
            n += 1

    if not dry_run:
        for old, new in NORMALISE:
            for side in ("buyer", "seller"):
                r = conn.execute(text(
                    f"update transactions set {side}_canonical = :new "
                    f"where {side}_canonical = :old"), {"new": new, "old": old})
                if r.rowcount:
                    log.info("normalised %d %s rows: %r -> %r", r.rowcount,
                             side, old, new)
        for rid, note in LEADS:
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
