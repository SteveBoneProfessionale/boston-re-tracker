r"""Fourteenth pass. A second partner-continuation, and a negative result worth more.

733 CONCORD AVENUE: KING STREET IS ON BOTH SIDES. The two record entities are

    seller   PPF OFF KING 733 CONCORD OWNER LLC
    buyer    CPI/KING 733 CONCORD OWNER LLC

The seller side already read "Morgan Stanley Prime Property Fund" alone, from the
PPF prefix confirmed at 200 CambridgePark Drive. But KING sits in the same slot
in BOTH strings. King Street Properties is confirmed in this table against a
named property -- the Globe and GlobeSt both report it selling 200 CambridgePark
Drive for $165.5M in December 2015 -- and West Cambridge life science is exactly
its market. So this is Morgan Stanley exiting, CPI entering, and King Street
staying in place as the operating partner.

That is the SECOND partner-continuation found this way, after Brickman selling
535-545 Boylston Street with Investcorp and immediately re-entering with Shimizu
and Capital Security. Neither is an affiliated transfer and neither is
quarantined -- real capital changed hands at arm's length both times -- but
neither is a clean whole-asset trade either, and in both cases recording only
ONE partner is what hid it. The joint-venture rule is not bookkeeping pedantry;
it is the only thing that makes this pattern visible at all.

CPI IS STILL NOT EXPANDED, and it now appears twice with two different partners:
CPI/BRICKMAN at 237 Putnam Avenue and CPI/KING here. That pattern suggests an
institutional co-investor placing money with local operators, which makes the
temptation to name it stronger and the discipline more necessary.

THE NEGATIVE RESULT. A sweep matched every unresolved entity above $25M against
distinctive word tokens from the 60-odd sponsors already established in this
table. It returned 33 candidates. THIRTY-ONE WERE FALSE, and they failed the same
way: entity names are built out of STREET AND PLACE NAMES, and Boston's street
and place names are also firm names.

    CAMBRIDGE 1030 MASS AVE LLC     matched "Cambridge College"
    920 STORAGE LLC                 matched "Extra Space Storage"
    ONE 85 FRANKLIN ST              matched a sponsor containing "Franklin"
    VILLAGE AT CHESTNUT HILL LLC    matched "Chestnut Hill Realty"
    KENMORE SQUARE HOTEL LLC        matched a sponsor containing "Square"

A 6% hit rate means token matching against known sponsors is not a resolution
method here; it is a generator of plausible wrong answers, which is the single
most expensive thing this table can contain. Recorded so nobody builds it again.

    python scraper/acq_press14.py --apply
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

KING = (
    "KING STREET PROPERTIES IS ON BOTH SIDES OF THIS TRANSACTION. The seller "
    "entity is PPF OFF KING 733 CONCORD OWNER LLC and the buyer entity is CPI/KING "
    "733 CONCORD OWNER LLC -- KING in the same slot in both. King Street "
    "Properties is confirmed against a named property elsewhere in this table: the "
    "Boston Globe and GlobeSt both report it selling 200 CambridgePark Drive for "
    "$165.5 million in December 2015, and West Cambridge life science is its "
    "market. PPF is confirmed as Morgan Stanley Prime Property Fund at that same "
    "address. So the structure is Morgan Stanley exiting, CPI entering, and King "
    "Street staying on as operating partner. NOT AN AFFILIATED TRANSFER and not "
    "quarantined: Morgan Stanley genuinely exited and CPI genuinely entered, so "
    "real capital changed hands at arm's length. But it is a partial continuation "
    "rather than a clean whole-asset trade, and recording only one partner on "
    "either side would hide that completely -- which is exactly what happened at "
    "535-545 Boylston Street until this pass's method caught it. CPI IS NOT "
    "EXPANDED: it appears twice in this table with two different local partners, "
    "CPI/BRICKMAN at 237 Putnam Avenue and CPI/KING here, which suggests an "
    "institutional co-investor, but no source found says what it stands for."
)

RESOLVE = [
    (1658, "buyer", "CPI / King Street Properties", True, KING),
    (1658, "seller", "Morgan Stanley Prime Property Fund / King Street Properties",
     True, "CORRECTION AND COMPLETION. This side read \"Morgan Stanley Prime "
           "Property Fund\" alone, written from the confirmed PPF prefix. The "
           "entity is PPF OFF KING 733 CONCORD OWNER LLC and the KING element was "
           "dropped. " + KING),
]

SCAN_NOTE = (
    " | METHOD NOTE, RECORDED ON THIS ROW BECAUSE IT WAS ONE OF THE FALSE "
    "POSITIVES. A sweep matched every unresolved entity above $25M against "
    "distinctive word tokens taken from the sponsors already established in this "
    "table. It produced 33 candidates and 31 of them were WRONG, all failing the "
    "same way: entity names are built out of street and place names, and Boston's "
    "street and place names are also firm names. This row's entity matched a "
    "sponsor purely on a shared place word. A 6% hit rate makes token matching a "
    "generator of plausible wrong answers rather than a resolution method, and a "
    "plausible wrong answer is the most expensive thing this table can hold. Not "
    "applied."
)
FALSE_POSITIVES = [1502, 1653, 1092, 1592, 1585, 1662, 1681, 1161, 1693, 1656,
                   1691, 1503, 964, 1685, 1514, 1145, 1414, 1177, 1224, 1245,
                   1689, 1237, 1520, 1551, 1116, 1043, 1225]


def main(dry_run: bool):
    conn = engine.connect()
    n = 0
    for rid, side, sponsor, force, why in RESOLVE:
        cur = conn.execute(text(
            f"select {side}, coalesce({side}_canonical,'') from transactions "
            f"where id = :id"), {"id": rid}).first()
        if not cur:
            log.warning("id %s missing", rid)
            continue
        if cur[1] and not force:
            log.info("id=%-5s %-6s already %s, skipped", rid, side, cur[1])
            continue
        tag = f"(WAS {cur[1]}) " if cur[1] else ""
        log.info("id=%-5s %-6s %-36s -> %s%s", rid, side, (cur[0] or "")[:36],
                 tag, sponsor)
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
        marked = 0
        for rid in FALSE_POSITIVES:
            marked += conn.execute(text(
                "update transactions set notes = coalesce(notes,'') || :n "
                "where id = :id"), {"n": SCAN_NOTE, "id": rid}).rowcount
        log.info("\ntoken-scan false positive recorded on %d rows", marked)
        conn.commit()

    log.info("%d sides written", n)
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
