r"""Thirteenth pass. BRREP confirmed, and a three-owner chain reconstructed.

BRREP = BROOKFIELD, confirmed against a named property. The Real Reporter, "Hong
Kong Based Nan Fung Life Sciences Buys 51 Sleeper for $115M from Brookfield",
Connect CRE, "Nan Fung Life Sciences Buys Boston Offices from Brookfield", and
NEREJ all name Brookfield as the seller of the eight-storey, 152,000 SF building
at 51 Sleeper Street on 24 January 2020 for $115 million. The seller entity on
this row is BRREP 51 SLEEPER STREET LLC. Brookfield had bought it at the end of
2018 for $91 million, so it made about $24M in thirteen months.

TOWER POINT'S WHOLE OWNERSHIP CHAIN IS NOW IN VIEW, from one Real Reporter brief
plus what the table already held:

    2015 autumn   The Davis Companies -> Rockpoint Group       $62,100,000
    2017 spring   Rockpoint Group     -> Northwood Investors   $86,850,000  <- this row

The press says Rockpoint sold "upwards of $88 million" as of April 2017 and this
row records $86,850,000 in May 2017; a recording a month behind the reported
agreement, at a number the press rounded up, is the ordinary pattern. Rockpoint
appears four times in this table now -- 75-101 Federal, 99 Summer, 226 Causeway
and Tower Point -- which is a real portfolio picture rather than four unrelated
rows.

A SELLER THAT LOOKS RESOLVABLE AND IS NOT. 745 Atlantic Avenue's seller entity
is MOF-745 ATLANTIC AV BOSTON. The building's history is well documented: it
sold for $54.65M in 2003, changed hands in 2008 inside a $1.7 billion portfolio
involving Beacon Capital Partners and Charter Hall Office REIT of Sydney, and
Oxford Properties bought it in May 2015 for $114.5 million at $657/SF. So the
2015 seller is very probably one or both of Beacon and Charter Hall. "Very
probably" is not a resolution, and MOF matches neither name, so it stays blank
with the chain recorded.

    python scraper/acq_press13.py --apply
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
    (1137, "seller", "Brookfield", "prefix_confirmed",
     'The Real Reporter, "Hong Kong Based Nan Fung Life Sciences Buys 51 Sleeper '
     'for $115M from Brookfield", with Connect CRE and NEREJ reporting the same: '
     'Nan Fung closed on the eight-storey, 152,000 SF building on 24 January 2020 '
     'for $115 million, planning a 50/50 office and lab conversion. Brookfield had '
     'bought it at the end of 2018 for $91 million. THIS CONFIRMS BRREP AS '
     'BROOKFIELD against a named property: the seller entity is BRREP 51 SLEEPER '
     'STREET LLC. Brookfield is independently in this table as the seller of 75 '
     'State Street.'),

    (1382, "buyer", "Northwood Investors", "web",
     'The Real Reporter, "Rockpoint Via Newmark Juggling $450M+ in Hub Office '
     'Trades": Rockpoint Group bought Tower Point at 27-43 Wormwood Street for '
     '$62.1 million in autumn 2015 and sold it to Northwood Investors for upwards '
     'of $88 million as of April 2017. This row records $86,850,000 in May 2017 -- '
     'a recording a month behind the reported agreement at a figure the brief '
     'rounded up.'),
    (1382, "seller", "Rockpoint Group", "web",
     'Same brief. Boston Real Estate Times separately records The Davis Companies '
     'selling Tower Point in 2015, which is the leg before this one, so the chain '
     'reads Davis -> Rockpoint (autumn 2015, $62.1M) -> Northwood (spring 2017, '
     'this row). Rockpoint now appears four times in this table: 75-101 Federal, '
     '99 Summer Street, 226 Causeway and Tower Point.'),

    (1341, "buyer", "PGIM Real Estate", "web",
     'Multi-Housing News, "Tower at One Greenway Changes Hands", and BLDUP: PGIM '
     'Real Estate bought One Greenway at 99 Kneeland Street, on the edge of '
     'Chinatown, for $144.5 million in 2017 -- this row to the dollar. The '
     'building opened in 2015 with 217 market-rate units and a 135-space, '
     'three-level garage, which is why this row is addressed as a garage unit. The '
     'seller of record, PARCEL 24 NORTH LLC, is a parcel designation and is not '
     'resolved: One Greenway sits on the old Parcel 24 and both the Asian '
     'Community Development Corporation and New Boston Fund are associated with '
     'that development, so guessing which entity conveyed is not resolution.'),

    (1538, "seller", "Equity Residential", "web",
     'NEREJ, "Cushman & Wakefield handle $123.3 million sale of 50 Staniford '
     'Street": C&W acted ON BEHALF OF EQUITY RESIDENTIAL in the $123.3 million '
     'sale of the ten-storey, 193,230 SF class A medical office building on the '
     'Massachusetts General Hospital campus, 100% let and anchored by MGH at 74% '
     'of the building, with Ophthalmic Consultants of Boston and Boston Eye '
     'Surgery. The buyer entity RAR2 50 STANIFORD LLC is NOT decoded and the buyer '
     'stays blank.'),
]

FAMILY = ("BRREP%", "Brookfield", "prefix_confirmed",
          "BRREP IS BROOKFIELD, confirmed against a named property: The Real "
          "Reporter, Connect CRE and NEREJ all name Brookfield as the seller of 51 "
          "Sleeper Street to Nan Fung for $115M in January 2020, and that row's "
          "seller entity is BRREP 51 SLEEPER STREET LLC.")

NOTES = [
    (1533, "745 ATLANTIC AVENUE SELLER: THE CHAIN IS KNOWN, THE SELLER IS NOT. "
           "Boston Office Spaces records Oxford Properties buying 745 Atlantic "
           "Avenue for $114.5 million, $657/SF, closing 29 May 2015 -- this row -- "
           "and gives the history: $54.65M in 2003, then a 2008 change of hands "
           "inside a $1.7 BILLION office portfolio involving Beacon Capital "
           "Partners and Charter Hall Office REIT of Sydney. So the 2015 seller is "
           "very probably Beacon, Charter Hall, or their venture. The record "
           "entity is MOF-745 ATLANTIC AV BOSTON, which matches neither name and "
           "is not decoded. Very probably is not a resolution; the chain is "
           "recorded so a licensed feed can close it."),
    (1647, "36-64 WHITTEMORE AVENUE, $125,000,000, August 2020. SEARCHED AND NOT "
           "FOUND. Whittemore Avenue in North Cambridge is overwhelmingly "
           "residential and every search on the street returns condominium and "
           "single-family sales between roughly $800,000 and $1,400,000. A "
           "nine-figure commercial trade on it is invisible to the press."),
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
        log.info("id=%-5s %-6s %-34s -> %-30s [%s]", rid, side,
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

    pat, sponsor, basis, why = FAMILY
    for side in ("buyer", "seller"):
        for rid, ent, price, addr in conn.execute(text(f"""
                select id, {side}, price, address from transactions
                 where coalesce(quarantined,0) = 0
                   and upper(coalesce({side},'')) like :k
                   and coalesce({side}_canonical,'') = ''"""), {"k": pat}):
            log.info("id=%-5s %-6s %-34s -> %-30s [family sweep]", rid, side,
                     (ent or "")[:34], sponsor)
            if not dry_run:
                conn.execute(text(f"""
                    update transactions
                       set {side}_canonical = :s,
                           {side}_confidence = 'registry_confirmed',
                           {side}_resolution_basis = :b,
                           notes = coalesce(notes,'') || :n
                     where id = :id"""), {
                    "s": sponsor, "id": rid, "b": basis,
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
