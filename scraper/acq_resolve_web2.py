r"""Second web-resolution pass, worked by dollar value descending.

Also handles the first case where the record and the press DISAGREE about who
bought a building, and the honest answer is to un-resolve a row rather than
pick a side.

101 SEAPORT BOULEVARD, $452M, 12 April 2016. Skanska's own release, Goodwin's
deal record, Connect CRE and The Boston Globe all say Skanska sold 101 Seaport
to UNION INVESTMENT REAL ESTATE for exactly that sum on that date. The
assessment roll's owner of record for the parcel is WS SEAPORT L-1 LLC, which
the pattern layer resolved to WS Development.

Both are probably true of different things. WS Development is the master
developer of Seaport Square and holds the ground; Union Investment bought the
building. The row conflates them, because the buyer on every spine row is
derived from the parcel's owner of record, and that derivation assumes the last
recorded sale produced the current owner. On a parcel later condominiumised or
reconfigured, it does not.

So buyer_canonical is REVERTED to null on that row. Crediting WS Development
with $452M it did not pay would be exactly the poisoning the rules exist to
prevent, and asserting Union Investment over a record that names someone else
would be substituting a guess for the evidence. The seller is not in doubt and
is recorded.

125 BROADWAY is a milder version of the same thing. BXP's acquisition from
Biogen in September 2022 is documented everywhere, and Biogen's own statement
gives gross proceeds of nearly $603M against this row's $602.84M. But the
record grantor is NORTH PARCEL LIMITED PARTNERSHIP, not Biogen. The buyer is
recorded and the seller left null.

    python scraper/acq_resolve_web2.py --apply
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

RESOLUTIONS = [
    ("121 Seaport BLVD", "seller", "Skanska", 455_000_000,
     'Skanska\'s own release, "Skanska Sells 100 percent leased 121 Seaport", '
     'and Skanska Group: "Skanska sells office tower in Boston, USA, for USD '
     '455M". GlobeSt dates it 14 December 2018 and names the buyer as the '
     'American Realty Advisors / Norges Bank venture already recorded on this '
     'row. Address, date and price all match.'),

    ("125 BROADWAY LLC", "buyer", "BXP", 602_840_000,
     'BusinessWire, "BXP Expands Life Sciences Portfolio in Kendall Square in '
     'Cambridge, MA", and Boston Real Estate Times: BXP completed the '
     'acquisition of 125 Broadway, a six-storey, 271,000 SF laboratory property, '
     'in September 2022. Biogen\'s own statement gives gross proceeds of nearly '
     '$603 million on the sale-leaseback, matching this row\'s $602,840,000; the '
     '$592M widely reported is BXP\'s net figure.'),

    ("DW NP PROPERTY", "buyer", "DivcoWest", None,
     'BLDUP and The Real Reporter: DivcoWest acquired the 45-acre NorthPoint '
     'development site in East Cambridge for $291 million in August 2015 and '
     'rechristened it Cambridge Crossing. The entity prefix DW NP is DivcoWest '
     'NorthPoint, and DivcoWest lists Cambridge Crossing in its own portfolio.'),

    ("CHARLES PARK TWO", "seller",
     "The Davis Cos. / Principal Real Estate Investors", None,
     'Boston Real Estate Times: "Davis & Principal Real Estate Investors Sell '
     'Charles Park in Cambridge for $815 Million to an affiliate of Alexandria '
     'Real Estate Equities." Corroborated by Newmark\'s release and Connect CRE. '
     'The $815M is the whole campus; this row is one parcel\'s allocation at '
     '$775M, and the counterparty already recorded on it is Alexandria.'),
]

# Fund-name prefixes decoded by inspection rather than search, because the fund
# name IS the sponsor's name and there is nothing to look up:
#   CLPF  Clarion Lion Properties Fund, Clarion Partners' flagship
#   MPT   Medical Properties Trust, buying from Steward on this row
INSPECTION = [
    ("CLPF-", "buyer", "Clarion Partners",
     "CLPF is the Clarion Lion Properties Fund, Clarion Partners' flagship "
     "open-end fund. The counterparty and asset are consistent with it."),
    ("MPT OF ", "buyer", "Medical Properties Trust",
     "MPT is Medical Properties Trust. The seller on this row is STEWARD CARNEY "
     "HOSPITAL INC, and MPT's acquisition of the Steward hospital portfolio is "
     "a matter of public record."),
]

# Rows where the record and the press name different buyers. See the module
# docstring: the resolution is removed, not decided.
CONFLICTS = [
    (569, "buyer",
     "CONFLICT, BUYER RESOLUTION REMOVED. The pattern layer resolved this row's "
     "record owner, WS SEAPORT L-1 LLC, to WS Development. But Skanska's own "
     "release, Goodwin's deal record, Connect CRE and The Boston Globe all state "
     "that Skanska sold 101 Seaport to UNION INVESTMENT REAL ESTATE for $452 "
     "million on 12 April 2016 -- this row's exact date and price. Both can be "
     "true of different things: WS Development is the master developer of "
     "Seaport Square and holds the ground, while Union Investment bought the "
     "building. The buyer on every spine row is derived from the parcel's owner "
     "of record, which assumes the last recorded sale produced the current "
     "owner; on a parcel later condominiumised that assumption fails. Crediting "
     "WS Development with $452M it did not pay would poison the rankings, and "
     "asserting Union Investment over a record naming someone else would "
     "substitute a guess for evidence. Left null. The seller is not in doubt."),
]


def main(dry_run: bool):
    conn = engine.connect()
    n = 0

    for rid, side, note in CONFLICTS:
        cur = conn.execute(text(
            f"select {side}_canonical from transactions where id = :id"),
            {"id": rid}).scalar()
        if cur and not dry_run:
            conn.execute(text(f"""
                update transactions
                   set {side}_canonical = null, {side}_confidence = null,
                       {side}_resolution_basis = null,
                       notes = coalesce(notes,'') || :n
                 where id = :id"""), {"n": " | " + note, "id": rid})
        log.info("conflict id=%d %s: %s", rid, side,
                 f"reverted {cur}" if cur else "already null")

    for key, side, sponsor, price, passage in RESOLUTIONS:
        q = (f"select id from transactions where address like :k "
             f"and coalesce({side}_canonical,'') = ''") if " " in key and key[0].isdigit() \
            else (f"select id from transactions where upper(coalesce({side},'')) like :k "
                  f"and coalesce({side}_canonical,'') = ''")
        params = {"k": f"%{key}%"}
        if price:
            q += " and price = :p"
            params["p"] = price
        rows = conn.execute(text(q), params).fetchall()
        if not rows:
            log.warning("no unresolved %s row matching %r", side, key)
            continue
        log.info("%-24s %-6s -> %-46s (%d rows)", key[:24], side, sponsor[:46],
                 len(rows))
        if not dry_run:
            for (rid,) in rows:
                conn.execute(text(f"""
                    update transactions
                       set {side}_canonical = :s,
                           {side}_confidence = 'web_corroborated',
                           {side}_resolution_basis = 'web',
                           notes = coalesce(notes,'') || :n
                     where id = :id"""), {
                    "s": sponsor, "id": rid,
                    "n": (f" | {side.upper()} RESOLVED TO SPONSOR. Record entity "
                          f"kept verbatim. Evidence: " + passage)})
                n += 1

    for key, side, sponsor, why in INSPECTION:
        rows = conn.execute(text(
            f"select id from transactions where upper(coalesce({side},'')) like :k "
            f"and coalesce({side}_canonical,'') = ''"), {"k": f"%{key}%"}).fetchall()
        if not rows:
            log.warning("no unresolved %s row matching %r", side, key)
            continue
        log.info("%-24s %-6s -> %-46s (%d rows, by inspection)", key[:24], side,
                 sponsor[:46], len(rows))
        if not dry_run:
            for (rid,) in rows:
                conn.execute(text(f"""
                    update transactions
                       set {side}_canonical = :s,
                           {side}_confidence = 'pattern_matched',
                           {side}_resolution_basis = 'pattern',
                           notes = coalesce(notes,'') || :n
                     where id = :id"""), {
                    "s": sponsor, "id": rid,
                    "n": f" | {side.upper()} RESOLVED FROM THE FUND NAME. " + why})
                n += 1

    if not dry_run:
        conn.commit()
    tot = conn.execute(text("select count(*) from transactions")).scalar()
    log.info("\n%d rows resolved", n)
    for side in ("buyer", "seller"):
        v, d = conn.execute(text(
            f"select count(*), sum(coalesce(price,0)) from transactions "
            f"where coalesce({side}_canonical,'') <> ''")).first()
        log.info("%s_canonical: %d of %d rows, $%.2fB", side, v, tot, (d or 0) / 1e9)
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
