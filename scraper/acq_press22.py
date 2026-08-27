r"""Twenty-second pass. A SIXTH half-recorded venture, and NW confirmed.

GRIFFITH PROPERTIES IS HALF A VENTURE AGAIN, AND IT IS THE SAME FIRM AS LAST
TIME. 63 Sprague Street read "Griffith Properties" alone, written by the care-of
pass. Newmark's release is titled "Griffith Properties Acquires Boston Dedham
Commerce Park WITH JOINT VENTURE PARTNER DUNE REAL ESTATE PARTNERS LP" -- a
632,188 SF, five-building, 22-acre industrial portfolio bought off-market from
First Highland for $76 million in November 2020. Two passes ago the same firm
turned out to be half of Griffith/Artemis at 20 Guest Street, also written from a
care-of line.

That is the SIXTH venture in this table recorded as a single party, and the
third of the six where the care-of line was the source. A care-of line names one
addressee. It structurally cannot name a partnership, so every venture it
touches comes out halved.

    535-545 Boylston   ASB -> Shimizu / Capital Security / Brickman
    733 Concord Ave    PPF -> Morgan Stanley PPF / King Street
    100 Franklin St    Synergy -> Clarion / Synergy
    20 Guest St        Griffith -> Griffith / Artemis          (care-of)
    327-333 Summer     ASB -> ASB / Lincoln
    63 Sprague St      Griffith -> Griffith / Dune             (care-of)

NW IS NORTHWOOD INVESTORS, confirmed against a named property. Connect CRE and
Northwood's own portfolio page both carry 230 Congress Street, a 151,000 SF
office, retail and telecommunications building the firm bought in 2015 for $77
million, and the buyer entity on that row is NW 230 CONGRESS STREET. Northwood
is independently in this table buying Tower Point from Rockpoint in 2017. The
prefix is therefore applied to 60 First Street in Cambridge as well.

FIRST HIGHLAND SELLS TWICE IN THIS TABLE, seven years apart -- Boston Dedham
Commerce Park to Griffith and Dune in 2020, and the Yard 5 portfolio to
Intercontinental in 2022. Both industrial, both off-market-ish, both to
institutional buyers. That is a disposition programme, and it only becomes
visible once both sellers are resolved.

    python scraper/acq_press22.py --apply
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

NORTHWOOD = (
    "NW IS NORTHWOOD INVESTORS, confirmed against a named property. Connect CRE "
    "(\"TEI Makes First Boston Office Acquisition with Deal for 230 Congress "
    "St.\") and Northwood's own portfolio page both record Northwood acquiring "
    "230 Congress Street -- 151,000 SF of office, retail and telecommunications "
    "space -- in 2015 for $77 million, and that row's buyer entity is NW 230 "
    "CONGRESS STREET. Northwood is independently established in this table buying "
    "Tower Point at 27-43 Wormwood Street from Rockpoint in 2017."
)

RESOLVE = [
    (1444, "buyer", "New York Life Real Estate Investors", False,
     'Bisnow, "NY Life Snaps Up One Bowdoin Square": New York Life Real Estate '
     'Investors acquired the eleven-storey boutique office building at 15 New '
     'Chardon Street for $61.8 million in 2016 -- this row exactly. Built as seven '
     'storeys in 1972, fully renovated with four floors added in 1989. The seller '
     'side already read Brickman Associates, from the entity BRICKMAN ONE BOWDOIN '
     'LLC, and the same reporting confirms it: Brickman had bought the building in '
     '2006 for $41 million. Entity and press agree independently.'),

    (1155, "buyer", "Hawkins Way Capital", False,
     'Banker & Tradesman, "Copley Square Hotel Sold to West Coast Investor for '
     '$66M", with Bisnow, Connect CRE, GlobeSt and Hawkins Way\'s own release: the '
     '143-room Copley Square Hotel at 47 Huntington Avenue, in operation since '
     '1891 and Boston\'s second-oldest hotel, sold on 4 November 2019 for $66 '
     'million. This row is $66,000,000 in November 2019. Hawkins Way Capital is '
     'independently in this table buying the Sheraton Boston at 39 Dalton Street '
     'with Varde Partners.'),
    (1155, "seller", "Barings (MassMutual)", False,
     'Same reporting: Hawkins Way acquired the property from MASSMUTUAL\'S BARINGS '
     'unit, which had owned it since 2013, with JLL marketing on Barings\' behalf.'),

    (1516, "buyer", "Northwood Investors", False, NORTHWOOD +
     " AFTERWARDS: Bisnow's deal sheet of 24 June 2026 records Hudson Assembly -- "
     "a venture of Evan Papanastasiou and Noam Ron backed by Time Equities -- "
     "buying 230 Congress from Northwood for $23.7 million, a 69% discount to this "
     "row. Hudson Assembly / Time Equities already appears in this table."),

    (1691, "buyer", "Northwood Investors", False, NORTHWOOD +
     " This row's buyer entity is NW CAMBRIDGE PROPERTY OWNER LLC, the same "
     "convention. NOTE ON THE SELLER, which stays blank: US BANK TRUST NATIONAL "
     "ASSOCIATION is a SECURITISATION TRUSTEE holding title for bondholders, not "
     "an owner -- the same reason Asset Preservation Inc was refused at 711 "
     "Atlantic Avenue. A conveyance out of a CMBS trust means the previous owner "
     "lost the building; the trustee is the mechanism, not a party with an "
     "interest to rank."),

    (1091, "buyer", "Griffith Properties / Dune Real Estate Partners", True,
     'CORRECTION AND COMPLETION. This row read "Griffith Properties" alone, '
     'written from the assessment roll\'s care-of line. Newmark\'s release is '
     'titled "Griffith Properties Acquires Boston Dedham Commerce Park WITH JOINT '
     'VENTURE PARTNER DUNE REAL ESTATE PARTNERS LP": a 632,188 SF, five-building '
     'industrial portfolio across 22 acres in Hyde Park and Dedham, bought '
     'off-market for $76 million in November 2020. SIXTH VENTURE IN THIS TABLE '
     'FOUND RECORDED AS ONE PARTY, and the third of those six sourced from a '
     'care-of line -- which names one addressee and therefore cannot represent a '
     'partnership at all. The same firm was half of Griffith/Artemis at 20 Guest '
     'Street, also from a care-of line. PORTFOLIO ALLOCATION: this row is one '
     'address of five buildings.'),
    (1091, "seller", "First Highland Management & Development", False,
     'Same release: the portfolio was acquired off-market from FIRST HIGHLAND. '
     'First Highland now sells twice in this table -- Boston Dedham Commerce Park '
     'here in 2020, and the Yard 5 industrial portfolio at 50 Industrial Drive to '
     'Intercontinental in 2022 -- which reads as a disposition programme rather '
     'than two unrelated trades, and is only visible once both sellers resolve.'),
]


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
        log.info("id=%-5s %-6s %-32s -> %s%s", rid, side, (cur[0] or "")[:32],
                 tag, sponsor)
        if not dry_run:
            basis = "prefix_confirmed" if "NW IS NORTHWOOD" in why else "web"
            conf = ("registry_confirmed" if basis == "prefix_confirmed"
                    else "web_corroborated")
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
