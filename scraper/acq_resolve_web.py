r"""Layer 4 of entity resolution: web corroboration, worked top-down by dollars.

The Secretary of the Commonwealth corporate database -- layer 2, and the one
that would have resolved the long tail -- is unreachable. corp.sec.state.ma.us
returns the same Imperva page as masslandrecords: a 403 with
_Incapsula_Resource, visid_incap_* cookies and NOINDEX/NOFOLLOW. That is a
deployed bot control on a state portal, not an absent access policy, so it was
not attempted further. Layer 4 therefore carries weight it was not meant to.

Worked in descending dollar order, because a ranking is decided by its top rows
and the tail cannot change it. Every entry below is a named publication naming
the sponsor behind a named vehicle.

THE PREFIXES DECODE, AND THAT IS THE REAL PRIZE. Several of these are not
one-off answers but keys to a naming convention:

    MT   BACK BAY ONE          Mori Trust
    CSP  109 BROOKLINE         Creative Science Properties, now IQHQ
    BDC  SUMMER ST 121A        Benderson Development Company
    SVF  SEAPORT OWNER         the American Realty / Norges venture

Once "CSP" is known, IQHQ-645 BEACON and its siblings follow, which is why each
decoded prefix is folded back into the pattern table rather than left here.

STILL UNDECODED, and named so the next pass starts here rather than rediscovering
them: HART (28 State Street, $417.6M), SPUS7 (100 High Street, $370M), IREP (the
Newbury Hotel, $196M), LS and AP (Cambridge Discovery Park, $445M), FSP (Pier 4,
$450M). Each is one search away, and each would resolve a nine-figure row.

A JOINT VENTURE RESOLVES TO ITS NAMED PARTNERS, not to one of them. 53 State
Street was bought by Allianz, MassPRIM and Beacon Capital together; recording
only Beacon would credit it with $845M it did not put up alone. Where a source
names a venture, the canonical name carries both parties, and the ranking shows
what the source said rather than a tidier fiction.

    python scraper/acq_resolve_web.py --apply
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

# entity string (upper, LIKE-matched) -> (side, sponsor, sources)
RESOLUTIONS = [
    ("500 BOYLSTON & 222 BERKELEY", "buyer", "Oxford Properties Group / JPMorgan Chase",
     'The Boston Globe, 20 Nov 2015, and CoStar: "A group led by Canadian '
     'investment firm Oxford Properties Group finalized its purchase of 500 '
     'Boylston and 222 Berkeley in the Back Bay for $1.29 billion. Oxford, which '
     'invests on behalf of the Ontario Municipal Employees Retirement System, '
     'joined with JPMorgan Chase & Co. to buy the two-building complex from '
     'Blackstone Group LP." Corroborated by Jones Day\'s deal record.'),
    ("FIVE HUNDRED BOYLSTON WEST", "seller", "Blackstone (Equity Office)",
     'Same reporting: the complex was sold BY Blackstone Group, through Equity '
     'Office Management. Jones Day: "Blackstone Group through Equity Office '
     'Management completes $1.3 billion sale of two office buildings in Boston '
     'to JPMorgan Chase and Oxford Properties."'),
    ("TWO TWENTY TWO BERKELEY", "seller", "Blackstone (Equity Office)",
     'Same transaction as 500 Boylston, sold by Blackstone through Equity '
     'Office Management; the two buildings were conveyed at separately '
     'allocated prices of $755.3M and $534.7M.'),

    ("100 SUMMER OWNER LLC", "buyer", "Rockpoint Group",
     'Boston Real Estate Times: "Blackstone and EQ Office Sell 100 Summer Street '
     'to Rockpoint Group." Sale recorded 27 September 2019 at $806,000,000, the '
     'largest Boston office deal of that year. Corroborated by CoStar.'),
    ("ONE HUNDRED SUMMER ST LLC", "seller", "Blackstone (Equity Office)",
     'Same reporting: Blackstone and EQ Office were the sellers.'),

    ("53 STATE PROPERTY LP", "buyer",
     "Allianz Real Estate / MassPRIM / Beacon Capital Partners",
     'Boston Real Estate Times: "Boston\'s Trophy Building 53 State Street Sold '
     'for $845 Million to a Joint Venture that Includes Beacon Capital '
     'Partners." The Boston Globe, 6 Dec 2018, names the investment group as '
     'Allianz Real Estate, the Massachusetts Pension Reserves Investment '
     'Management Board and Beacon Capital Partners. Recorded as the venture '
     'rather than any one partner, because crediting Beacon alone would assign '
     'it $845M it did not put up by itself.'),
    ("FIFTY THREE STATE STREET", "seller", "UBS Asset Management",
     'The Boston Globe, 6 Dec 2018: the seller was a fund managed by UBS Asset '
     'Management of Zurich, which had bought the building for $610 million in '
     '2011. Corroborated by NEREJ and MBA Newslink on the Newmark Knight Frank '
     'brokerage.'),

    ("MT BACK BAY ONE", "buyer", "Mori Trust",
     'Commercial Property Executive, "Mori Trust Enters US with Big Boston Buy", '
     'and REBusinessOnline: "Mori Trust Acquires 10 St. James and 75 Arlington '
     'Office Buildings in Boston for $673M." The entity prefix MT is Mori Trust. '
     'It was the Japanese group\'s first US acquisition.'),
    ("ST JAMES/ARLINGTON", "seller", "Liberty Mutual",
     'The Boston Globe, 5 Jan 2017: "Liberty Mutual sells two Back Bay buildings '
     'for $673 million." Liberty Mutual\'s own release confirms it marketed 10 '
     'St. James Avenue and 75 Arlington Street for sale, the two buildings '
     'totalling 824,772 SF.'),

    ("CSP-109 BROOKLINE", "buyer", "IQHQ",
     'GlobeSt, 18 Feb 2020: "Newly Formed REIT Pays $270M for Fenway '
     'Office-Medical Property in Boston" -- IQHQ Inc., FOUNDED AS CREATIVE '
     'SCIENCE PROPERTIES, acquired 109 Brookline Ave. from Equity Commonwealth. '
     'That founding name is the CSP prefix, which also explains the sibling '
     'IQHQ-branded vehicles in this table.'),
    ("HUB PROPERTIES TRUST", "seller", "Equity Commonwealth",
     'GlobeSt and the Boston Business Journal both name Equity Commonwealth '
     '(NYSE: EQC) as the seller of 109 Brookline Avenue; Hub Properties Trust is '
     'an EQC subsidiary. Corroborated by Equity Commonwealth\'s own 8-K.'),

    ("245 SUMMER STREET LLC", "buyer", "Fidelity Investments (Pembroke)",
     'CoStar: "Fidelity Buys Back Boston HQ in $729 Million Deal." Commercial '
     'Real Estate Direct: "U.S. Bank Lends $684Mln for Fidelity\'s Purchase of '
     '245 Summer St. in Boston." The acquiring vehicle is Horizon Real Estate '
     'Investors LLC, a Fidelity affiliate; Pembroke is Fidelity\'s real estate '
     'arm. Closed 8 April 2020.'),
    ("BDC SUMMER ST 121A", "seller", "Benderson Development Co.",
     'The same reporting names Benderson Development Company as the seller, and '
     'the entity prefix BDC is Benderson Development Company.'),

    ("SVF SEAPORT OWNER", "buyer", "American Realty Advisors / Norges Bank",
     'GlobeSt, 14 Dec 2018: "Skanska Sells 121 Seaport Office Tower for $455M to '
     'American Realty/Norges JV." Skanska\'s own release and The Boston Globe '
     'confirm the $455.0M price on the 17-storey, 400,342 SF tower. Recorded as '
     'the venture, not one partner.'),

    ("UNITED STATES OF AMERICA", "seller", "U.S. Government (GSA)",
     'Read directly rather than researched: the 182 Binney Street parcel is the '
     'Volpe Center site, conveyed by the federal government to MIT, and the '
     'counterparty on this row is MIT VOLPE FEE OWNER LLC. The record grantor is '
     'the United States of America, disposed through the General Services '
     'Administration.'),
]


def main(dry_run: bool):
    conn = engine.connect()
    n = 0
    for key, side, sponsor, passage in RESOLUTIONS:
        rows = conn.execute(text(
            f"select id, {side} from transactions "
            f"where upper(coalesce({side},'')) like :k "
            f"and coalesce({side}_canonical,'') = ''"),
            {"k": f"%{key}%"}).fetchall()
        if not rows:
            log.warning("no unresolved %s row matching %r", side, key)
            continue
        log.info("%-30s %-6s -> %-46s (%d rows)", key[:30], side, sponsor[:46],
                 len(rows))
        if not dry_run:
            for rid, _name in rows:
                conn.execute(text(f"""
                    update transactions
                       set {side}_canonical = :s,
                           {side}_confidence = 'web_corroborated',
                           {side}_resolution_basis = 'web',
                           notes = coalesce(notes,'') || :n
                     where id = :id"""), {
                    "s": sponsor, "id": rid,
                    "n": (f" | {side.upper()} ENTITY RESOLVED TO SPONSOR. The "
                          f"record entity is kept verbatim in `{side}`; "
                          f"`{side}_canonical` holds the sponsor. Evidence: "
                          + passage)})
                n += 1
    if not dry_run:
        conn.commit()
    tot = conn.execute(text("select count(*) from transactions")).scalar()
    log.info("\n%d rows resolved", n)
    for side in ("buyer", "seller"):
        v = conn.execute(text(
            f"select count(*) from transactions "
            f"where coalesce({side}_canonical,'') <> ''")).scalar()
        log.info("%s_canonical: %d of %d (%.0f%%)", side, v, tot, v / tot * 100)
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
