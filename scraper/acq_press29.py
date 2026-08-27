r"""Twenty-ninth pass. TRPF confirmed, and an entity that turned out to be real.

TRPF IS NUVEEN, confirmed against a named property, and it unlocks two rows.
BLDUP, "Alduwaliya Acquires 99 Chauncy for $70.351 Million", and Boston Real
Estate Times: Alduwaliya bought 99 Chauncy Street and 101 Summer Street together
on 7 October 2019 -- 159,781 SF of office and ground-floor retail, 100% let to
19 tenants on a 7.23-year weighted average term -- FROM NUVEEN REAL ESTATE, with
CBRE acting for the seller. The seller entity on that row is TRPF 99/101 BOSTON
OFFICE, and 99/101 is exactly the two addresses named. TRPF is the TIAA Real
Property Fund, which Nuveen manages; TREA, the TIAA Real Estate Account, is
already confirmed as Nuveen in this table at 350 Washington Street. Two different
TIAA vehicles, one manager, both now confirmed against properties rather than
decoded.

PARK SQUARE REVIVAL CORP IS NOT A SHELL, AND AN EARLIER REFUSAL WAS RIGHT FOR
THE WRONG ASSET. A previous pass declined to read that entity as the Park Square
Building at 31 St James Avenue, on the ground that this row is a commercial
garage. Correct -- and it is also not a single-purpose vehicle concealing a
sponsor. The Real Reporter describes Cushman & Wakefield acting for "LONGTIME
STEWARDS PARK SQUARE REVIVAL CORP" in the $162.5 million Motor Mart Garage sale
to CIM Group, a 1,027-stall garage with 50,000 SF of retail, with Citibank
funding CIM's offer at $90.1 million. The entity names its own owner; it just
does not name the building it was assumed to belong to.

THE SAME VENTURE APPEARS ON BOTH ENDS OF A DIFFERENT BUILDING. Invesco Real
Estate sold 10 Fawcett Street to Healthpeak for $73 million in October 2021, and
Invesco with The Davis Cos. had bought it in 2019 for $59.7 million FROM
GRIFFITH PROPERTIES AND ARTEMIS REAL ESTATE PARTNERS -- the identical pairing
this table found at 20 Guest Street, where the care-of line had recorded only
Griffith. Two independent deals, same two partners, which is corroboration the
care-of line could never have supplied.

    python scraper/acq_press29.py --apply
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

TRPF = (
    "TRPF IS THE TIAA REAL PROPERTY FUND, MANAGED BY NUVEEN, confirmed against a "
    "named property. BLDUP (\"Alduwaliya Acquires 99 Chauncy for $70.351 "
    "Million\") and Boston Real Estate Times (\"Alduwaliya Purchases Two Boston "
    "Office and Retail Properties\") both record Nuveen Real Estate as the seller "
    "of 99 Chauncy Street and 101 Summer Street on 7 October 2019, with CBRE's "
    "Boston Capital Markets team acting for it -- and the seller entity on that "
    "row is TRPF 99/101 BOSTON OFFICE, naming both addresses. TREA, the TIAA Real "
    "Estate Account, is separately confirmed as Nuveen in this table at 350 "
    "Washington Street. Two TIAA vehicles, one manager, each confirmed against a "
    "property rather than decoded from initials."
)

RESOLVE = [
    (1164, "buyer", "Alduwaliya Asset Management", "web",
     'BLDUP, "Alduwaliya Acquires 99 Chauncy for $70.351 Million" -- this row to '
     'the dollar, October 2019. Boston Real Estate Times covers it as "Alduwaliya '
     'Purchases Two Boston Office and Retail Properties". 99 Chauncy is a 106,523 '
     'SF class B office building downtown; it sold together with 101 Summer '
     'Street, the pair totalling 159,781 SF of office and ground-floor retail, '
     '100% let to 19 tenants on a 7.23-year weighted average lease term. '
     'PORTFOLIO: this price covers two addresses, not one.'),
    (1164, "seller", "Nuveen Real Estate", "prefix_confirmed", TRPF),

    (1394, "buyer", "Nuveen Real Estate", "prefix_confirmed", TRPF +
     " This row's buyer entity is TRPF 201 NEWBURY STREET, the same convention. "
     "NOTE that the buyer on this row previously read \"Newmark Christopher "
     "Ruggiero\" -- a broker plus an executive, taken from the assessment roll's "
     "care-of line -- and was cleared in the canonical audit. The correct answer "
     "was reachable only after the wrong one was removed. The seller entity TWO 01 "
     "NEWBURY-PRINCE LLC names the asset, a retail condominium at the front of the "
     "former Prince School building, and stays undecoded."),

    (1437, "seller", "Park Square Revival Corp", "self_identifying",
     'The Real Reporter, "LA Firm Wraps Up $162.5M Motor Mart Sale Via C&W": '
     'Cushman & Wakefield acted as advisers to "longtime stewards PARK SQUARE '
     'REVIVAL CORP" in the sale of the Motor Mart Garage to CIM Group, a '
     '1,027-stall garage with 50,000 SF of retail, with Citibank funding CIM\'s '
     'offer through a $90.1 million loan. THE ENTITY IS THE OWNER, not a '
     'single-purpose vehicle -- which is why an earlier pass was right to refuse '
     'it as the Park Square BUILDING at 31 St James Avenue and wrong to assume it '
     'therefore concealed someone. The buyer side already read CIM Group / LAZ '
     'Parking Realty Investors.'),

    (1616, "seller", "Invesco Real Estate", "web",
     'Traded records the deal outright: "Healthpeak Properties Acquires Office In '
     'Cambridge, MA From INVESCO REAL ESTATE For $73M | 10 Fawcett Street", with '
     'Newmark\'s Boston Capital Markets Group representing the seller and procuring '
     'Healthpeak, October 2021. Bisnow covers it as "Healthpeak Bolsters Alewife '
     'Holdings With $73M Office Purchase". The asset is a six-storey, 132,000 SF '
     '1985 building on 2.5 acres, home to the Social Security Administration\'s '
     'Cambridge office. CONTEXT WORTH KEEPING: Invesco bought it in 2019 for $59.7 '
     'million WITH THE DAVIS COS., from GRIFFITH PROPERTIES AND ARTEMIS REAL '
     'ESTATE PARTNERS -- the same pairing this table established at 20 Guest '
     'Street. Only Invesco is named as the 2021 seller, so only Invesco is '
     'written; Davis may have exited earlier. Invesco is separately confirmed here '
     'at 179 Lincoln Street and 226 Causeway Street.'),
]

NOTES = [
    (1693, "100 CAMBRIDGEPARK DRIVE SELLER, $60,200,000, June 2017: A LEAD, NOT A "
           "RESOLUTION. The buyer side reads Morgan Stanley Prime Property Fund. "
           "NEREJ records ROSEVIEW-PMRG FUND I acquiring 100 CambridgePark Drive "
           "for $41.5 million in December 2014 FROM TRANSATLANTIC INVESTMENT "
           "MANAGEMENT, which makes Roseview/PMRG the likely owner going into this "
           "June 2017 sale and therefore the likely seller. That is the "
           "earlier-buyer inference this table refuses on principle, and the "
           "record entity KT CAMBRIDGE PARK LLC matches neither name, which makes "
           "it weaker still. Transatlantic Investment Management is separately "
           "confirmed in this table selling the American Twine complex to New "
           "England Development in 2019."),
    (1516, "230 CONGRESS STREET SELLER, $77,000,000, September 2015: NOT FOUND. "
           "The buyer is established as Northwood Investors, from its own "
           "portfolio page and Connect CRE, and the entity NW 230 CONGRESS STREET "
           "confirms the NW convention. But no source found names the 2015 "
           "SELLER, and the entity TWO-30 CONGRESS ST OWNER LLC is the address "
           "with a hyphen in it. Coverage of this building is dominated by "
           "Northwood's own tenure and by its 2026 exit at $23.7M to Hudson "
           "Assembly, a 69% discount."),
    (1577, "1416 MASSACHUSETTS AVENUE BUYER, $79,225,000, December 2022: NOT "
           "FOUND. The seller is now established as Piedmont Office Realty Trust, "
           "and BLDUP headlines the pair as \"Two Harvard Square Commercial "
           "Corners Trade for Combined $160 Million\" with a companion piece at "
           "$78.2 million, very close to this row. But neither Piedmont's release "
           "nor Connect CRE's write-up names the BUYER -- a REIT reporting a "
           "disposition has no reason to -- and the buyer entity 1414 "
           "MASSACHUSETTS AVENUE LLC is the address."),
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
        log.info("id=%-5s %-6s %-34s -> %-34s [%s]", rid, side,
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
        v = conn.execute(text(
            f"select count(*) from transactions where coalesce(quarantined,0)=0 "
            f"and coalesce({side}_canonical,'') <> ''")).scalar()
        log.info("%s_canonical: %d of %d (%.0f%%)", side, v, tot, v / tot * 100)
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
