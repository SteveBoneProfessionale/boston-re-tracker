r"""Layer 1 of entity resolution: pattern matching, free and certain.

Sponsors name their vehicles consistently, so one resolved stem resolves many
rows. ARE-MA REGION NO. 40 / NO. 45 / NO. 59 / NO. 94 / NO. 102 / NO. 107 are
all Alexandria. MEPT SEAPORT, MEPT FORT, MEPT 147 MILK are all the
Multi-Employer Property Trust. RREF II, DWF IV, PPF OFF likewise.

BOSTON STREET NAMES ARE FIRM NAMES, which is the trap this layer has to survive.
A naive scan for HARVARD matches "138 HARVARD LLC", which is Harvard STREET in
Allston and nothing to do with the university. BULFINCH matches "GWL DIRECT 1
BULFINCH PALCE LLC" -- Bulfinch Place, not The Bulfinch Cos. The same applies to
Beacon, Lincoln, Franklin, Congress, Tremont and Washington, all of which are
both major Boston streets and real firm names.

So every pattern carries a guard, and the guards are of two kinds:

  ADDRESS-FORM: the stem is rejected when it sits in "<number> <STEM>" position,
  because that is an address and not a sponsor.

  REQUIRE-QUALIFIER: the stem only counts with its corporate qualifier attached
  -- HARVARD only as HARVARD UNIVERSITY or PRESIDENT AND FELLOWS, BULFINCH only
  as BULFINCH COS, BEACON only as BEACON CAPITAL.

Nothing is written on a bare stem. Where a name is genuinely ambiguous it is
left unresolved, because the instruction is that a blank sponsor is correct and
a wrong one poisons the rankings.

    python scraper/acq_resolve_patterns.py            # verify, writes nothing
    python scraper/acq_resolve_patterns.py --apply
"""

import argparse
import logging
import re
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from db.database import engine

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

# (regex, canonical sponsor, guard)
#   guard=None        no guard needed, the stem is unambiguous
#   guard="notaddr"   reject when the stem follows a street number
#   guard=<regex>     reject when this also matches
PATTERNS = [
    # ── unambiguous sponsor-coded vehicles ──────────────────────────
    (r"\bARE-MA\b|\bARE-\b|ALEXANDRIA REAL ESTATE", "Alexandria Real Estate Equities", None),
    (r"\bBRE-BMR\b|\bBIOMED\b", "BioMed Realty (Blackstone)", None),
    (r"\bMEPT\b", "MEPT / BentallGreenOak", None),
    (r"\bRREF\b", "Rialto Capital", None),
    (r"\bDWF [IVX]+\b|\bDIVCOWEST\b|\bDIVCO WEST\b", "DivcoWest", None),
    (r"\bPPF (OFF|INDUSTRIAL|AMLI|RTL)\b", "Morgan Stanley Prime Property Fund", None),
    (r"\bGAZIT\b", "Gazit Horizons", None),
    (r"\bLINEAR RETAIL\b", "Linear Retail Properties", None),
    (r"\bKBS\b", "KBS Realty Advisors", None),
    (r"\bTA REALTY\b|\bTA ASSOCIATES REALTY\b", "TA Realty", None),
    (r"\bCROWN COLONY\b", "Crown Colony", None),
    (r"\bJAMESTOWN\b", "Jamestown", "notaddr"),
    (r"\bWS (SEAPORT|BLOCK|DEV|RETAIL|FAN PIER)", "WS Development", None),
    (r"\bSKANSKA\b", "Skanska", None),
    (r"\bOXFORD PROPERT", "Oxford Properties Group", None),
    (r"\bCLARION\b", "Clarion Partners", None),
    (r"\bAEW\b", "AEW Capital Management", None),
    (r"\bNUVEEN\b|\bTIAA\b", "Nuveen (TIAA)", None),
    (r"\bBLACKSTONE\b", "Blackstone", None),
    (r"\bGREYSTAR\b", "Greystar", None),
    (r"\bAVALON ?BAY\b|\bAVALONBAY\b", "AvalonBay Communities", None),
    (r"\bEQUITY RESIDENTIAL\b|\bEQR\b", "Equity Residential", None),
    (r"\bBOSTON PROPERTIES\b|^BXP\b|\bBXP\b", "BXP", None),
    (r"\bSAMUELS\b", "Samuels & Associates", None),
    (r"\bHYM\b", "HYM Investment Group", None),
    (r"\bNATIONAL DEVELOPMENT\b|\bNDNE\b", "National Development", None),
    (r"\bALPHA MANAGEMENT\b", "Alpha Management", None),
    (r"\bMOUNT VERNON CO\b", "The Mount Vernon Co.", None),
    (r"\bCABOT,? CABOT", "Cabot, Cabot & Forbes", None),
    (r"\bNORTH COLONY\b", "North Colony Asset Management", None),
    (r"\bSYNERGY INVESTMENT|SYNERGY BOSTON", "Synergy Investments", None),
    (r"\bDAVIS COS\b|\bDAVIS COMPANIES\b", "The Davis Cos.", None),
    (r"\bHINES\b", "Hines", None),
    (r"\bRELATED BEAL\b", "Related Beal", None),
    (r"\bTISHMAN SPEYER\b", "Tishman Speyer", None),
    (r"\bBOSTON REALTY ADVISOR", "Boston Realty Advisors", None),
    (r"\bTRINITY (FINANCIAL|PROPERT)", "Trinity Financial", None),
    (r"\bWINN(DEVELOPMENT|COMPANIES| )", "WinnCompanies", None),
    (r"\bCORCORAN JENNISON\b|\bJOHN M\.? CORCORAN\b", "John M. Corcoran & Co.", None),
    (r"\bPONTE ?GADEA\b|\bAMANCIO ORTEGA\b", "Pontegadea (Amancio Ortega)", None),
    (r"\bACADIA REALTY\b", "Acadia Realty Trust", None),
    (r"\bMESIROW\b", "Mesirow", None),
    (r"\bPGIM\b", "PGIM Real Estate", None),
    (r"\bKKR\b", "KKR", None),
    (r"\bSTARWOOD\b|\bLNR PARTNERS\b", "Starwood / LNR Partners", None),
    (r"\bBROOKFIELD\b", "Brookfield", None),
    (r"\bEASTERN REAL ESTATE\b", "Eastern Real Estate", None),
    (r"\bTAURUS\b", "Taurus Investment Holdings", None),
    (r"\bBOYLSTON PROPERTIES\b", "Boylston Properties", None),
    (r"\bDRAPER\b", "Draper Laboratory", None),
    (r"\bDIVCO\b", "DivcoWest", None),

    # ── prefixes decoded during web research, folded back in so the
    #    sponsor's other vehicles resolve for free ────────────────────
    # Word boundaries matter more in this block than above, because these
    # stems are short and live inside ordinary words: MORI sits inside
    # MEMORIAL ("777 MEMORIAL OWNER LP"), UBS inside SUBSIDIARY, and EQC and
    # OMERS are short enough to collide by accident. The unanchored version of
    # this block produced exactly the MEMORIAL/MORI match on verification.
    (r"\bIQHQ\b|\bCSP-\d", "IQHQ", None),
    (r"\bMORI TRUST\b", "Mori Trust", None),
    (r"\bROCKPOINT\b", "Rockpoint Group", None),
    (r"\bBENDERSON\b|\bBDC SUMMER\b", "Benderson Development Co.", None),
    (r"\bPEMBROKE\b|\bFIDELITY\b|\bHORIZON REAL ESTATE INVESTORS\b",
     "Fidelity Investments (Pembroke)", None),
    (r"\bEQUITY COMMONWEALTH\b|\bHUB PROPERTIES\b",
     "Equity Commonwealth", None),
    (r"\bLIBERTY MUTUAL\b", "Liberty Mutual", None),
    (r"\bUBS\b", "UBS Asset Management", None),
    (r"\bALLIANZ\b", "Allianz Real Estate", None),
    (r"\bNORGES\b", "Norges Bank Investment Management", None),
    (r"\bEQUITY OFFICE\b|\bEQ OFFICE\b", "Blackstone (Equity Office)", None),
    (r"\bUNITED STATES OF AMERICA\b", "U.S. Government (GSA)", None),

    # ── institutions, which need their corporate qualifier ──────────
    (r"\bMIT\b|MASSACHUSETTS INSTITUTE OF TECH", "MIT", "notaddr"),
    (r"HARVARD UNIVERSITY|PRESIDENT AND FELLOWS|HARVARD REAL ESTATE|"
     r"HARVARD MANAGEMENT", "Harvard University", None),
    (r"TRUSTEES (OF )?BOSTON UNIVERSITY|BOSTON UNIVERSITY", "Boston University", None),
    (r"\bNORTHEASTERN UNIVERSITY\b", "Northeastern University", None),
    (r"\bTUFTS (UNIVERSITY|MEDICAL|MEDICINE)", "Tufts", None),
    (r"\bBERKLEE\b", "Berklee College of Music", None),
    (r"\bWENTWORTH INSTITUTE\b", "Wentworth Institute of Technology", None),
    (r"MASS(ACHUSETTS)? GENERAL|BRIGHAM AND WOMEN|PARTNERS HEALTHCARE|"
     r"\bMASS GENERAL BRIGHAM\b", "Mass General Brigham", None),
    (r"BOSTON MEDICAL CENTER\b", "Boston Medical Center", None),
    (r"BETH ISRAEL\b", "Beth Israel Lahey Health", None),
    (r"DANA[- ]FARBER", "Dana-Farber Cancer Institute", None),
    (r"CHILDREN'?S HOSPITAL", "Boston Children's Hospital", None),

    # ── firm names that collide with major Boston streets ───────────
    (r"BEACON CAPITAL PARTNERS", "Beacon Capital Partners", None),
    (r"BULFINCH (COS|COMPANIES)", "The Bulfinch Cos.", None),
    (r"LINCOLN PROPERTY", "Lincoln Property Co.", None),
    (r"FRANKLIN STREET PROPERTIES|\bFSP CORP\b", "Franklin Street Properties", None),
    (r"CONGRESS GROUP", "The Congress Group", None),
]

# Street numbers followed immediately by the stem indicate an address.
ADDR_FORM = re.compile(r"^\s*[\d-]+[A-Z]?\s+$")


def resolve(name: str):
    """Return (sponsor, matched_pattern) or (None, None)."""
    n = " " + re.sub(r"[^A-Z0-9&'.,/ -]", " ", (name or "").upper()) + " "
    for pat, sponsor, guard in PATTERNS:
        m = re.search(pat, n)
        if not m:
            continue
        if guard == "notaddr":
            before = n[:m.start()]
            tail = before.rsplit(",", 1)[-1]
            if ADDR_FORM.match(tail[-12:]) or re.search(r"\d+\s*$", before):
                continue
        elif guard and re.search(guard, n):
            continue
        return sponsor, pat
    return None, None


def main(dry_run: bool):
    conn = engine.connect()
    hits = defaultdict(set)
    counts = defaultdict(int)
    updates = []

    for col, canon, conf, basis in (
            ("buyer", "buyer_canonical", "buyer_confidence", "buyer_resolution_basis"),
            ("seller", "seller_canonical", "seller_confidence", "seller_resolution_basis")):
        rows = conn.execute(text(
            f"select id, {col} from transactions "
            f"where coalesce({col},'') <> '' and coalesce({canon},'') = ''")).fetchall()
        for rid, name in rows:
            sponsor, _pat = resolve(name)
            if not sponsor:
                continue
            hits[sponsor].add(name.strip().upper())
            counts[sponsor] += 1
            updates.append((rid, canon, conf, basis, sponsor))

    log.info("%d sponsors matched, %d party mentions resolved\n", len(hits), len(updates))
    for sponsor in sorted(hits, key=lambda s: -counts[s]):
        ex = sorted(hits[sponsor])
        log.info("%-38s %3d rows, %d distinct entities", sponsor, counts[sponsor], len(ex))
        for e in ex[:4]:
            log.info("      %s", e[:72])
        if len(ex) > 4:
            log.info("      ... and %d more", len(ex) - 4)

    if not dry_run:
        for rid, canon, conf, basis, sponsor in updates:
            conn.execute(text(
                f"update transactions set {canon} = :s, {conf} = 'pattern_matched', "
                f"{basis} = 'pattern' where id = :id"), {"s": sponsor, "id": rid})
        conn.commit()
        tot = conn.execute(text("select count(*) from transactions")).scalar()
        for side in ("buyer", "seller"):
            n = conn.execute(text(
                f"select count(*) from transactions "
                f"where coalesce({side}_canonical,'') <> ''")).scalar()
            log.info("\n%s_canonical resolved on %d of %d rows (%.0f%%)",
                     side, n, tot, n / tot * 100)
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
