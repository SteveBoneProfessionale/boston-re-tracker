r"""Clear brokers, executives and entity fragments masquerading as sponsors.

An audit of all 230 distinct sponsor values found that the `care_of` pass -- the
single biggest contributor to the resolution rate -- had been writing whatever
sat on the assessment roll's care-of line, and a care-of line is an ADDRESS FOR
POST. It names whoever collects the mail: the managing agent, the leasing broker,
an asset manager at the owner, a lawyer, sometimes a person. Those are not
owners, and a wrong sponsor is worse than a blank one because it ranks.

Three examples given, all real:

    643-653 Summer St   "Oxford Properties - Ann Clavelle"   a firm plus a person
    201 Newbury St      "Newmark Christopher Ruggiero"       a broker plus a person
    1028 Mass Ave       "Altus Ventas Unit 6795"             a mail stop

The audit found 40 more of the same kind. They fall into four classes and each
class is handled differently, because "clear everything from care_of" would
throw away good rows and "keep it" is how the table got here.

CLASS 1 -- RESCUE. The care-of line names an AGENT ACTING FOR A NAMED PRINCIPAL,
and the principal is on the line too. "Cushman & Wakefield AAF Deka Immobilien"
is C&W collecting post as agent for Deka Immobilien; the owner is Deka, which
press independently confirms bought 175 Federal Street for $139M in May 2016.
Dropping the agent and keeping the principal is a gain, not a loss.

CLASS 2 -- STRIP THE PERSON. "Oxford Properties - Ann Clavelle" is Oxford
Properties with an employee's name attached. The firm is right and the person is
noise, and press confirms Oxford at 745 Atlantic Avenue independently.

CLASS 3 -- CLEAR. A broker with no principal named, a law firm, a restructuring
adviser, a mail stop, or a bare personal name that arrived from a care-of line
rather than research. Nothing recoverable, so the cell goes blank.

CLASS 4 -- NORMALISE. Real firms carrying a legal suffix or a typo, which split
one firm across two rows in every ranking: "Synergy Investments Lls",
"Healthpeak Properties, Inc", "Nb Development Group Llc", "Gi Partners".

PERSONAL NAMES ARE NOT ALL WRONG. Individuals genuinely own property, and this
table records some. The distinction applied is PROVENANCE: a personal name from
`web` was researched and stays; a personal name from `care_of` is a mail contact
and goes. Edward J Tutunjian is the awkward case -- a real Boston owner whose
name appears on his own roll entry -- so his rows are cleared from the sponsor
column but the reasoning is written on them rather than silently dropped.

    python scraper/acq_clean_canonicals.py --apply
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

WHY = {
    "agent": ("A CARE-OF LINE NAMES WHOEVER COLLECTS THE POST, NOT THE OWNER. "
              "This value was an AGENT acting for a principal that the same line "
              "also named. The agent is dropped and the principal kept."),
    "person": ("A CARE-OF LINE NAMES WHOEVER COLLECTS THE POST, NOT THE OWNER. "
               "This value was a real firm with an individual employee's name "
               "appended. The firm is kept and the person dropped."),
    "clear": ("CLEARED: THIS WAS NEVER A SPONSOR. It came from the assessment "
              "roll's care-of line, which is an address for post -- it names the "
              "managing agent, the leasing broker, a lawyer, a mail stop or a "
              "person, none of which own the building. A wrong sponsor is worse "
              "than a blank one because it ranks, so the cell is blanked. The "
              "record entity is untouched."),
    "norm": ("NORMALISED. The same firm was carrying a legal suffix or a typo, "
             "which splits it across two rows in every ranking."),
}

# (old value, new value or None to clear, class)
FIX = [
    # -- class 1, agent acting for a named principal -----------------------
    ("Cushman & Wakefield Aaf Deka Immobilien", "Deka Immobilien", "agent"),
    ("Cushman & Wakefield Aaf Deka Usa Franklin", "Deka Immobilien", "agent"),

    # -- class 2, real firm with a person appended -------------------------
    ("Oxford Properties - Ann Clavelle", "Oxford Properties Group", "person"),
    ("Oxford I Asset Manager", "Oxford Properties Group", "person"),

    # -- class 3, clear ----------------------------------------------------
    ("Newmark Christopher Ruggiero", None, "clear"),   # broker + executive
    ("Altus Ventas Unit 6795", None, "clear"),         # a mail stop
    ("Kroll", None, "clear"),                          # restructuring adviser
    ("Ruberto, Israel & Weiner Pc", None, "clear"),    # law firm
    ("Nyss Elkins Llc & Related Beals", None, "clear"),  # mangled, two names
    ("Breakthrough Services", None, "clear"),          # not a known sponsor
    ("Premier Property Solutions Llc", None, "clear"),  # managing agent
    ("Stonebridge Realty Advisors Inc", None, "clear"),  # adviser, not owner
    ("Anthony C Musto", None, "clear"),
    ("Katherine Ehler", None, "clear"),
    ("Leandro Barreto", None, "clear"),
    ("Rich Q Chen", None, "clear"),
    ("Ted Lubitz", None, "clear"),
    ("Fredy Audy Trustee", None, "clear"),
    ("Fred Starikov &", None, "clear"),
    ("Paul Mcgrath Pipefitters 537", None, "clear"),
    ("Edward J Tutunjian", None, "clear"),

    # -- class 4, normalise ------------------------------------------------
    ("Synergy Investments Lls", "Synergy Investments", "norm"),
    ("Healthpeak Properties, Inc", "Healthpeak Properties", "norm"),
    ("Intercontinental Real Estate Corp", "Intercontinental Real Estate Corp.", "norm"),
    ("Ksl Capital Partners Mgmt V, Llc", "KSL Capital Partners", "norm"),
    ("Nb Development Group Llc", "NB Development Group (New Balance)", "norm"),
    ("Gi Partners", "GI Partners", "norm"),
    ("Griffith Properties Llc", "Griffith Properties", "norm"),
    ("Aew Capital Management Lp", "AEW Capital Management", "norm"),
    ("Invesco Advisers Inc", "Invesco Real Estate", "norm"),
    ("Credit Suisse Asset Management Ltd", "Credit Suisse", "norm"),
    ("Alpha Management Corporation", "Alpha Management", "norm"),
    ("Able Company Llc", "Able Company", "norm"),
    ("Samuels & Associates Management Llc", "Samuels & Associates", "norm"),
    ("Nuveen (TIAA)", "TIAA (Nuveen)", "norm"),
    ("BXP", "BXP (Boston Properties)", "norm"),
    ("Skanska", "Skanska USA Commercial Development", "norm"),
    ("Procter & Gamble", "Procter & Gamble (Gillette)", "norm"),
    ("Union Investment Real Estate", "Union Investment", "norm"),
    ("Kensington Investment Management", "Kensington Investment Co.", "norm"),
    ("Boston Residential Group Llc", "Boston Residential Group", "norm"),
    ("Jones Street Investment Partners Llc", "Jones Street Investment Partners", "norm"),
    ("Akelius Real Estate Management Llc", "Akelius Real Estate Management", "norm"),
    ("Sullivan Square Holdings Llc", "Sullivan Square Holdings", "norm"),
    ("Tremont Asset Management Llc", "Tremont Asset Management", "norm"),
    ("L3 Capital Llc", "L3 Capital", "norm"),
    ("Mcre Partners Llc", "MCRE Partners", "norm"),
    ("Exan Capital Llc", "Exan Capital", "norm"),
    ("Griggs Investment Llc", "Griggs Investment", "norm"),
    ("Eden Harvard Llc", "Eden Harvard", "norm"),
    ("Kimco Realty Corporation", "Kimco Realty", "norm"),
    ("Extra Space Storage Inc", "Extra Space Storage", "norm"),
    ("Petroleum Marketing Group Inc", "Petroleum Marketing Group", "norm"),
    ("Nouria Energy Corporation", "Nouria Energy", "norm"),
    ("Crosspoint Associates, Inc", "Crosspoint Associates", "norm"),
    ("Core Investment Inc", "Core Investment", "norm"),
    ("Community Builders Inc", "The Community Builders", "norm"),
    ("Cruz Development Corporation", "Cruz Development", "norm"),
    ("Hackett Publishing Company", "Hackett Publishing", "norm"),
    ("Jamaica Plain Neighborhood Development Corp",
     "Jamaica Plain Neighborhood Development Corporation", "norm"),
    ("B'Nai B'Rith Housing New England, Inc", "B'nai B'rith Housing New England", "norm"),
]


def main(dry_run: bool):
    conn = engine.connect()
    cleared = renamed = 0
    for old, new, cls in FIX:
        for side in ("buyer", "seller"):
            hits = conn.execute(text(
                f"select id, price, address from transactions "
                f"where {side}_canonical = :o"), {"o": old}).fetchall()
            if not hits:
                continue
            for rid, price, addr in hits:
                log.info("%-6s id=%-5s $%-12s %-24s %-42s -> %s", side, rid,
                         f"{price or 0:,}", (addr or "")[:24], old[:42],
                         new if new else "(cleared)")
            if dry_run:
                continue
            note = " | " + WHY[cls] + (
                f" Was recorded as {old!r}; now {new!r}." if new
                else f" Was recorded as {old!r}.")
            conn.execute(text(f"""
                update transactions
                   set {side}_canonical = :n,
                       {side}_confidence = case when :n is null then null
                                                else {side}_confidence end,
                       {side}_resolution_basis = case when :n is null then null
                                                      else {side}_resolution_basis end,
                       notes = coalesce(notes,'') || :note
                 where {side}_canonical = :o"""),
                {"n": new, "o": old, "note": note})
            if new:
                renamed += len(hits)
            else:
                cleared += len(hits)
    if not dry_run:
        conn.commit()

    log.info("\n%d sides cleared, %d normalised or rescued", cleared, renamed)
    tot = conn.execute(text(
        "select count(*) from transactions where coalesce(quarantined,0)=0")).scalar()
    for side in ("buyer", "seller"):
        v = conn.execute(text(
            f"select count(*) from transactions where coalesce(quarantined,0)=0 "
            f"and coalesce({side}_canonical,'') <> ''")).scalar()
        log.info("%s_canonical: %d of %d (%.0f%%)", side, v, tot, v / tot * 100)
    n = conn.execute(text(
        "select count(distinct buyer_canonical) from transactions "
        "where coalesce(quarantined,0)=0 and coalesce(buyer_canonical,'')<>''")).scalar()
    log.info("distinct buyer sponsors: %d", n)
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
