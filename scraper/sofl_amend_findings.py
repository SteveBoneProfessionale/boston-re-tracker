"""One-off amendments to the agent findings, after checking the sources myself.

Three of the reported findings failed the validator in scraper/
sofl_apply_findings.py, and the right response differed per case:

  The Perigon / Eldridge     the quote did not say the facility was
  Shore Club / JPMorgan      construction financing, but the articles DO say so
                             in a sentence the reporter did not quote. Verified
                             by reading both articles, and the quote is extended
                             to include the sentence that establishes it.

  425 Ocean Drive / 150 units  150 is 90 condominiums plus 60 workforce
                               apartments. The source states the two components
                               and prints no total, so the figure is the
                               reporter's arithmetic rather than the source's.
                               Dropped, with the components kept in the note.

The first two are a quote that was too short. The third is a number that does
not exist in any source, which is the thing the validator is for.
"""

import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MB = os.path.join(ROOT, "data", "sofl", "agent_findings_miami_beach.json")

PERIGON = ("Mast Capital and Starwood Capital Group secured construction financing for their "
           "oceanfront luxury condo project in Miami Beach. The joint venture closed on a $390 "
           "million loan from Eldridge Real Estate Credit for the Perigon.")
SHORE_CLUB = ("Witkoff Group and Monroe Capital scored one of South Florida's biggest construction "
              "loans ever: $430 million for a luxury condo development on Miami Beach's Collins "
              "Avenue. The financing from JPMorgan Chase will go toward the Shore Club Private "
              "Collection")
OCEAN_DRIVE_NOTE = (
    "Residential units are deliberately NOT stored: the source states 90 condominiums and 60 "
    "workforce rental apartments separately and prints no total, so 150 would be a figure of "
    "mine rather than the source's. Allied Partners owns the Savoy Hotel & Beach Club at 425 "
    "and 455 Ocean Drive. The proposal relies on both the Live Local Act and the Resilience and "
    "Safe Structures Act, partly preserving the 1937 Savoy and demolishing the 1941 Arlington. "
    "No architect is named by any acceptable source.")


def run(verbose=True):
    with open(MB, encoding="utf-8") as fh:
        findings = json.load(fh)
    changed = []
    for rec in findings:
        if rec["id"] == 1323:
            rec["quotes"]["lender"] = PERIGON
            changed.append("The Perigon: lender quote extended to the verified sentence")
        elif rec["id"] == 1328:
            rec["quotes"]["lender"] = SHORE_CLUB
            changed.append("Shore Club: lender quote extended to the verified sentence")
        elif rec["id"] == 1342:
            for block in ("values", "sources", "quotes"):
                rec[block].pop("residential_units", None)
            rec["notes"] = OCEAN_DRIVE_NOTE
            changed.append("425 Ocean Drive: derived 150-unit figure dropped")
    with open(MB, "w", encoding="utf-8") as fh:
        json.dump(findings, fh, indent=1, ensure_ascii=False)
    if verbose:
        for line in changed:
            print("  " + line)
    return changed


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    run()
