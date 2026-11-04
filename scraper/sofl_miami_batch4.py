"""The six largest Miami projects that the first three passes left blank.

Each was found by searching the project's NAME rather than its address, after
the address pass had named enough of them for the names to be guessable. All
six now rest on a sentence that states a role, which is the bar: "developed
by", "designed by", "is the developer", "is listed as the architect".

Two of them needed the team to be assembled from two articles -- 830 Brickell's
architect comes from its construction coverage and its sponsors from the later
refinancing -- and both are cited per field rather than from one default.
"""

import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OVERLAY = os.path.join(ROOT, "data", "sofl", "miami_overlay.json")

B830_FIN = ("https://floridayimby.com/2025/12/cain-and-oko-group-secure-630-million-"
            "refinancing-for-830-brickell.html")
B830_CON = ("https://floridayimby.com/2021/10/830-brickells-glass-facade-installation-"
            "and-vertical-ascent-continues-making-progress-in-miamis-financial-district.html")
DT6 = ("https://floridayimby.com/2026/03/melo-groups-downtown-6-tops-out-at-46-ne-6th-"
       "street-in-downtown-miami.html")
DT6_UDRB = ("https://floridayimby.com/2023/03/54-story-downtown-6th-gets-unanimous-approval-"
            "from-miamis-urban-development-review-board.html")
NATIIVO = ("https://floridayimby.com/2021/04/51-story-natiivo-to-be-developed-at-601-ne-"
           "1st-avenue-in-downtown-miami.html")
NATIIVO2 = ("https://floridayimby.com/2021/10/natiivos-core-rises-several-levels-above-"
            "grade-in-downtown-miami.html")
LOFTY = ("https://floridayimby.com/2025/03/construction-progress-advances-on-the-standard-"
         "residences-brickell-and-lofty-brickell.html")
MFP = ("https://floridayimby.com/2022/06/new-renderings-from-arquitectonica-revealed-for-"
       "proposed-miami-freedom-park-in-grapeland-heights.html")
MFP_BUILD = ("https://floridayimby.com/2023/08/construction-begins-on-miami-freedom-park-"
             "inter-miami-cf-stadium-set-to-open-in-2025.html")
MFP_ADMIN = ("https://floridayimby.com/2025/01/city-of-miami-to-break-ground-on-eight-"
             "story-administration-building-at-miami-freedom-park.html")

MFP_NOTE = (
    "A component of Miami Freedom Park, a 131-acre development at 1802 NW 37th Avenue in "
    "Grapeland Heights on the former Melreese golf course, anchored by a 25,000-seat stadium. "
    "Miami Freedom Park, LLC is named as the developer and Arquitectonica as the architect for "
    "the master plan, with ArquitectonicaGEO as landscape architect; those are recorded on each "
    "of the three parcels the City permits separately. A later article covers the scheme's "
    "eight-storey administration building specifically, developed by Adler Development and "
    "built by Moss Construction -- the City's permit records do not say which parcel that "
    "building sits on, so that team is not attached to any one row."
)

RECORDS = [
 {"name": "830 Brickell", "match_address": "830 BRICKELL PLZ",
  "notes": "A 57-storey Class A+ office tower, the first stand-alone Class A+ office building "
           "delivered in Brickell in more than a decade. Storey count published as 55 during "
           "construction in 2021 and 57 on completion, so the later figure is stored. Adrian "
           "Smith + Gordon Gill Architecture is named as designer with Revuelta Architecture "
           "International alongside; Adrian Smith + Gordon Gill is stored. The $630m Goldman "
           "Sachs and J.P. Morgan package of December 2025 is a refinancing, not construction "
           "financing, so no lender is recorded. The press dates completion to 2023 while the "
           "City's permit record carries a later certificate; the City record drives the stage.",
  "values": {"developer": "Cain; OKO Group",
             "architect": "Adrian Smith + Gordon Gill Architecture", "stories": 57},
  "sources": {"_default": {"name": "Florida YIMBY, 12 Dec 2025", "date": "2025-12-12",
                           "url": B830_FIN},
              "architect": {"name": "Florida YIMBY, 21 Oct 2021", "date": "2021-10-21",
                            "url": B830_CON}}},

 {"name": "Downtown 6", "match_address": "46 NE 6 ST",
  "notes": "Also written Downtown 6th. Melo Architecture is the design architect and G3aec the "
           "architect of record, so G3aec is stored and Melo Architecture noted here, per the "
           "field rule. Storey count rose through the filings -- 54 at Urban Development Review "
           "Board approval in 2023, 58 in April 2025, 60 on topping out in March 2026 -- and "
           "the latest is stored. The identification is firm: the 2023 article's 1,081,913 sq ft "
           "matches the City's permit figure exactly. The permit contractor, Melo Contractors "
           "Corp, is Melo Group's in-house construction arm.",
  "values": {"developer": "Melo Group", "architect": "G3aec", "residential_units": 824,
             "stories": 60, "height_ft": 576, "commercial_gsf": 2427, "parking_spaces": 639},
  "sources": {"_default": {"name": "Florida YIMBY, 9 Mar 2023", "date": "2023-03-09",
                           "url": DT6_UDRB},
              "stories": {"name": "Florida YIMBY, 10 Mar 2026", "date": "2026-03-10",
                          "url": DT6},
              "developer": {"name": "Florida YIMBY, 10 Mar 2026", "date": "2026-03-10",
                            "url": DT6}}},

 {"name": "Natiivo Miami", "match_address": "601 NE 1 AV",
  "notes": "Also marketed as 601 Miami. Sixth Street Miami Partners, LLC is an entity "
           "affiliated with the Galmut Family. An earlier read of the topping-off article was "
           "left blank here because it listed GFO Investments, Menin Hospitality, Cervera Real "
           "Estate, Arquitectonica, Urban Robot Associates, Gale Hotels and L&R Structural Corp "
           "together without assigning roles; these two articles state the roles outright. L&R "
           "Structural Corp is the shell contractor and KAST Construction is named at the "
           "topping-off.",
  "values": {"developer": "Sixth Street Miami Partners", "architect": "Arquitectonica",
             "height_ft": 588, "stories": 51},
  "sources": {"_default": {"name": "Florida YIMBY, 14 Apr 2021", "date": "2021-04-14",
                           "url": NATIIVO},
              "height_ft": {"name": "Florida YIMBY, 18 Oct 2021", "date": "2021-10-18",
                            "url": NATIIVO2}}},

 {"name": "LOFTY Brickell (One Brickell Riverfront)", "match_address": "600 SW 1 AV",
  "notes": "One of the two towers of One Brickell Riverfront, alongside the 45-storey, "
           "422-residence Standard Residences, Brickell; both are designed by Arquitectonica. "
           "KD Construction of Florida is the shell contractor and the City's permit names GT "
           "Construction & Development as contractor. The development secured $513m of "
           "construction financing in late 2025 without the lender being named.",
  "values": {"developer": "Newgard Development Group", "architect": "Arquitectonica",
             "residential_units": 363, "stories": 44},
  "sources": {"_default": {"name": "Florida YIMBY, 30 Mar 2025", "date": "2025-03-30",
                           "url": LOFTY}}},

 {"name": "Miami Freedom Park — stadium", "match_address": "1000 STADIUM DR",
  "notes": MFP_NOTE + " This row is the stadium parcel; the stadium seats 25,000.",
  "values": {"developer": "Miami Freedom Park, LLC", "architect": "Arquitectonica"},
  "sources": {"_default": {"name": "Florida YIMBY, 19 Jun 2022", "date": "2022-06-19",
                           "url": MFP}}},

 {"name": "Miami Freedom Park — park", "match_address": "4000 NW 14 ST",
  "notes": MFP_NOTE + " This row is the public park parcel; its building permit scope reads "
           "NEW PARK ONLY.",
  "values": {"developer": "Miami Freedom Park, LLC", "architect": "Arquitectonica"},
  "sources": {"_default": {"name": "Florida YIMBY, 19 Jun 2022", "date": "2022-06-19",
                           "url": MFP}}},

 {"name": "Miami Freedom Park — commercial component", "match_address": "1896 NW 37 AV",
  "notes": MFP_NOTE + " This row is the office and commercial parcel.",
  "values": {"developer": "Miami Freedom Park, LLC", "architect": "Arquitectonica"},
  "sources": {"_default": {"name": "Florida YIMBY, 19 Jun 2022", "date": "2022-06-19",
                           "url": MFP}}},
]


def run(verbose=True):
    with open(OVERLAY, encoding="utf-8") as fh:
        overlay = json.load(fh)
    by_addr = {r.get("match_address"): i for i, r in enumerate(overlay)}
    added = replaced = 0
    for rec in RECORDS:
        idx = by_addr.get(rec["match_address"])
        if idx is None:
            overlay.append(rec)
            by_addr[rec["match_address"]] = len(overlay) - 1
            added += 1
        else:
            overlay[idx] = rec
            replaced += 1
    with open(OVERLAY, "w", encoding="utf-8") as fh:
        json.dump(overlay, fh, indent=1, ensure_ascii=False)
    if verbose:
        print(f"added {added}, replaced {replaced}; overlay now holds {len(overlay)}")
    return {"added": added, "replaced": replaced, "total": len(overlay)}


if __name__ == "__main__":
    run()
