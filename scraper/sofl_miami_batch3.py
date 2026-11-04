"""The last Miami records: the very large projects the press names, not addresses.

After the address-pattern pass, the biggest remaining gaps were projects whose
coverage never prints the City's address of record at all -- Swire's Brickell
Key towers are written up at 500 Brickell Key Drive and 750 Claughton Island
Drive, not 1 and 2 Island Drive; the Melreese parcels are written up as Miami
Freedom Park. These were found by searching the project name instead.

Where the coverage cannot be tied to a specific permit row with confidence, the
project is NAMED and the team is left blank with the reason on the row. A name
is worth having on its own -- "1000 STADIUM DR" tells a reader nothing -- and
it is a separate claim from who built it.
"""

import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OVERLAY = os.path.join(ROOT, "data", "sofl", "miami_overlay.json")

SWIRE = ("https://floridayimby.com/2026/10/swire-properties-breaks-ground-on-the-"
         "residences-at-mandarin-oriental-miami-on-brickell-key.html")
FREEDOM = ("https://floridayimby.com/2025/01/city-of-miami-to-break-ground-on-eight-"
           "story-administration-building-at-miami-freedom-park.html")
LOFTY = ("https://floridayimby.com/2026/06/lofty-brickell-and-the-standard-residences-"
         "brickell-reach-full-architectural-height-at-one-brickell-riverfront.html")

SWIRE_NOTE = (
    "Part of The Residences at Mandarin Oriental, Miami, a two-tower residential and hotel "
    "development Swire Properties broke ground on in October 2026, with completion scheduled "
    "for 2030. The coverage gives the site as 500 Brickell Key Drive and 750 Claughton Island "
    "Drive and describes a 66-storey South Tower of 850 ft and a 33-storey North Tower of "
    "426 ft, but does not say which tower corresponds to the City's 1 Island Drive and which "
    "to 2 Island Drive. Storey count and height are therefore not stored on either row: "
    "assigning them would be a guess about which permit is which tower. Developer, architect "
    "and contractor are shared by both towers and so are safe to record."
)

FREEDOM_NOTE_COMMON = (
    "A component of Miami Freedom Park, a 130-acre master plan on the former Melreese golf "
    "course. Florida YIMBY's coverage of the master plan is specifically about its eight-storey "
    "administration building, designed by Arquitectonica, developed by Adler Development and "
    "built by Moss Construction; the City's permit records do not say which parcel that "
    "building sits on, so that team is not attached to any of the three Freedom Park rows."
)

RECORDS = [
 {"name": "The Residences at Mandarin Oriental, Miami — tower at 1 Island Drive",
  "match_address": "1 ISLAND DR",
  "alt_addresses": ["500 Brickell Key Dr", "750 Claughton Island Dr"],
  "notes": SWIRE_NOTE,
  "values": {"developer": "Swire Properties", "architect": "Kohn Pedersen Fox"},
  "sources": {"_default": {"name": "Florida YIMBY, 6 Oct 2026", "date": "2026-10-06",
                           "url": SWIRE}}},

 {"name": "The Residences at Mandarin Oriental, Miami — tower at 2 Island Drive",
  "match_address": "2 ISLAND DR",
  "alt_addresses": ["500 Brickell Key Dr", "750 Claughton Island Dr"],
  "notes": SWIRE_NOTE + " This row is the one the City records under Swire Brickell Key Hotel "
           "Ltd, so it is the tower carrying the hotel component.",
  "values": {"developer": "Swire Properties", "architect": "Kohn Pedersen Fox"},
  "sources": {"_default": {"name": "Florida YIMBY, 6 Oct 2026", "date": "2026-10-06",
                           "url": SWIRE}}},

 {"name": "Miami Freedom Park — stadium",
  "match_address": "1000 STADIUM DR",
  "notes": FREEDOM_NOTE_COMMON + " This row is the stadium parcel.",
  "values": {},
  "sources": {"_default": {"name": "Florida YIMBY, 13 Jan 2025", "date": "2025-01-13",
                           "url": FREEDOM}}},

 {"name": "Miami Freedom Park — park",
  "match_address": "4000 NW 14 ST",
  "notes": FREEDOM_NOTE_COMMON + " This row is the public park parcel; its building permit "
           "scope reads NEW PARK ONLY.",
  "values": {},
  "sources": {"_default": {"name": "Florida YIMBY, 13 Jan 2025", "date": "2025-01-13",
                           "url": FREEDOM}}},

 {"name": "Miami Freedom Park — commercial component",
  "match_address": "1896 NW 37 AV",
  "notes": FREEDOM_NOTE_COMMON + " This row is the office/commercial parcel. Its use, its "
           "Moss contractor and its City ownership all fit the administration building, but "
           "724,470 sq ft does not reconcile with an eight-storey administration building, so "
           "the identification is not made and no team is recorded.",
  "values": {},
  "sources": {"_default": {"name": "Florida YIMBY, 13 Jan 2025", "date": "2025-01-13",
                           "url": FREEDOM}}},

 {"name": "LOFTY Brickell (One Brickell Riverfront)",
  "match_address": "600 SW 1 AV",
  "notes": "One of the two towers of One Brickell Riverfront, alongside The Standard "
           "Residences, Brickell; the pair offer more than 700 residences between them and no "
           "per-tower count is published. Architectural crowns were installed in June 2026 "
           "after both towers topped out. The development secured $513m of construction "
           "financing in late 2025 without the lender being named, and no article read for "
           "this run names a developer entity, so both fields are not found. The contractor on "
           "the City's permit, GT Construction & Development, matches the general contractor "
           "the coverage names.",
  "values": {"stories": 44},
  "sources": {"_default": {"name": "Florida YIMBY, 24 Jun 2026", "date": "2026-06-24",
                           "url": LOFTY}}},
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
