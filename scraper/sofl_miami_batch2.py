"""Append the second research pass's findings to the Miami overlay.

The first pass matched 16 of 34 projects because it required the permit's
address string to appear verbatim in a cue sentence. The press does not write
addresses that way -- see scraper/sofl_address_match.py -- so this pass
re-searched every Miami project of 100,000 sq ft or more that still had no
developer, matched on the address PATTERN with a title-first preference, and
these are the records whose article title names the address or the project.

Run once, then re-run scraper/sofl_miami_overlay.py and
scraper/sofl_provenance.py. Re-running this is harmless: a record whose
match_address is already in the overlay is skipped.

Only values a sentence states WITH A ROLE are recorded. Where an article lists
firms together as a "development team" or "major players" without saying which
is the developer and which the architect, nothing is read off it -- that is
noted on the row instead, because guessing the role is how the wrong firm ends
up in the wrong column.
"""

import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OVERLAY = os.path.join(ROOT, "data", "sofl", "miami_overlay.json")

FY = "Florida YIMBY, %s"


def src(date, url):
    return {"_default": {"name": FY % date, "date": date, "url": url}}


RECORDS = [
 {"name": "1401 Brickell by Santander", "match_address": "1401 BRICKELL AV",
  "notes": "Delivery targeted for 2029. Handel Architects is designing in collaboration with "
           "Bernardi & Peschard Arquitectura; Handel is stored and Bernardi & Peschard noted here. "
           "The article's 1.6m sq ft figure covers the whole development; the City's permit states "
           "1,280,763 sq ft and the permit figure is what this row carries.",
  "values": {"developer": "Santander", "architect": "Handel Architects", "stories": 50},
  "sources": src("2026-04-09",
                 "https://floridayimby.com/2026/04/50-story-1401-brickell-by-santander-breaks-ground-in-miami.html")},

 {"name": "Natiivo Miami", "match_address": "601 NE 1 AV",
  "notes": "Also marketed as 601 Miami. Only the storey count is stored: the article lists GFO "
           "Investments, Menin Hospitality, Cervera Real Estate, Arquitectonica, Urban Robot "
           "Associates, Gale Hotels and L&R Structural Corp together as major players involved in "
           "the development without assigning roles, so neither developer nor architect can be read "
           "off it. Sixth Street Miami Partners LLC and KAST Construction are named as hosting the "
           "topping-off.",
  "values": {"stories": 51},
  "sources": src("2022-11-17",
                 "https://floridayimby.com/2022/11/natiivo-tops-out-51-story-pinnacle-at-601-northeast-1st-avenue-in-downtown-miami.html")},

 {"name": "Forma Miami", "match_address": "2900 BISCAYNE BLVD",
  "notes": "Arquitectonica is the design architect and ODP Architecture & Design the architect of "
           "record, so ODP is stored and Arquitectonica noted here, per the field rule. The general "
           "contractor Stiles McHugh, LLC is a joint venture of Stiles Construction and McHugh "
           "Construction.",
  "values": {"developer": "Crescent Heights", "architect": "ODP Architecture & Design",
             "height_ft": 428, "stories": 39},
  "sources": src("2025-04-10",
                 "https://floridayimby.com/2025/04/yimby-scopes-views-of-forma-miami-at-2900-biscayne-boulevard-in-edgewater.html")},

 {"name": "Okan Tower", "match_address": "555 N MIAMI AV",
  "notes": "At roughly 75 per cent structural completion in April 2026, with 22 floors remaining.",
  "values": {"developer": "Okan Group", "architect": "Behar Font & Partners", "stories": 72},
  "sources": src("2026-04-22",
                 "https://floridayimby.com/2026/04/construction-reaches-50th-floor-on-okan-tower-at-555-north-miami-avenue-in-downtown-miami.html")},

 {"name": "The St. Regis Residences, Miami", "match_address": "1809 BRICKELL AV",
  "notes": "Storey count conflict: 50 storeys reported November 2024, 47 storeys and 632 ft in "
           "July 2025. The later figures are stored under the conflict rule.",
  "values": {"developer": "Related Group; Integra Investments",
             "architect": "Robert A.M. Stern Architects", "height_ft": 632, "stories": 47},
  "sources": {"_default": {"name": FY % "2025-07-10", "date": "2025-07-10",
                           "url": "https://floridayimby.com/2025/07/archaeological-discovery-surfaces-at-1809-brickell-avenue-during-construction-of-st-regis-residences.html"},
              "architect": {"name": FY % "2024-11-23", "date": "2024-11-23",
                            "url": "https://floridayimby.com/2024/11/the-st-regis-residences-miami-now-under-construction-at-1809-brickell-avenue.html"}}},

 {"name": "The Residences at 1428 Brickell", "match_address": "1428 BRICKELL AV",
  "notes": "Interior architecture by ACPV ARCHITECTS Antonio Citterio Patricia Viel. Ytech secured "
           "$565m of construction financing in November 2025; the lender's name is cut off in the "
           "sentence read for this run, so lender is not found rather than half-recorded.",
  "values": {"developer": "Ytech", "architect": "Arquitectonica",
             "residential_units": 195, "stories": 70},
  "sources": src("2026-09-03",
                 "https://floridayimby.com/2026/09/1428-brickell-continues-ascent-into-miamis-brickell-skyline.html")},

 {"name": "Mr. C Residences Coconut Grove", "match_address": "2655 S BAYSHORE DR",
  "notes": "Meyer Davis is the interior designer and ArqGEO the landscape architect. The coverage "
           "names no developer entity.",
  "values": {"architect": "Arquitectonica", "residential_units": 112, "stories": 21},
  "sources": src("2023-04-13",
                 "https://floridayimby.com/2023/04/mr-c-residences-slated-for-a-late-2023-opening-at-2655-s-bayshore-drive-coconut-grove-florida.html")},

 {"name": "One Brickell Tower II", "match_address": "77 SE 5 ST",
  "notes": "Enea Garden Design Inc. is the landscape architect.",
  "values": {"developer": "Related Group", "architect": "Arquitectonica",
             "residential_units": 506, "height_ft": 478, "stories": 47},
  "sources": src("2022-11-30",
                 "https://floridayimby.com/2022/11/foundation-permit-issued-to-the-related-group-for-47-story-tower-at-77-se-5th-street-in-brickell-miami.html")},

 {"name": "Modera Riverside", "match_address": "300 SW 2 AV",
  "values": {"developer": "Mill Creek Residential; Adler Development",
             "architect": "Nichols Architects", "residential_units": 428,
             "height_ft": 410, "stories": 36},
  "sources": src("2025-03-28",
                 "https://floridayimby.com/2025/03/finishing-touches-underway-for-modera-riverside-at-300-sw-2nd-avenue-in-downtown-miami.html")},

 {"name": "UniDos", "match_address": "1445 N MIAMI AV",
  "notes": "Also known as Uni + Dos. Developed in partnership with the City of Miami.",
  "values": {"developer": "NR Investments", "architect": "Corwil Architects",
             "residential_units": 398, "stories": 31},
  "sources": src("2026-02-03",
                 "https://floridayimby.com/2026/02/ceremionial-groundbreaking-set-for-unidos-at-1445-n-miami-ave-miami-fl.html")},

 {"name": "Sense 22", "match_address": "114 NE 22 ST",
  "alt_addresses": ["138 NE 22 St"],
  "notes": "New Sense, LLC is an affiliate of Argentina-based HA Emprendimientos, which is stored as "
           "the developer; the LLC is the owner of record. 132 of the 328 units are affordable "
           "rental housing under the Live Local Act.",
  "values": {"developer": "HA Emprendimientos", "architect": "Anillo Toledo Lopez Architecture",
             "residential_units": 328, "height_ft": 381, "stories": 36},
  "sources": src("2025-05-20",
                 "https://floridayimby.com/2025/05/plans-submitted-for-381-foot-tall-sense-22-at-114-ne-22nd-street-in-edgewater-miami.html")},

 {"name": "River Rapids", "match_address": "2750 NW SOUTH RIVER DR",
  "notes": "Two mid-rise buildings of eight and 12 storeys; the taller is stored. The coverage names "
           "no developer entity.",
  "values": {"architect": "Anillo Toledo Lopez", "residential_units": 593, "stories": 12},
  "sources": src("2022-08-04",
                 "https://floridayimby.com/2022/08/river-rapids-could-offer-593-apartments-at-2750-northwest-south-river-drive-miami-florida.html")},

 {"name": "Rhapsody at Rainbow Village", "match_address": "2000 NW 3 AV",
  "notes": "A $185m affordable housing community in Overtown with nearly 12,000 sq ft of retail.",
  "values": {"developer": "Housing Trust Group", "residential_units": 310},
  "sources": src("2025-09-14",
                 "https://floridayimby.com/2025/09/rhapsody-at-rainbow-village-tops-out-at-2000-nw-3rd-avenue-in-overtown-miami.html")},

 {"name": "One West Twelve Residences", "match_address": "1129 NW 1 CT",
  "notes": "Also known as ONE W12. 819 parking spaces over eight floors and 4,892 sq ft of retail.",
  "values": {"developer": "PMG; Lion Development Group; LNDMRK Development",
             "residential_units": 372, "stories": 23, "parking_spaces": 819,
             "commercial_gsf": 4892},
  "sources": src("2024-12-20",
                 "https://floridayimby.com/2024/12/one-west-twelve-residences-slated-for-1129-nw-1st-ct-miami-fl-33136.html")},

 {"name": "The Perrin", "match_address": "244 SW 9 ST",
  "notes": "Interiors by OTJ Architects. Approximately 2,500 sq ft of ground-floor retail and a "
           "380-space garage.",
  "values": {"developer": "Empira Group", "architect": "Corwil Architects",
             "residential_units": 310, "height_ft": 313, "stories": 26},
  "sources": src("2026-05-06",
                 "https://floridayimby.com/2026/05/the-perrin-tops-out-at-244-sw-9th-street-in-brickell-miami.html")},

 {"name": "Wynhouse Miami", "match_address": "2200 NW 1 AV",
  "notes": "Interiors by ID & Design International. Approximately 26,000 sq ft of ground-floor "
           "retail and paseo space.",
  "values": {"developer": "Fisher Brothers", "architect": "Nichols Architects",
             "residential_units": 308, "stories": 8},
  "sources": src("2024-03-20",
                 "https://floridayimby.com/2024/03/fisher-brothers-and-suffolk-construction-top-off-wynhouse-miami-at-2200-nw-1st-avenue-in-wynwood.html")},

 {"name": "Amara Wynwood", "match_address": "45 NE 27 ST",
  "notes": "A temporary certificate of occupancy was issued in May 2025, which agrees with the "
           "certificate on the City's own permit record. The coverage names no developer or "
           "architect.",
  "values": {"stories": 8},
  "sources": src("2025-05-23",
                 "https://floridayimby.com/2025/05/amara-wynwood-receives-tco-welcomes-residents-to-new-multifamily-development-at-45-ne-27th-street-in-miami.html")},

 {"name": "2200 Brickell", "match_address": "2200 BRICKELL AV",
  "notes": "Interiors by ODA. Received a temporary certificate of occupancy in July 2026.",
  "values": {"developer": "Aria Development Group; Largo; Place Projects",
             "architect": "Revuelta Architecture International",
             "residential_units": 103, "stories": 5},
  "sources": src("2026-07-28",
                 "https://floridayimby.com/2026/07/2200-brickell-receives-temporary-certificate-of-occupancy-in-miamis-brickell-neighborhood.html")},

 {"name": "Excel Miami", "match_address": "1550 NE MIAMI PL",
  "notes": "Arquitectonica is the design architect and LEO A DALY the architect of record, so LEO A "
           "DALY is stored and Arquitectonica noted here, per the field rule.",
  "values": {"developer": "Clearline Real Estate", "architect": "LEO A DALY",
             "height_ft": 275, "stories": 24},
  "sources": src("2025-04-11",
                 "https://floridayimby.com/2025/04/construction-rises-on-excel-miami-at-1550-ne-miami-place-in-miamis-arts-entertainment-district.html")},

 {"name": "Una Residences", "match_address": "175 SE 25 RD",
  "notes": "A temporary certificate of occupancy was issued in February 2026.",
  "values": {"developer": "OKO Group; Cain",
             "architect": "Adrian Smith + Gordon Gill Architecture", "stories": 47},
  "sources": src("2026-02-26",
                 "https://floridayimby.com/2026/02/oko-group-and-cain-receive-tco-for-47-story-una-residences-at-175-se-25th-road-in-brickell.html")},

 {"name": "Wynwood Works", "match_address": "2035 N MIAMI AV",
  "notes": "Wynwood Works LLC is a partnership between Magellan Housing and Udonis Haslem. The site "
           "is mostly vacant land owned by the OMNI Community Redevelopment Agency.",
  "values": {"developer": "Magellan Housing; Udonis Haslem",
             "architect": "Studio MC+G Architecture", "stories": 12},
  "sources": src("2021-07-04",
                 "https://floridayimby.com/2021/07/updated-renderings-and-diagrams-unveiled-for-wynwood-works-at-2035-north-miami-avenue-in-wynwood-miami.html")},

 {"name": "Havana Enclave", "match_address": "315 NW 27 AV",
  "notes": "Delivery anticipated at the beginning of 2027.",
  "values": {"developer": "The Astor Companies", "architect": "Hamed Rodriguez Architects",
             "lender": "Ocean Bank", "residential_units": 179},
  "sources": src("2025-09-10",
                 "https://floridayimby.com/2025/09/astor-companies-start-construction-on-havana-enclave-at-315-nw-27th-avenue-in-miamis-little-havana.html")},

 {"name": "View 29", "match_address": "2901 NW 2 AV",
  "notes": "Affordable housing, with about 6,900 sq ft of ground-floor retail and 105 parking "
           "spaces. The coverage names no developer entity.",
  "values": {"architect": "Studio MC+G Architecture", "residential_units": 116,
             "stories": 12, "commercial_gsf": 6900, "parking_spaces": 105},
  "sources": src("2021-10-23",
                 "https://floridayimby.com/2021/10/view-29-slated-for-at-2901-northwest-2nd-avenue-miami-fl-33127.html")},

 {"name": "WNWD21", "match_address": "2150 N MIAMI AV",
  "notes": "A two-towered development with over 18,000 sq ft of retail. Interiors by ID & Design "
           "International.",
  "values": {"developer": "Related Group; Rockpoint",
             "architect": "Cohen Freedman Encinosa & Associates",
             "residential_units": 304, "stories": 12},
  "sources": src("2022-12-17",
                 "https://floridayimby.com/2022/12/developers-break-ground-on-wnwd21-at-2150-n-miami-avenue-in-wynwood-miami.html")},

 {"name": "Metro 2 at Edgewater", "match_address": "3055 NE 4 AV",
  "notes": "Referred to as Metro 2 at Edgewater in the application. The site adjoins the completed "
           "Metro 1, developed by an affiliate of the applicant.",
  "values": {"developer": "Metro Tower II LLC", "architect": "Burgos Lanza & Associates",
             "stories": 28},
  "sources": src("2024-04-16",
                 "https://floridayimby.com/2024/04/plans-filed-for-28-story-mixed-use-tower-at-3055-northeast-4th-avenue-in-edgewater-miami.html")},

 {"name": "Domus Brickell Park", "match_address": "1611 SW 2 AV",
  "notes": "Interiors by Urban Robot Associates.",
  "values": {"developer": "North Development", "architect": "Zyscovich Architects",
             "residential_units": 171, "stories": 12},
  "sources": src("2024-10-05",
                 "https://floridayimby.com/2024/10/domus-brickell-park-begins-vertical-ascent-at-1611-sw-2nd-avenue-in-miamis-brickell-financial-district.html")},

 {"name": "Flagami Storage", "match_address": "5402 W FLAGLER ST",
  "notes": "A self-storage facility of 179,175 sq ft. Atelior305 is named as design architect and no "
           "architect of record is named. Flagler & 54, LLC is the owner on the application and the "
           "coverage names no developer.",
  "values": {"architect": "Atelior305", "stories": 7},
  "sources": src("2021-05-14",
                 "https://floridayimby.com/2021/05/flagami-storage-proposed-for-5402-west-flagler-steet-in-flagami-miami.html")},

 {"name": "Fourteen Allapattah Residences", "match_address": "1470 NW 36 ST",
  "notes": "A $70m development of two buildings, one of 14 storeys and one of five; the taller is "
           "stored. JAXI Builders, Behar Font Architects and designBAR are named together on the "
           "development team without roles assigned, so only the developer is read from the article.",
  "values": {"developer": "Neology Development Group", "stories": 14},
  "sources": src("2023-12-01",
                 "https://floridayimby.com/2023/12/fourteen-allapattah-residences-tops-off-at-1470-nw-36th-street-miami-florida.html")},

 {"name": "Atelier Residences Miami", "match_address": "1265 SW 22 ST",
  "notes": "Pfeifer Haus is the interior designer.",
  "values": {"developer": "BLDG Ventures", "architect": "Frances Architects",
             "residential_units": 148, "stories": 10},
  "sources": src("2024-11-18",
                 "https://floridayimby.com/2024/11/atelier-residences-miami-planned-for-2025-groundbreaking-at-1265-sw-22nd-street-miami-fl.html")},

 {"name": "Sweetbird North", "match_address": "95 NE 40 ST",
  "notes": "More than 92,000 sq ft of office and approximately 22,500 sq ft of retail in an "
           "approximately 120,500 sq ft project. Atria Health and Research Institute is the first "
           "confirmed office tenant.",
  "values": {"developer": "Miami Design District Associates; Raycliff Capital",
             "architect": "Snøhetta", "stories": 8},
  "sources": src("2026-10-04",
                 "https://floridayimby.com/2026/10/sweetbird-north-breaks-ground-at-95-northeast-40th-street-in-miamis-design-district.html")},

 {"name": "ENSO", "match_address": "3025 SW 3 AV",
  "notes": "Previously named Ambienta Brickell, when it was to comprise 79 units; plans changed and "
           "no current unit count is stated in this report, so units are not found. OTJ Architects "
           "is the interior designer.",
  "values": {"architect": "Revuelta Architecture", "lender": "ABANCA"},
  "sources": src("2025-03-26",
                 "https://floridayimby.com/2025/03/construction-to-start-on-enso-at-3025-sw-3rd-ave-miami-fl.html")},

 {"name": "The Arbor Residences", "match_address": "3034 OAK AV",
  "notes": "Originally broke ground in 2017 and stalled; ownership transferred from 3034 Oak Park "
           "LLC to Arbor Grove Development LLC. The coverage names no developer beyond the owning "
           "entity, which is held as owner of record.",
  "values": {"architect": "Behar Font & Partners", "residential_units": 52, "stories": 5},
  "sources": src("2023-11-10",
                 "https://floridayimby.com/2023/11/the-arbor-residences-aiming-for-2024-release-at-3034-oak-avenue-miami-florida-33133.html")},

 {"name": "Parkside Residences", "match_address": "1741 SW 2 AV",
  "notes": "Interiors by 2ID Interiors. Topping out scheduled for the end of 2026.",
  "values": {"developer": "BI Group USA", "architect": "Kobi Karp",
             "residential_units": 187, "stories": 8},
  "sources": src("2026-10-01",
                 "https://floridayimby.com/2026/10/parkside-residences-reaches-second-floor-at-1741-sw-2nd-avenue-in-brickell-miami.html")},

 {"name": "7140 NE Miami Court self-storage", "match_address": "7140 NE MIAMI CT",
  "notes": "A self-storage facility heard by the Urban Development Review Board. The same article "
           "also covers an unrelated three-tower, 1,292-apartment scheme; that unit count belongs to "
           "the other project and is deliberately not stored here. No developer or architect named.",
  "values": {},
  "sources": src("2023-01-17",
                 "https://floridayimby.com/2023/01/developers-to-propose-self-storage-facility-at-7140-northeast-miami-court-miami-florida-33138.html")},
]


def run(verbose=True):
    with open(OVERLAY, encoding="utf-8") as fh:
        overlay = json.load(fh)
    have = {r.get("match_address") for r in overlay}
    added, skipped = 0, []
    for rec in RECORDS:
        if rec["match_address"] in have:
            skipped.append(rec["name"])
            continue
        overlay.append(rec)
        have.add(rec["match_address"])
        added += 1
    with open(OVERLAY, "w", encoding="utf-8") as fh:
        json.dump(overlay, fh, indent=1, ensure_ascii=False)
    if verbose:
        print(f"added {added} record(s); overlay now holds {len(overlay)}")
        if skipped:
            print("  already overlaid, skipped: " + ", ".join(skipped))
    return {"added": added, "skipped": skipped, "total": len(overlay)}


if __name__ == "__main__":
    sys.exit(0 if run() else 0)
