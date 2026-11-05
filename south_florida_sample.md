# South Florida — spot-check sample

Ten projects per city, drawn at random with a fixed seed (20260301) so the selection is reproducible and a regenerated sample compares like with like. Every filled value below has a citation in the tracker's FIELD SOURCES panel; the distinct source URLs for each project are listed.

A blank is one of three things and is never printed as an empty space: **not found** means researched and not published by any acceptable source; **none** is a stated zero, such as an office tower with no residences; **not applicable** means the asset class has no dwellings in it. A warehouse with no unit count and a tower whose unit count nobody has published are both blank in the database and are not the same fact.

## Coverage

Counted ABOVE THE SIZE FLOOR — projects with 50 or more residential units or 50,000 or more square feet, which is what the app shows by default. The rows below the floor are still in the database and one toggle away; they are left out here because the City of Miami permit feed catalogues every permit in the city, and its tail of small buildings has no published lender, height or project name to find.

| City | Projects | Units known | of residential-bearing | Height in feet | Storeys | Hotel keys | Lender |
| --- | --: | --: | --: | --: | --: | --: | --: |
| Miami | 133 | 93 | 93 of 115 | 48 | 95 | 7 | 56 |
| Miami Beach | 19 | 19 | 19 of 19 | 5 | 17 | 1 | 5 |
| West Palm Beach | 29 | 25 | 25 of 27 | 8 | 27 | 3 | 11 |
| Coral Gables | 14 | 14 | 14 of 14 | 9 | 12 | 0 | 7 |
| Doral | 12 | 11 | 11 of 11 | 3 | 9 | 0 | 5 |
| **All five** | **207** | **162** | **162 of 186** | **73** | **160** | **11** | **84** |

Height in feet is the thinnest field in this market and will stay that way: the trade press prints storey counts and prints feet only when a project reaches a board approval that states one. Where feet are unpublished the storey count usually is not, which is why both columns are shown. The FAA Digital Obstacle File was calibrated as a bulk source for the missing feet and REJECTED at a 53 per cent hit rate — see scraper/sofl_faa_heights.py; it would have written 395 ft onto the 1,049 ft Waldorf Astoria, that being a 2018 survey of the building demolished to make room for it.

## Programme — the 147 largest projects

Every project with 100 or more dwellings, 100 or more hotel keys, or a published height of 200 feet or more. Ordered by height, then by unit count, so the rows with no published height fall to the bottom.

| Project | City | Status | Class | Units | Keys | Height | Storeys |
| --- | --- | --- | --- | --: | --: | --: | --: |
| Waldorf Astoria Hotel & Residences Miami | Miami | Under Construction | Mixed-Use | 387 | 205 | 1,049 ft | 100 |
| 130 Biscayne Boulevard | Miami | Planning | Mixed-Use | 414 | 144 | 955 ft | 87 |
| Cipriani Residences Miami | Miami | Under Construction | Condo | 397 | — | 950 ft | 80 |
| Okan Tower | Miami | Under Construction | Mixed-Use | 399 | 316 | 902 ft | 72 |
| 619 Brickell Avenue | Miami | Planning | Mixed-Use | 321 | — | 886 ft | 75 |
| The Residences at 1428 Brickell | Miami | Under Construction | Condo | 195 | — | 861 ft | 70 |
| The Residences at Mandarin Oriental, Miami — South Tower | Miami | Under Construction | Condo | 228 | — | 856 ft | 66 |
| Baccarat Residences Miami | Miami | Under Construction | Condo | 355 | — | 848 ft | 75 |
| 1401 Brickell by Santander | Miami | Under Construction | Office | none | — | 765 ft | 50 |
| 830 Brickell | Miami | Complete | Office | none | — | 724 ft | 57 |
| E11EVEN Club Residences Beyond | Miami | Under Construction | Condo | 550 | — | 700 ft | 65 |
| E11EVEN Club Hotel & Residences Miami | Miami | Complete | Condo | 457 | — | 700 ft | 65 |
| JEM Miami Worldcenter | Miami | Under Construction | Mixed-Use | 273 | — | 700 ft | 67 |
| Casa Bella Residences by B&B Italia | Miami | Complete | Condo | 319 | — | 650 ft | 56 |
| Villa Miami | Miami | Under Construction | Condo | 70 | — | 650 ft | 56 |
| Aria Reserve Miami — North Tower | Miami | Complete | Condo | 391 | — | 649 ft | 62 |
| Aria Reserve Miami — South Tower | Miami | Complete | Condo | 383 | — | 649 ft | 62 |
| Missoni Baia | Miami | Complete | Condo | 249 | — | 649 ft | 57 |
| The River District — Phase 1 | Miami | Complete | Multifamily Rental | 632 | — | 640 ft | 54 |
| The St. Regis Residences, Miami | Miami | Under Construction | Condo | 152 | — | 632 ft | 47 |
| Natiivo Miami | Miami | Complete | Mixed-Use | 448 | 240 | 588 ft | 51 |
| Miami World Tower 1 | Miami | Complete | Multifamily Rental | 560 | — | 579 ft | 53 |
| Una Residences | Miami | Complete | Condo | 129 | — | 579 ft | 47 |
| Downtown 6 | Miami | Under Construction | Multifamily Rental | 824 | — | 574 ft | 60 |
| LOFTY Brickell | Miami | Under Construction | Condo | 362 | — | 574 ft | 44 |
| 1575 Alton Road | Miami Beach | Planning | Multifamily Rental | 63 | — | 557 ft | 19 |
| 2600 Biscayne | Miami | Complete | Condo | 400 | — | 521 ft | 41 |
| FIVESIXTY Waterfront Residences + Club | Miami | Under Construction | Multifamily Rental | 114 | — | 497 ft | 38 |
| 38 West Eleventh Residences Miami | Miami | Under Construction | Condo | 659 | — | 482 ft | 44 |
| One Brickell Tower II | Miami | Complete | Condo | 506 | — | 478 ft | 47 |
| Rybovich Marina redevelopment | West Palm Beach | Approved | Mixed-Use | 660 | — | 461 ft | 36 |
| 222 Namdar Towers — Phase I | Miami | Complete | Multifamily Rental | 680 | — | 450 ft | 43 |
| 501 First Residences | Miami | Complete | Condo | 476 | — | 440 ft | 40 |
| Forma Miami | Miami | Complete | Condo | 588 | — | 428 ft | 39 |
| Flow House | Miami | Complete | Multifamily Rental | 466 | — | 413 ft | 40 |
| Modera Riverside | Miami | Complete | Multifamily Rental | 428 | — | 410 ft | 36 |
| District 225 | Miami | Complete | Condo | 343 | — | 394 ft | 37 |
| Sense 22 | Miami | Under Construction | Multifamily Rental | 328 | — | 381 ft | 36 |
| The Mandarin Oriental Residences, West Palm Beach | West Palm Beach | Approved | Condo | 87 | — | 380 ft | 31 |
| The Crosby Miami Worldcenter | Miami | Complete | Condo | 450 | — | 376 ft | 33 |
| South Flagler House | West Palm Beach | Under Construction | Condo | 105 | — | 373 ft | 28 |
| 2900 Terrace | Miami | Under Construction | Multifamily Rental | 324 | — | 362 ft | 32 |
| Midtown 7 | Miami | Under Construction | Multifamily Rental | 389 | — | 352 ft | 31 |
| 600 Miami Worldcenter | Miami | Under Construction | Condo | 606 | — | 330 ft | 32 |
| 1250 West Avenue | Miami Beach | Approved | Condo | 106 | — | 330 ft | 28 |
| Olara | West Palm Beach | Under Construction | Condo | 275 | — | 329 ft | 26 |
| 1501 Collins Avenue | Miami Beach | Planning | Mixed-Use | 124 | — | 325 ft | 25 |
| Metro 2 at Edgewater | Miami | Under Construction | Multifamily Rental | 103 | 60 | 320 ft | 28 |
| Residences at 464 Fern Street | West Palm Beach | Planning | Residential | 194 | — | 316 ft | 25 |
| The Perrin | Miami | Under Construction | Multifamily Rental | 310 | — | 313 ft | 26 |
| Shorecrest | West Palm Beach | Under Construction | Condo | 98 | — | 306 ft | 28 |
| Palm Beach Atlantic University residence hall | West Palm Beach | Under Construction | Institutional | 275 | — | 300 ft | 25 |
| Excel Miami | Miami | Under Construction | Multifamily Rental | 427 | — | 275 ft | 24 |
| 1826 Collins Avenue | Miami Beach | Approved | Multifamily Rental | 29 | — | 213 ft | 15 |
| Doral International Towers (Trump) | Doral | Approved | Mixed-Use | 1,498 | — | 210 ft | 20 |
| River District 14 | Miami | Under Construction | Condo | 283 | — | 180 ft | 16 |
| Ponce 8 | Coral Gables | Planning | Multifamily Rental | 201 | — | 175 ft | 16 |
| Gables Village (University of Miami Housing Phase 3) | Coral Gables | Approved | Institutional | 429 | — | 141 ft | — |
| 6100 Caballero Boulevard | Coral Gables | Planning | Residential | 251 | — | 140 ft | 13 |
| Fourteen Allapattah Residences | Miami | Complete | Multifamily Rental | 237 | — | 135 ft | 14 |
| The Aurelian | Coral Gables | Under Construction | Multifamily Rental | 160 | — | 130 ft | 12 |
| Ella Miami Beach | Miami Beach | Under Construction | Condo | 103 | — | 130 ft | 10 |
| The Mark | Coral Gables | Approved | Mixed-Use | 393 | — | 105 ft | 8 |
| Crystal Residences | Coral Gables | Planning | Mixed-Use | 184 | — | 93 ft | 9 |
| Amara Wynwood | Miami | Complete | Multifamily Rental | 210 | — | 92 ft | 8 |
| Deco Northwood | West Palm Beach | Planning | Multifamily Rental | 167 | — | 84 ft | 8 |
| Dulce Vida Apartments | Miami | Under Construction | Multifamily Rental | 227 | — | 80 ft | 8 |
| Hyatt Hotel | Doral | Approved | Hotel | — | 124 | 78.50 ft | 6 |
| Alexan Crafts | Coral Gables | Approved | Multifamily Rental | 263 | — | 77 ft | 7 |
| Legacy at Doral (EWE Warehouse) | Doral | Approved | Residential | 185 | — | 62.33 ft | — |
| Miami International Mall redevelopment | Doral | Planning | Residential | 900 | — | — | 6 |
| Midtown PUD Phase II Modification | Doral | Approved | Mixed-Use | 734 | — | — | — |
| 222 Namdar Towers — Phase II | Miami | Under Construction | Multifamily Rental | 714 | — | — | 43 |
| The Oasis at Doral | Doral | Approved | Mixed-Use | 623 | — | — | 9 |
| River Rapids | Miami | Complete | Multifamily Rental | 593 | — | — | 12 |
| Doral Parc (Midtown Doral Phases IV, V and VI) | Doral | Approved | Mixed-Use | 552 | — | — | 10 |
| Southgate Towers Live Local redevelopment | Miami Beach | Planning | Multifamily Rental | 442 | — | — | 38 |
| Sevilla at Downtown Doral | Doral | Under Construction | Multifamily Rental | 405 | — | — | 7 |
| 120 South Dixie Highway | West Palm Beach | Planning | Multifamily Rental | 399 | — | — | 25 |
| UniDos | Miami | Under Construction | Multifamily Rental | 398 | — | — | 31 |
| Doral City Place 7 | Doral | Approved | Residential | 391 | — | — | 8 |
| Hazel Doral (ZOM / Westside Live Local Act) | Doral | Under Construction | Mixed-Use | 380 | — | — | 7 |
| One West Twelve Residences | Miami | Under Construction | Condo | 372 | — | — | 23 |
| The Sound Apartments | West Palm Beach | Under Construction | Multifamily Rental | 358 | — | — | 8 |
| 501 Monceaux Road | West Palm Beach | Planning | Multifamily Rental | 350 | — | — | 8 |
| NoMad Residences Wynwood | Miami | Complete | Condo | 329 | — | — | 9 |
| Soleste Palm Station | West Palm Beach | Under Construction | Residential | 321 | — | — | 8 |
| Society Wynwood | Miami | Complete | Multifamily Rental | 318 | — | — | 10 |
| Rhapsody at Rainbow Village | Miami | Under Construction | Multifamily Rental | 310 | — | — | — |
| 2000 Wynwood | Miami | Under Construction | Multifamily Rental | 310 | — | — | 12 |
| Soleste Palm Station Two | West Palm Beach | Planning | Multifamily Rental | 310 | — | — | 15 |
| Wynhouse Miami | Miami | Complete | Multifamily Rental | 308 | — | — | 8 |
| WNWD21 | Miami | Complete | Multifamily Rental | 304 | — | — | 12 |
| Mohawk at Wynwood | Miami | Under Construction | Multifamily Rental | 300 | — | — | 12 |
| 2085 North Flagler Drive | West Palm Beach | Planning | Condo | 281 | — | — | 31 |
| 71 NoBe | Miami Beach | Approved | Mixed-Use | 277 | — | — | — |
| 534 Datura | West Palm Beach | Planning | Multifamily Rental | 261 | — | — | 25 |
| Uni Tower | Miami | Complete | Multifamily Rental | 252 | — | — | 29 |
| Little River Plaza | Miami | Under Construction | Multifamily Rental | 250 | — | — | 12 |
| Culmer Place | Miami | Complete | Multifamily Rental | 239 | — | — | 7 |
| NoBe PARC Miami Beach | Miami Beach | Planning | Condo | 232 | — | — | 15 |
| The Standard Residences Midtown Miami | Miami | Complete | Condo | 228 | — | — | 12 |
| 5250 Condo Tower Phase 2 | Doral | Approved | Condo | 228 | — | — | 20 |
| 7200 Collins | Miami Beach | Under Construction | Condo | 222 | — | — | 12 |
| The WELL Coconut Grove | Miami | Under Construction | Condo | 194 | — | — | — |
| The Berkeley Palm Beach | West Palm Beach | Under Construction | Condo | 193 | — | — | 25 |
| 208 NE 78 ST | Miami | Under Construction | Condo | 191 | — | — | 12 |
| Parkside Residences | Miami | Under Construction | Mixed-Use | 187 | — | — | 8 |
| Alida Residences and Tribute Portfolio by Marriott Hotel | West Palm Beach | Under Construction | Mixed-Use | 181 | 112 | — | 21 |
| Havana Enclave | Miami | Under Construction | Multifamily Rental | 179 | — | — | — |
| AVARA Residences | Miami Beach | Complete | Multifamily Rental | 178 | — | — | 8 |
| Cassia | Coral Gables | Under Construction | Condo | 174 | — | — | 12 |
| Domus Brickell Park | Miami | Complete | Hotel | 171 | — | — | 12 |
| Edgeworth | West Palm Beach | Planning | Condo | 168 | — | — | 28 |
| 209 North Sapodilla Avenue | West Palm Beach | Planning | Multifamily Rental | 164 | — | — | 7 |
| The Rider Residences | Miami | Under Construction | Condo | 152 | — | — | 12 |
| 611 27th Street | West Palm Beach | Planning | Multifamily Rental | 151 | — | — | 7 |
| 824 Alton Road | Miami Beach | Planning | Multifamily Rental | 149 | — | — | 39 |
| Atelier Residences Miami | Miami | Under Construction | Hotel | 148 | — | — | 10 |
| Mr. C Hotel & Residences West Palm Beach | West Palm Beach | Under Construction | Mixed-Use | 146 | 110 | — | 27 |
| The Alton | Miami Beach | Planning | Mixed-Use | 142 | — | — | 14 |
| The Ritz-Carlton Residences, West Palm Beach | West Palm Beach | Under Construction | Condo | 138 | — | — | 27 |
| 72 Carlyle | Miami Beach | Planning | Condo | 134 | — | — | 20 |
| Liberty Square Block 8 | Miami | Under Construction | Multifamily Rental | 132 | — | — | 7 |
| Regency Parc | Coral Gables | Complete | Residential | 126 | — | — | 20 |
| PALMA Miami Beach Residences | Miami Beach | Under Construction | Condo | 121 | — | — | 14 |
| The Villages Apartments Phase II | Miami | Complete | Condo | 120 | — | — | — |
| Wynwood Works | Miami | Complete | Multifamily Rental | 120 | — | — | 12 |
| Seventeen Gables Residences | Miami | Under Construction | Condo | 117 | — | — | 8 |
| Seventeen Gables Residences | Coral Gables | Under Construction | Condo | 117 | — | — | 8 |
| LILLI | Miami | Planning | Condo | 117 | — | — | 53 |
| View 29 | Miami | Under Construction | Mixed-Use | 116 | — | — | 12 |
| Mr. C Residences Coconut Grove | Miami | Complete | Condo | 112 | — | — | 21 |
| The Triangle | Miami | Under Construction | Multifamily Rental | 112 | — | — | 8 |
| Doral Inn/ IMF Doral Residential Conversion Live L | Doral | Approved | Multifamily Rental | 112 | — | — | — |
| Brisas del Sol | Miami | Complete | Multifamily Rental | 110 | — | — | 8 |
| Visions at Brickell Station | Miami | Under Construction | Hotel | 110 | — | — | 8 |
| Cassi | Miami | Under Construction | Multifamily Rental | 107 | — | — | — |
| 75 Broadway | Miami | Planning | Condo | 106 | — | — | 52 |
| The Ambar | Miami | Complete | Multifamily Rental | 105 | — | — | — |
| 1940 Park Avenue workforce housing | Miami Beach | Planning | Multifamily Rental | 105 | — | — | 6 |
| 2200 Brickell | Miami | Complete | Condo | 103 | — | — | 5 |
| Moxy Wynwood | Miami | Complete | Hotel | — | 120 | — | 12 |
| The Residences at Mandarin Oriental, Miami — North Tower | Miami | Under Construction | Mixed-Use | — | 121 | — | 33 |
| Hotel at NW 53 Terrace and Paseo Boulevard | Doral | Planning | Hotel | — | 168 | — | 8 |
| Grand Hyatt Miami Beach Convention Center Hotel | Miami Beach | Under Construction | Hotel | — | 800 | — | 17 |
| The Nora Hotel | West Palm Beach | Under Construction | Hotel | — | 201 | — | 5 |

## Random sample by city

## Miami — random 10 of 133

### 1129 SW 3 AV
- **Address:** 1129 SW 3 AV
- **Parcel id:** 0141380510260
- **Status:** Permitting
- **Asset class:** Hotel   **Neighbourhood:** West Brickell
- Developer: not found   |   Architect: not found   |   General contractor: KAST CONSTRUCTION COMPANY LLC   |   Civil engineer: not found   |   Lender: not found
- Residential units: not found   |   Height: not found   |   Stories: not found   |   Total sq ft: 287,323
- **Construction lender:** not found
- **Completion basis:** permit_issued_never_started
- **Sources (9 field citations, 3 distinct):**
    - City of Miami building permit BD18003273001B001 — https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD18003273001B001%27&outFields=*&f=pjson
    - Miami-Dade property boundary record, folio 0141380510260 — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0/query?where=FOLIO%3D%270141380510260%27&outFields=*&f=pjson
    - Miami-Dade property record, folio 01-4138-051-0260 (vacant land, year built 0), against City of Miami permit BD18003273001B001 Active since 2020-03-06 — https://www.miamidade.gov/Apps/PA/propertysearch/#/report/detail/0141380510260
- *Note:* Building permit BD18003273001B001 was issued on 6 March 2020 and is still Active with no finalised inspections six years later, and Miami-Dade assesses folio 01-4138-051-0260 as vacant land with year built 0. A 287,323 sq ft hotel has not been built here, so the row is Permitting -- a permit on file -- rather than Under Construction. NO NAME FROM AN ACCEPTABLE SOURCE, though the project is well documented: a 30-storey, 340 ft, 353-key dual-brand Hilton — 211 Embassy Suites keys plus 142 Home2 keys — for US Hospitality Group, architect Idea Architecture, reviewed by the City's Urban Development Review Board in February 2017, with a utilities agreement in 2018 and a tree permit in November 2019, all of which fits this row's 2018 master permit, KAST Construction and 287,323 sq ft. Every source carrying a name is off-limits. Note also that Embassy Suites and Home2 Suites are Hilton flags rather than a development name. THE ONE PLACE LEFT WORTH LOOKING is the February 2017 UDRB agenda item itself, which would be acceptable and could not be retrieved.

### 758 NW 2 ST
- **Address:** 758 NW 2 ST
- **Parcel id:** 0102000701090
- **Status:** Under Construction
- **Asset class:** Condo   **Neighbourhood:** Little Managua
- Developer: not found   |   Architect: not found   |   General contractor: FORTH BUILDING  LLC   |   Civil engineer: not found   |   Lender: not found
- Residential units: not found   |   Height: not found   |   Stories: not found   |   Total sq ft: 80,454
- **Construction lender:** not found
- **Completion basis:** permit_active
- **Sources (9 field citations, 2 distinct):**
    - City of Miami building permit BD22026231001B001 — https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22026231001B001%27&outFields=*&f=pjson
    - Miami-Dade property boundary record, folio 0102000701090 — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0/query?where=FOLIO%3D%270102000701090%27&outFields=*&f=pjson
- *Note:* NO STORABLE NAME. The project is Botanic Residences, an eight-storey, 51-unit short-term-rental condominium at 758 NW 2nd Street by Forth Development, architect Carl Levin Architect + Design, with a $17m Arixa Capital construction loan. Corroboration is strong — this row's contractor FORTH BUILDING LLC matches Forth Development and the owner of record is 752 AND 758 NW 2 ST LLC — but the only outlet carrying it is The Real Deal, which returns HTTP 403 to every automated fetch, and Florida YIMBY, Commercial Observer and Bisnow have nothing for this address. One browser visit would settle it.

### Cipriani Residences Miami
- **Address:** 1420 S MIAMI AV
- **Parcel id:** 0102090601040
- **Status:** Under Construction
- **Asset class:** Condo   **Neighbourhood:** Brickell Village
- Developer: Mast Capital   |   Architect: Arquitectonica   |   General contractor: Moss & Associates LLC   |   Civil engineer: not found   |   Lender: Banco Inbursa; Ascendant Capital Partners
- Residential units: 397   |   Height: 950 ft   |   Stories: 80   |   Total sq ft: 1,250,643
- **Construction lender:** Banco Inbursa; Ascendant Capital Partners
- **Completion basis:** permit_active
- **Sources (15 field citations, 5 distinct):**
    - City of Miami building permit BD22010804001B001 — https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22010804001B001%27&outFields=*&f=pjson
    - Florida YIMBY, 21 Apr 2026 — https://floridayimby.com/2026/04/cipriani-residences-miami-reaches-872-feet-becomes-miamis-tallest-residential-tower.html
    - Commercial Observer, 6 Feb 2024 — https://commercialobserver.com/2024/02/cipriani-residences-miami-mast-capital/
    - Miami-Dade property boundary record, folio 0102090601040 — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0/query?where=FOLIO%3D%270102090601040%27&outFields=*&f=pjson
    - Florida YIMBY, 24 Oct 2025 — https://floridayimby.com/2025/10/cipriani-residences-miami-reaches-halfway-mark-as-80-story-tower-rises-over-brickell.html
- *Note:* Height: the tower is reported as expected to reach approximately 950 ft on completion (April 2026), having reached 872 ft at that date; an approximately 940 ft figure was published in October 2025. The 950 ft completion figure is stored as the building height under the conflict rule and the measured 872 ft is noted here. Interiors by 1508 London; structural engineering by Thornton Tomasetti. Newmark arranged the loan. Identity confirmed against the row's address, developer and storey count. CONSTRUCTION LENDER: both parties sit on the same $600m senior construction facility, split $350m from Banco Inbursa and $250m from Ascendant Capital Partners, so both are recorded. No mezzanine or preferred-equity piece is reported. Borrower is Mast Capital.

### Courtside Apartments II
- **Address:** 1698 NW 3 AV
- **Parcel id:** 0131360640020
- **Status:** Complete
- **Asset class:** Multifamily Rental   **Neighbourhood:** Town Park
- Developer: Housing Trust Group; AM Affordable Housing   |   Architect: Corwil Architects   |   General contractor: BDI CONSTRUCTION COMPANY   |   Civil engineer: not found   |   Lender: not found
- Residential units: not found   |   Height: not found (5 storeys known)   |   Stories: 5   |   Total sq ft: 61,939
- **Construction lender:** not found
- **Completion basis:** co_issued  (2026-09-24)
- **Sources (14 field citations, 3 distinct):**
    - City of Miami building permit BD23002556001B001 — https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23002556001B001%27&outFields=*&f=pjson
    - Florida YIMBY, 20 Oct 2026 — https://floridayimby.com/2026/10/housing-trust-group-and-am-affordable-housing-celebrate-opening-of-58-million-courtside-apartments-ii-in-overtown-miami.html
    - Miami-Dade property boundary record, folio 0131360640020 — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0/query?where=FOLIO%3D%270131360640020%27&outFields=*&f=pjson
- *Note:* NAMED on an exact address match plus the contractor: the article gives the project's addresses as 1698 NW 3rd Avenue and 1501 NW 4th Avenue, the first being this row's exactly, and names BDI Construction as general contractor, matching this row. Third signal: the county's owner of record for folio 0131360640020 is Miami-Dade County Human Services, consistent with a county-land affordable development with Alonzo Mourning's AM Affordable Housing, and this row's certificate date of 24 September 2026 sits just before the October opening. UNITS NOT STORED: the 120 are split across the two buildings and no source gives this building's share.

### Culmer Place
- **Address:** 610 NW 10 ST
- **Parcel id:** 0101010102030
- **Status:** Complete
- **Asset class:** Multifamily Rental   **Neighbourhood:** Culmer
- Developer: Atlantic Pacific Cos.   |   Architect: Corwil Architects   |   General contractor: ATLANTIC PACIFIC COMMUNITY BUILDERS LLC   |   Civil engineer: not found   |   Lender: not found
- Residential units: 239   |   Height: not found (7 storeys known)   |   Stories: 7   |   Total sq ft: 124,749
- **Construction lender:** not found
- **Completion basis:** co_issued  (2026-08-27)
- **Sources (15 field citations, 3 distinct):**
    - City of Miami building permit BD21023937001B001 — https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD21023937001B001%27&outFields=*&f=pjson
    - Florida YIMBY, 23 Apr 2024 — https://floridayimby.com/2024/04/atlantic-pacific-cos-breaks-ground-on-culmer-place-at-800-n-w-fifth-ave-miami-fl.html
    - Miami-Dade property boundary record, folio 0101010102030 — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0/query?where=FOLIO%3D%270101010102030%27&outFields=*&f=pjson
- *Note:* NAMED on a parcel match plus a contractor match, two signals because the published address is the site's other frontage. The source places Culmer Place at 800 NW Fifth Ave and the county gives TRUE_SITE_ADDR 800 NW 5 AVE for this row's exact folio 0101010102030 — the Culmer superblock, of which 610 NW 10 ST is the permit frontage. Second signal: the source names Atlantic Pacific Cos. as both developer and general contractor and this row's master permit contractor is ATLANTIC PACIFIC COMMUNITY BUILDERS LLC. Owner of record is the Miami-Dade Housing Agency, consistent with a public-housing redevelopment. TWO NAMES EXIST FOR ONE PROJECT: the county's HCD report calls the same 239-unit scheme Culmer Apartments, after the entity Culmer Apartments Ltd, so Culmer Place is the marketed name and Culmer Apartments the funding-record one. The source describes two seven-storey buildings and this row's permit is the larger of two 2021 masters on the folio.

### Rhapsody at Rainbow Village
- **Address:** 2000 NW 3 AV
- **Parcel id:** 0131250630020
- **Status:** Under Construction
- **Asset class:** Multifamily Rental   **Neighbourhood:** Rainbow Village
- Developer: Housing Trust Group   |   Architect: not found   |   General contractor: ANF GROUP INC   |   Civil engineer: not found   |   Lender: KeyBank Real Estate Capital; J.P. Morgan Chase
- Residential units: 310   |   Height: not found   |   Stories: not found   |   Total sq ft: 501,584
- **Construction lender:** KeyBank Real Estate Capital; J.P. Morgan Chase
- **Completion basis:** permit_active
- **Sources (12 field citations, 4 distinct):**
    - City of Miami building permit BD22011051001B001 — https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22011051001B001%27&outFields=*&f=pjson
    - Florida YIMBY, 2025-09-14 — https://floridayimby.com/2025/09/rhapsody-at-rainbow-village-tops-out-at-2000-nw-3rd-avenue-in-overtown-miami.html
    - Commercial Observer, 11 Feb 2025 — https://commercialobserver.com/2025/02/miami-housing-trust-htg-jp-morgan-keybank-construction/
    - Miami-Dade property boundary record, folio 0131250630020 — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0/query?where=FOLIO%3D%270131250630020%27&outFields=*&f=pjson
- *Note:* A $185m affordable housing community in Overtown with nearly 12,000 sq ft of retail. CONSTRUCTION LENDERS: $109.2m of construction debt in two separate construction loans, KeyBank Real Estate Capital at $52m and J.P. Morgan Chase at $57.2m. Neither is subordinate to the other, so both are recorded. A long list of other facilities in the same capital stack is REJECTED by kind: roughly $70m of 4 per cent LIHTC equity from Raymond James, which is equity; $15m of SAIL financing, $15m from the SEOPW CRA, a $3m PHCD HOME loan and a $500k Miami-Dade grant, all public subsidy; a $3.1m PHCD seller note; $11.8m of bond reinvestment earnings; and KeyBank's separate $60.4m of PERMANENT financing. Florida YIMBY's Feb 2025 groundbreaking piece gives the $185m total cost but names no lender.

### River District 14
- **Address:** 1428 NW 14 AV
- **Parcel id:** 0131350100570
- **Status:** Under Construction
- **Asset class:** Condo   **Neighbourhood:** Civic Center
- Developer: Alta Development   |   Architect: Behar Font & Partners   |   General contractor: JAXI BUILDERS INC   |   Civil engineer: not found   |   Lender: Forman Capital
- Residential units: 283   |   Height: 180 ft   |   Stories: 16   |   Total sq ft: 459,623
- **Construction lender:** Forman Capital
- **Completion basis:** permit_active
- **Sources (16 field citations, 4 distinct):**
    - City of Miami building permit BD23026274001B001 — https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23026274001B001%27&outFields=*&f=pjson
    - Florida YIMBY, 16 Apr 2023 — https://floridayimby.com/2023/04/alta-development-proposes-16-story-short-term-rental-condo-project-at-1418-nw-14th-avenue-in-allapattah-miami.html
    - Florida YIMBY, 22 Sep 2024 — https://floridayimby.com/2024/09/77-8-million-loan-secured-for-short-term-rental-condominium-project-in-miamis-river-district.html
    - Miami-Dade property boundary record, folio 0131350100570 — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0/query?where=FOLIO%3D%270131350100570%27&outFields=*&f=pjson
- *Note:* Identity confirmed by the source giving this permit's exact address and by the county owner matching the project's branding. The 180 ft is a stated structure height, not a sea-level figure. The permit's 459,623 sq ft exceeds the 414,102 sq ft proposal, and the permit figure is the later government one and is kept.

### Sense 22
- **Address:** 114 NE 22 ST
- **Parcel id:** 0131250140080
- **Status:** Under Construction
- **Asset class:** Multifamily Rental   **Neighbourhood:** Edgewater
- Developer: HA Emprendimientos   |   Architect: Anillo Toledo Lopez Architecture   |   General contractor: GRYCON  LLC   |   Civil engineer: not found   |   Lender: S3 Capital
- Residential units: 328   |   Height: 381 ft   |   Stories: 36   |   Total sq ft: 502,266
- **Construction lender:** S3 Capital
- **Completion basis:** permit_active
- **Sources (15 field citations, 4 distinct):**
    - City of Miami building permit BD25007343001B001 — https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD25007343001B001%27&outFields=*&f=pjson
    - Florida YIMBY, 2025-05-20 — https://floridayimby.com/2025/05/plans-submitted-for-381-foot-tall-sense-22-at-114-ne-22nd-street-in-edgewater-miami.html
    - Commercial Observer, 8 Jul 2026 — https://commercialobserver.com/2026/07/s3-capital-miami-apartments/
    - Miami-Dade property boundary record, folio 0131250140080 — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0/query?where=FOLIO%3D%270131250140080%27&outFields=*&f=pjson
- *Note:* New Sense, LLC is an affiliate of Argentina-based HA Emprendimientos, which is stored as the developer; the LLC is the owner of record. 132 of the 328 units are affordable rental housing under the Live Local Act. An identity trap caught here: web searches attribute this $111m S3 Capital loan to Swire's Mandarin Oriental on Brickell Key. The article names Sense 22 at 114-138 NE 22nd Street with borrower HA Emprendimientos, which matches this row's address and developer, so it is recorded here and on neither Mandarin Oriental row. CONSTRUCTION LENDER: a $111m three-year construction loan to HA Emprendimientos for the 36-storey, 328-unit tower at 114-138 NE 22nd Street, Edgewater. REJECTED on this project: an earlier land-and-predevelopment mortgage on the same site, which is excluded by kind and names no lender in any case. IMPORTANT: this is the loan that search engines misattribute to the Mandarin Oriental towers — see the note on those rows.

### Smart Brickell
- **Address:** 255 SW 9 ST
- **Parcel id:** 0102050801110
- **Status:** Under Construction
- **Asset class:** Mixed-Use   **Neighbourhood:** West Brickell
- Developer: not found   |   Architect: not found   |   General contractor: HABITAT DEVELOPMENT  LLC   |   Civil engineer: not found   |   Lender: not found
- Residential units: not found   |   Height: not found   |   Stories: not found   |   Total sq ft: 115,334
- **Construction lender:** not found
- **Completion basis:** permit_active
- **Sources (10 field citations, 3 distinct):**
    - City of Miami building permit BD21007347001B001 — https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD21007347001B001%27&outFields=*&f=pjson
    - Florida YIMBY, 9 May 2021 — https://floridayimby.com/2021/05/tower-ones-superstructure-reaches-parapet-at-smart-brickell-in-miami.html
    - Miami-Dade property boundary record, folio 0102050801110 — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0/query?where=FOLIO%3D%270102050801110%27&outFields=*&f=pjson
- *Note:* NAMED. Identified by the same article naming this row's address as one of the project's identical towers, plus the contractor: this row's HABITAT DEVELOPMENT LLC is the construction arm of developer Habitat Group, and the owner of record is 255 BRICKELL INVESTMENT LLC. QUOTE CAVEAT: no single sentence in an acceptable source carries both the name and the 255 address — the quoted sentence names the project and the address tie is the sentence above it in the same article. THE TOWER NUMBER IS DELIBERATELY NOT ASSERTED: Commercial Observer of 16 May 2025 places phases one and two at 239 and 258 SW Ninth Street, which conflicts with Florida YIMBY's 255, so calling this Tower Two would be a guess even though the project is certain.

### WNWD21
- **Address:** 2150 N MIAMI AV
- **Parcel id:** 0131250480290
- **Status:** Complete
- **Asset class:** Multifamily Rental   **Neighbourhood:** Wynwood Industrial District
- Developer: Related Group; Rockpoint   |   Architect: Cohen Freedman Encinosa & Associates   |   General contractor: BEAUCHAMP CONSTRUCTION CO.  INC.   |   Civil engineer: not found   |   Lender: Bank OZK
- Residential units: 304   |   Height: not found (12 storeys known)   |   Stories: 12   |   Total sq ft: 211,052
- **Construction lender:** Bank OZK
- **Completion basis:** co_issued  (2025-03-11)
- **Sources (15 field citations, 4 distinct):**
    - City of Miami building permit BD21022699001B001 — https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD21022699001B001%27&outFields=*&f=pjson
    - Florida YIMBY, 2022-12-17 — https://floridayimby.com/2022/12/developers-break-ground-on-wnwd21-at-2150-n-miami-avenue-in-wynwood-miami.html
    - Bisnow, 19 Dec 2022 — https://www.bisnow.com/news/south-florida/deal-sheet/this-weeks-south-florida-deal-sheet-116924
    - Miami-Dade property boundary record, folio 0131250480290 — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0/query?where=FOLIO%3D%270131250480290%27&outFields=*&f=pjson
- *Note:* A two-towered development with over 18,000 sq ft of retail. Interiors by ID & Design International. CONSTRUCTION LENDER: about $118m, reported as $117.1m in some accounts, a single facility to the Related Group and Rockpoint joint venture with no mezzanine or preferred-equity piece. The Bisnow entry cites 2150 N Miami Ave, which is this row's address. THE SAME FACILITY COVERS THE SIBLING ROW, Wynwood Diamedix — east building at 2115 N Miami Ave: the two buildings are one financed assemblage running 2141 to 2115 and 2150 to 2160 North Miami Avenue, and both rows carry this lender. REJECTED: a $58m facility of 2017 belonging to Wynwood 25, a different Related project, and a $136m refinancing of 2020.

## Miami Beach — random 10 of 19

### 1030 6th Street
- **Address:** 1030 6 St
- **Parcel id:** 0242030097860
- **Status:** Planning
- **Asset class:** Multifamily Rental   **Neighbourhood:** FlamingoLummus
- Developer: not found   |   Architect: not found   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 72   |   Height: not found (5 storeys known)   |   Stories: 5   |   Total sq ft: not found
- **Construction lender:** not found
- **Sources (5 field citations, 4 distinct):**
    - City of Miami Beach, Live Local Act Applications Under Review — https://www.miamibeachfl.gov/city-hall/planning/live-local-act-applications-under-review/
    - Miami Beach neighborhoods layer — https://gis.miamibeachfl.gov/public/rest/services/gc/gc_Cadastral/MapServer/12
    - City of Miami Beach Civic Access portal, Live Local Act plan record LLA26-0007 (FKA PB26-0837) — https://energovcss.miamibeachfl.gov/EnerGovProd/SelfService/#/plan/b0b26cd2-986f-4315-bcb6-5313b9b5adc7
    - Miami-Dade property appraiser parcel record — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0
- *Note:* Programme fields are not found; the City's under-review page publishes only the address and application number. One of the three Live Local applications that defeated the earlier pass. Recovered from the EnerGov plan record itself (api/energov/plans/b0b26cd2-986f-4315-bcb6-5313b9b5adc7 with tenantId:3 and tenantName:miamibeachflprod2), not from any hosted PDF — the city page links only the portal. Status Under Review, applied 2026-03-30, district CPS-2. Elderly (age-restricted) workforce housing with ground-floor retail. No height in feet anywhere in the record.

### 1250 West Avenue
- **Address:** 1250 West Ave
- **Parcel id:** not found
- **Status:** Approved
- **Asset class:** Condo   **Neighbourhood:** not found
- Developer: Terra; RG Development Group; GV Development; LORE Development   |   Architect: ODP Architecture and Design   |   General contractor: not found   |   Civil engineer: not found   |   Lender: TYKO Capital
- Residential units: 106   |   Height: 330 ft   |   Stories: 28   |   Total sq ft: not found
- **Construction lender:** TYKO Capital
- **Sources (7 field citations, 2 distinct):**
    - Florida YIMBY, 17 May 2026 — https://floridayimby.com/2026/05/40002.html
    - Florida YIMBY, 21 Oct 2026 — https://floridayimby.com/2026/10/terra-and-partners-secure-507-million-construction-loan-for-28-story-condominium-at-1250-west-avenue-in-miami-beach.html
- *Note:* Design Review Board approved unanimously in May 2026 and a $507m TYKO Capital construction loan closed in October 2026, with sales expected to begin in 2027; no source reports a construction start, so the stage is Approved. An earlier JDS Development Group scheme for 29 storeys and 100 condos is superseded.

### 1501 Collins Avenue
- **Address:** 1501 Collins Ave
- **Parcel id:** 0232341630001
- **Status:** Planning
- **Asset class:** Mixed-Use   **Neighbourhood:** CityCenter
- Developer: Pebb Capital; Maxwell Real Estate Group; GFO Investments   |   Architect: AS+GG   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 124   |   Height: 325 ft   |   Stories: 25   |   Total sq ft: 487,633
- **Construction lender:** not found
- **Sources (9 field citations, 3 distinct):**
    - Florida YIMBY, 20 Sep 2026 — https://floridayimby.com/2026/09/developers-seek-to-restore-historic-hotel-and-build-25-story-tower-at-1501-collins-avenue-miami-beach-florida.html
    - Miami Beach neighborhoods layer — https://gis.miamibeachfl.gov/public/rest/services/gc/gc_Cadastral/MapServer/12
    - Miami-Dade property appraiser parcel record — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0
- *Note:* Feet above grade for the 25-storey tower, consistent with the floor area and unit count already stored. The sponsor sued the City in January 2026 alleging it blocked the redevelopment; the application remains on the under-review list.

### 71 NoBe
- **Address:** 400 71st St
- **Parcel id:** not found
- **Status:** Approved
- **Asset class:** Mixed-Use   **Neighbourhood:** not found
- Developer: Pacific Star Capital   |   Architect: not found   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 277   |   Height: not found   |   Stories: not found   |   Total sq ft: not found
- **Construction lender:** loan reported, lender not named
- **Sources (4 field citations, 1 distinct):**
    - Florida YIMBY, 7 Jan 2026 — https://floridayimby.com/2026/01/mixed-use-71-nobe-planned-for-2027-completion-at-400-71st-st-miami-beach-florida.html
- *Note:* The $94m facility is described as a construction loan but the lender is not named. A loan closing is not a construction start and no source reports one, so the stage is Approved. No unit count or storey count is published. The $94m construction loan is reported but its lender is named nowhere, so the field stays empty. Over 30,000 sq ft of retail is also planned. CONSTRUCTION LOAN CONFIRMED, LENDER NOT NAMED ANYWHERE ACCEPTABLE. Florida YIMBY of 7 Jan 2026 states that an affiliate of Pacific Star Capital has secured a $94 million construction loan for the project, and the article was fetched in full: the lender is simply not identified in it. No Commercial Observer, Bisnow or Real Deal article names it either. This is the cleanest example in the pass of a facility that exists, is unambiguously construction financing, and has no attributable lender. REJECTED: a $98m loan of 2022 to the Galbut family and Matis Cohen, a different North Beach project. NEXT STEP: Miami-Dade official records for a construction mortgage at 400 71st Street. LENDER NOT NAMED. Facility: $94m construction loan, reported Jan 2026. Reported as not named by any source. The cleanest case of the eight: Florida YIMBY states that an affiliate of Pacific Star Capital secured a $94 million construction loan, and the article was fetched in full -- the lender is simply not identified in it. No Commercial Observer, Bisnow or Real Deal article names it either.

### 7200 Collins
- **Address:** 7200 Collins Ave
- **Parcel id:** not found
- **Status:** Under Construction
- **Asset class:** Condo   **Neighbourhood:** not found
- Developer: Northlink Capital   |   Architect: Revuelta Architecture International   |   General contractor: Ortega Construction Company   |   Civil engineer: not found   |   Lender: Deutsche Bank
- Residential units: 222   |   Height: not found (12 storeys known)   |   Stories: 12   |   Total sq ft: 274,150
- **Construction lender:** Deutsche Bank
- **Completion basis:** news_confirmed
- **Sources (8 field citations, 2 distinct):**
    - Florida YIMBY, 2 Sep 2026 — https://floridayimby.com/2026/09/foundation-work-underway-at-7200-collins-in-north-beach-miami-beach.html
    - Florida YIMBY, 21 Mar 2026 — https://floridayimby.com/2026/03/groundbreaking-held-for-12-story-222-unit-7200-collins-in-miami-beach.html

### Ella Miami Beach
- **Address:** 6940 Abbott Ave
- **Parcel id:** 0232110010590
- **Status:** Under Construction
- **Asset class:** Condo   **Neighbourhood:** NorthShore
- Developer: Constellation Group; The Boschetti Group; Vietmar   |   Architect: Arquitectonica   |   General contractor: Ortega Construction Co.   |   Civil engineer: Biscayne Engineering   |   Lender: Amerant Bank
- Residential units: 103   |   Height: 130 ft   |   Stories: 10   |   Total sq ft: not found
- **Construction lender:** Amerant Bank
- **Completion basis:** news_confirmed
- **Sources (11 field citations, 5 distinct):**
    - Florida YIMBY, 13 Mar 2026 — https://floridayimby.com/2026/03/ella-miami-beach-to-top-out-in-two-weeks-at-6940-abbott-ave-miami-beach-florida.html
    - Florida YIMBY, 24 Feb 2023 — https://floridayimby.com/2023/02/developers-seek-design-review-approval-for-proposed-10-story-mixed-use-building-at-6940-abbott-avenue-in-miami-beach.html
    - Commercial Observer, 25 Mar 2025 — https://commercialobserver.com/2025/03/constellation-boschetti-ella-amerant-finance-miami-florida/
    - Miami Beach neighborhoods layer — https://gis.miamibeachfl.gov/public/rest/services/gc/gc_Cadastral/MapServer/12
    - Miami-Dade property appraiser parcel record — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0
- *Note:* A $42.3m three-year floating-rate construction loan brokered by Berkadia. The 130 ft height and the civil engineer both come from the 2023 design-review submittal, which described 96 units against the 103 built; the storey count is unchanged across all coverage, which is why the height is carried forward, but neither figure has been restated since 2023.

### NoBe PARC Miami Beach
- **Address:** 7140 Abbott Ave
- **Parcel id:** 0232110020470
- **Status:** Planning
- **Asset class:** Condo   **Neighbourhood:** NorthShore
- Developer: Robert Finvarb Cos.   |   Architect: Arquitectonica & The Architects Group   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 232   |   Height: not found (15 storeys known)   |   Stories: 15   |   Total sq ft: not found
- **Construction lender:** not found
- **Sources (7 field citations, 3 distinct):**
    - Florida YIMBY, 29 May 2026 — https://floridayimby.com/2026/05/15-story-nobe-parc-miami-beach-planned-for-2028-completion-at-7140-abbott-ave-miami-beach-florida.html
    - Miami Beach neighborhoods layer — https://gis.miamibeachfl.gov/public/rest/services/gc/gc_Cadastral/MapServer/12
    - Miami-Dade property appraiser parcel record — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0

### PALMA Miami Beach Residences
- **Address:** 600 71st St
- **Parcel id:** not found
- **Status:** Under Construction
- **Asset class:** Condo   **Neighbourhood:** not found
- Developer: Lefferts   |   Architect: Built Form   |   General contractor: KAST Construction   |   Civil engineer: not found   |   Lender: not found
- Residential units: 121   |   Height: not found (14 storeys known)   |   Stories: 14   |   Total sq ft: 127,600
- **Construction lender:** not found
- **Completion basis:** news_confirmed
- **Sources (7 field citations, 2 distinct):**
    - Florida YIMBY, 29 Jun 2026 — https://floridayimby.com/2026/06/foundation-work-underway-for-palma-miami-beach-at-600-71st-street-in-north-beach.html
    - Florida YIMBY, 24 Dec 2025 — https://floridayimby.com/2025/12/ground-breaks-on-14-story-palma-miami-beach-residences-at-600-71st-street-in-north-beach.html
- *Note:* NO CONSTRUCTION LOAN REPORTED. The project broke ground on 16 December 2025 and foundation work is under way, but neither Florida YIMBY article — the December 2025 groundbreaking or the June 2026 foundation piece — mentions any loan, lender or financing, and the groundbreaking article was fetched to confirm. Bisnow's deal-sheet coverage of the groundbreaking also carries none. Consistent with a heavily pre-sold condominium, roughly half sold at groundbreaking, funded on deposits and sponsor equity. REJECTED: Lefferts' $97.5m Ocean Bank facility for 72 Park and a $42.3m loan for Ella Miami Beach, both different projects.

### Southgate Towers Live Local redevelopment
- **Address:** 910 West Ave
- **Parcel id:** not found
- **Status:** Planning — **Denied** (dead)
- **Asset class:** Multifamily Rental   **Neighbourhood:** not found
- Developer: not found   |   Architect: Arquitectonica   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 442   |   Height: not found (38 storeys known)   |   Stories: 38   |   Total sq ft: not found
- **Construction lender:** not found
- **Sources (5 field citations, 1 distinct):**
    - The Real Deal, 30 Oct 2026 — https://therealdeal.com/miami/2026/10/30/miami-beach-axes-38-story-live-local-act-project-over-zoning/
- *Note:* Dead, recorded under the existing Denied status. The 495 units are the EXISTING buildings, not the proposal, so no unit count is stored for the new scheme. The applicant is Southgate Towers LLC; the property is Blackstone-owned. This corrects the row's earlier note that no unit count was published for the proposal: 442 units, being 264 condominiums and 178 apartments at workforce rents, replacing two existing 14-storey buildings of 495 units. The denial is appealable to the city board of adjustment. Blackstone Real Estate took the owner private in 2024.

### The Alton
- **Address:** 1656-1680 Alton Rd
- **Parcel id:** not found
- **Status:** Planning
- **Asset class:** Mixed-Use   **Neighbourhood:** not found
- Developer: Infinity Collective   |   Architect: PBDW Architects   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 142   |   Height: not found (14 storeys known)   |   Stories: 14   |   Total sq ft: not found
- **Construction lender:** not found
- **Sources (6 field citations, 2 distinct):**
    - Florida YIMBY, 3 Nov 2026 — https://floridayimby.com/2026/11/14-story-tower-proposed-for-1-38-acre-site-at-1680-alton-road-miami-beach-florida-33139.html
    - Florida YIMBY, 21 Mar 2026 — https://floridayimby.com/2026/03/miami-beach-planning-board-to-review-rezoning-plans-for-project-located-at-1677-west-ave-miami-beach-florida.html
- *Note:* Unit count revised from 184 residences to 142 apartments; the November 2026 figure is stored under the conflict rule. A separate, earlier six-storey SHVO office scheme designed by Foster + Partners with Kobi Karp was approved for this site in 2023 and never built; it is superseded and not recorded as its own project.

## West Palm Beach — random 10 of 29

### 2001 North Flagler Drive
- **Address:** 2001 N Flagler Dr
- **Parcel id:** 74434310190060110
- **Status:** Planning
- **Asset class:** Condo   **Neighbourhood:** not found
- Developer: Related Group; BH Group   |   Architect: Arquitectonica   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 90   |   Height: not found (27 storeys known)   |   Stories: 27   |   Total sq ft: 480,000
- **Construction lender:** not found
- **Sources (7 field citations, 3 distinct):**
    - Florida YIMBY, 29 Jan 2026 — https://floridayimby.com/2026/01/developers-propose-luxury-27-story-for-2001-n-flagler-dr-west-palm-beach-fl.html
    - Palm Beach County property appraiser parcel record — https://services1.arcgis.com/ZWOoUZbtaYePLlPw/arcgis/rest/services/Parcels_and_Property_Details_WebMercator/FeatureServer/0
    - City of West Palm Beach Planning Board agenda, meeting of 19 May 2026, Case No. 2025 SP-25100073 — https://www.wpb.org/files/assets/city/v/1/development-services/documents/planning-board/2026-pb-agendas/pb-agenda-2026.5.19-updated.pdf
- *Note:* The coverage names no developer entity and no unit count. The named affiliate matches this row's owner of record, which corroborates the attribution rather than resting on the LLC name. Residential units are deliberately not stored: the source gives 90 condominiums and eight lower-level townhouses separately and prints no total, so 98 would be arithmetic of mine. This is a different project from the Ritz-Carlton at 1717 N Flagler, also a Related Group and BH Group venture, and the two must not be merged. UNIT COUNT WITHHELD ON PURPOSE. Florida YIMBY of 29 Jan 2026 gives components with no total — 90 condos, with the average unit spanning 3,382 square feet, and eight lower-level townhouses — and these are not summed to 98. The same article's 27 stories and just over 480,000 square feet match the tracker. West Palm Beach 2026 Planning Board and PPRC notices separately describe a Level III Special Review for a 90-unit multifamily residential development in the CMUD Core-I subdistrict on a roughly 0.78-acre site at this address, which would make 90 the city's own figure, but that text could not be pinned to a specific dated agenda URL with a clean verbatim line, so it is not recorded as a value. No height in feet from any acceptable source. UNIT COUNT NOW RECORDED, replacing the earlier deliberate blank. A government primary record states a single total, so the no-arithmetic rule no longer forces a blank: the City counts 90 dwelling units total for the whole Level III Special Review on a roughly 0.78-acre tract. That means Florida YIMBY's 90 condos and eight lower-level townhouses is most likely that outlet's own gloss rather than two additive components, and must NOT be read as 98. Corroborated by The Real Deal of 27 Oct 2025 — a five-story parking podium and a 22-story tower with 90 units — and by the City's PPRC pre-application agenda, case FSPR 25-05 of 22 Jan 2026. HEIGHT POSITIVELY ABSENT rather than merely unsearched: the extracted agenda text contains no instance of feet, height or stor. The only foot figures in existence are a 195 ft height bonus requested against a 111 ft base cap, and summing those would be arithmetic. TRAP: searches readily return 306 feet and 27 stories for this address; that belongs to Shorecrest at 1901 North Flagler Drive, a different Related tower that is also 27 storeys, also on North Flagler and also uses 195 ft of bonus height. It must not be assigned here.

### 2085 North Flagler Drive
- **Address:** 2085 N Flagler Dr
- **Parcel id:** 74434310190060070
- **Status:** Planning
- **Asset class:** Condo   **Neighbourhood:** not found
- Developer: Terra; BH Group   |   Architect: not found   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 281   |   Height: not found (31 storeys known)   |   Stories: 31   |   Total sq ft: 440,000
- **Construction lender:** not found
- **Sources (6 field citations, 1 distinct):**
    - Florida YIMBY, 19 Jun 2026 — https://floridayimby.com/2026/06/developers-plan-two-31-story-towers-for-2085-n-flagler-dr-west-palm-beach-fl.html
- *Note:* The site was under contract from its owner, 2111 Flagler LLC, at the time of reporting. No unit count or architect is published. The same article contains a sentence naming two further partners on a Terra project, but that paragraph is about a different scheme in Miami Beach, so those firms are not added here. This address is a distinct project from 1919, 2001 and 1865/1901 North Flagler.

### 209 North Sapodilla Avenue
- **Address:** 209 N Sapodilla Ave
- **Parcel id:** 74434321010380010
- **Status:** Planning
- **Asset class:** Multifamily Rental   **Neighbourhood:** NORTHWEST RESIDENTS IN ACTION
- Developer: Related Ross   |   Architect: MSA Architects   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 164   |   Height: not found (7 storeys known)   |   Stories: 7   |   Total sq ft: not found
- **Construction lender:** not found
- **Sources (7 field citations, 3 distinct):**
    - Florida YIMBY, 22 Jul 2026 — https://floridayimby.com/2026/07/related-ross-revises-plans-for-affordable-housing-venture-at-209-n-sapodilla-ave-west-palm-beach-fl.html
    - West Palm Beach neighborhoods layer — https://wpbgisportal.wpb.org/server/rest/services/MayorOffice/WPB_Neighborhoods/FeatureServer/0
    - Palm Beach County property appraiser parcel record — https://services1.arcgis.com/ZWOoUZbtaYePLlPw/arcgis/rest/services/Parcels_and_Property_Details_WebMercator/FeatureServer/0
- *Note:* Plans were revised in July 2026; that report restates the approved 164-unit, seven-storey programme.

### Banyan Tree Residences West Palm Beach
- **Address:** 400 Hibiscus St
- **Parcel id:** not found
- **Status:** Approved
- **Asset class:** Condo   **Neighbourhood:** not found
- Developer: Mast Capital; Curated JCZM Development   |   Architect: Corwil Architects   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 88   |   Height: not found (25 storeys known)   |   Stories: 25   |   Total sq ft: not found
- **Construction lender:** not found
- **Sources (5 field citations, 2 distinct):**
    - Florida YIMBY, 22 Mar 2026 — https://floridayimby.com/2026/03/developers-propose-28-story-mixed-use-tower-for-400-hibiscus-street-west-palm-beach-florida.html
    - Florida YIMBY, 19 Aug 2026 — https://floridayimby.com/2026/08/banyan-tree-residences-west-palm-beach-secures-unanimous-downtown-action-committee-approval.html
- *Note:* OMA (Jason Long) is the design architect and Corwil Architects is the architect of record, so Corwil is stored and OMA recorded here, per the field rule. The storey count was published as 24 and 28 in March 2026 and as 25 in August 2026; the later figure is stored. The project is reported at 400 Hibiscus Street and at 635 South Dixie Highway with the same 88-residence, Mast Capital/Curated JCZM programme, which is one corner site under two frontages. NO CONSTRUCTION LOAN REPORTED. Approved only — the West Palm Beach Downtown Action Committee approved it unanimously in August 2026 with construction expected to begin in 2027 for 2029 delivery. Three Florida YIMBY articles, Commercial Observer's Mast Capital coverage of March and October 2026, and Bisnow were all checked and none mentions a loan or lender for 400 Hibiscus Street. Mast paid $5.2m for the site in early 2025, which is a purchase.

### Deco Northwood
- **Address:** 2900 Broadway
- **Parcel id:** 74434309050210100
- **Status:** Planning — **Denied** (dead)
- **Asset class:** Multifamily Rental   **Neighbourhood:** not found
- Developer: The Prime Company   |   Architect: not found   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 167   |   Height: 84 ft   |   Stories: 8   |   Total sq ft: not found
- **Construction lender:** not found
- **Sources (7 field citations, 3 distinct):**
    - Florida YIMBY, 9 Oct 2025 — https://floridayimby.com/2025/10/deco-northwood-proposed-for-2900-broadway-west-palm-beach-florida.html
    - City of West Palm Beach, City Commission Agenda — background for Resolution No. 228-25 — https://www.wpb.org/files/assets/city/v/2/city-clerk/documents/agendas/2025-pass-fail-agendas-pfa/2025-10-oct-pfa/pf-city-commission-agenda_10_27_25.pdf
    - Palm Beach County property appraiser parcel record — https://services1.arcgis.com/ZWOoUZbtaYePLlPw/arcgis/rest/services/Parcels_and_Property_Details_WebMercator/FeatureServer/0
- *Note:* The coverage names neither the developer nor the architect. The architect stays empty deliberately: the only firm named on the design is the developer's in-house studio, credited for the style and the renderings, which is not an architect-of-record statement. Municipality checked — 2900 Broadway is inside the City of West Palm Beach, in Northwood, and not Mangonia Park, which begins further north on the same road. Storey cross-check PASSES: the same staff section states an eight-story multi-family development with 40% workforce housing consisting of a total of 167 residential units, and both figures match the tracker. The 88 ft allowable in Table XVII-1 referenced in the same sentence is a cap and was not used. STATUS: the City Commission DENIED this application on 27 Oct 2025, so 84 ft is the height of the denied scheme; the status is corrected to Dead on that basis.

### Edgeworth
- **Address:** 1155 S Flagler Dr
- **Parcel id:** not found
- **Status:** Planning
- **Asset class:** Condo   **Neighbourhood:** not found
- Developer: Related Ross   |   Architect: Kohn Pedersen Fox   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 168   |   Height: not found (28 storeys known)   |   Stories: 28   |   Total sq ft: not found
- **Construction lender:** not found
- **Sources (5 field citations, 1 distinct):**
    - Florida YIMBY, 20 Mar 2026 — https://floridayimby.com/2026/03/two-tower-164-unit-edgeworth-slated-for-1155-south-flagler-drive-west-palm-beach-florida.html
- *Note:* The March 2026 report's headline says 164 units and its text says 168; the text figure is stored and the headline figure noted.

### Soleste Palm Station Two
- **Address:** 411 W Railroad Ave
- **Parcel id:** 74434321060270010
- **Status:** Planning
- **Asset class:** Multifamily Rental   **Neighbourhood:** NORTHWEST RESIDENTS IN ACTION
- Developer: Estate Cos.   |   Architect: BC Architects   |   General contractor: not found   |   Civil engineer: Craven Thompson & Associates   |   Lender: not found
- Residential units: 310   |   Height: not found (15 storeys known)   |   Stories: 15   |   Total sq ft: not found
- **Construction lender:** not found
- **Sources (8 field citations, 3 distinct):**
    - Florida YIMBY, 12 May 2026 — https://floridayimby.com/2026/05/15-story-soleste-palm-station-two-proposed-for-411-w-railroad-ave-west-palm-beach-fl.html
    - West Palm Beach neighborhoods layer — https://wpbgisportal.wpb.org/server/rest/services/MayorOffice/WPB_Neighborhoods/FeatureServer/0
    - Palm Beach County property appraiser parcel record — https://services1.arcgis.com/ZWOoUZbtaYePLlPw/arcgis/rest/services/Parcels_and_Property_Details_WebMercator/FeatureServer/0
- *Note:* Civil engineer is filled because the coverage names the full consultant team, which is the only circumstance in which that field is filled for this market.

### The Berkeley Palm Beach
- **Address:** 550 S Australian Ave
- **Parcel id:** 74434321120000040
- **Status:** Under Construction
- **Asset class:** Condo   **Neighbourhood:** not found
- Developer: Al Adelson   |   Architect: Arquitectonica   |   General contractor: John Moriarty & Associates   |   Civil engineer: not found   |   Lender: not found
- Residential units: 193   |   Height: not found (25 storeys known)   |   Stories: 25   |   Total sq ft: not found
- **Construction lender:** no construction loan reported
- **Completion basis:** news_confirmed
- **Sources (8 field citations, 3 distinct):**
    - Florida YIMBY, 7 Jul 2026 — https://floridayimby.com/2026/07/the-berkeley-palm-beach-breaks-ground-at-550-south-australian-avenue-in-west-palm-beach.html
    - financing structure documented; see notes — https://floridayimby.com/2025/09/the-berkeley-palm-beach-secures-62-5-million-pre-construction-loan-from-jvp-management-in-downtown-west-palm-beach.html
    - Palm Beach County property appraiser parcel record — https://services1.arcgis.com/ZWOoUZbtaYePLlPw/arcgis/rest/services/Parcels_and_Property_Details_WebMercator/FeatureServer/0
- *Note:* JVP Management provided a $62.5m pre-construction loan in September 2025. A pre-construction loan is not a construction loan, so no lender is recorded. The project is reported at both 500 and 550 South Australian Avenue. NO SENIOR CONSTRUCTION LOAN REPORTED YET, with direct evidence of its absence. Florida YIMBY of September 2025 reports $62.5m from JVP Management explicitly as a PRE-CONSTRUCTION loan, and quotes JVP saying the facility will support the project through its predevelopment phase and that it hopes to provide the construction financing in 2026 — which is a lender stating on the record that no construction facility existed. ALSO REJECTED: a $30m facility from Windsor Private Capital that property records style a construction loan, which is a site-level mortgage on the 1.92-acre parcel and is superseded five months later by the larger predevelopment loan, so it cannot be the senior construction loan for a 25-storey, 193-unit tower. The July 2026 groundbreaking article carries no financing content. JVP Management is the likely construction lender if and when it closes. FINANCING: A lender said on the record that no construction facility existed: Florida YIMBY of September 2025 reports $62.5m from JVP Management explicitly as a PRE-construction loan, and quotes JVP saying the facility will support the project through its predevelopment phase and that it hopes to provide the construction financing in 2026. A $30m Windsor Private Capital facility that property records style a construction loan is a site-level mortgage on the 1.92-acre parcel, superseded five months later by that larger predevelopment loan.

### The Ritz-Carlton Residences, West Palm Beach
- **Address:** 1717 N Flagler Dr
- **Parcel id:** not found
- **Status:** Under Construction
- **Asset class:** Condo   **Neighbourhood:** not found
- Developer: Related Group; BH Group   |   Architect: Arquitectonica   |   General contractor: not found   |   Civil engineer: not found   |   Lender: Madison Realty Capital
- Residential units: 138   |   Height: not found (27 storeys known)   |   Stories: 27   |   Total sq ft: not found
- **Construction lender:** Madison Realty Capital
- **Completion basis:** news_confirmed
- **Sources (6 field citations, 3 distinct):**
    - Florida YIMBY, 24 Feb 2026 — https://floridayimby.com/2026/02/related-group-and-bh-group-break-ground-on-the-ritz-carlton-residences-west-palm-beach.html
    - Commercial Observer, 3 Mar 2026 — https://commercialobserver.com/2026/03/related-bh-west-palm-beach-ritz-carlton-madison-realty-flagler/
    - Florida YIMBY, 20 Oct 2025 — https://floridayimby.com/2025/10/wave-of-new-residential-development-takes-shape-across-west-palm-beach.html
- *Note:* Storey count published as 26 in October 2025 and 27 in February 2026; the later figure is stored. The 138-residence figure comes from a multi-project round-up where attribution is by paragraph, so it is less firmly attributed than the rest of this record. A $600m Bank OZK construction loan reported in the same round-up is attributed in its own headline to South Flagler House, not to this project, so no lender is recorded here. CONSTRUCTION LENDER: a $200m construction loan for the 27-storey, 138-unit tower at 1717 North Flagler Drive, both figures matching this row. A single facility with no mezzanine or preferred-equity companion reported.

### The Sound Apartments
- **Address:** 8111 S Dixie Hwy
- **Parcel id:** 74434416400020000
- **Status:** Under Construction
- **Asset class:** Multifamily Rental   **Neighbourhood:** SOUTH END NEIGHBORHOOD ASSOCIATION
- Developer: Woodfield Development; Flagler Realty & Development Inc.   |   Architect: Spina O'Rourke + Partners   |   General contractor: Verdex Construction   |   Civil engineer: not found   |   Lender: not found
- Residential units: 358   |   Height: not found (8 storeys known)   |   Stories: 8   |   Total sq ft: not found
- **Construction lender:** loan reported, lender not named
- **Completion basis:** news_confirmed
- **Sources (9 field citations, 3 distinct):**
    - Florida YIMBY, 4 Mar 2026 — https://floridayimby.com/2026/03/construction-nears-completion-on-the-sound-apartments-at-8111-south-dixie-highway-in-west-palm-beach.html
    - West Palm Beach neighborhoods layer — https://wpbgisportal.wpb.org/server/rest/services/MayorOffice/WPB_Neighborhoods/FeatureServer/0
    - Palm Beach County property appraiser parcel record — https://services1.arcgis.com/ZWOoUZbtaYePLlPw/arcgis/rest/services/Parcels_and_Property_Details_WebMercator/FeatureServer/0
- *Note:* The most recent dated source says construction is nearing completion, not complete, so the stage is Under Construction; no source states a certificate of occupancy or the start of leasing. A CONSTRUCTION LOAN EXISTS AND IS NOT STORABLE. $80.6m of construction financing is reported for this Woodfield Development and Flagler Realty project at 8111 S Dixie Highway, with the leasehold mortgage provided by PNC BANK — but only via bldup, citybiz, cre-sources, multifamilybiz and yieldpro, none acceptable. Both Florida YIMBY articles were fetched, the July 2025 topping-out and the March 2026 nearing-completion piece, and neither names any lender or financing. REJECTED: Bisnow's Woodfield coverage of a $47m Northwestern Mutual construction loan, which is a DIFFERENT West Palm Beach building of 223 units later sold to Griffis Residential. NEXT STEP: a Palm Beach County mortgage record would be enough to store PNC Bank. LENDER NOT NAMED. Facility: $80.6m construction financing, as a leasehold mortgage. Reported as PNC Bank. Named only by bldup, citybiz, cre-sources, multifamilybiz and yieldpro. Both Florida YIMBY articles were fetched, the July 2025 topping-out and the March 2026 nearing-completion piece, and neither names any lender.

## Coral Gables — random 10 of 14

### 6100 Caballero Boulevard
- **Address:** 6100 Caballero Blvd
- **Parcel id:** 0341300060290
- **Status:** Planning
- **Asset class:** Residential   **Neighbourhood:** not found
- Developer: Fortune International Group   |   Architect: Portuondo Perotti Architects   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 251   |   Height: 140 ft   |   Stories: 13   |   Total sq ft: not found
- **Construction lender:** not found
- **Sources (7 field citations, 3 distinct):**
    - Florida YIMBY, 12 Jun 2025 — https://floridayimby.com/2025/06/two-tower-development-planned-for-6100-caballero-blvd-coral-gables-florida.html
    - Coral Gables Gazette, 15 May 2025 — https://gables-gazette.com/18-development-projects-under-city-review-in-coral-gables/
    - Miami-Dade property appraiser parcel record — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0
- *Note:* Portuondo Perotti Architects and Arquitectonica are both named as architects and the source does not say which is architect of record; Portuondo Perotti is stored and Arquitectonica recorded here. The taller of the two buildings is stored for storeys. The source gives a range across the two buildings rather than one number; 140 ft is the taller, which is the 13-storey building already stored, and the nine-storey building is 97 ft. The 251-unit figure in the same entry matches the current scheme, confirming this is not the superseded earlier proposal.

### Alexan Crafts
- **Address:** 300 Catalonia Ave
- **Parcel id:** not found
- **Status:** Approved
- **Asset class:** Multifamily Rental   **Neighbourhood:** not found
- Developer: Trammell Crow Residential   |   Architect: Corwil Architects   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 263   |   Height: 77 ft   |   Stories: 7   |   Total sq ft: not found
- **Construction lender:** not found
- **Sources (7 field citations, 1 distinct):**
    - Florida YIMBY, 2 Jun 2021 — https://floridayimby.com/2021/06/2255.html
- *Note:* Stage is Approved because no source on the acceptable list carries a dated construction report for this project. Sources outside that list report exterior walls reaching the seventh floor during 2025, which would make it Under Construction; that has not been written because the brief restricts sources to government records, the named city pages and the named trade press. NO CONSTRUCTION LOAN REPORTED, which is expected at this stage — the row is only Approved. Florida YIMBY's Trammell Crow Residential category, Commercial Observer, Bisnow and the Coral Gables Legistar record were all searched and no financing event exists.

### Cassia
- **Address:** 4011 Salzedo Ave
- **Parcel id:** not found
- **Status:** Under Construction
- **Asset class:** Condo   **Neighbourhood:** not found
- Developer: Alta Developers   |   Architect: Behar Font & Partners   |   General contractor: not found   |   Civil engineer: not found   |   Lender: JVP Management
- Residential units: 174   |   Height: not found (12 storeys known)   |   Stories: 12   |   Total sq ft: not found
- **Construction lender:** JVP Management
- **Completion basis:** news_confirmed
- **Sources (6 field citations, 2 distinct):**
    - Florida YIMBY, 12 Mar 2025 — https://floridayimby.com/2025/03/construction-underway-at-cassia-a-12-story-condominium-in-coral-gables-merrick-park-district.html
    - Bisnow South Florida Deal Sheet, 2 Jun 2025 — https://www.bisnow.com/news/south-florida/deal-sheet/this-weeks-south-florida-deal-sheet-alta-developers-land-94m-construction-loan-for-luxury-condo-is-coral-gables-129620
- *Note:* Behar Font & Partners is named as design architect and no architect of record is named. The most recent dated source found in this run is the March 2025 construction report; no source states completion, so the stage is Under Construction rather than Complete. JLL arranged the financing. The construction character of the facility comes from the same item's own headline, quoted alongside the sentence naming the lender.

### Cora Merrick Park
- **Address:** 4241 Aurora St
- **Parcel id:** 0341200171410
- **Status:** Under Construction
- **Asset class:** Mixed-Use   **Neighbourhood:** not found
- Developer: Constellation Group; The Boschetti Group   |   Architect: Arquitectonica   |   General contractor: Winmar Construction   |   Civil engineer: not found   |   Lender: BHI
- Residential units: 74   |   Height: not found (13 storeys known)   |   Stories: 13   |   Total sq ft: not found
- **Construction lender:** BHI
- **Completion basis:** news_confirmed
- **Sources (8 field citations, 3 distinct):**
    - Florida YIMBY, 10 Jun 2026 — https://floridayimby.com/2026/06/construction-loan-secured-for-cora-merrick-park-at-4241-aurora-street-in-coral-gables.html
    - Florida YIMBY, 24 Apr 2026 — https://floridayimby.com/2026/04/cora-merrick-park-set-for-2028-completion-at-4241-aurora-st-coral-gables-fl.html
    - Miami-Dade property appraiser parcel record — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0
- *Note:* The $67.5m construction loan is reported without naming the lender, so lender is not found. Berkadia arranged the loan and BHI provided it. Height is deliberately not recorded: the only published figure, 137.5 ft, describes a superseded eight-storey-over-three scheme, and the current design is 13 storeys. HEIGHT FOUND BUT WITHHELD ON A STOREY MISMATCH. City of Coral Gables PZB Staff Report, 4241 Aurora, file 23-6279 of 21 Sep 2023 (https://coralgables.legistar1.com/coralgables/attachments/42f4d46e-7800-4a50-9654-8f516fb4102e.pdf) states building height is 12 stories at 137.5 feet to the top of the roof, and its public notice repeats that the proposed building is 137.5 feet tall. The measurement basis is sound, but it comes with 12 stories and 80 residential units against the 13 storeys and 74 units stored, and Cora Merrick Park is a redesign of the approved 4241 Aurora scheme, so 137.5 ft is probably superseded. Signed Resolution 2023-315 phrases it as a cap, and there is no later site-plan amendment in Legistar.

### Crystal Residences
- **Address:** 110 Phoenetia Ave
- **Parcel id:** 0341080091840
- **Status:** Planning
- **Asset class:** Mixed-Use   **Neighbourhood:** not found
- Developer: Fifield Cos.   |   Architect: Corwil Architects   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 184   |   Height: 93 ft   |   Stories: 9   |   Total sq ft: 226,193
- **Construction lender:** not found
- **Sources (8 field citations, 3 distinct):**
    - Florida YIMBY, 16 Jun 2026 — https://floridayimby.com/2026/06/fifield-cos-seeks-rezoning-approval-for-crystal-residences-at-110-phoenetia-ave-coral-gables-fl.html
    - Coral Gables Gazette, 14 Jun 2026 — https://gables-gazette.com/after-years-of-appeals-crystal-residences-reaches-the-zoning-stage/
    - Miami-Dade property appraiser parcel record — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0
- *Note:* 93 ft is the height sought in the planned-area-development request for the current nine-storey scheme, which matches the storey count on the row. A superseded 83 ft figure belongs to an earlier 177-unit scheme by a different sponsor.

### Gables Village (University of Miami Housing Phase 3)
- **Address:** 1201 Stanford Dr
- **Parcel id:** not found
- **Status:** Approved
- **Asset class:** Institutional   **Neighbourhood:** not found
- Developer: University of Miami   |   Architect: Arquitectonica   |   General contractor: not found   |   Civil engineer: Langan   |   Lender: not found
- Residential units: 429   |   Height: 141 ft   |   Stories: not found   |   Total sq ft: not found
- **Construction lender:** no construction loan reported
- **Sources (7 field citations, 2 distinct):**
    - Florida YIMBY, 30 Nov 2025 — https://floridayimby.com/2025/11/gables-village-phase-3-of-the-university-of-miamis-housing-facilities-strategic-plan-to-replace-mahoney-pearson-residential-college-in-coral-gables.html
    - financing structure documented; see notes — https://www.miamidade.gov/govaction/legistarfiles/Matters/Y2024/241790.pdf
- *Note:* Stored as Institutional because it is university-owned on-campus housing delivered under a Campus Master Plan Amendment, not a rental or for-sale residential business. The taller of the two parapet heights is stored. Langan is named as civil engineer in the project team, which is why that field is filled here and left blank elsewhere. NO CONSTRUCTION LOAN REPORTED. The project is funded through the Miami-Dade County Educational Facilities Authority, which approved up to $500m of tax-exempt revenue bonds on behalf of the University of Miami to finance and refinance capital improvements across its campuses — a conduit bond programme rather than a project construction loan, with no construction lender named anywhere. Florida YIMBY's Nov 2025 article, Commercial Observer, Bisnow, the county bond application and the Board of County Commissioners legislative file were all checked. Demolition is set for summer 2026 with construction immediately after, so a facility may yet be reported. FINANCING: Funded through the Miami-Dade County Educational Facilities Authority, which approved up to $500m of tax-exempt revenue bonds on behalf of the University of Miami to finance and refinance capital improvements across its campuses. That is a conduit bond programme rather than a project construction loan. Demolition is set for summer 2026 with construction immediately after, so a facility may yet be reported.

### Regency Parc
- **Address:** 2601 Salzedo St
- **Parcel id:** 0341170052330
- **Status:** Complete
- **Asset class:** Residential   **Neighbourhood:** not found
- Developer: Codina Partners   |   Architect: Nichols Architects   |   General contractor: not found   |   Civil engineer: not found   |   Lender: Goldman Sachs
- Residential units: 126   |   Height: not found (20 storeys known)   |   Stories: 20   |   Total sq ft: not found
- **Construction lender:** Goldman Sachs
- **Completion basis:** news_confirmed  (2026-06)
- **Sources (7 field citations, 3 distinct):**
    - Florida YIMBY, 2 Jun 2026 — https://floridayimby.com/2026/06/regency-parc-begins-welcoming-first-residents-in-downtown-coral-gables.html
    - Commercial Observer, 15 Nov 2023 — https://commercialobserver.com/2023/11/codina-goldman-sachs-coral-gables/
    - Miami-Dade property appraiser parcel record — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0
- *Note:* The coverage does not state whether the residences are rental or for sale, so the project is stored under Residential rather than assigned to Condo on an assumption. The loan predates the final design: the 2023 coverage describes an 18-storey, 124-unit building at this address while the delivered tower is 20 storeys with 126 residences. Same address and same developer, so it is the same project at an earlier design stage. HEIGHT FOUND BUT WITHHELD ON A STOREY MISMATCH. City of Coral Gables Planning and Zoning Staff Report, Regency Tower, files 22-3727 and 22-3729 of 9 Mar 2022 (https://coralgables.legistar1.com/coralgables/attachments/f7884e7a-f30f-4d4e-a919-69020aaca756.pdf) states the proposed building height is 18 stories at 204 feet 8 inches to the top of roof, 239 feet 8 inches to the top of ornamental tower. The 239 feet 8 inches is REJECTED as a measurement to an ornamental tower rather than the roof. The 204 feet 8 inches is a genuine roof height and is carried into signed Ordinance 2022-32 and Resolution 2022-170 of 28 Jun 2022, but it comes with 18 storeys against the 20 stored, which Florida YIMBY also reports as 20. The 2024 and April 2026 amendments cover public-art escrow and a temporary CO and restate no height, so 204 feet 8 inches is the standing approved roof height and becomes usable once the 18-versus-20 storey question is settled.

### Seventeen Gables Residences
- **Address:** 1715 Douglas Rd
- **Parcel id:** not found
- **Status:** Under Construction
- **Asset class:** Condo   **Neighbourhood:** not found
- Developer: BAM Development; Ascendra Capital   |   Architect: Caymares Martin A&E Design   |   General contractor: not found   |   Civil engineer: not found   |   Lender: Dwight Mortgage Trust
- Residential units: 117   |   Height: not found (8 storeys known)   |   Stories: 8   |   Total sq ft: not found
- **Construction lender:** Dwight Mortgage Trust
- **Completion basis:** news_confirmed
- **Sources (6 field citations, 2 distinct):**
    - Florida YIMBY, 12 Feb 2026 — https://floridayimby.com/2026/02/seventeen-gables-condominium-secures-60-million-construction-loan-set-for-immediate-construction-at-1715-douglas-road-in-coral-gables.html
    - Florida YIMBY, 6 May 2026 — https://floridayimby.com/2026/05/seventeen-gables-residences-breaks-ground-at-1715-douglas-road-in-coral-gables.html
- *Note:* Two architects are named across the coverage: Caymares Martin A&E Design (February 2026) and Asprea Studio, described in May 2026 as design architect and interior designer. Caymares Martin A&E Design is stored as the architect of record and Asprea Studio recorded here as design architect, per the field rule. RETIRED AS A DUPLICATE, not deleted. This row and the City of Miami permit row at 1715 SW 37 AV are the same building: SW 37th Avenue IS Douglas Road, and both are Under Construction condominiums of 117 units over eight storeys. The permit row is kept as canonical because it carries the master building permit, the folio, the contractor and the floor area, and because the City of Miami issuing the permit means the parcel is in Miami rather than Coral Gables — this row's Coral Gables placement came from the marketing address. The row is excluded from the view rather than removed so the duplicate stays visible to anyone auditing the Coral Gables ingest, which is where it came from.

### The Aurelian
- **Address:** 921 E Ponce de Leon Blvd
- **Parcel id:** not found
- **Status:** Under Construction
- **Asset class:** Multifamily Rental   **Neighbourhood:** not found
- Developer: Empira Group   |   Architect: Behar Font & Partners   |   General contractor: Ortega Construction   |   Civil engineer: not found   |   Lender: Pearlmark
- Residential units: 160   |   Height: 130 ft   |   Stories: 12   |   Total sq ft: 324,000
- **Construction lender:** Pearlmark
- **Completion basis:** news_confirmed
- **Sources (9 field citations, 3 distinct):**
    - Florida YIMBY, 13 Aug 2026 — https://floridayimby.com/2026/08/the-aurelian-planned-to-feature-161-high-end-residences-at-921-east-ponce-de-leon-boulevard-coral-gables-florida.html
    - Florida YIMBY, 18 Oct 2026 — https://floridayimby.com/2026/10/empira-group-secures-81-million-construction-loan-for-the-aurelian-in-coral-gables.html
    - City of Coral Gables, Planning and Zoning Board Staff Report — Regency on the Park Planned Area Development (file 18-7445) — https://coralgables.legistar1.com/coralgables/attachments/0dde20d3-4318-4b69-996b-08ee5ad5029e.pdf
- *Note:* Unit count published as 161 in August 2026 and 160 in October 2026; the later figure is stored under the conflict rule. The $81m construction facility is reported without naming the lender, so lender is not found. Identity confirmed across the two addresses this project is reported at by matching developer, architect, contractor, storey count and the 324,000 sq ft figure already on the row. Storey cross-check PASSES: 12 stories in the same sentence, matching the tracker, and the unit check passes too — 152 residential plus 8 live/work is the 160 already stored. The descriptive sentence was used rather than the same report's cap-phrased condition of a maximum habitable building height of 130 feet. The entitlement was adopted as signed Ordinance 2018-29 on 28 Aug 2018 and no newer city height approval exists for this site through Nov 2026. NAME AND ADDRESS CAUTION: the city records call the site 912, and 912/918, E Ponce de Leon Blvd and the project Regency on the Park, while the city's own 2025 public-art resolution refers to the Aurelian development project at 912 E Ponce de Leon Blvd — this row holds 921 E Ponce de Leon Blvd.

### The Mark
- **Address:** 1250 S Dixie Hwy
- **Parcel id:** 0341300040010
- **Status:** Approved
- **Asset class:** Mixed-Use   **Neighbourhood:** not found
- Developer: Landmark Properties   |   Architect: Behar Font and Partners   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 393   |   Height: 105 ft   |   Stories: 8   |   Total sq ft: not found
- **Construction lender:** not found
- **Sources (9 field citations, 2 distinct):**
    - Coral Gables Gazette, April 2026 — https://gables-gazette.com/commission-approves-new-u-s-1-project-and-expands-rtz-overlay-in-4-1-votes/
    - Coral Gables Gazette, 9 Apr 2026 — https://gables-gazette.com/editorial-the-alley-at-1250-south-dixie/
- *Note:* The applicant of record is LCD Acquisitions LLC. Coverage outside the acceptable-source list attributes the project to Landmark Properties; no acceptable source confirms that, and Sunbiz refuses automated requests, so the developer is not found and the LLC is kept as owner of record per the field rule. This closes the gap the row was first recorded with. The Gazette's news coverage of the April 2026 site-plan approval names only the applicant of record, LCD Acquisitions LLC; its editorial on the same project names the developer firm, and the Gazette is on the acceptable-source list. The 105 ft height already on the row matches the Gazette's approval story; a superseded 97 ft appears in its May 2025 city-review list. NO CONSTRUCTION LOAN REPORTED, which fits the stage: as of January 2026 Landmark Properties had refiled with Miami-Dade County after years of Coral Gables debate, so construction has not started. Florida YIMBY, Commercial Observer and Bisnow were all searched for 1250 S Dixie Highway and for Landmark Properties with no financing found. REJECTED: Landmark's $300m Landmark Sponsor Fund LP of April 2026, a corporate equity vehicle rather than a project construction loan. Do not confuse with The Mark Tampa or The Mark at Tallahassee.

## Doral — random 10 of 12

### 5250 Condo Tower Phase 2
- **Address:** 5225 NW 85 AVE
- **Parcel id:** 3530220360098
- **Status:** Approved
- **Asset class:** Condo   **Neighbourhood:** not found
- Developer: Codina Partners   |   Architect: not found   |   General contractor: not found   |   Civil engineer: not found   |   Lender: Wells Fargo
- Residential units: 228   |   Height: not found (20 storeys known)   |   Stories: 20   |   Total sq ft: not found
- **Construction lender:** Wells Fargo
- **Completion basis:** news_confirmed  (2023)
- **Sources (8 field citations, 3 distinct):**
    - City of Doral Development Projects layer — https://experience.arcgis.com/experience/f53641e40f6b40d88fa7721bf5ca38cb/
    - Bisnow, 21 Jan 2025 — https://www.bisnow.com/news/south-florida/multifamily/codina-partners-secures-full-ownership-of-palma-tower-one-in-downtown-doral-with-124m-refinancing-deal-127632
    - The Real Deal, 21 Jan 2025 — https://therealdeal.com/miami/2025/01/21/codina-nabs-doral-rentals-refi-buys-out-partner-invesco/
- *Note:* Now named Palma Tower Two; the City's '5250' is the original marketing name of the FIRST tower, which is a different building at 5250 NW 84th Avenue. Identity confirmed on a triple match of address, unit count and storey count: 5225 NW 85th Avenue, 228 units, 20 storeys. The $55.7m Wells Fargo facility is explicitly a construction loan; the separate $124m January 2025 deal is a REFINANCING from Guardian Life c/o HPS Investment Partners and is deliberately not recorded as the lender. Bisnow misstates this tower's address as 5250 NW 84th Avenue, which is the other tower; The Real Deal of the same date has it right. COMPLETE: delivered in 2023 as Palma Tower Two, and let as rentals rather than sold as condominiums despite the City's 'Condo Tower' naming. Predates the 2025-26 loading window; corrected rather than removed for the same reason.

### Doral Inn/ IMF Doral Residential Conversion Live L
- **Address:** 1212 NW 82 AVE
- **Parcel id:** 3530340470001
- **Status:** Approved
- **Asset class:** Multifamily Rental   **Neighbourhood:** not found
- Developer: not found   |   Architect: not found   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 112   |   Height: not found   |   Stories: not found   |   Total sq ft: not found
- **Construction lender:** not found
- **Sources (5 field citations, 2 distinct):**
    - City of Doral Development Projects layer — https://experience.arcgis.com/experience/f53641e40f6b40d88fa7721bf5ca38cb/
    - City of Doral, City Council Memo — IMF Doral LLC Parking Variance (file 25-6153, hearing 25-05-DOR-02) — https://legistar1.granicus.com/Doral/attachments/de115cc9-a484-4294-a018-0172c501c502.docx
- *Note:* Conversion of an existing 1988 extended-stay hotel at 1212 NW 82nd Avenue into age-restricted multifamily apartments under the Live Local Act; the floor plans stay largely unchanged, so the 112 existing keys become 112 apartments. The memo states the figure directly as the project's unit count for parking purposes, and separately itemises the existing stock as 52 one-bedroom units plus 60 studios. Florida YIMBY of 30 Oct 2025 independently calls it a 112-key hospitality property. No height in feet: the existing two- and three-storey buildings keep their exteriors. Quote whitespace normalised from the DOCX. NO CONSTRUCTION LOAN REPORTED. Approved only; IMF Doral LLC's parking variance for the proposed conversion of the 112-key Doral Inn & Suites went to a City of Doral public hearing in May 2025. REJECTED: a $15.85m senior ACQUISITION loan from W Financial of Oct 2025, excluded by kind; WD Capital Group and Ordeg Capital arranged it and are brokers.

### Doral International Towers (Trump)
- **Address:** 4400 NW 87 AVE
- **Parcel id:** 3530210010010
- **Status:** Approved
- **Asset class:** Mixed-Use   **Neighbourhood:** not found
- Developer: The Trump Organization   |   Architect: Pascual, Perez, Kiliddjian, Starr Architects + Planners   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 1,498   |   Height: 210 ft   |   Stories: 20   |   Total sq ft: not found
- **Construction lender:** not found
- **Sources (9 field citations, 5 distinct):**
    - City of Doral Development Projects layer — https://experience.arcgis.com/experience/f53641e40f6b40d88fa7721bf5ca38cb/
    - Commercial Observer, 16 Jan 2025 — https://commercialobserver.com/2025/01/trump-organization-doral-golf-resort/
    - City of Doral, City Council Memo — Doral International Towers Site Plan (file 25-5998) — https://legistar1.granicus.com/Doral/attachments/17444729-ac80-4b15-b728-8e6b39a1ca42.docx
    - Bisnow, 15 Jan 2025 — https://www.bisnow.com/south-florida/news/construction-development/donald-trump-doral-international-towers-gain-final-approval-127576
    - City of Doral Resolution No. 25-36 — https://www.cityofdoral.com/files/assets/city/v/1/city-hall/city-clerk/resolutions-archived/2025/res.-no.-25-36-doral-international-towers-site-plan.pdf
- *Note:* The architect is corroborated by the primary record: City of Doral Resolution 25-36 requires the project to be built in substantial compliance with plans prepared by Pascual Perez Kiliddjian Starr. Trump Endeavor 12 LLC is the applicant entity and is deliberately NOT used to derive the developer, which would be inferring from an LLC name; The Trump Organization is recorded on Bisnow's explicit role statement instead. The resolution carries an internal typo giving the property as 9300 NW 13th Street in one section while its title and recitals correctly say 4400 NW 87th Avenue, folio 35-3028-029-0010, which matches the City's record. Landscape plan by Landscape Design Workshop. From the Development Standards for the DMU District table in the city staff memo; the two columns are permitted and provided, so provided is 20 stories at 210 ft. The 20 storeys match the tracker exactly. The same memo corroborates the 1,498 residential units already stored, and makes clear the 470 hotel units are existing rather than new. Quote whitespace normalised from the DOCX. NO CONSTRUCTION LOAN REPORTED. The project received final Doral City Council approval in January 2025 but has not broken ground. REJECTED: a $125m Axos Financial loan that appears in searches, which refinances the existing Trump National Doral resort rather than funding the towers.

### Doral Parc (Midtown Doral Phases IV, V and VI)
- **Address:** East side of NW 107 Ave between NW 86 St and NW 90 St
- **Parcel id:** 3530080000041  (also 3530080000048,3530080000051)
- **Status:** Approved
- **Asset class:** Mixed-Use   **Neighbourhood:** not found
- Developer: MG Developer   |   Architect: Pascual Perez Kiliddjian & Starr   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 552   |   Height: not found (10 storeys known)   |   Stories: 10   |   Total sq ft: not found
- **Construction lender:** not found
- **Sources (6 field citations, 2 distinct):**
    - Florida YIMBY, 24 Aug 2024 — https://floridayimby.com/2024/08/doral-approves-mixed-use-development-with-552-units-and-retail-space-at-midtown-doral-in-miami.html
    - City of Doral Development Projects layer — https://experience.arcgis.com/experience/f53641e40f6b40d88fa7721bf5ca38cb/
- *Note:* A joint venture between MG Developer and Ernesto De Lucas. Approved by the City of Doral in August 2024; no source states a construction start, so the stage is Approved. NO CONSTRUCTION LOAN REPORTED, with strong negative evidence: the project is approved but, per October 2026 reporting on MG Developer's debt troubles, planned and NOT under construction. The only capital at the project is excluded by type — about $2.2m of preferred equity from two Area Capital entities and about $2.25m of small loans from S.I. Munich, both in default. REJECTED: the $66.8m Pacific Western Bank construction loan that searches surface, which belongs to an EARLIER Midtown Doral phase with different sponsors in 2022, and a $105m Slate Property Group and Carlyle package belonging to a different MG project.

### Hazel Doral (ZOM / Westside Live Local Act)
- **Address:** 8300 NW 33 St
- **Parcel id:** 3530270530010  (also 3530270580010)
- **Status:** Under Construction
- **Asset class:** Mixed-Use   **Neighbourhood:** not found
- Developer: ZOM Living; Ascentris   |   Architect: MSA Architects   |   General contractor: Verdex Construction   |   Civil engineer: not found   |   Lender: PNC Bank
- Residential units: 380   |   Height: not found (7 storeys known)   |   Stories: 7   |   Total sq ft: not found
- **Construction lender:** PNC Bank
- **Completion basis:** news_confirmed
- **Sources (8 field citations, 2 distinct):**
    - Florida YIMBY, 7 Feb 2026 — https://floridayimby.com/2026/02/construction-starts-on-hazel-doral-at-8300-northwest-33rd-street-doral-florida.html
    - City of Doral Development Projects layer — https://experience.arcgis.com/experience/f53641e40f6b40d88fa7721bf5ca38cb/
- *Note:* The City lists this as "ZOM / Westside Live Local Act"; the press name is Hazel Doral. Unit count agrees across both sources at 380.

### Legacy at Doral (EWE Warehouse)
- **Address:** 2525 NW 82 AVE
- **Parcel id:** 3530270240010
- **Status:** Approved
- **Asset class:** Residential   **Neighbourhood:** not found
- Developer: not found   |   Architect: not found   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 185   |   Height: 62.33 ft   |   Stories: not found   |   Total sq ft: not found
- **Construction lender:** not found
- **Sources (6 field citations, 2 distinct):**
    - City of Doral Development Projects layer — https://experience.arcgis.com/experience/f53641e40f6b40d88fa7721bf5ca38cb/
    - City of Doral, Planning and Zoning Staff Report — EWE Warehouse (Legacy at Doral) Site Plan (file 22-4925) — https://legistar1.granicus.com/Doral/attachments/c51f5a33-07be-4e99-b320-9285e7aae638.docx
- *Note:* 62.33 ft is 62 feet 4 inches in decimal feet. This is the building's own peak height and not a cap: the cap in the same passage is the right-of-way width of 85 feet on NW 82 Avenue, and the Development Standards table lists 62 feet 4 inches in the Provided column against a Maximum height standard of R-O-W Width. CAVEAT: this is the original 2023 site plan and a later site-plan modification for Legacy at Doral exists in local press, so the figure could have moved. The staff report gives no storey count. Quote whitespace normalised from the DOCX. NO CONSTRUCTION LOAN REPORTED. Approved only; the site-plan modification was approved by Doral City Council on 22 March 2023 on the application of EWE Warehouse Investments XXXII, Ltd. Florida YIMBY, Commercial Observer and Bisnow were searched for 2525 NW 82 Ave and for the project name with no result. The Doral construction loans that surface — Midtown Doral $67m, Lincoln $103m, Codina, Torchlight $238m — are all other properties.

### Miami International Mall redevelopment
- **Address:** 1603 NW 107 Ave
- **Parcel id:** 3530320080140
- **Status:** Planning
- **Asset class:** Residential   **Neighbourhood:** not found
- Developer: Greystar   |   Architect: MSA Architects   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 900   |   Height: not found (6 storeys known)   |   Stories: 6   |   Total sq ft: not found
- **Construction lender:** not found
- **Sources (6 field citations, 2 distinct):**
    - Florida YIMBY, 9 Jun 2026 — https://floridayimby.com/2026/06/greystar-proposes-about-900-units-at-miami-international-mall-site-at-1603-n-w-107th-ave-doral-florida.html
    - Miami-Dade property appraiser parcel record — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0
- *Note:* Reviewed by the Doral City Council in June 2026; no approval is recorded in the City's Development Projects layer, so the stage is Planning. The unit count is published as approximately 900 and is stored as such.

### Midtown PUD Phase II Modification
- **Address:** Generally located at the SE and NE corner of SW 82 Street and NW 107 Avenue
- **Parcel id:** 3530080000033  (also 3530080000038,3530080000036)
- **Status:** Approved
- **Asset class:** Mixed-Use   **Neighbourhood:** not found
- Developer: Century Homebuilders; Mattoni Group; Conconcreto   |   Architect: not found   |   General contractor: Conconcreto   |   Civil engineer: not found   |   Lender: Pacific Western Bank
- Residential units: 734   |   Height: not found   |   Stories: not found   |   Total sq ft: not found
- **Construction lender:** Pacific Western Bank
- **Sources (8 field citations, 2 distinct):**
    - City of Doral Development Projects layer — https://experience.arcgis.com/experience/f53641e40f6b40d88fa7721bf5ca38cb/
    - The Real Deal, 13 Apr 2022 — https://therealdeal.com/miami/2022/04/13/midtown-doral-scores-67m-construction-loan-for-apartment-development/
- *Note:* The sponsor group, contractor and construction lender come from April 2022 coverage of a $66.8m Pacific Western Bank construction loan for a pair of six-storey buildings totalling 326 units on this site. That is a SUB-PHASE of the programme this row holds: the City's record is the modified Phase II of 734 dwelling units with a 58,052 sq ft clubhouse. The team is recorded because it is a project-level fact about the same PUD phase on the same corner; the storey count from that article is NOT recorded, because six storeys describes those two buildings and not the 734-unit programme. An earlier two-phase total of 675 units is superseded by the 734 in the City's modification.

### Prime Square Doral
- **Address:** 7200 NW 102 Ave
- **Parcel id:** not found
- **Status:** Permitting
- **Asset class:** Retail   **Neighbourhood:** not found
- Developer: Rieber Developments; Berca Development Group   |   Architect: PPK Architect   |   General contractor: Vandrak Construction   |   Civil engineer: Fortin, Leavy, Skiles   |   Lender: not found
- Residential units: not applicable   |   Height: 51.67 ft   |   Stories: 3   |   Total sq ft: 58,000
- **Construction lender:** not found
- **Sources (8 field citations, 2 distinct):**
    - Florida YIMBY, 19 Sep 2026 — https://floridayimby.com/2026/09/pre-construction-begins-on-prime-square-doral-at-7200-nw-102nd-avenue-in-doral.html
    - City of Doral, City Council Memo — Prime Square Site Plan (file 25-6428) — https://legistar1.granicus.com/Doral/attachments/83ae3e80-4346-47a1-b6ae-42d2f6cb0280.docx
- *Note:* Stage is Permitting: the project is described as approaching final permitting, and Miami-Dade County's Plat Committee approved a resubmission in January 2026. Civil engineer is filled because the article names the full design team, which is the only circumstance in which that field is filled for this market. The source states 51 feet 8 inches to the top of parapet; 51.67 is that same figure in decimal feet, not a derived number. A parapet is the top of the building, not a spire or a mechanical feature. NO RESIDENTIAL UNITS: Prime Square Doral is a commercial condominium — Florida YIMBY of 19 Sep 2026 describes a 58,000-square-foot commercial condominium development of roughly 40,000 sq ft retail and restaurant and 18,000 sq ft office — and the city memo's parking table covers only retail, restaurant and office. No source states a unit count, so the field is left empty rather than set to zero. The permitted six-storey figure in the same memo is a zoning cap and was not used.

### Sevilla at Downtown Doral
- **Address:** 8301 NW 53 St
- **Parcel id:** 3530220070350
- **Status:** Under Construction
- **Asset class:** Multifamily Rental   **Neighbourhood:** not found
- Developer: Codina Partners   |   Architect: MSA Architects   |   General contractor: ANF Group   |   Civil engineer: not found   |   Lender: Regions Bank; Ocean Bank
- Residential units: 405   |   Height: not found (7 storeys known)   |   Stories: 7   |   Total sq ft: not found
- **Construction lender:** Regions Bank; Ocean Bank
- **Completion basis:** news_confirmed
- **Sources (7 field citations, 3 distinct):**
    - Florida YIMBY, 1 Mar 2026 — https://floridayimby.com/2026/03/codina-partners-tops-out-7-story-405-unit-sevilla-at-downtown-doral.html
    - Florida YIMBY, 20 Jul 2025 — https://floridayimby.com/2025/07/general-contractor-named-for-sevilla-at-downtown-doral.html
    - Florida YIMBY, 8 Jul 2025 — https://floridayimby.com/2025/07/codina-partners-breaks-ground-on-sevilla-at-downtown-doral-secures-100-million-construction-loan.html
- *Note:* Unit count: the City's own site-plan record states 412 multifamily units and Florida YIMBY stated 412 in June 2024 and July 2025, then 405 on topping off in March 2026. The City record carries no date, so the most recent dated report wins and 405 is stored; 412 is the earlier figure.
