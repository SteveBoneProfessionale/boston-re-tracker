# South Florida — spot-check sample

Ten projects per city, drawn at random with a fixed seed (20260301) so the selection is reproducible and a regenerated sample compares like with like. Every filled value below has a citation in the tracker's FIELD SOURCES panel; the distinct source URLs for each project are listed.

A blank is one of three things and is never printed as an empty space: **not found** means researched and not published by any acceptable source; **none** is a stated zero, such as an office tower with no residences; **not applicable** means the asset class has no dwellings in it. A warehouse with no unit count and a tower whose unit count nobody has published are both blank in the database and are not the same fact.

## Coverage

| City | Projects | Units known | of residential-bearing | Height in feet | Storeys | Hotel keys |
| --- | --: | --: | --: | --: | --: | --: |
| Miami | 314 | 76 | 76 of 251 | 47 | 78 | 6 |
| Miami Beach | 35 | 25 | 25 of 34 | 7 | 28 | 4 |
| West Palm Beach | 30 | 25 | 25 of 28 | 8 | 27 | 3 |
| Coral Gables | 17 | 17 | 17 of 17 | 12 | 15 | 0 |
| Doral | 57 | 11 | 11 of 24 | 9 | 13 | 2 |
| **All five** | **453** | **154** | **154 of 354** | **83** | **161** | **15** |

Height in feet is the thinnest field in this market and will stay that way: the trade press prints storey counts and prints feet only when a project reaches a board approval that states one. Where feet are unpublished the storey count usually is not, which is why both columns are shown. The FAA Digital Obstacle File was calibrated as a bulk source for the missing feet and REJECTED at a 53 per cent hit rate — see scraper/sofl_faa_heights.py; it would have written 395 ft onto the 1,049 ft Waldorf Astoria, that being a 2018 survey of the building demolished to make room for it.

## Programme — the 135 largest projects

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
| NoBe PARC Miami Beach | Miami Beach | Planning | Condo | 232 | — | — | 15 |
| 5250 Condo Tower Phase 2 | Doral | Approved | Condo | 228 | — | — | 20 |
| 7200 Collins | Miami Beach | Under Construction | Condo | 222 | — | — | 12 |
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
| Residences in the Grove | Miami | Under Construction | Condo | 148 | — | — | — |
| Mr. C Hotel & Residences West Palm Beach | West Palm Beach | Under Construction | Mixed-Use | 146 | 110 | — | 27 |
| The Alton | Miami Beach | Planning | Mixed-Use | 142 | — | — | 14 |
| The Ritz-Carlton Residences, West Palm Beach | West Palm Beach | Under Construction | Condo | 138 | — | — | 27 |
| 72 Carlyle | Miami Beach | Planning | Condo | 134 | — | — | 20 |
| Regency Parc | Coral Gables | Complete | Residential | 126 | — | — | 20 |
| PALMA Miami Beach Residences | Miami Beach | Under Construction | Condo | 121 | — | — | 14 |
| Wynwood Works | Miami | Complete | Multifamily Rental | 120 | — | — | 12 |
| Seventeen Gables Residences | Coral Gables | Under Construction | Condo | 117 | — | — | 8 |
| LILLI | Miami | Planning | Condo | 117 | — | — | 53 |
| View 29 | Miami | Under Construction | Mixed-Use | 116 | — | — | 12 |
| Mr. C Residences Coconut Grove | Miami | Complete | Condo | 112 | — | — | 21 |
| Doral Inn/ IMF Doral Residential Conversion Live L | Doral | Approved | Multifamily Rental | 112 | — | — | — |
| 75 Broadway | Miami | Planning | Condo | 106 | — | — | 52 |
| 1940 Park Avenue workforce housing | Miami Beach | Planning | Multifamily Rental | 105 | — | — | 6 |
| 2200 Brickell | Miami | Complete | Condo | 103 | — | — | 5 |
| The Residences at Mandarin Oriental, Miami — North Tower | Miami | Under Construction | Mixed-Use | — | 121 | — | 33 |
| Hotel at NW 53 Terrace and Paseo Boulevard | Doral | Planning | Hotel | — | 168 | — | 8 |
| Grand Hyatt Miami Beach Convention Center Hotel | Miami Beach | Under Construction | Hotel | — | 800 | — | 17 |
| The Nora Hotel | West Palm Beach | Under Construction | Hotel | — | 201 | — | 5 |

## Random sample by city

## Miami — random 10 of 314

### 123 NW 14 ST
- **Address:** 123 NW 14 ST
- **Parcel id:** 0131250481790
- **Status:** Complete
- **Asset class:** Mixed-Use   **Neighbourhood:** Northeast Overtown
- Developer: not found   |   Architect: not found   |   General contractor: Amit Construction  LLC.   |   Civil engineer: not found   |   Lender: not found
- Residential units: not found   |   Height: not found   |   Stories: not found   |   Total sq ft: 7,360
- **Completion basis:** co_issued  (2025-12-02)
- **Sources (10 field citations, 2 distinct):**
    - City of Miami building permit BD22003174001B001 — https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22003174001B001%27&outFields=*&f=pjson
    - Miami-Dade property boundary record, folio 0131250481790 — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0/query?where=FOLIO%3D%270131250481790%27&outFields=*&f=pjson

### 1336 SW 7 ST
- **Address:** 1336 SW 7 ST
- **Parcel id:** 0141020066420
- **Status:** Under Construction
- **Asset class:** Multifamily Rental   **Neighbourhood:** Latin Quarter
- Developer: not found   |   Architect: not found   |   General contractor: RH BUILDER LLC   |   Civil engineer: not found   |   Lender: not found
- Residential units: not found   |   Height: not found   |   Stories: not found   |   Total sq ft: 13,618
- **Completion basis:** permit_active
- **Sources (9 field citations, 2 distinct):**
    - City of Miami building permit BD20017558001B001 — https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD20017558001B001%27&outFields=*&f=pjson
    - Miami-Dade property boundary record, folio 0141020066420 — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0/query?where=FOLIO%3D%270141020066420%27&outFields=*&f=pjson

### 1526 NW 2 ST
- **Address:** 1526 NW 2 ST
- **Parcel id:** 0141020058700
- **Status:** Under Construction
- **Asset class:** Multifamily Rental   **Neighbourhood:** Orange Bowl
- Developer: not found   |   Architect: not found   |   General contractor: FERROM5 General Contractors  INC.   |   Civil engineer: not found   |   Lender: not found
- Residential units: not found   |   Height: not found   |   Stories: not found   |   Total sq ft: 14,589
- **Completion basis:** permit_active
- **Sources (9 field citations, 2 distinct):**
    - City of Miami building permit BD22009892001B001 — https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22009892001B001%27&outFields=*&f=pjson
    - Miami-Dade property boundary record, folio 0141020058700 — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0/query?where=FOLIO%3D%270141020058700%27&outFields=*&f=pjson

### 1548 BRICKELL AV
- **Address:** 1548 BRICKELL AV
- **Parcel id:** 0141390010250
- **Status:** Under Construction
- **Asset class:** Office   **Neighbourhood:** Miami Avenue
- Developer: not found   |   Architect: not found   |   General contractor: ADF USA INC.   |   Civil engineer: not found   |   Lender: not found
- Residential units: not applicable   |   Height: not found   |   Stories: not found   |   Total sq ft: 19,783
- **Completion basis:** permit_active
- **Sources (9 field citations, 2 distinct):**
    - City of Miami building permit BD26001424001B001 — https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD26001424001B001%27&outFields=*&f=pjson
    - Miami-Dade property boundary record, folio 0141390010250 — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0/query?where=FOLIO%3D%270141390010250%27&outFields=*&f=pjson

### 2545 NW 20 ST
- **Address:** 2545 NW 20 ST
- **Parcel id:** 0131270460470
- **Status:** Under Construction
- **Asset class:** Industrial   **Neighbourhood:** Allapattah Industrial District
- Developer: not found   |   Architect: not found   |   General contractor: Brickell Group Construction LLC   |   Civil engineer: not found   |   Lender: not found
- Residential units: not applicable   |   Height: not found   |   Stories: not found   |   Total sq ft: 42,125
- **Completion basis:** permit_active
- **Sources (9 field citations, 2 distinct):**
    - City of Miami building permit BD23000864001B001 — https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23000864001B001%27&outFields=*&f=pjson
    - Miami-Dade property boundary record, folio 0131270460470 — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0/query?where=FOLIO%3D%270131270460470%27&outFields=*&f=pjson
- *Note:* Earlier completed building permit(s) on this parcel, outside the 2025-26 loading window: BD17018177001B001 (CO 2022-02-04, 130,697 sq ft). This may be the building being replaced, an earlier phase, or evidence that the site is already built out -- it is not treated as a completion of this project.

### 3000 ELIZABETH ST
- **Address:** 3000 ELIZABETH ST
- **Parcel id:** 0141210010150
- **Status:** Under Construction
- **Asset class:** Condo   **Neighbourhood:** Bird Grove West
- Developer: not found   |   Architect: not found   |   General contractor: Nandez D&D  LLC   |   Civil engineer: not found   |   Lender: not found
- Residential units: not found   |   Height: not found   |   Stories: not found   |   Total sq ft: 8,000
- **Completion basis:** permit_active
- **Sources (9 field citations, 2 distinct):**
    - City of Miami building permit BD17008194001B001 — https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD17008194001B001%27&outFields=*&f=pjson
    - Miami-Dade property boundary record, folio 0141210010150 — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0/query?where=FOLIO%3D%270141210010150%27&outFields=*&f=pjson

### 500 NE 1 AV
- **Address:** 500 NE 1 AV
- **Parcel id:** 0101060301010
- **Status:** Complete
- **Asset class:** Institutional   **Neighbourhood:** Parkwest
- Developer: not found   |   Architect: not found   |   General contractor: T&G CONSTRUCTORS   |   Civil engineer: not found   |   Lender: not found
- Residential units: not found   |   Height: not found   |   Stories: not found   |   Total sq ft: 37,809
- **Completion basis:** co_issued  (2025-07-16)
- **Sources (10 field citations, 2 distinct):**
    - City of Miami building permit BD19022167001B001 — https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD19022167001B001%27&outFields=*&f=pjson
    - Miami-Dade property boundary record, folio 0101060301010 — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0/query?where=FOLIO%3D%270101060301010%27&outFields=*&f=pjson

### 637 SW 10 ST
- **Address:** 637 SW 10 ST
- **Parcel id:** 0102060501120
- **Status:** Under Construction
- **Asset class:** Condo   **Neighbourhood:** East Little Havana
- Developer: not found   |   Architect: not found   |   General contractor: BERNAL CONSTRUCTION CORP   |   Civil engineer: not found   |   Lender: not found
- Residential units: not found   |   Height: not found   |   Stories: not found   |   Total sq ft: 15,128
- **Completion basis:** permit_active
- **Sources (9 field citations, 2 distinct):**
    - City of Miami building permit BD23024970001B001 — https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23024970001B001%27&outFields=*&f=pjson
    - Miami-Dade property boundary record, folio 0102060501120 — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0/query?where=FOLIO%3D%270102060501120%27&outFields=*&f=pjson

### 6601 NE MIAMI PL
- **Address:** 6601 NE MIAMI PL
- **Parcel id:** 0131130190030
- **Status:** Under Construction
- **Asset class:** Condo   **Neighbourhood:** Lemon City/Little Haiti
- Developer: not found   |   Architect: not found   |   General contractor: EDGE CONSTRUCTION USA  INC.   |   Civil engineer: not found   |   Lender: not found
- Residential units: not found   |   Height: not found   |   Stories: not found   |   Total sq ft: 19,778
- **Completion basis:** permit_active
- **Sources (9 field citations, 2 distinct):**
    - City of Miami building permit BD20023346001B001 — https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD20023346001B001%27&outFields=*&f=pjson
    - Miami-Dade property boundary record, folio 0131130190030 — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0/query?where=FOLIO%3D%270131130190030%27&outFields=*&f=pjson

### 845 SW 8 ST
- **Address:** 845 SW 8 ST
- **Parcel id:** 0141380032361
- **Status:** Under Construction
- **Asset class:** Multifamily Rental   **Neighbourhood:** East Little Havana
- Developer: not found   |   Architect: not found   |   General contractor: MANAGEMENT & CONSULTING INC   |   Civil engineer: not found   |   Lender: not found
- Residential units: not found   |   Height: not found   |   Stories: not found   |   Total sq ft: 149,431
- **Completion basis:** permit_active
- **Sources (9 field citations, 2 distinct):**
    - City of Miami building permit BD21019029001B001 — https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD21019029001B001%27&outFields=*&f=pjson
    - Miami-Dade property boundary record, folio 0141380032361 — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0/query?where=FOLIO%3D%270141380032361%27&outFields=*&f=pjson

## Miami Beach — random 10 of 35

### 1030 6th Street
- **Address:** 1030 6 St
- **Parcel id:** 0242030097860
- **Status:** Planning
- **Asset class:** Multifamily Rental   **Neighbourhood:** FlamingoLummus
- Developer: not found   |   Architect: not found   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 72   |   Height: not found (5 storeys known)   |   Stories: 5   |   Total sq ft: not found
- **Sources (5 field citations, 4 distinct):**
    - City of Miami Beach, Live Local Act Applications Under Review — https://www.miamibeachfl.gov/city-hall/planning/live-local-act-applications-under-review/
    - Miami Beach neighborhoods layer — https://gis.miamibeachfl.gov/public/rest/services/gc/gc_Cadastral/MapServer/12
    - City of Miami Beach Civic Access portal, Live Local Act plan record LLA26-0007 (FKA PB26-0837) — https://energovcss.miamibeachfl.gov/EnerGovProd/SelfService/#/plan/b0b26cd2-986f-4315-bcb6-5313b9b5adc7
    - Miami-Dade property appraiser parcel record — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0
- *Note:* Programme fields are not found; the City's under-review page publishes only the address and application number. One of the three Live Local applications that defeated the earlier pass. Recovered from the EnerGov plan record itself (api/energov/plans/b0b26cd2-986f-4315-bcb6-5313b9b5adc7 with tenantId:3 and tenantName:miamibeachflprod2), not from any hosted PDF — the city page links only the portal. Status Under Review, applied 2026-03-30, district CPS-2. Elderly (age-restricted) workforce housing with ground-floor retail. No height in feet anywhere in the record.

### 1101 71st Street
- **Address:** 1101 71st St
- **Parcel id:** not found
- **Status:** Planning
- **Asset class:** Multifamily Rental   **Neighbourhood:** not found
- Developer: Jerika Properties   |   Architect: not found   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 98   |   Height: not found   |   Stories: not found   |   Total sq ft: 116,000
- **Sources (4 field citations, 1 distinct):**
    - Florida YIMBY, 24 Nov 2025 — https://floridayimby.com/2025/11/developer-proposes-affordable-housing-development-for-1101-71st-st-miami-beach-fl-33141.html
- *Note:* The coverage states the building would measure no more than 235 ft, which is a cap rather than a stated height, so height is not stored. No unit count is published. HEIGHT REJECTED: the same article says the building would measure no more than 235 feet high, which is a stated maximum rather than a height, so no height is recorded. Developer Jerika Properties; Live Local Act; replaces a shuttered Chase bank. The article states no storey count, so no storey cross-check was possible.

### 1826 Collins Avenue
- **Address:** 1826 Collins Ave
- **Parcel id:** 0232340190050
- **Status:** Approved
- **Asset class:** Multifamily Rental   **Neighbourhood:** CityCenter
- Developer: Crescent Heights (Russell Galbut); Lefferts (Mendy Chudaitov)   |   Architect: Built Form Architecture   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 29   |   Height: 213 ft   |   Stories: 15   |   Total sq ft: not found
- **Sources (8 field citations, 3 distinct):**
    - Florida YIMBY, 11 May 2026 — https://floridayimby.com/2026/05/planning-board-approves-mechanical-parking-system-for-1826-collins-amid-neighbor-opposition-in-miami-beach.html
    - Miami Beach neighborhoods layer — https://gis.miamibeachfl.gov/public/rest/services/gc/gc_Cadastral/MapServer/12
    - Miami-Dade property appraiser parcel record — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0
- *Note:* On the City's list of approved Live Local Act applications. The ownership is described as an entity tied to Crescent Heights' Russell Galbut and Lefferts' Mendy Chudaitov; no single developer entity is named, so both sponsors are recorded.

### 1940 Park Avenue workforce housing
- **Address:** 1940 Park Ave
- **Parcel id:** 0232340160110
- **Status:** Planning
- **Asset class:** Multifamily Rental   **Neighbourhood:** CityCenter
- Developer: The Peebles Corp.   |   Architect: Brooks + Scarpa Architects   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 105   |   Height: not found (6 storeys known)   |   Stories: 6   |   Total sq ft: not found
- **Sources (7 field citations, 4 distinct):**
    - The Real Deal, 16 Jun 2026 — https://therealdeal.com/miami/2026/06/16/housing-coming-for-historic-miami-beach-site-city-condemned/
    - Florida YIMBY, 30 Jun 2026 — https://floridayimby.com/2026/06/shuttered-hotel-could-be-replaced-with-workforce-housing-at-1940-park-ave-miami-beach-florida.html
    - Miami Beach neighborhoods layer — https://gis.miamibeachfl.gov/public/rest/services/gc/gc_Cadastral/MapServer/12
    - Miami-Dade property appraiser parcel record — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0
- *Note:* No unit count, floor area, storey count or architect is published for this proposal, so those fields are not found. DEVELOPER CONFLICT LEFT UNRESOLVED: The Real Deal of 16 June 2026 attributes the project to Legacy Real Estate Development and records that the City selected Legacy and Urban American in 2024, with Urban American no longer involved; Florida YIMBY two weeks later says an affiliate of The Peebles Corp. and names 1940 Barclay Partners LLC. The stored value follows the later source under the conflict rule, but Legacy Real Estate Development is the more specific attribution and this is flagged for a human call. The site is City-owned, 1.6 acres, replacing the condemned Barclay Hotel: 54 studios, 39 one-bed, 12 two-bed, 1,000 sq ft of retail, 78,700 sq ft.

### 6945 Abbott Avenue hotel
- **Address:** 6945 Abbott Ave
- **Parcel id:** 0232110010290
- **Status:** Planning
- **Asset class:** Hotel   **Neighbourhood:** NorthShore
- Developer: Abbott M 6945   |   Architect: M77R Architecture   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: not found   |   Hotel keys: 48   |   Height: 90 ft   |   Stories: 6   |   Total sq ft: not found
- **Sources (8 field citations, 3 distinct):**
    - Florida YIMBY, 6 Oct 2025 — https://floridayimby.com/2025/10/48-room-hotel-proposed-for-6945-abbott-ave-miami-beach-fl.html
    - Miami Beach neighborhoods layer — https://gis.miamibeachfl.gov/public/rest/services/gc/gc_Cadastral/MapServer/12
    - Miami-Dade property appraiser parcel record — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0

### 71 NoBe
- **Address:** 400 71st St
- **Parcel id:** not found
- **Status:** Approved
- **Asset class:** Mixed-Use   **Neighbourhood:** not found
- Developer: Pacific Star Capital   |   Architect: not found   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 277   |   Height: not found   |   Stories: not found   |   Total sq ft: not found
- **Sources (4 field citations, 1 distinct):**
    - Florida YIMBY, 7 Jan 2026 — https://floridayimby.com/2026/01/mixed-use-71-nobe-planned-for-2027-completion-at-400-71st-st-miami-beach-florida.html
- *Note:* The $94m facility is described as a construction loan but the lender is not named. A loan closing is not a construction start and no source reports one, so the stage is Approved. No unit count or storey count is published. The $94m construction loan is reported but its lender is named nowhere, so the field stays empty. Over 30,000 sq ft of retail is also planned.

### 824 Alton Road
- **Address:** 824 Alton Rd
- **Parcel id:** 0242030010530
- **Status:** Planning
- **Asset class:** Multifamily Rental   **Neighbourhood:** FlamingoLummus
- Developer: Crescent Heights   |   Architect: RSP Architects   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 149   |   Height: not found (39 storeys known)   |   Stories: 39   |   Total sq ft: not found
- **Sources (7 field citations, 4 distinct):**
    - Florida YIMBY, 4 Oct 2026 — https://floridayimby.com/2026/10/developer-revises-plans-for-workforce-housing-high-rise-at-824-alton-road-miami-florida.html
    - Florida YIMBY, 9 Oct 2025 — https://floridayimby.com/2025/10/31-story-tower-with-workforce-housing-proposed-for-824-alton-rd-miami-beach-fl.html
    - Miami Beach neighborhoods layer — https://gis.miamibeachfl.gov/public/rest/services/gc/gc_Cadastral/MapServer/12
    - Miami-Dade property appraiser parcel record — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0
- *Note:* Plans were revised in October 2026; that report restates the original 31-storey, 120-residence programme and does not publish revised figures, so the original programme is stored. SUPERSEDES the 31 storeys and 120 residences previously stored, which came from the October 2025 filing at 404 ft. The revision is 72 workforce plus 77 market-rate units. Height is not recorded: the revision is described only as more than 500 ft above grade, which is not a figure.

### AVARA Residences
- **Address:** 3900 Alton Rd
- **Parcel id:** 0232220460010
- **Status:** Complete
- **Asset class:** Multifamily Rental   **Neighbourhood:** BayShore
- Developer: Mast Capital; Rockpoint   |   Architect: Arquitectonica   |   General contractor: Kaufman Lynn Construction   |   Civil engineer: not found   |   Lender: PNC Bank
- Residential units: 178   |   Height: not found (8 storeys known)   |   Stories: 8   |   Total sq ft: not found
- **Completion basis:** leasing_active  (2025-12)
- **Sources (9 field citations, 4 distinct):**
    - Florida YIMBY, 6 Dec 2025 — https://floridayimby.com/2025/12/avara-residences-debut-at-3900-alton-road-with-178-rental-homes-in-miami-beach.html
    - Florida YIMBY, 15 Mar 2023 — https://floridayimby.com/2023/03/178-unit-residential-project-at-3900-alton-road-set-for-completion-in-fall-2024.html
    - Miami Beach neighborhoods layer — https://gis.miamibeachfl.gov/public/rest/services/gc/gc_Cadastral/MapServer/12
    - Miami-Dade property appraiser parcel record — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0
- *Note:* A $64m construction loan closed in November 2022; the contractor's role is restated at topping-off. The project was called 3900 Alton Road before delivery.

### Grand Hyatt Miami Beach Convention Center Hotel
- **Address:** 1701 Convention Center Dr
- **Parcel id:** not found
- **Status:** Under Construction
- **Asset class:** Hotel   **Neighbourhood:** not found
- Developer: Terra; Turnberry   |   Architect: Arquitectonica   |   General contractor: Balfour Beatty   |   Civil engineer: not found   |   Lender: Tyko Capital
- Residential units: not found   |   Hotel keys: 800   |   Height: not found (17 storeys known)   |   Stories: 17   |   Total sq ft: not found
- **Completion basis:** news_confirmed
- **Sources (8 field citations, 2 distinct):**
    - Florida YIMBY, 30 Jul 2026 — https://floridayimby.com/2026/07/construction-tops-out-on-17-story-grand-hyatt-miami-beach-convention-center-hotel.html
    - Commercial Observer, 11 Apr 2025 — https://commercialobserver.com/2025/04/terra-turnberry-miami-beach-convention-center-hotel-grand-hyatt/
- *Note:* A $392m construction loan arranged by Eastdil Secured. The separate $75m Miami Beach Redevelopment Agency grant is not debt. A civil engineer is named only in 2022 coverage, in the same sentence as a general contractor who was later replaced, so it is not recorded. Hotel keys recorded as hotel rooms, not as residential units. The 17 storeys in the same article match the tracker. No height in feet published.

### The Perigon Miami Beach
- **Address:** 5333 Collins Ave
- **Parcel id:** 0232140010220
- **Status:** Under Construction
- **Asset class:** Condo   **Neighbourhood:** OceanFront
- Developer: Mast Capital; Starwood Capital Group   |   Architect: ODP Architecture & Design   |   General contractor: Moss Construction   |   Civil engineer: not found   |   Lender: Eldridge Real Estate Credit
- Residential units: 73   |   Height: not found (17 storeys known)   |   Stories: 17   |   Total sq ft: not found
- **Completion basis:** news_confirmed
- **Sources (9 field citations, 5 distinct):**
    - Florida YIMBY, 4 Apr 2024 — https://floridayimby.com/2024/04/developers-break-ground-on-17-story-the-perigon-at-5333-collins-avenue-in-miami-beach.html
    - Florida YIMBY, 6 Mar 2026 — https://floridayimby.com/2026/03/the-perigon-miami-beach-tops-out-at-5333-collins-avenue-in-mid-beach.html
    - The Real Deal, 17 Jan 2025 — https://therealdeal.com/miami/2025/01/17/mast-capital-starwood-get-390m-loan-for-miami-beach-condos/
    - Miami Beach neighborhoods layer — https://gis.miamibeachfl.gov/public/rest/services/gc/gc_Cadastral/MapServer/12
    - Miami-Dade property appraiser parcel record — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0
- *Note:* No unit count is stated in the coverage read for this run, so residential units is not found. ARCHITECT CORRECTED: OMA was stored, but OMA is the design architect and ODP Architecture & Design is the architect of record, which is what the field rule asks for. A $390m construction loan. Malcolm Drilling was the foundation contractor, not the general contractor.

## West Palm Beach — random 10 of 30

### 2001 North Flagler Drive
- **Address:** 2001 N Flagler Dr
- **Parcel id:** 74434310190060110
- **Status:** Planning
- **Asset class:** Condo   **Neighbourhood:** not found
- Developer: Related Group; BH Group   |   Architect: Arquitectonica   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 90   |   Height: not found (27 storeys known)   |   Stories: 27   |   Total sq ft: 480,000
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
- **Sources (5 field citations, 2 distinct):**
    - Florida YIMBY, 22 Mar 2026 — https://floridayimby.com/2026/03/developers-propose-28-story-mixed-use-tower-for-400-hibiscus-street-west-palm-beach-florida.html
    - Florida YIMBY, 19 Aug 2026 — https://floridayimby.com/2026/08/banyan-tree-residences-west-palm-beach-secures-unanimous-downtown-action-committee-approval.html
- *Note:* OMA (Jason Long) is the design architect and Corwil Architects is the architect of record, so Corwil is stored and OMA recorded here, per the field rule. The storey count was published as 24 and 28 in March 2026 and as 25 in August 2026; the later figure is stored. The project is reported at 400 Hibiscus Street and at 635 South Dixie Highway with the same 88-residence, Mast Capital/Curated JCZM programme, which is one corner site under two frontages.

### Cox Science Center Hall of Science expansion
- **Address:** Cox Science Center, West Palm Beach
- **Parcel id:** not found
- **Status:** Approved
- **Asset class:** Institutional   **Neighbourhood:** not found
- Developer: not found   |   Architect: PBK   |   General contractor: Skanska   |   Civil engineer: not found   |   Lender: not found
- Residential units: not found   |   Height: not found   |   Stories: not found   |   Total sq ft: 86,000
- **Sources (4 field citations, 1 distinct):**
    - Florida YIMBY, 26 Dec 2025 — https://floridayimby.com/2025/12/skanska-selected-to-build-86000-square-foot-hall-of-science-expansion-at-cox-science-center-in-west-palm-beach.html
- *Note:* No street address is published in the coverage, so the project is recorded by its location and carries no parcel control number. Skanska was selected as builder in December 2025; selection of a builder is not a construction start, so the stage is Approved.

### Deco Northwood
- **Address:** 2900 Broadway
- **Parcel id:** 74434309050210100
- **Status:** Planning — **Denied** (dead)
- **Asset class:** Multifamily Rental   **Neighbourhood:** not found
- Developer: The Prime Company   |   Architect: not found   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 167   |   Height: 84 ft   |   Stories: 8   |   Total sq ft: not found
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
- **Sources (5 field citations, 1 distinct):**
    - Florida YIMBY, 20 Mar 2026 — https://floridayimby.com/2026/03/two-tower-164-unit-edgeworth-slated-for-1155-south-flagler-drive-west-palm-beach-florida.html
- *Note:* The March 2026 report's headline says 164 units and its text says 168; the text figure is stored and the headline figure noted.

### The Berkeley Palm Beach
- **Address:** 550 S Australian Ave
- **Parcel id:** 74434321120000040
- **Status:** Under Construction
- **Asset class:** Condo   **Neighbourhood:** not found
- Developer: Al Adelson   |   Architect: Arquitectonica   |   General contractor: John Moriarty & Associates   |   Civil engineer: not found   |   Lender: not found
- Residential units: 193   |   Height: not found (25 storeys known)   |   Stories: 25   |   Total sq ft: not found
- **Completion basis:** news_confirmed
- **Sources (7 field citations, 2 distinct):**
    - Florida YIMBY, 7 Jul 2026 — https://floridayimby.com/2026/07/the-berkeley-palm-beach-breaks-ground-at-550-south-australian-avenue-in-west-palm-beach.html
    - Palm Beach County property appraiser parcel record — https://services1.arcgis.com/ZWOoUZbtaYePLlPw/arcgis/rest/services/Parcels_and_Property_Details_WebMercator/FeatureServer/0
- *Note:* JVP Management provided a $62.5m pre-construction loan in September 2025. A pre-construction loan is not a construction loan, so no lender is recorded. The project is reported at both 500 and 550 South Australian Avenue.

### The Ritz-Carlton Residences, West Palm Beach
- **Address:** 1717 N Flagler Dr
- **Parcel id:** not found
- **Status:** Under Construction
- **Asset class:** Condo   **Neighbourhood:** not found
- Developer: Related Group; BH Group   |   Architect: Arquitectonica   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 138   |   Height: not found (27 storeys known)   |   Stories: 27   |   Total sq ft: not found
- **Completion basis:** news_confirmed
- **Sources (5 field citations, 2 distinct):**
    - Florida YIMBY, 24 Feb 2026 — https://floridayimby.com/2026/02/related-group-and-bh-group-break-ground-on-the-ritz-carlton-residences-west-palm-beach.html
    - Florida YIMBY, 20 Oct 2025 — https://floridayimby.com/2025/10/wave-of-new-residential-development-takes-shape-across-west-palm-beach.html
- *Note:* Storey count published as 26 in October 2025 and 27 in February 2026; the later figure is stored. The 138-residence figure comes from a multi-project round-up where attribution is by paragraph, so it is less firmly attributed than the rest of this record. A $600m Bank OZK construction loan reported in the same round-up is attributed in its own headline to South Flagler House, not to this project, so no lender is recorded here.

### The Sound Apartments
- **Address:** 8111 S Dixie Hwy
- **Parcel id:** 74434416400020000
- **Status:** Under Construction
- **Asset class:** Multifamily Rental   **Neighbourhood:** SOUTH END NEIGHBORHOOD ASSOCIATION
- Developer: Woodfield Development; Flagler Realty & Development Inc.   |   Architect: Spina O'Rourke + Partners   |   General contractor: Verdex Construction   |   Civil engineer: not found   |   Lender: not found
- Residential units: 358   |   Height: not found (8 storeys known)   |   Stories: 8   |   Total sq ft: not found
- **Completion basis:** news_confirmed
- **Sources (9 field citations, 3 distinct):**
    - Florida YIMBY, 4 Mar 2026 — https://floridayimby.com/2026/03/construction-nears-completion-on-the-sound-apartments-at-8111-south-dixie-highway-in-west-palm-beach.html
    - West Palm Beach neighborhoods layer — https://wpbgisportal.wpb.org/server/rest/services/MayorOffice/WPB_Neighborhoods/FeatureServer/0
    - Palm Beach County property appraiser parcel record — https://services1.arcgis.com/ZWOoUZbtaYePLlPw/arcgis/rest/services/Parcels_and_Property_Details_WebMercator/FeatureServer/0
- *Note:* The most recent dated source says construction is nearing completion, not complete, so the stage is Under Construction; no source states a certificate of occupancy or the start of leasing.

## Coral Gables — random 10 of 17

### 2501 Galiano Street
- **Address:** 2501 Galiano St
- **Parcel id:** 0341170051020
- **Status:** Planning
- **Asset class:** Condo   **Neighbourhood:** not found
- Developer: not found   |   Architect: Bermello Ajamil Partners   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 93   |   Height: 177 ft   |   Stories: 13   |   Total sq ft: 98,335
- **Sources (7 field citations, 1 distinct):**
    - Florida YIMBY, 23 Oct 2024 — https://floridayimby.com/2024/10/13-story-traditional-condo-could-replace-closed-truist-bank-at-2501-galiano-st-coral-gables-fl.html
- *Note:* The applicant is 2501 Galiano Street Holdings LLC and no source names a party behind it. Sunbiz refuses automated requests, so the chain stops at the LLC: developer is not found and the LLC is kept as owner of record.

### 300 Malaga
- **Address:** 300 Malaga Ave
- **Parcel id:** 0341170058270
- **Status:** Planning
- **Asset class:** Condo   **Neighbourhood:** not found
- Developer: Roger Development Group   |   Architect: Nichols Architects   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 25   |   Height: 56 ft   |   Stories: 4   |   Total sq ft: not found
- **Sources (7 field citations, 2 distinct):**
    - Florida YIMBY, 18 May 2026 — https://floridayimby.com/2026/05/25-unit-300-malaga-proposed-for-300-malaga-avenue-coral-gables-florida.html
    - Miami-Dade property appraiser parcel record — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0
- *Note:* No architect is named in the coverage; the article describes the design's inspiration but not the firm, so architect is not found. No design/record distinction is drawn, so this is stored as the architect.

### 6100 Caballero Boulevard
- **Address:** 6100 Caballero Blvd
- **Parcel id:** 0341300060290
- **Status:** Planning
- **Asset class:** Residential   **Neighbourhood:** not found
- Developer: Fortune International Group   |   Architect: Portuondo Perotti Architects   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 251   |   Height: 140 ft   |   Stories: 13   |   Total sq ft: not found
- **Sources (7 field citations, 3 distinct):**
    - Florida YIMBY, 12 Jun 2025 — https://floridayimby.com/2025/06/two-tower-development-planned-for-6100-caballero-blvd-coral-gables-florida.html
    - Coral Gables Gazette, 15 May 2025 — https://gables-gazette.com/18-development-projects-under-city-review-in-coral-gables/
    - Miami-Dade property appraiser parcel record — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0
- *Note:* Portuondo Perotti Architects and Arquitectonica are both named as architects and the source does not say which is architect of record; Portuondo Perotti is stored and Arquitectonica recorded here. The taller of the two buildings is stored for storeys. The source gives a range across the two buildings rather than one number; 140 ft is the taller, which is the 13-storey building already stored, and the nine-storey building is 97 ft. The 251-unit figure in the same entry matches the current scheme, confirming this is not the superseded earlier proposal.

### Cassia
- **Address:** 4011 Salzedo Ave
- **Parcel id:** not found
- **Status:** Under Construction
- **Asset class:** Condo   **Neighbourhood:** not found
- Developer: Alta Developers   |   Architect: Behar Font & Partners   |   General contractor: not found   |   Civil engineer: not found   |   Lender: JVP Management
- Residential units: 174   |   Height: not found (12 storeys known)   |   Stories: 12   |   Total sq ft: not found
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
- **Completion basis:** news_confirmed
- **Sources (8 field citations, 3 distinct):**
    - Florida YIMBY, 10 Jun 2026 — https://floridayimby.com/2026/06/construction-loan-secured-for-cora-merrick-park-at-4241-aurora-street-in-coral-gables.html
    - Florida YIMBY, 24 Apr 2026 — https://floridayimby.com/2026/04/cora-merrick-park-set-for-2028-completion-at-4241-aurora-st-coral-gables-fl.html
    - Miami-Dade property appraiser parcel record — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0
- *Note:* The $67.5m construction loan is reported without naming the lender, so lender is not found. Berkadia arranged the loan and BHI provided it. Height is deliberately not recorded: the only published figure, 137.5 ft, describes a superseded eight-storey-over-three scheme, and the current design is 13 storeys. HEIGHT FOUND BUT WITHHELD ON A STOREY MISMATCH. City of Coral Gables PZB Staff Report, 4241 Aurora, file 23-6279 of 21 Sep 2023 (https://coralgables.legistar1.com/coralgables/attachments/42f4d46e-7800-4a50-9654-8f516fb4102e.pdf) states building height is 12 stories at 137.5 feet to the top of the roof, and its public notice repeats that the proposed building is 137.5 feet tall. The measurement basis is sound, but it comes with 12 stories and 80 residential units against the 13 storeys and 74 units stored, and Cora Merrick Park is a redesign of the approved 4241 Aurora scheme, so 137.5 ft is probably superseded. Signed Resolution 2023-315 phrases it as a cap, and there is no later site-plan amendment in Legistar.

### Ponce Park
- **Address:** 3000 Ponce de Leon Blvd
- **Parcel id:** 0341170057170
- **Status:** Under Construction
- **Asset class:** Condo   **Neighbourhood:** not found
- Developer: The Allen Morris Co.   |   Architect: Zyscovich Architects   |   General contractor: KAST Construction   |   Civil engineer: not found   |   Lender: Bank OZK
- Residential units: 58   |   Height: not found (11 storeys known)   |   Stories: 11   |   Total sq ft: not found
- **Completion basis:** news_confirmed
- **Sources (9 field citations, 4 distinct):**
    - Florida YIMBY, 20 Dec 2025 — https://floridayimby.com/2025/12/construction-starts-on-ponce-park-at-3000-ponce-de-leon-blvd-coral-gables-florida.html
    - Florida YIMBY, 14 Aug 2026 — https://floridayimby.com/2026/08/new-amenity-renderings-revealed-for-ponce-park-at-3000-ponce-de-leon-boulevard-in-coral-gables.html
    - Florida YIMBY, 24 Jan 2026 — https://floridayimby.com/2026/01/allen-morris-co-secures-132-5-million-construction-loan-from-bank-ozk-for-ponce-park-in-coral-gables.html
    - Miami-Dade property appraiser parcel record — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0
- *Note:* John Cunningham is named as designer alongside Zyscovich Architects; Zyscovich is the architecture firm the coverage names for the project and is stored as architect. HEIGHT FOUND BUT WITHHELD ON A STOREY MISMATCH. City of Coral Gables PZB Staff Report, Ponce Park Residences, file 23-6708 of 13 Dec 2023 (https://coralgables.legistar1.com/coralgables/attachments/862a4686-fdcf-43aa-925d-1e36caaaaf5e.pdf) states the proposed building is 115 feet 8 inches to the roof deck with a 20-foot pool cabana — 115.67 ft to the roof deck, the cabana being a rooftop feature and rejected. But the same report's data table reads 9 stories and 57 residential units against the 11 storeys and 58 units stored, so the figure may belong to a superseded scheme or the city may be counting residential floors only. Signed Resolution 2024-107 restates it as a cap, and the two later amendments, Res. 2025-463 and Res. 2026-1777 of June 2026, change only Ponce Circle Park cost and timing conditions rather than height. Resolve the storey discrepancy and 115.67 becomes usable.

### Regency Parc
- **Address:** 2601 Salzedo St
- **Parcel id:** 0341170052330
- **Status:** Complete
- **Asset class:** Residential   **Neighbourhood:** not found
- Developer: Codina Partners   |   Architect: Nichols Architects   |   General contractor: not found   |   Civil engineer: not found   |   Lender: Goldman Sachs
- Residential units: 126   |   Height: not found (20 storeys known)   |   Stories: 20   |   Total sq ft: not found
- **Completion basis:** news_confirmed  (2026-06)
- **Sources (7 field citations, 3 distinct):**
    - Florida YIMBY, 2 Jun 2026 — https://floridayimby.com/2026/06/regency-parc-begins-welcoming-first-residents-in-downtown-coral-gables.html
    - Commercial Observer, 15 Nov 2023 — https://commercialobserver.com/2023/11/codina-goldman-sachs-coral-gables/
    - Miami-Dade property appraiser parcel record — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0
- *Note:* The coverage does not state whether the residences are rental or for sale, so the project is stored under Residential rather than assigned to Condo on an assumption. The loan predates the final design: the 2023 coverage describes an 18-storey, 124-unit building at this address while the delivered tower is 20 storeys with 126 residences. Same address and same developer, so it is the same project at an earlier design stage. HEIGHT FOUND BUT WITHHELD ON A STOREY MISMATCH. City of Coral Gables Planning and Zoning Staff Report, Regency Tower, files 22-3727 and 22-3729 of 9 Mar 2022 (https://coralgables.legistar1.com/coralgables/attachments/f7884e7a-f30f-4d4e-a919-69020aaca756.pdf) states the proposed building height is 18 stories at 204 feet 8 inches to the top of roof, 239 feet 8 inches to the top of ornamental tower. The 239 feet 8 inches is REJECTED as a measurement to an ornamental tower rather than the roof. The 204 feet 8 inches is a genuine roof height and is carried into signed Ordinance 2022-32 and Resolution 2022-170 of 28 Jun 2022, but it comes with 18 storeys against the 20 stored, which Florida YIMBY also reports as 20. The 2024 and April 2026 amendments cover public-art escrow and a temporary CO and restate no height, so 204 feet 8 inches is the standing approved roof height and becomes usable once the 18-versus-20 storey question is settled.

### The George Residences
- **Address:** 717 Valencia Ave
- **Parcel id:** 0341170081800
- **Status:** Under Construction
- **Asset class:** Condo   **Neighbourhood:** not found
- Developer: MG Developer   |   Architect: De la Guardia Victoria Architects & Urbanists   |   General contractor: not found   |   Civil engineer: not found   |   Lender: Pine Bay Capital
- Residential units: 13   |   Height: 43.50 ft   |   Stories: 3   |   Total sq ft: not found
- **Completion basis:** news_confirmed
- **Sources (8 field citations, 4 distinct):**
    - Florida YIMBY, 20 Jul 2025 — https://floridayimby.com/2025/07/mg-developer-secures-41-million-construction-loan-for-the-george-residences-in-coral-gables.html
    - City of Coral Gables, Planning and Zoning Board Staff Report — The George (file 24-7521) — https://coralgables.legistar1.com/coralgables/attachments/fa4d6f75-51b7-4629-8966-1fa00ce287c1.pdf
    - Florida YIMBY, 5 Nov 2025 — https://floridayimby.com/2025/11/mg-developer-breaks-ground-on-the-george-residences-the-final-development-in-coral-gables-biltmore-square.html
    - Miami-Dade property appraiser parcel record — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0
- *Note:* A 13-unit for-sale townhome development, so in scope as multifamily of five or more units rather than as one-to-four-family housing. 43.5 ft is 43 feet 6 inches in decimal feet. The report's 13 units match the tracker exactly. Its data table lists the proposal at 43 feet 6 inches over 3 stories against a permitted 150 feet over 13 stories — the 150 ft is the cap and was not used. Adopted as signed Resolution 2024-154 on 9 Jul 2024. A later variance at 717/741 Valencia, file 26-2183 approved 26 Oct 2026, concerns an interior side setback rather than height.

### The Mark
- **Address:** 1250 S Dixie Hwy
- **Parcel id:** 0341300040010
- **Status:** Approved
- **Asset class:** Mixed-Use   **Neighbourhood:** not found
- Developer: Landmark Properties   |   Architect: Behar Font and Partners   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 393   |   Height: 105 ft   |   Stories: 8   |   Total sq ft: not found
- **Sources (9 field citations, 2 distinct):**
    - Coral Gables Gazette, April 2026 — https://gables-gazette.com/commission-approves-new-u-s-1-project-and-expands-rtz-overlay-in-4-1-votes/
    - Coral Gables Gazette, 9 Apr 2026 — https://gables-gazette.com/editorial-the-alley-at-1250-south-dixie/
- *Note:* The applicant of record is LCD Acquisitions LLC. Coverage outside the acceptable-source list attributes the project to Landmark Properties; no acceptable source confirms that, and Sunbiz refuses automated requests, so the developer is not found and the LLC is kept as owner of record per the field rule. This closes the gap the row was first recorded with. The Gazette's news coverage of the April 2026 site-plan approval names only the applicant of record, LCD Acquisitions LLC; its editorial on the same project names the developer firm, and the Gazette is on the acceptable-source list. The 105 ft height already on the row matches the Gazette's approval story; a superseded 97 ft appears in its May 2025 city-review list.

### Via Veneto
- **Address:** 915 Palermo Ave
- **Parcel id:** 0341180110010
- **Status:** Approved
- **Asset class:** Condo   **Neighbourhood:** not found
- Developer: The Calta Group   |   Architect: Locus Architecture   |   General contractor: not found   |   Civil engineer: not found   |   Lender: Banesco USA
- Residential units: 10   |   Height: 40 ft   |   Stories: 3   |   Total sq ft: not found
- **Sources (8 field citations, 4 distinct):**
    - Florida YIMBY, 18 Jan 2025 — https://floridayimby.com/2025/01/via-veneto-to-start-construction-this-spring-at-915-palermo-ave-coral-gables-fl.html
    - City of Coral Gables, Planning and Zoning Staff Report — 915 Palermo Avenue Height Variance, VARI-25-05-0021, Board of Adjustment (file 25-9128) — https://coralgables.legistar1.com/coralgables/attachments/c6ead0cd-b2b0-4744-b271-5ef9629dfdeb.pdf
    - Commercial Observer, 7 Jan 2025 — https://commercialobserver.com/2025/01/calta-via-veneto-banesco-coral-gables/
    - Miami-Dade property appraiser parcel record — https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0
- *Note:* The January 2025 report says construction was to start that spring, which is a forecast and not a construction start, so the stage is Approved. The same report contains a '28 stories above grade' sentence that contradicts the project's own description of ten three-storey residences; it is not stored. The $30m construction loan is reported without naming the lender. Florida YIMBY corroborates with the full bank name in February 2025. The 10 townhouse units match the tracker exactly, and the 3 storeys already stored are consistent with a 40 ft townhouse. The 35 ft and 45 ft also in this document are the zoning caps for the south and north sides, not the building. Staff recommended approval and the Board of Architects approved the proposal on 24 Apr 2025.

## Doral — random 10 of 57

### Bridge Point Doral 826
- **Address:** 7775 NW 48 St
- **Parcel id:** 3530220200010  (also 3530220080010)
- **Status:** Approved
- **Asset class:** Industrial   **Neighbourhood:** not found
- Developer: Bridge Industrial   |   Architect: not found   |   General contractor: not found   |   Civil engineer: Langan Engineering and Environmental Services   |   Lender: not found
- Residential units: not applicable   |   Height: 50 ft   |   Stories: not found   |   Total sq ft: not found
- **Sources (4 field citations, 4 distinct):**
    - City of Doral Development Projects layer — https://experience.arcgis.com/experience/f53641e40f6b40d88fa7721bf5ca38cb/
    - City of Doral, City Council Memo — BP Doral 826 FL, LLC Site Plan (file 25-6304) — https://legistar1.granicus.com/Doral/attachments/63bcd132-e4bb-4d4b-af7b-56bcc62393d0.docx
    - City of Doral Resolution No. 25-235 — https://www.cityofdoral.com/files/assets/city/v/2/city-hall/city-clerk/resolutions-archived/2025/res.-no.-25-235-bp-doral-826-fl-llc-site-plan.pdf
    - Florida YIMBY, 16 Dec 2025 — https://floridayimby.com/2025/12/bridge-point-doral-826-planned-for-2027-completion-at-7775-nw-48th-st-doral-florida.html
- *Note:* The City's record states the programme as two warehouse buildings without a floor area, and the press coverage states no area for this project either, so square footage is not found. Civil engineer is recorded on a qualified basis: the City's resolution says the site plan set was prepared by Langan Engineering and Environmental Services and does not use the words 'civil engineer'. In Florida a site plan set is sealed by the engineer of record, and Langan is an engineering firm, which is why it is recorded here — but it is an inference about the role, not a quoted role. The applicant is BP Doral 826 FL, LLC at 7705, 7775 and 7785 NW 48th Street, two warehouses of about 284,713 sq ft on 15.7 acres. The Associated Bank facility is not recorded as the lender because the sentence naming it does not describe it as construction financing. An architect attribution to Arcadis exists in coverage that could not be fetched for a verbatim quote, and 'design firm' does not distinguish architect from engineer, so it is not recorded. The memo's address, 7705-7775-7785 NW 48th Street, contains the tracker's 7775 NW 48 St. Industrial, so no residential component. The 70 ft right-of-way width in the same sentence is the zoning cap, not the building. Quote whitespace normalised from the DOCX.

### Chick-Fil-A East
- **Address:** 8705 NW 35 LN
- **Parcel id:** 3530280180010
- **Status:** Approved
- **Asset class:** Retail   **Neighbourhood:** not found
- Developer: not found   |   Architect: not found   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: not applicable   |   Height: not found   |   Stories: not found   |   Total sq ft: not found
- **Sources (4 field citations, 1 distinct):**
    - City of Doral Development Projects layer — https://experience.arcgis.com/experience/f53641e40f6b40d88fa7721bf5ca38cb/

### Doral Gateway Garage
- **Address:** 8750 NW 36 ST & 3750 NW 87 AVE
- **Parcel id:** 3530280100020  (also 3530280100030)
- **Status:** Approved
- **Asset class:** Parking   **Neighbourhood:** not found
- Developer: not found   |   Architect: Pascual, Perez, Kiliddjian, Starr & Associates   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: not applicable   |   Height: not found   |   Stories: not found   |   Total sq ft: not found
- **Sources (5 field citations, 2 distinct):**
    - City of Doral Development Projects layer — https://experience.arcgis.com/experience/f53641e40f6b40d88fa7721bf5ca38cb/
    - Florida YIMBY, 9 Oct 2024 — https://floridayimby.com/2024/10/banyan-street-capital-requests-modification-for-8750-n-w-36th-street-and-3787-n-w-87th-ave-doral-florida.html
- *Note:* Identity is partial: the article covers the Gateway modification at 8750 NW 36th Street, which matches the City's record exactly, and at 3787 NW 87th Avenue, where the City's record says 3750. The modification expressly includes expanding the property's parking garage, which is what this row is. No developer is recorded: the article's only ownership sentence is an acquisition by Centrum Doral Owner LLC, and Banyan Street Capital appears only as an affiliate seeking the modification, neither of which states a development role.

### Doral Square Expansion
- **Address:** N/A
- **Parcel id:** 3530270010243  (also 3530270010245)
- **Status:** Approved
- **Asset class:** Retail   **Neighbourhood:** not found
- Developer: not found   |   Architect: not found   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: not applicable   |   Height: not found   |   Stories: not found   |   Total sq ft: not found
- **Sources (4 field citations, 1 distinct):**
    - City of Doral Development Projects layer — https://experience.arcgis.com/experience/f53641e40f6b40d88fa7721bf5ca38cb/

### Flightway Ten
- **Address:** 6175 NW 102 AVE
- **Parcel id:** 3530170010610
- **Status:** Approved
- **Asset class:** Industrial   **Neighbourhood:** not found
- Developer: not found   |   Architect: not found   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: not applicable   |   Height: not found   |   Stories: not found   |   Total sq ft: not found
- **Sources (4 field citations, 1 distinct):**
    - City of Doral Development Projects layer — https://experience.arcgis.com/experience/f53641e40f6b40d88fa7721bf5ca38cb/

### Legacy at Doral (EWE Warehouse)
- **Address:** 2525 NW 82 AVE
- **Parcel id:** 3530270240010
- **Status:** Approved
- **Asset class:** Residential   **Neighbourhood:** not found
- Developer: not found   |   Architect: not found   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: 185   |   Height: 62.33 ft   |   Stories: not found   |   Total sq ft: not found
- **Sources (6 field citations, 2 distinct):**
    - City of Doral Development Projects layer — https://experience.arcgis.com/experience/f53641e40f6b40d88fa7721bf5ca38cb/
    - City of Doral, Planning and Zoning Staff Report — EWE Warehouse (Legacy at Doral) Site Plan (file 22-4925) — https://legistar1.granicus.com/Doral/attachments/c51f5a33-07be-4e99-b320-9285e7aae638.docx
- *Note:* 62.33 ft is 62 feet 4 inches in decimal feet. This is the building's own peak height and not a cap: the cap in the same passage is the right-of-way width of 85 feet on NW 82 Avenue, and the Development Standards table lists 62 feet 4 inches in the Provided column against a Maximum height standard of R-O-W Width. CAVEAT: this is the original 2023 site plan and a later site-plan modification for Legacy at Doral exists in local press, so the figure could have moved. The staff report gives no storey count. Quote whitespace normalised from the DOCX.

### MDPLS Doral Branch Library
- **Address:** 5360 NW 84 AVE
- **Parcel id:** 3530220500010
- **Status:** Approved
- **Asset class:** Institutional   **Neighbourhood:** not found
- Developer: not found   |   Architect: not found   |   General contractor: Lee Construction Group, Inc.   |   Civil engineer: not found   |   Lender: not found
- Residential units: not found   |   Height: not found   |   Stories: not found   |   Total sq ft: not found
- **Sources (5 field citations, 2 distinct):**
    - City of Doral Development Projects layer — https://experience.arcgis.com/experience/f53641e40f6b40d88fa7721bf5ca38cb/
    - Miami-Dade County legislative item 212405, adopted 19 Oct 2021 — https://www.miamidade.gov/govaction/matter.asp?file=true&fileAnalysis=false&matter=212405&yearFolder=Y2021
- *Note:* A government record: a design-build contract award of $11,209,560.50, lowest responsive of 14 bids. The architect is deliberately not recorded — Ferguson Glasgow Schuster Soto appears in the county record only as a bare entry under a SUBCONSULTANTS heading with no role stated, and the design credit exists only on the firm's own website, which is not an acceptable source. The county record describes a two-storey building while the City's record and the 2023 county press release say three.

### Prime Square Doral
- **Address:** 7200 NW 102 Ave
- **Parcel id:** not found
- **Status:** Permitting
- **Asset class:** Retail   **Neighbourhood:** not found
- Developer: Rieber Developments; Berca Development Group   |   Architect: PPK Architect   |   General contractor: Vandrak Construction   |   Civil engineer: Fortin, Leavy, Skiles   |   Lender: not found
- Residential units: not applicable   |   Height: 51.67 ft   |   Stories: 3   |   Total sq ft: 58,000
- **Sources (8 field citations, 2 distinct):**
    - Florida YIMBY, 19 Sep 2026 — https://floridayimby.com/2026/09/pre-construction-begins-on-prime-square-doral-at-7200-nw-102nd-avenue-in-doral.html
    - City of Doral, City Council Memo — Prime Square Site Plan (file 25-6428) — https://legistar1.granicus.com/Doral/attachments/83ae3e80-4346-47a1-b6ae-42d2f6cb0280.docx
- *Note:* Stage is Permitting: the project is described as approaching final permitting, and Miami-Dade County's Plat Committee approved a resubmission in January 2026. Civil engineer is filled because the article names the full design team, which is the only circumstance in which that field is filled for this market. The source states 51 feet 8 inches to the top of parapet; 51.67 is that same figure in decimal feet, not a derived number. A parapet is the top of the building, not a spire or a mechanical feature. NO RESIDENTIAL UNITS: Prime Square Doral is a commercial condominium — Florida YIMBY of 19 Sep 2026 describes a 58,000-square-foot commercial condominium development of roughly 40,000 sq ft retail and restaurant and 18,000 sq ft office — and the city memo's parking table covers only retail, restaurant and office. No source states a unit count, so the field is left empty rather than set to zero. The permitted six-storey figure in the same memo is a zoning cap and was not used.

### Shoppes @ 97
- **Address:** 9690 NW 58 ST
- **Parcel id:** 3530210220010
- **Status:** Approved
- **Asset class:** Retail   **Neighbourhood:** not found
- Developer: not found   |   Architect: not found   |   General contractor: not found   |   Civil engineer: not found   |   Lender: not found
- Residential units: not applicable   |   Height: not found   |   Stories: not found   |   Total sq ft: not found
- **Sources (4 field citations, 1 distinct):**
    - City of Doral Development Projects layer — https://experience.arcgis.com/experience/f53641e40f6b40d88fa7721bf5ca38cb/

### University of Miami UHealth
- **Address:** 8375 NW 53 TER
- **Parcel id:** 3530220320050  (also 3530220320062)
- **Status:** Approved
- **Asset class:** Institutional   **Neighbourhood:** not found
- Developer: Codina Partners   |   Architect: Arcadis   |   General contractor: Robins & Morton   |   Civil engineer: not found   |   Lender: not found
- Residential units: not found   |   Height: not found (6 storeys known)   |   Stories: 6   |   Total sq ft: not found
- **Completion basis:** news_confirmed  (2024-11)
- **Sources (8 field citations, 3 distinct):**
    - City of Doral Development Projects layer — https://experience.arcgis.com/experience/f53641e40f6b40d88fa7721bf5ca38cb/
    - Florida YIMBY, 21 Dec 2024 — https://floridayimby.com/2024/12/uhealth-doral-opens-160000-square-foot-medical-center-in-downtown-doral.html
    - Florida YIMBY, 18 Feb 2023 — https://floridayimby.com/2023/02/uhealth-has-plans-for-a-new-medical-facility-at-8375-northwest-53rd-terrace-doral-florida.html
- *Note:* The University of Miami Health System owns the venture. The developer attribution is weaker than the rest of this row: the source states the role plainly but prints the firm as 'Codina Partners Corporate Headquarters', which reads as a mangled field; Codina Partners is independently the master developer of the surrounding Downtown Doral, and the name is stored in its clean form. The six storeys are the medical building; a separate seven-storey parking garage is not counted. An earlier 150,000 sq ft figure is superseded by the 160,000 sq ft the City's record also carries. COMPLETE: opened with a ribbon-cutting on 21 November 2024. The City's layer carries it as Approved because that layer records approvals, not deliveries. This predates the 2025-26 loading window, so it would not have been loaded as a completion had the City's record said so; it is corrected rather than removed, because leaving a finished building in the pipeline is the worse error.
