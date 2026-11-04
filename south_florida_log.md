# South Florida ingest log

Run completed 4 November 2026. 452 projects across the five cities, loaded into
the existing tracker as a new market (`market = "South Florida"` in
`app/data.py::MARKETS`). Pipeline totals exclude Complete and Dead, same as
every other market.

| City | Projects | Planning | Permitting | Approved | Under constr. | Complete | Dead |
|---|---|---|---|---|---|---|---|
| Miami | 313 | 4 | — | — | 177 | 132 | — |
| Miami Beach | 35 | 19 | — | 8 | 7 | 1 | 1 |
| West Palm Beach | 30 | 12 | — | 4 | 13 | 1 | — |
| Coral Gables | 17 | 5 | — | 5 | 6 | 1 | — |
| Doral | 57 | 2 | 2 | 51 | 2 | — | — |
| **Total** | **452** | **42** | **2** | **68** | **205** | **135** | **1** |

Pipeline (excluding Complete and Dead): 316 projects, 18,033 residential units,
36.2m sq ft. 417 of 452 projects carry a parcel folio or PCN; no duplicate
parcel identifier exists within any city.

---

## City of Miami — 313 projects

**Master list: the City's own permit feed.** "Building Permits Since 2014" on
the City's ArcGIS hub, read through the FeatureServer REST endpoint. 236,660
permits in the layer; 74,785 fetched in construction scopes; 313 projects after
the three tests, the loading window and the folio dedupe.

How a permit becomes a project: the permit number is the process number plus a
trade code, so one tower files a `B` (building) permit and `E`, `P`, `MA`
siblings for its trades. Only the `B` permit is read — otherwise one tower
becomes six rows, each with a subcontractor in the contractor field. Permits
are then grouped by application number, and applications by folio.

**Cost floor used: $1,000,000, OR 20,000 sq ft of permitted area** — a project
clears on either limb. Each catches what the other misses: cost alone drops
genuinely small new apartment buildings whose permits are cheap, area alone
admits six-figure refits of large floor plates. The floor removed 303 records.
Examples of what it removed: 930 NE 79 St ($63,125 / 1,534 sq ft), 55 NE 24 St
($69,000 / 1,401 sq ft), 111 NW 54 St ($132,305 / 1,869 sq ft). Without it the
commercial new-construction set is dominated by jobs that are commercial and
permitted and still not development projects.

**Records skipped, by test** (74,476 permit rows):

| Reason | Rows |
|---|---|
| `test1_not_a_building_permit` — an electrical, plumbing, mechanical, roofing or fire permit, not the main building permit | 40,373 |
| `test3_no_stated_program` — work items name only components (trusses, railings, post-tension, a pool), no use and so no building programme | 21,088 |
| `test1_no_building_change` — a scope that creates no building: demolition with no replacement stated, recertification, signage, fences | 8,825 |
| `test2_not_commercial` — single-family, two-family or accessory dwelling only | 3,245 |
| `test1_single_unit_interior` — addressed to one unit inside a standing building (e.g. 485 Brickell Av # 4010, $58,648) | 322 |
| `below_floor` — passes all three tests but below the cost/area floor above | 303 |
| `completed_before_2025` — an older completion, outside the loading window | 288 |
| `permit_expired` / `permit_revoked` — not live work and no building delivered | 14 |

**Field fill (313 projects):** asset class 313, status 313, parcel folio 311,
total sq ft 310, general contractor 309, neighbourhood 307, owner of record 287,
developer 22, stories 22, architect 21, residential units 16, height 15,
lender 5, civil engineer 0.

The shape of that is the point: a building permit carries the contractor, the
folio, the permitted area, the cost, the use, the status and the certificate of
occupancy, and it carries **no unit count, no height, no developer and no
architect** — there are no such fields. Those four are filled only where a
named source states them, which in this run means the top of the pipeline by
construction cost plus the acceptance-list projects: 20 projects researched in
`data/sofl/miami_overlay.json`. For the remaining 291, the owner of record from
the county parcel layer is in `owner_or_agency` (287 of them) and developer
reads "not found", because the owner is usually a single-purpose LLC and Sunbiz
refuses automated requests.

The property appraiser is used **only** for the owner of record, never for
units or floor area: it describes the building that is standing, which on a
redevelopment site is the one coming down.

**Neighbourhood:** spatial join against the "Miami Neighborhoods Shapefile"
layer on the same hub (107 polygons), point-in-polygon on the permit's own
coordinates. 307 of 313 matched; six sites fall outside every polygon.

**Not-yet-permitted projects:** four added from Florida YIMBY coverage of UDRB
filings and county plan filings — 619 Brickell Avenue, 130 Biscayne Boulevard,
LILLI, 75 Broadway (`data/sofl/miami.json`).

**"Is it already complete?" check.** Completion is tested *before* construction
in `sofl_miami.stage_of`, so a finalled permit with a certificate reads Complete
and never Under Construction. 261 parcels carry an earlier completed building
permit from before the window; where the live project on that parcel is not
itself Complete, that fact is written to the row's notes rather than acted on —
it is usually the building being replaced or an earlier phase. Four projects
that resolved to Complete on a 2019–2024 certificate were dropped as older
completions even though their permits still read Active or Hold.

**Certificate codes:** `CO`, `CC`, `TCO`, `TCC` establish completion. `C40` does
not — it is the county's forty-year recertification, issued to buildings that
have stood for four decades, and reading it as a completion would date a 1970s
tower's delivery to last month.

---

## Miami Beach — 35 projects

**Master list:** the City's two Live Local Act pages (5 approved, 7 under
review — all 12 loaded) plus Florida YIMBY coverage from 2025–26 whose headline
names a Miami Beach address, filtered to exclude North Miami Beach, Surfside,
Bay Harbor Islands, Sunny Isles and the rest.

**Sources dropped:**

- **Land Development Dashboard** — the page is a Power BI (gov cloud) iframe.
  Two attempts: fetching the page gives the embed, and the public report API
  returns a Power BI error page. Dropped; the Live Local pages and press are
  the backup.
- **Commission Approved Development Orders** — a Laserfiche WebLink browse
  that refuses access without cookies. Dropped.
- **City permit portal (EnerGov Self Service)** — a JavaScript single-page app;
  its search API returns HTTP 500 and its settings endpoint 404. Dropped, so
  status comes from board actions and dated press rather than from permits.
- **`gc_Cadastral` MapServer at `gis.miamibeachfl.gov`** — not gone, just at a
  different path than the brief gave: `/public/rest/services/...`, not
  `/arcgis/rest/services/...`. Layer 12 (Neighborhoods, 14 polygons) is in use.

**Field fill (35):** asset class 35, status 35, owner of record 27, neighbourhood
25, parcel folio 25, developer 23, architect 21, stories 21, residential units 19,
general contractor 5, height 5, total sq ft 4, lender 3, civil engineer 0.

The weak spots are deliberate. Eight of the 35 are Live Local applications for
which the City publishes **only** the address, the application number and the
approval — the development order itself sits behind the EnerGov portal. They
are loaded with the programme fields reading "not found" and a note saying why,
because an approved municipal development order on a named parcel is a real
project and dropping it would have omitted 1960 Normandy Drive, which is on
your list. General contractor is thin because this market's contractors are
named on permits, and the permit portal is the source that refused.

**Records skipped for failing the three tests:**

- *The Ritz-Carlton Residences, South Beach* — test 3. The coverage names the
  sponsor (Sobe Sky Development: Flag Luxury Group, Ben-Josef Group, Lionstone
  Development), the architect (Kobi Karp) and a $61.2m predevelopment loan from
  BHI, but no unit count, floor area or storey count, and no street address to
  key it on.
- *Fontainebleau Miami Beach outdoor amenity redevelopment* — test 1. The
  application expressly does not expand the footprint, alter the massing or
  increase density.

**Southgate Towers comes out Dead,** as expected: a pair of 38-storey towers at
910 West Avenue, applicant Southgate Towers LLC, Blackstone-owned, determined
ineligible under the Live Local Act because the site is residentially zoned —
an administrative determination by Planning Director Deborah Tackett dated
30 October 2026. Recorded under the existing `Denied` status, so it keeps the
stage it died at and drops out of pipeline totals through the same filter
Boston's withdrawn projects already use. The 495 units in the coverage are the
*existing* buildings, not the proposal, so no unit count is stored.

---

## West Palm Beach — 30 projects

**Master list:** there is no bulk dataset, as the brief said. The list is
Florida YIMBY and The Real Deal coverage from 2025–26 whose headline names a
West Palm Beach address, with Riviera Beach, Singer Island, Mangonia Park, Palm
Springs and unincorporated Westgate excluded by hand — Florida YIMBY files
several of those under "West Palm Beach" and they are not in the city.

**PPRC agendas:** found and reachable at a predictable URL pattern
(`wpb.org/files/assets/city/v/1/development-services/documents/plans-amp-plats-review-committee/2026-pprc-agendas/...`).
Two projects in this run are recorded from PPRC items (209 N Sapodilla Avenue,
501 Monceaux Road) via press reports of those meetings. The agendas themselves
were **not** read item by item: that is a PDF-reading pass per meeting, which is
the "new parser" this run is not allowed to grow. Flagging it as the obvious
next increment if you want WPB's early pipeline fuller.

**Sources dropped:**

- **Civic Access permit portal** — not reached in this run; status comes from
  dated construction reports instead.
- **Notices of Commencement** — Palm Beach County blocked automated fetches, as
  you found. Not pursued.

**Field fill (30):** asset class 30, status 30, developer 26, stories 25,
architect 24, residential units 22, owner of record 17, parcel PCN 16,
neighbourhood 9, total sq ft 8, general contractor 6, lender 6,
civil engineer 1.

Neighbourhood is 9 of 30 because the "WPB Neighborhoods" layer (59 polygons)
covers the residential neighbourhood associations, and most of these projects
are in the downtown core and the Flagler Drive corridor, which fall outside
every polygon. That is the layer's coverage, not a join failure. PCN is 16 of
30 because seven projects publish no street address at all (10 and 15
CityPlace, Alida Residences, the Rybovich redevelopment, the Cox Science Center
expansion) and the rest did not match the county's street-number index.

**Records skipped for failing the three tests:**

- *The District at Northwood* (2501 Pinewood Ave) — test 3; architect and a Q4
  2026 completion are published, no programme.
- *The Club at Ibis campus transformation* — test 3; stated scope is bocce and
  basketball courts, a fitness-centre renovation and a restaurant, no area.
- *The Estates at Nomar* — test 2; 100 single-family homes.
- *The Domaine at SoSo* — test 2; Pulte townhome product, no unit count.

Also excluded as out of scope (wrong municipality): 3140 Lake Shore Drive and
Residences at Marina Village (Riviera Beach), Singer Island Gateway at 2525
Lake Drive, the Waterview Apartments (Mangonia Park), Edera Palm Springs,
Neighborlee Living Westgate. And Lumaire at 201 Clearwater Drive, completed
August 2024 — an older completion.

**Alba Palm Beach comes out Complete,** as expected, but not on a certificate
of occupancy: the City's permit portal was not reachable. It rests on two
things instead — Florida YIMBY's 22 June 2026 photographs of the finished
building, and the Palm Beach County parcel record, which now carries individual
condominium units at 4714 N Flagler Drive conveyed to separate owners
(TRAULSEN REGINE TRUST, GOULDEN DAVID, SPADONE MELINA and others). Per-unit
parcels with individual owners are sales closing, which the brief accepts. The
caveat is on the row.

**120 S Dixie Highway: 399 units**, not 366. Florida YIMBY and The Real Deal
both state 399 in reporting dated 28 May 2026, with about 160 of them at or
below 120% AMI. The 366 figure is older and is superseded under the conflict
rule.

---

## Coral Gables — 17 projects

**Master list:** Florida YIMBY's Coral Gables coverage (every 2025–26 article)
cross-checked against the Coral Gables Gazette, which supplied The Mark
outright. Development Review Committee and Commission items are reached through
that coverage rather than by reading agendas, per the "site plan and major
development items only, never whole agendas" instruction.

**Field fill (17):** asset class 17, status 17, residential units 17, architect 16,
developer 15, stories 14, parcel folio 10, owner of record 10, height 5,
lender 3, total sq ft 3, general contractor 3, neighbourhood 0.

**Neighbourhood is 0 by design** — Coral Gables publishes no neighbourhood
layer, and the rule is the source document's own area name or nothing. None of
the source documents used one.

**Records skipped for failing the three tests:**

- *Ponce Circle Park redesign* — test 1; a $10m park redesign creates no
  building.
- *2020 Ponce office leases* — test 1; new leases in a standing 14-storey
  office tower are not construction.

**Two field-rule cases worth knowing:**

- *The Mark* (1250 S Dixie Hwy) is **Approved** — site plan and alley vacation
  approved 4-1 on 14 April 2026, conditional on withdrawal of a pending county
  rapid-transit-zone application. 393 units, two eight-storey towers, 105 ft,
  17,000 sq ft of retail, a 670-space garage, FAR 3.46 against a 3.5 maximum.
  The applicant of record is LCD Acquisitions LLC. Coverage outside your
  acceptable-source list attributes it to Landmark Properties; no acceptable
  source confirms that and Sunbiz is blocked, so **developer reads "not found"**
  and the LLC stays as owner of record.
- *Alexan Crafts* (300 Catalonia Ave, on Salzedo between Catalonia and Malaga)
  is **Approved, not Under Construction.** 263 apartments, 22 live-work units,
  seven storeys, 77 ft, Trammell Crow Residential, Corwil Architects. Sources
  outside the acceptable list report exterior walls reaching the seventh floor
  during 2025, which would make it Under Construction; no government record or
  named trade press on your list carries a dated construction report, so the
  stage was not advanced. The note on the row says exactly this.

---

## Doral — 57 projects

**Master list: the City's own Development Projects GIS layer** (75 records with
name, folio, address, status, development type and a programme description),
behind the City's "Development Projects" web experience. This is a substitution
I made deliberately: it is the City's own record of what it has approved, which
is a better and cheaper version of reading Council agendas item by item. Florida
YIMBY's Doral coverage is overlaid on it.

**Doral's paid open-permit search was not used,** per instruction.

**The classification is a pinned hand review, not a rule.** The first version of
`sofl_doral.py` decided the three tests from the description text with keyword
rules. Reviewing its output found eleven wrong calls in 75 records: a 5,812 SF
Walmart addition dropped because the rule wanted "sq ft" and the City wrote
"SF"; the 552-unit Doral Parc dropped because the rule wanted "residential
units" and the City wrote "units"; a mixed-use tower filed as Office because
the word "office" appeared before the word "residential". Fixing those one at a
time is exactly the twenty-parser spiral you told me to avoid, so the module now
holds an explicit table of every project with its asset class and programme, and
an explicit exclusion list with the test each one fails. A name in neither list
reports as `unreviewed_new_city_record` rather than being guessed at — which is
what will happen when the City adds a project.

**Folio parsing was the one real trap.** Doral writes an assembled site as a
single string in its own shorthand: `/`, `,`, `;`, `&` and newlines as
separators, `Portion of` before a partial take, and abbreviated continuations
that give only the digits that change — `35-3008-000-0041, -0048, -0051` is
three parcels. Read naively those concatenate into a 39-digit string that
matches no parcel and silently breaks the dedupe, which is what happened on the
first pass. 19 Doral projects now carry parcel aliases.

**Field fill (57):** asset class 57, status 57, parcel folio 55, residential
units 10, developer 7, architect 7, stories 5, general contractor 3, lender 2,
civil engineer 1, total sq ft 1, neighbourhood 0.

Developer and architect are thin because the City's layer holds neither, and
Florida YIMBY has covered 8 of these 57 projects. The eight that are covered are
the ones that matter by size — Sevilla, Oasis at Doral, Hazel Doral, Doral Parc,
the Miami International Mall redevelopment, Prime Square, Bridge Point Doral 826
— and they carry the full team. Neighbourhood is 0 by design: Doral publishes no
layer and the City's records use no area names.

**Records skipped (20 pinned exclusions):**

| Project | Reason |
|---|---|
| ALS San Marco FHSLA Charter School | test 1 — an existing 11,921 sq ft building, no construction stated |
| BP Plaza | test 1 — urban plazas |
| Downtown Doral Entrance | test 1 — NW 87th Avenue improvements |
| Downtown Doral Paseo and Pocket Park | test 3 — no description |
| Hub Lobby (Doral Yard) | test 1 — outdoor portion of a restaurant |
| LMC Doral / Downtown Doral South & White Course | test 1 — pattern-book modification adding a roadway section |
| Lithia Modifications | test 1 — reduces showroom floor area, adds two parking spaces |
| Marmol Export USA S&S International | test 1 — storage area and truck trailer parking |
| Neovita (Doral Palms North) | test 1 — playground in a green area |
| Pickleball Yard @ Downtown Doral | test 1 — four pickleball and four padel courts |
| Smash Padel Courts | test 1 — padel courts |
| TA Realty Parking Reconfiguration @ 1910 NW 97 Ave | test 1 — parking capacity 74 → 121 |
| Tesla Motors | test 1 — service and sales use in an existing building |
| The Future Begins Here Daycare | test 1 — new daycare inside an existing 20,000 SF building |
| White Course Park | test 1 — 130,728 sq ft of park, no building programme |
| Pollo Campero | test 3 — no description |
| Kia Subaru | duplicate — same dealership at 10155 NW 12 St as Lehman Doral |
| Site Plan for 10405 NW 19 ST | duplicate — same 40,141 sq ft building as Farmasi #2 |
| Century Towne Center / Midtown Phase II | superseded — earlier 675-unit programme for Midtown PUD Phase II |
| Midtown Doral PUD / Century Towne Center | superseded — earlier 675-unit programme for Midtown PUD Phase II |

---

## The two conflicts you flagged

**Waldorf Astoria Miami height: 1,049 ft.** Florida YIMBY states 1,049 ft on
25 February 2026 and again on 27 August 2026; the 1,041 ft figure is earlier and
is superseded. The 1,255 ft in the November 2024 FAA filing is the requested
*crane* height above ground, not the building, and is not stored.

**120 S Dixie Highway units: 399.** Florida YIMBY and The Real Deal, both
28 May 2026. The 366 figure is superseded.

## The "already complete?" sweep

Every Under Construction project outside Miami that carries a parcel identifier
(14 of them) was checked against the county parcel record. None sits on a
parcel with a building completed in 2025 or later. The two year-built values
that exist are 1998 at Hazel Doral and 1938 at Shore Club Private Collection —
the buildings being replaced and restored respectively, consistent with those
projects being under construction. Palm Beach County's parcel layer publishes no
year-built field, so its five rows rest on the dated construction reports alone.

One row to re-check on the next run: **The Nora Hotel** (1021 N Railroad Ave).
The most recent acceptable-source report is its May 2025 topping-off and no
source states a certificate of occupancy or an opening, so it is recorded Under
Construction. For a five-storey hotel topped off eighteen months ago that is
probably stale.

## Sources not used, and why

- **Notices of Commencement** — Miami-Dade charges $1 per advanced search unit;
  Palm Beach blocks automated fetches. Not paid for, not circumvented. Lender
  and construction-start therefore come from permits and press.
- **Sunbiz** — returns HTTP 403 to automated requests. Not circumvented. Where
  the only named party is an LLC, that LLC is the owner of record and developer
  reads "not found", which is the brief's rule.
- **Miami-Dade property appraiser proxy** (`PaServicesProxy.ashx`) — returns a
  bot-protection page. The county's published parcel GIS layer was used instead,
  for the owner of record only.
- **Miami DDA 3D Development Pipeline Map** — published as a CityEngine web
  scene with no addressable feature service. Not used; its coverage area (CBD,
  Brickell, Arts & Entertainment District) is served by the permit feed and by
  UDRB coverage.

## What is in the code

| File | What it does |
|---|---|
| `scraper/sofl_common.py` | shared helpers: ArcGIS paging, point-in-polygon, folio/PCN normalisation, the three tests, the construction/completion proof rules, per-field source recording |
| `scraper/sofl_miami.py` | City of Miami permit ingest |
| `scraper/sofl_miami_overlay.py` | names Miami permit rows and adds the fields a permit has not got, without overwriting permit fields |
| `scraper/sofl_doral.py` | Doral Development Projects layer ingest, with the pinned decision table |
| `scraper/sofl_records.py` | loader for the reviewed records in `data/sofl/*.json` |
| `scraper/sofl_press.py` | reads the named trade press; returns cue sentences with citations, decides nothing |
| `scraper/sofl_provenance.py` | sets `developer_canonical` to the lead sponsor and writes the evidence tier for each team value |
| `data/sofl/*.json` | the reviewed records, one file per city, every value with a source URL |

Run order matters:

```
sofl_miami        ->  sofl_miami_overlay   (overlay adds to permit rows)
sofl_doral        ->  sofl_records Doral   (records add to City-layer rows)
sofl_records Miami / "Miami Beach" / "West Palm Beach" / "Coral Gables"
sofl_provenance                            (last: reads the citations the above wrote)
```

## Two display defects found by running the app, and fixed

Driving the app with Streamlit's `AppTest` rather than only checking the
database turned up two things the data looked fine without:

1. **`developer_canonical` was empty on all 452 rows.** The screener grid falls
   back to `developer`, so the table looked correct — but the Overview tab's
   MOST ACTIVE DEVELOPERS chart groups on `developer_canonical`, so scoping
   Overview to West Palm Beach reported "30 project(s) in this selection have
   no developer on record" when 26 of the 30 had one. `sofl_provenance.py` now
   sets it to the **lead sponsor** (the first party named), so a joint venture
   counts under the firm the press names first and Terra's projects group
   together rather than appearing as separate partnership strings. The caption
   now reads 4 for West Palm Beach, and 292 for Miami, which is accurate.

2. **Every contractor read off a City of Miami permit was marked "·"** — the
   weakest mark in the table, meaning "carried forward from earlier work and
   never checked against a document" — because `mark_team_value` applies that
   to any value with no `field_provenance` row, and South Florida had none. 418
   provenance rows are now written, tiered by what the citation actually is:
   `◉ registry_confirmed` for a contractor on a government permit,
   `▣ document_confirmed` for a value in a city's own published record,
   `◇ web_low_confidence` for a value from one named trade-press article.
   Developers resolve to `◐ document only`, which is the honest state: every
   one rests on a single named source, so none can claim the two-independent-
   sources tier.

## Second Miami research pass — the address matcher was the bottleneck

The first pass searched the top 34 Miami projects by construction cost and
reported coverage for 16 of them. That was wrong about the market, not about
the market's coverage: the press writes about all of these buildings, and the
matcher could not find them. It required the permit's address string to appear
verbatim in a cue sentence, and almost no article writes an address that way.

| the permit says | the press writes |
|---|---|
| `1401 BRICKELL AV` | "1401 Brickell" — no suffix |
| `90 NE 11 ST` | "E11EVEN Club Residences" — a name, no address |
| `249 NE 3 ST` | "300 Biscayne Boulevard" — the other frontage |
| `1 ISLAND DR` | "500 Brickell Key Drive", "on Brickell Key" |
| `555 N MIAMI AV` | "555 North Miami Avenue" — direction spelled out |
| `46 NE 6 ST` | "Northeast 6th Street" — ordinal added |

`scraper/sofl_address_match.py` now matches by pattern over the whole article:
street number, optional direction in either spelling, street name with an
optional ordinal. The number is anchored on both sides, so 1410 Brickell never
matches 1401 Brickell — that would silently attach the wrong developer to a
tower. Re-run over every Miami project of 100,000 sq ft or more that still had
no developer, it matched 67 of 94 against 16 of 34 before.

**Selection matters as much as matching.** The first version of the re-run
took the most recent article whose body matched, and gave 1401 Brickell a
*Mohawk at Wynwood* story that mentioned the address in passing. Candidates are
now ranked title-match first, which puts "50-Story 1401 Brickell By Santander
Breaks Ground In Miami" ahead of it.

Results, written to `data/sofl/miami_overlay.json` by
`scraper/sofl_miami_batch2.py` and `sofl_miami_batch3.py`:

| field | before | after |
|---|---|---|
| developer | 22 | 49 |
| architect | 21 | 50 |
| storeys | 22 | 52 |
| residential units | 16 | 35 |
| height | 15 | 22 |
| lender | 5 | 7 |
| projects with a real name rather than an address | 19 | 63 |

**What was deliberately not recorded.** Only values stated with a role are
stored. Where an article lists firms together as a "development team" or
"major players" without saying which is which, nothing is read off it — Natiivo
Miami and Fourteen Allapattah are both in that position and say so on the row.
Three further cases worth knowing:

- *7140 NE Miami Court* is a self-storage proposal, and its article also covers
  an unrelated three-tower, 1,292-apartment scheme. That unit count is not
  stored here.
- *The Residences at Mandarin Oriental, Miami* is two towers, and the coverage
  does not say which is the City's 1 Island Drive and which 2 Island Drive. The
  developer, architect and contractor are shared and are recorded on both; the
  storey counts and heights are not, because assigning them would be a guess.
- *Miami Freedom Park* has three permit rows (stadium, park, commercial). They
  are named, but the Adler Development / Arquitectonica / Moss team from the
  coverage is attached to none of them: that article is about an eight-storey
  administration building, and 724,470 sq ft does not reconcile with one.

Still unresolved at size: 830 Brickell Plaza (1.08m sq ft), 46 NE 6 St
(1.08m sq ft), and a developer for LOFTY Brickell (854k sq ft) — no article on
the acceptable-source list names them.

## Basemap replaced (4 November 2026)

The map's tiles were folium's built-in `"CartoDB dark_matter"` shorthand, which
points at `basemaps.cartocdn.com`. Carto now requires an API key there, and the
failure mode is the reason it went unnoticed: it does **not** error. It answers
200 with a valid PNG, and every tile is the words "API KEY REQUIRED" on a dark
ground — the same 2,513 bytes for downtown Miami as for open ocean. The map
still drew, still took its markers and still fit its bounds; only the
cartography was gone.

Replaced with Esri's `Canvas/World_Dark_Gray_Base`, which serves the same kind
of dark cartography without a key, plus `World_Dark_Gray_Reference` as a
transparent label overlay. Checkable in a way Carto's no longer is: a downtown
tile comes back at 12KB against 2KB over open water. This affects every market,
not just South Florida — Boston, Cambridge and Rhode Island were showing the
same watermark.

## Known gaps

- **40 of 452 projects have no coordinates** and so do not appear on the map:
  West Palm Beach 14, Miami Beach 10, Coral Gables 7, Doral 7, Miami 2. These
  are the projects whose address did not match the county parcel index, plus
  the seven that publish no street address at all. The table and all charts
  carry them; only the map drops them.
- **Miami developer/architect coverage is 49 and 50 of 313** after the second
  research pass below. The remainder are small permit rows; of the twenty
  largest projects by floor area, fifteen now carry both.
- **West Palm Beach PPRC agendas are reachable but unread** — see the West Palm
  Beach section.
- **The Nora Hotel** needs its status re-checked; its last acceptable-source
  report is eighteen months old.
