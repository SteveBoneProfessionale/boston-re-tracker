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
named rather than addressed 65, architect 57, developer 56, stories 54,
residential units 37, height 24, lender 7, civil engineer 0.

The shape of that is the point: a building permit carries the contractor, the
folio, the permitted area, the cost, the use, the status and the certificate of
occupancy, and it carries **no project name, no unit count, no height, no
developer and no architect** — there are no such fields. Those are filled only
where a named source states them, which after three research passes means 61
projects researched in `data/sofl/miami_overlay.json`, weighted to the top of
the pipeline by floor area: **all twenty of the largest Miami projects now
carry both a developer and an architect, and 27 of the top 30.** For the
remainder — small permit rows, mostly under 100,000 sq ft — the owner of record
from the county parcel layer is in `owner_or_agency` (287 of them) and
developer reads "not found", because the owner is usually a single-purpose LLC
and Sunbiz refuses automated requests.

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

## Third Miami pass — the last six of the top twenty

After the address pass, six of the twenty largest Miami projects still had no
developer or architect. All six were found by searching the project NAME, which
only became possible once the address pass had named enough of the market for
the names to be guessable. Each now rests on a sentence that states a role —
"developed by", "designed by", "is listed as the architect" — not on a list of
firms. Written by `scraper/sofl_miami_batch4.py`:

| project | sq ft | resolved to |
|---|---|---|
| Miami Freedom Park — park | 1,372,696 | Miami Freedom Park, LLC / Arquitectonica |
| 830 Brickell Plaza | 1,082,368 | Cain; OKO Group / Adrian Smith + Gordon Gill |
| 46 NE 6 St → **Downtown 6** | 1,081,913 | Melo Group / G3aec |
| Natiivo Miami | 1,073,159 | Sixth Street Miami Partners / Arquitectonica |
| Miami Freedom Park — stadium | 920,470 | Miami Freedom Park, LLC / Arquitectonica |
| LOFTY Brickell | 853,887 | Newgard Development Group / Arquitectonica |

Downtown 6 is the one worth singling out: its 2023 Urban Development Review
Board article states 1,081,913 sq ft, which matches the City's permit figure to
the square foot. That is what makes the identification firm rather than
plausible, and it also supplied the height (576 ft), the commercial area and
the parking count. Its storey count rose through the filings — 54 at approval,
58 in 2025, 60 on topping out — and the latest is stored.

Natiivo had been left deliberately blank in the second pass because the article
read then listed seven firms as "major players" without roles. Two earlier
articles state the roles outright, so it is now filled; the blank was the right
call on the evidence available, and the fix was finding better evidence rather
than relaxing the rule.

Three of these required the architect-of-record rule: Downtown 6 (Melo
Architecture designing, G3aec of record), 830 Brickell (Adrian Smith + Gordon
Gill with Revuelta alongside) and Forma Miami from the previous pass.

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

## Height and unit count: a fourth pass (4 November 2026)

These two fields were the thinnest in the market — 49 heights and 114 unit
counts out of 452 — and three research agents were run against them in
parallel. The result is **82 heights and 153 unit counts**, plus 160 storey
counts and 15 hotel-key counts, and 20 projects that the permit feed knew only
by address now carry their name.

Named: 38 West Eleventh Residences, both E11EVEN towers, both 222 Namdar
towers, 600 Miami Worldcenter, Flow House, Miami World Tower 1, 501 First
Residences, District 225, Four Seasons Private Residences Coconut Grove,
Society Wynwood, River District 14, The Crosby, 2000 Wynwood, Uni Tower,
NoMad Residences Wynwood, Wynwood Diamedix and Wynwood Works.

### The reporting change that matters more than the counts

A blank meant two unrelated things and rendered identically, which made 161
correct blanks look like 161 holes. A warehouse has no residential units; that
is not a gap in the research. So the detail panel and the sample document now
print one of three things and never an empty space:

    not found        researched and not published by any acceptable source
    none             a stated zero, e.g. an office tower with no residences
    not applicable   the asset class has no dwellings in it

The honest denominator for a unit count is the 367 rows that can have dwellings
in them, not 452. Where a height in feet is unpublished the panel now names the
storey count as the scale that IS known, rather than leaving two blanks beside
each other. `south_florida_sample.md` gained a coverage table with those split
denominators and a programme table listing all 135 projects of 100-plus units,
100-plus keys or 200-plus feet in one place.

A **stated zero** is a real finding and it needed its own check, because no
source ever writes the digit 0. The validator's rule for every other number —
the figure must appear in the quoted sentence — cannot work here, so what is
checked instead is the sentence: a programme enumeration that lists office,
retail, restaurant and parking and names no dwelling establishes the zero, and
one that mentions residences does not. That is not a formality. The Wynwood
Plaza office row would otherwise have been given the 509 apartments of the
*separate residential building* on the same site, because the only sentence
available says both in one breath. The check rejected it, the row keeps no
zero, and the reason is in its notes.

### Four heights found and refused

- **ENSO, 113 ft.** The FAA record states it and names an elevator enclosure as
  the highest point on the roof. A mechanical overrun is not the roof slab. The
  closest call in the batch, since the same record calls it "maximum height of
  the building"; the figure is in the notes for anyone who wants to override.
- **Mandarin Oriental North Tower, 446 ft.** Sound figure, unquotable. FAA case
  2026-ASO-7022-OE states it only in a coordinate table whose AGL/AMSL split
  lives in a column header, so no figure can be tied to a basis. Its companion
  case 7021 for the South Tower carries the prose sentence and that tower's 856
  ft **is** stored. Checked rather than assumed: one extractor over both
  letters found the six prose patterns present in 7021 return zero hits in
  7022, because 7022 is a four-page letter with no additional-information page.
  The press alternative, 426 ft, would have passed validation — it mentions no
  sea level at all — and was refused on judgement: a design figure of unknown
  basis in a field that means feet above ground is worse than an empty cell.
- **1101 71st Street, 235 ft** and **824 Alton Road, "more than 500 feet"** —
  a zoning cap and a non-figure.
- **Regency Parc 239'-8", Ocean Terrace 270 ft, Ponce Park's cabana** — all
  measured to an ornamental tower, a mechanical feature or a rooftop addition.

Three Coral Gables roof heights were found, are real, and are **withheld**:
Ponce Park 115'-8", Cora Merrick Park 137.5', Regency Parc 204'-8". Each
arrived with a storey count that disagrees with the tracker — 9 against 11, 12
against 13, 18 against 20 — which means the figure may belong to a superseded
scheme. They sit in notes with the document references, usable the moment the
storey question is settled.

### Two new government veins

- **Miami Beach EnerGov is readable anonymously after all.** `GET
  /energovprod/selfservice/api/energov/plans/{guid}` with headers `tenantId: 3`
  and `tenantName: miamibeachflprod2` returns the plan record including a
  free-text description, which for several Live Local applications carries the
  storey and unit count. This cracked 1030 6th Street (72 units, 5 storeys),
  one of three applications that had defeated two earlier passes. Attachments
  can be listed but not downloaded anonymously, so plan sheets stay out of
  reach.
- **Doral and Coral Gables both run Legistar**, and
  `webapi.legistar.com/v1/{doral|coralgables}/matters` plus
  `/matters/{id}/attachments` is open. The Doral council memo carries a
  Development Standards table stating proposed height in feet, which produced
  six Doral heights and the IMF Doral unit count. The vein is confined to
  projects that went to Council: eight Doral matters have memos with no height
  in feet at all, and six projects have no site-plan matter in Legistar.

**Add the Downtown Action Committee to the skip list** alongside the West Palm
Beach Planning Board. Most downtown WPB projects — CityPlace, Banyan Tree, 534
Datura, Alida, The Berkeley, Mr. C — are approved at the DAC rather than the
City Commission, and DAC agendas carry no heights. Figures for those exist only
on off-list broker and blog sites, and the CityPlace ones carry storey counts
that disagree with the tracker.

### Three status corrections

- **1129 SW 3 Ave** was Under Construction on `permit_active` alone. The
  building permit issued 6 March 2020 is still Active with no finalised
  inspections six years on, and Miami-Dade assesses the folio as vacant land
  with year built 0. A 287,323 sq ft hotel has not been built there, so it is
  now **Permitting** — a permit on file. This is the Rhode Island error in the
  other direction: not finished buildings left in the pipeline, but a pipeline
  inflated by projects that never started. Pinned by folio in
  `scraper/sofl_miami.py::STAGE_REVIEW` so a re-ingest keeps it, with the basis
  stored as `permit_issued_never_started`.
- **Deco Northwood** is **Dead** — the City Commission denied the application
  on 27 October 2025. Recorded through the existing denied field rather than as
  a sixth stage, so the row keeps the stage it died at, and corrected in
  `data/sofl/west_palm_beach.json` so it survives a re-ingest. The 167 units,
  eight storeys and 84 ft are kept as the record of what was refused.
- **720 SW 7 Ave** was already Complete on a certificate of occupancy dated 26
  March 2026, and the research corroborated it independently: the building is
  the automotive showroom at 701 SW 8th Street, which the County records as
  built 2025. No change, note added.

### Two identities resolved by geometry

The Mandarin Oriental rows arrived from the permit feed as "1 Island Drive" and
"2 Island Drive", which does not say which tower is which. Settled four ways:
each permit's floor area matches its row exactly; each folio's recorded address
and legal description identify the parcel; the FAA footprint corners of each
tower, ray-cast against the parcel polygons, fall inside the folio its permit
names and none fall inside the other; and Tower 1's centroid sits about 140 m
south of Tower 2's. **1 Island Drive is the South Tower** (228 residences, 856
ft, 66 storeys); **2 Island Drive is the North Tower** (121 hotel keys, 33
storeys). Permit use codes agree but were not relied on. Both rows are renamed
accordingly.

### Conflicts logged rather than silently resolved

Forma Miami 39 against 40 storeys; Okan Tower 70 against 72, and 902 against
903 ft; 830 Brickell 55 against 57; 1401 Brickell's 765 ft published when the
tower was 41 storeys and never restated at 50; Alba Palm Beach 262 ft in the
press against the City's 235 ft paired with a storey count that fails the
check; Wynwood Works 179,105 against ~265,000 sq ft; Amara Wynwood 331,846
against 358,576; and Natiivo's 448 units, which may exclude 140 Gale-branded
hotel-condos because no source prints a combined total.

### A bug worth recording

The validator's `\b` word boundaries reached the file as literal `\x08`
backspace characters, silently breaking three regex alternations — including
the one that recognises `856 feet AGL`. It presented as a correct-looking
rejection, which is the worst kind: the guard appeared to be working. Found by
printing the raw bytes of the function after the regex passed standalone and
failed inside the module. All sources are now scanned for control characters,
and the thirteen guard cases are exercised directly rather than inferred from
whether a findings file applied cleanly.

### Still not found

- **Heights, four Miami rows**: Wynwood Plaza office tower, Sweetbird North,
  Wynwood Works, 2001 N Flagler. The last two are now a *confirmed* absence
  rather than an unsearched gap — no FAA case for either building, and no
  instance of "feet", "height" or "stor" anywhere in the WPB agenda text.
- **Miami Beach heights, twenty-one rows.** The Live Local administrative
  orders simply do not state heights, and the Design Review Board staff reports
  sit behind a Laserfiche repository that redirects every anonymous search.
- **709 Alton Road.** The city's own link for this address is wrong, now
  confirmed: it resolves to the 1920 Alton Road record. Its real plan is
  LLA26-0009 and the portal's search and address endpoints return HTTP 500 to
  anonymous callers.
- **810 Alton Road may be a duplicate of 824 Alton Road** — the city links 810
  to a plan record whose own MainAddress is 824. Flagged in notes, not merged.
- **The Wynwood Plaza residential building** has no row at all. Its office
  tower does. That is a genuine gap worth opening.

## City of Miami review: asset class, size and the master permit (5 November 2026)

A spot-check of 21 City of Miami projects against trade press and filings found
developer, architect, contractor and status mostly right, asset class mostly
wrong, and size figures off on multi-tower projects. The rules were rewritten
and applied to all 452 South Florida records, not just the 21.

### Asset class was the real problem: 226 of 452 rows reclassified

The old rule called a project Mixed-Use whenever its permit named two uses.
Almost every tower in Miami has ground-floor retail and a parking podium, so
that swept **131 of 452 rows** into one bucket — pure condominium towers
(Cipriani, Baccarat, LOFTY, 1428 Brickell) beside pure rental towers (Downtown
6, Midtown 7, UniDos, 222 Namdar Phase II). A label holding 29 per cent of the
market and several unlike things is not a classification. Mixed-Use is now
**35 rows**, and in Miami alone it fell from 131 to 17.

Uses are split into **major** programmes and **minor** ones, and only majors
decide the class:

    majors   residential-rental, residential-for-sale, hotel, office,
             industrial, institutional, lab
    minors   retail, parking, other

One major is that major's class; two or more is Mixed-Use; none falls back to
the dominant minor. Ground-floor retail and a garage are what a tower sits on,
not what it is.

**"Residential" is gone from South Florida**, replaced by `Multifamily Rental`
(170 rows) and `Condo` (94). It survives for Boston, Cambridge and Rhode
Island, whose sources do not say which, and splitting those would be a claim
their sources never made. South Florida's do say: Miami's permit feed carries
MULTI-FAMILY (RENTAL) or MULTI-FAMILY (CONDO) as a work item.

**ONE DELIBERATE NARROWING, and it is the only departure from the rule as
written.** The brief counts "an office component" as a real second programme.
The permit feed cannot support that: its office token is
`PROFESSIONAL OFFICE / BANK / FINANCIAL INSTITUTION`, which a residential
tower's permit carries for a leasing office or a ground-floor bank branch just
as readily as an office tower's does for 600,000 leasable square feet. It names
the use and never the area. So residential-plus-office alone resolves to the
residential class, and Mixed-Use needs a second major that is not office. That
is not a shortcut — it is the rule applied to what the record actually says,
and **it reproduces all 14 of the review's own hand classifications**, which is
how it was checked. Where the press does state a distinct office programme with
an area, Mixed-Use is pinned by hand with the source: Okan Tower's
"approximately 64,000 square feet of Class A office space" is the one case.

**A condo-hotel unit is residential, not a hotel key.** It is a for-sale
residence carrying a hotel-branded rental programme. Getting this backwards is
what had put Natiivo and the E11EVEN towers in the wrong bucket. Only the
"N LODGING UNITS" tokens are a traditional hotel.

**The Live Local Act settles 16 Miami Beach rows on statutory grounds** rather
than on a press adjective. Florida's Live Local Act (ss. 125.01055(7) and
166.04151(7), Fla. Stat.) grants its zoning and height relief only to a
development reserving at least 40 per cent of its residential units as RENTAL
units affordable at or below 120 per cent of AMI — so what a project applied
under establishes its tenure. The exception is 425 Ocean Drive, which pairs 90
condominiums with 60 workforce rental apartments: both tenures in one project,
which is the brief's own test for Mixed-Use.

**Seven rows keep "Residential" and are listed by name** in
`scraper/sofl_asset_class_cities.py::UNSTATED`. No acceptable source states
rental or for-sale for them, and the choice was between an honest vague value
and an invented precise one. Regency Parc's own note had already recorded this:
"the coverage does not state whether the residences are rental or for sale, so
the project is stored under Residential rather than assigned to Condo on an
assumption." Four of them — The Mark, Crystal Residences, Midtown PUD Phase II
and Doral Parc — are also still sitting in Mixed-Use pending that answer, since
the tenure decides the class. Resolving the seven is the cheapest open item in
the market.

### Rules 3 and 4 were already satisfied — now verified, not assumed

Every one of the **309 permit-derived Miami rows was re-checked against its
master building permit**, and all 309 already carried that permit's square
footage and contractor. The audit is `scraper/sofl_master_permit.py` and it is
re-runnable.

That matters because the master is not obvious. The City issues many building
permits per project and **all carry the `B` trade letter**, so filtering on
trade is not enough:

    NEW CONSTRUCTION, TotalSQFT > 0     the master
    NEW CONSTRUCTION, TotalSQFT = 0     shoring, post-tension, glazing, piles
    PHASED PERMIT                       a trade's phased permit
    DEMOLITION                          the teardown

Okan Tower has 49 permits on its folio, 23 of them `B` permits, exactly one a
master. Its phased permits carry $100 costs and name the glazing, plumbing,
electrical and sprinkler subcontractors — Ready Window, Suntech, Power Design,
Sprinklermatic. Taking a contractor from any of those would name a
subcontractor as the general contractor.

**The audit's first version was wrong and the report caught it**, which is the
argument for reporting before writing. It preferred NEW CONSTRUCTION scope over
magnitude, so it picked a 943 sq ft ancillary building ahead of a 14,838 sq ft
project and a 355 sq ft remodel ahead of 130 Biscayne's 1,667,604 sq ft of
filed plans. It also crossed project boundaries: at 1500 NW North River Drive a
730,731 sq ft permit from 2015 sits beside a 5,261 sq ft permit from 2023, and
re-pointing the row at the older one would have swapped in a building finished
years before the project it tracks. Both fixed: rank by **area**, with cost only
as a tiebreak, and confine the search to **one application**, which is the unit
the ingest already treats as a project.

### Four size figures where the primary record beat the review

The brief's own instruction, applied: where the review's trade-press figure
disagreed with the master permit, the permit won and the review's figure is
recorded on the row.

| Project | Review | Master permit | Permit |
|---|---|---|---|
| 1401 Brickell by Santander | 1.6M sq ft (Santander) | **1,280,763** | BD24024742001B001 |
| 222 Namdar Phase II | ~395,000 (Florida YIMBY) | **569,969** | BD23028187001B001 |
| Midtown 7 | 421,101 (2022 filing) | **725,802** | BD23016341001B001 |
| JEM Miami Worldcenter | ~1.29M | **741,252** | BD23029000001B001 |

The likeliest reconciliation in each case is that the smaller figure is a
residential or office programme area and the permit's is gross including the
garage, which is the definition the brief asks for. **Okan Tower's contractor
went the same way**: the review reports Okan Construction, the master permit
names 1ST UNITED COMPANIES OF SOUTH FLA L.L.C., and the rule is that the
contractor comes from the master permit only.

### Part 4 status checks: no changes, and one answered backwards

All four were already right, and Midtown 7 answers the review's doubt in the
opposite direction from what it expected.

| Project | Permit | Issued | Certificate | Status |
|---|---|---|---|---|
| LOFTY / 600 SW 1 Ave | BD22025768001B001 | 2025-09-18 | none, CO required | Under Construction |
| 600 Miami Worldcenter | BD23015044001B001 | 2024-11-25 | none, CO required | Under Construction |
| Downtown 6 | BD23010187001B001 | 2025-03-14 | none, CO required | Under Construction |
| Midtown 7 | BD23016341001B001 | **2025-06-05** | none, CO required | Under Construction |

Midtown 7's permit is no longer under review — it **issued on 5 June 2025**,
a year after the report the review was working from. An issued main building
permit is the brief's own test for Under Construction, and no certificate of
any kind rules out Complete.

### The LOFTY row describes two different towers

The single biggest open item. Row 1107's name, unit count, storey count and
height are **LOFTY Brickell**, a 44-storey tower at 99 SW 7th Street whose
$170m construction loan from 3650 REIT closed in July 2022. Its permit anchor,
address, floor area, contractor of record and lender are a **different, later
tower**: master permit BD22025768001B001 at 600 SW 1 AV, 853,887 sq ft,
$156,263,351, GT Construction & Development, **issued 18 September 2025**, with
work items including a DOCK, condo, office, retail and condo-hotel. A permit
issued in September 2025 cannot be the tower financed in July 2022, and the
stored Carlyle Group lender fits the permit rather than the units — it comes
from Commercial Observer of 1 October 2025, weeks after that permit issued.

Sweeping the feed found **no NEW CONSTRUCTION master permit at 99 SW 7 ST at
all**. The corrections the review asked for were applied to the LOFTY half —
362 units not 363, 574 ft from the FAA filing, G.T. McDonald Enterprises as
contractor, and the conflated "(One Brickell Riverfront)" dropped from the name
— and the other half's permit, address, floor area and lender were **knowingly
left in place rather than deleted**, because the later tower's own unit count
and height are not in hand and inventing them would be worse than flagging it.
Splitting it needs one research pass on One Brickell Riverfront.

### Miami Freedom Park was four things in three rows

All three rows carried the **same developer and the same architect**, and each
row's own note recorded why: "Florida YIMBY's coverage of the master plan is
specifically about its eight-storey administration building." One article about
the City's administration building had seeded the developer and architect of
the stadium, the garage and the commercial component alike.

Every permit on the 130-acre site was swept. What they say:

- **BD25026033001B001**, 1896 NW 37 AV, 724,470 sq ft, Moss & Associates,
  work items `PROFESSIONAL OFFICE / BANK`, issued 2026-07-01 — the **City of
  Miami administration building**, not Miami Freedom Park LLC's commercial
  space. Three independent locks: the City's own topping-out notice gives the
  Civic Center's address as exactly this permit's delivery address; the notice
  names Moss Construction, which is the permit's contractor; and the permit's
  date and cost match the trade press to the day and the dollar ($193,000,000,
  permit received 1 July). So **row 1220 became Miami Civic Center**, with
  Adler Development as developer, eight storeys and an 889-space garage.
- **BD24019509001B001**, 4000 NW 14 ST, Finfrock Construction, TCO 2026-04-10
  — **not the park**, despite the row's name. Its work items are chain link
  fence, bollards and `PARKING GARAGE-MULTIPLE FLOORS`, and Finfrock is a
  precast parking-structure builder. Renamed the **stadium parking garage**.
- **BD25003093001B001**, 1550 NW 37 AV, 1,916,640 sq ft, work items
  `NEW PARK ONLY`, issued 2026-03-10, no certificate — the **actual park**,
  which no row tracks. Whether a park belongs in a building pipeline at all is
  a judgement for a later pass.
- **One Heron**, Miami Freedom Park LLC's Phase 1 office building of about
  250,000 sq ft, has **no permit anywhere on the site** — BD25026033001B001 is
  the only permit of any scope on any of the site's folios whose work items
  mention office, and that one is the City's. It got a new row at **Planning**,
  inheriting nothing: not Adler, not Arquitectonica, not Moss, none of which is
  attested for it. The Real Deal of 10 April 2026 has it in final design.

The appraiser cannot arbitrate any of this — the whole site is City of Miami
fee land and Miami Freedom Park LLC holds a 99-year leasehold that never
appears as owner of record. The inherited Arquitectonica was also **cleared
from the stadium and the garage**, and the stadium moved from Retail to Other:
a stadium is not a shop, and a retail token for the concourse retail was what
had classified it.

### Lenders: the five confirmed, six confirmed empty

All five lenders the review named were already stored and all five survived
scrutiny. Both two-party cases are genuine: J.P. Morgan and Sculptor are the
senior and junior tranches of one $565m construction facility, and Bank OZK and
Related Fund Management the senior and subordinate tranches of one $668m loan —
in neither case does an acceptable source call the second piece a separate
mezzanine or preferred-equity loan, so both are kept.

Six under-construction projects are now **confirmed to have no reported
construction loan**, which is a result rather than a gap: Downtown 6, Midtown 7,
both Mandarin Oriental towers, 1401 Brickell and UniDos. Four are explicable —
Melo equity-builds its downtown rental towers, Midtown 7's sponsor is a PGIM
open-end fund vehicle, Santander is building for its own occupancy, and Swire is
funding from asset sales and presales.

**Okan Tower's C-PACE was refused**, and it is the one call worth disagreeing
with. A $200m C-PACE loan from Bayview PACE is the only construction-side
facility that exists, and Commercial Observer calls it outright "the first
construction financing for the project". It is refused because C-PACE is not
the construction lender: Florida YIMBY describes the same facility as funding
qualifying energy-efficiency, water-conservation, building-envelope and
resiliency components and completing the capital stack. Naming Bayview as the
lender for a 72-storey tower would tell a reader it is financing the building.
The figure and the reasoning are on the row so the call can be reversed.

Two false positives are recorded on the rows because searches keep producing
them: an **S3 Capital $111m construction loan is Sense 22's, not the Mandarin
Oriental's** — a Real Deal permits round-up covers both and summarisers merge
the items — and a **$65m Banco Santander loan is on a different Brickell
parcel** with Santander as the lender, not a loan on 1401 Brickell.

### Two guards added to the validator, one of them against the brief

**A declared sum.** The brief's rule is to count every residential unit
including condo-hotel units, and sources routinely print the components without
a total: Okan Tower's own sentence gives "236 condo-hotel residences … 163 Sky
Residences" and no 399 anywhere. Refusing the total left the field blank on
exactly the towers it matters most for; accepting a bare total would be the
arithmetic the validator exists to stop. So components are now **declared**,
each must appear in the quote, and they must add up to the value — the figures
stay the source's and only the addition is ours. Okan Tower is 399 on that
basis.

**A stated zero** needed its own check, because no source writes the digit 0.
What is checked is the sentence rather than the figure: a programme enumeration
listing office, retail, restaurant and parking and naming no dwelling
establishes the zero; one that mentions residences does not. That is not a
formality — it is what stopped the Wynwood Plaza office row being given the 509
apartments of the *separate residential building* on the same site, because the
only sentence available says both in one breath.

### Open items, in the order they are worth doing

1. **Seven rows' tenure** — rental or for-sale for Regency Parc, 6100
   Caballero, Doral City Place 7, Legacy at Doral, Miami International Mall,
   464 Fern Street and Soleste Palm Station. Four others sit in the wrong class
   until these are answered.
2. **Split the LOFTY row** — one research pass on One Brickell Riverfront's
   units, height and storeys.
3. **JEM's 789 units** — one fetch quoting 259 and 530 in a single sentence,
   then store it as a declared sum.
4. **Four heights the review supplied and this pass could not confirm**: St
   Regis 630 ft, Mandarin South 826 ft, 222 Namdar Phase II 450 ft and UniDos
   321 ft. Each needs its FAA determination fetched. The research budget for
   this pass ran out before they could be, and each row records the figure and
   what it conflicts with rather than storing an unsourced number.
5. **Baccarat's unit count**, where the stored 355 matches no source and the
   permit filing's 352 is the figure to adopt once the document is in hand.
6. **The actual Freedom Park park**, permit BD25003093001B001, which no row
   tracks.

## Known gaps

- **40 of 452 projects have no coordinates** and so do not appear on the map:
  West Palm Beach 14, Miami Beach 10, Coral Gables 7, Doral 7, Miami 2. These
  are the projects whose address did not match the county parcel index, plus
  the seven that publish no street address at all. The table and all charts
  carry them; only the map drops them.
- **Miami developer/architect coverage is 56 and 57 of 313** after the second
  and third research passes below. The remainder are small permit rows; all twenty
  of the largest projects by floor area now carry both, and 27 of the top 30.
- **West Palm Beach PPRC agendas are reachable but unread** — see the West Palm
  Beach section.
- **The Nora Hotel** needs its status re-checked by hand. At least six outlets
  report it opened to guests on 19 October 2026 with 201 rooms, and the room
  count is now stored from an acceptable source — but none of the outlets
  carrying the opening is on the acceptable list, and the one that is, the South
  Florida Business Journal, refuses both the search crawler and a direct fetch.
  Florida YIMBY has published nothing since the May 2025 topping-off. The row
  stays Under Construction and the signal is in its notes.
- **Height in feet is 82 of 452 and will not get much past that** from press
  sources. The two bulk routes were tried and measured: the FAA Digital Obstacle
  File at a 53 per cent hit rate (rejected, see `scraper/sofl_faa_heights.py`),
  and the city council memo vein, which works but only for projects that reached
  a Council agenda. Storey counts stand at 160 and are the usable proxy.

## Change log — City of Miami review (5 November 2026)

298 field changes across 251 projects. Grouped by field; within a field, ordered by project id. A source of “—” means the change was derived from a record already cited on the row (the master building permit, or the permit's own work items) rather than from a new document, and the row's existing FIELD SOURCES entry still applies.

| Field | Changes |
| --- | --: |
| asset_class | 226 |
| lender | 32 |
| lender_status | 18 |
| developer | 5 |
| name | 4 |
| architect | 3 |
| residential_units | 3 |
| building_height_ft | 2 |
| num_stories | 1 |
| parking_spaces | 1 |
| record created | 1 |
| general_contractor | 1 |
| neighborhood | 1 |


### asset_class — 226 change(s)

| id | Project | Old | New | Source |
| --: | --- | --- | --- | --- |
| 926 | 652 NE 85 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD15003114001B001%27&outFields=*&f=pjson) |
| 927 | 40 SW NORTH RIVER DR | Mixed-Use | Hotel | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD15008325001B001%27&outFields=*&f=pjson) |
| 930 | 2641 NW 24 CT | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD16014421001B001%27&outFields=*&f=pjson) |
| 931 | 1853 SW 7 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD16013152001B001%27&outFields=*&f=pjson) |
| 932 | 32 NW 14 AV | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD16016300001B001%27&outFields=*&f=pjson) |
| 934 | 1635 NW 2 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD17001238001B001%27&outFields=*&f=pjson) |
| 935 | 1435 NW 1 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD16017609001B001%27&outFields=*&f=pjson) |
| 937 | 244 NW 16 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD17003951001B001%27&outFields=*&f=pjson) |
| 938 | 826 SW 12 CT | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD17008031001B001%27&outFields=*&f=pjson) |
| 940 | 7924 NE 2 AV | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD17010068001B001%27&outFields=*&f=pjson) |
| 941 | 321 NW 4 AV | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD17010794001B001%27&outFields=*&f=pjson) |
| 942 | Flow House | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD17011264001B001%27&outFields=*&f=pjson) |
| 948 | 1780 SW 5 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD17017808001B001%27&outFields=*&f=pjson) |
| 949 | 444 SW 7 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD17017066001B001%27&outFields=*&f=pjson) |
| 952 | 526 SW 2 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD18000699001B001%27&outFields=*&f=pjson) |
| 954 | 30 NW 59 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD18003155001B001%27&outFields=*&f=pjson) |
| 955 | 1542 NW 2 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD18003525001B001%27&outFields=*&f=pjson) |
| 956 | 1552 NW 2 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD18003527001B001%27&outFields=*&f=pjson) |
| 957 | 147 NW 59 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD18005022001B001%27&outFields=*&f=pjson) |
| 959 | 136 NW 26 ST | Mixed-Use | Hotel | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD18013750001B001%27&outFields=*&f=pjson) |
| 960 | 1555 SW 6 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD18015131001B001%27&outFields=*&f=pjson) |
| 961 | 436 NE 35 TER | Hotel | Mixed-Use | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD18016505001B001%27&outFields=*&f=pjson) |
| 963 | 830 Brickell | Mixed-Use | Office | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD18018113001B001%27&outFields=*&f=pjson) |
| 964 | 2320 NW 26 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD18017726001B001%27&outFields=*&f=pjson) |
| 965 | 501 NE 61 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD18018263001B001%27&outFields=*&f=pjson) |
| 967 | 1329 NW 3 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD19001894001B001%27&outFields=*&f=pjson) |
| 969 | 1241 NW 7 CT | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD19010442001B001%27&outFields=*&f=pjson) |
| 970 | 616 SW 7 AV | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD19011343001B001%27&outFields=*&f=pjson) |
| 973 | 96 NE 65 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD19014732001B001%27&outFields=*&f=pjson) |
| 974 | 2600 Biscayne | Mixed-Use | Condo | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD19015454001B001%27&outFields=*&f=pjson) |
| 977 | 126 NW 10 AV | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD19019225001B001%27&outFields=*&f=pjson) |
| 978 | Mr. C Residences Coconut Grove | Mixed-Use | Condo | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD19021170001B001%27&outFields=*&f=pjson) |
| 982 | 255 NW 25 ST | Mixed-Use | Hotel | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD20000541001B001%27&outFields=*&f=pjson) |
| 983 | Uni Tower | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD20001480001B001%27&outFields=*&f=pjson) |
| 985 | 200 E FLAGLER ST | Mixed-Use | Office | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD20006286001B001%27&outFields=*&f=pjson) |
| 989 | 1576 SW 2 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD20007121001B001%27&outFields=*&f=pjson) |
| 990 | 636 SW 14 AV | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD20007495001B001%27&outFields=*&f=pjson) |
| 991 | 134 NE 78 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD20008873001B001%27&outFields=*&f=pjson) |
| 992 | 721 SW 11 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD20012541001B001%27&outFields=*&f=pjson) |
| 994 | 1311 NW 5 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD20012160001B001%27&outFields=*&f=pjson) |
| 996 | 5423 NE MIAMI CT | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD20015180001B001%27&outFields=*&f=pjson) |
| 997 | 1336 SW 7 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD20017558001B001%27&outFields=*&f=pjson) |
| 998 | 1158 NW 6 ST | Mixed-Use | Condo | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD20019487001B001%27&outFields=*&f=pjson) |
| 1000 | Havana Enclave | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD20022201001B001%27&outFields=*&f=pjson) |
| 1001 | 316 NW 13 AV | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD20022822001B001%27&outFields=*&f=pjson) |
| 1006 | 1567 NW 1 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD20025271001B001%27&outFields=*&f=pjson) |
| 1008 | 1250 NW 36 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD20026059001B001%27&outFields=*&f=pjson) |
| 1009 | Miami World Tower 1 | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD20025627001B001%27&outFields=*&f=pjson) |
| 1010 | Fourteen Allapattah Residences | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD20025864001B001%27&outFields=*&f=pjson) |
| 1011 | 1455 NW 35 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD20025852001B001%27&outFields=*&f=pjson) |
| 1014 | 650 SW 1 ST | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD20026566001B001%27&outFields=*&f=pjson) |
| 1015 | 32 NW 77 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD21001538001B001%27&outFields=*&f=pjson) |
| 1017 | 3728 NW 19 AV | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD21010115001B001%27&outFields=*&f=pjson) |
| 1019 | 1254 SW 2 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD21011921001B001%27&outFields=*&f=pjson) |
| 1021 | 1120 NW 29 ST | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD21011526001B001%27&outFields=*&f=pjson) |
| 1022 | The River District — Phase 1 | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD21011106001B001%27&outFields=*&f=pjson) |
| 1024 | Forma Miami | Mixed-Use | Condo | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD21013681001B001%27&outFields=*&f=pjson) |
| 1025 | Society Wynwood | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD21013237001B001%27&outFields=*&f=pjson) |
| 1026 | 8106 NE 1 AV | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD21015455001B001%27&outFields=*&f=pjson) |
| 1027 | 3702 NW 23 AV | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD21015486001B001%27&outFields=*&f=pjson) |
| 1028 | Modera Riverside | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD21017039001B001%27&outFields=*&f=pjson) |
| 1030 | Wynwood Works | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD21019028001B001%27&outFields=*&f=pjson) |
| 1031 | 845 SW 8 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD21019029001B001%27&outFields=*&f=pjson) |
| 1032 | 1620 NW 18 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD21018721001B001%27&outFields=*&f=pjson) |
| 1035 | The Wynwood Plaza — office tower | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD21020944001B001%27&outFields=*&f=pjson) |
| 1036 | 530 SW 11 AV | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD21021076001B001%27&outFields=*&f=pjson) |
| 1038 | 1559 NW 15 STRD | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD21022333001B001%27&outFields=*&f=pjson) |
| 1039 | WNWD21 | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD21022699001B001%27&outFields=*&f=pjson) |
| 1040 | Wynwood Diamedix — east building | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD21022867001B001%27&outFields=*&f=pjson) |
| 1041 | 610 NW 10 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD21023937001B001%27&outFields=*&f=pjson) |
| 1043 | 1601 SW 3 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD21025333001B001%27&outFields=*&f=pjson) |
| 1044 | 760 NW 21 ST | Mixed-Use | Industrial | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD21025539001B001%27&outFields=*&f=pjson) |
| 1045 | 91 NW 27 ST | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD21025466001B001%27&outFields=*&f=pjson) |
| 1046 | District 225 | Mixed-Use | Condo | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD21026999001B001%27&outFields=*&f=pjson) |
| 1048 | E11EVEN Club Hotel & Residences Miami | Mixed-Use | Condo | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD21028574001B001%27&outFields=*&f=pjson) |
| 1049 | 151 NE 82 TER | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD21028818001B001%27&outFields=*&f=pjson) |
| 1051 | 1441 NW 62 ST | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD24027528001B001%27&outFields=*&f=pjson) |
| 1052 | Aria Reserve Miami — South Tower | Mixed-Use | Condo | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD21029545001B001%27&outFields=*&f=pjson) |
| 1053 | 2320 NW 8 AV | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD21029666001B001%27&outFields=*&f=pjson) |
| 1054 | 501 First Residences | Mixed-Use | Condo | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD21030000001B001%27&outFields=*&f=pjson) |
| 1056 | 3335 THOMAS AV | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22000893001B001%27&outFields=*&f=pjson) |
| 1057 | Amara Wynwood | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22002454001B001%27&outFields=*&f=pjson) |
| 1058 | 1037 NW 2 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22003173001B001%27&outFields=*&f=pjson) |
| 1060 | Wynhouse Miami | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22004896001B001%27&outFields=*&f=pjson) |
| 1061 | 84 SW 28 RD | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22005460001B001%27&outFields=*&f=pjson) |
| 1063 | Baccarat Residences Miami | Mixed-Use | Condo | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22006164001B001%27&outFields=*&f=pjson) |
| 1064 | 3513 NW 5 AV | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22006454001B001%27&outFields=*&f=pjson) |
| 1065 | 3505 NW 5 AV | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22006557001B001%27&outFields=*&f=pjson) |
| 1066 | 1715 SW 37 AV | Mixed-Use | Condo | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22006533001B001%27&outFields=*&f=pjson) |
| 1068 | 1163 NW 8 AV | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22007817001B001%27&outFields=*&f=pjson) |
| 1069 | 1547 NW 2 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22007931001B001%27&outFields=*&f=pjson) |
| 1071 | 3069 PLAZA ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22008155001B001%27&outFields=*&f=pjson) |
| 1072 | 1376 SW 4 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22008412001B001%27&outFields=*&f=pjson) |
| 1075 | 169 NE 53 ST | Hotel | Condo | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22009300001B001%27&outFields=*&f=pjson) |
| 1076 | 1526 NW 2 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22009892001B001%27&outFields=*&f=pjson) |
| 1077 | 1165 NW 8 AV | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22010223001B001%27&outFields=*&f=pjson) |
| 1078 | 244 NW 34 TER | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22009899001B001%27&outFields=*&f=pjson) |
| 1080 | 1601 NW 15 STRD | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22010354001B001%27&outFields=*&f=pjson) |
| 1081 | Cipriani Residences Miami | Mixed-Use | Condo | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22010804001B001%27&outFields=*&f=pjson) |
| 1082 | Rhapsody at Rainbow Village | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22011051001B001%27&outFields=*&f=pjson) |
| 1083 | River Rapids | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22011628001B001%27&outFields=*&f=pjson) |
| 1084 | 7522 NE 3 PL | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22011133001B001%27&outFields=*&f=pjson) |
| 1085 | 351 NE 77 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22011135001B001%27&outFields=*&f=pjson) |
| 1086 | 2815 SW 27 TER | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22012897001B001%27&outFields=*&f=pjson) |
| 1087 | 2915 NW 36 ST | Mixed-Use | Industrial | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22013217001B001%27&outFields=*&f=pjson) |
| 1088 | 222 Namdar Towers — Phase I | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22014447001B001%27&outFields=*&f=pjson) |
| 1090 | 2267 NW 36 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22016256001B001%27&outFields=*&f=pjson) |
| 1094 | 2241 NW 7 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22018087001B001%27&outFields=*&f=pjson) |
| 1096 | The Crosby Miami Worldcenter | Mixed-Use | Condo | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22018410001B001%27&outFields=*&f=pjson) |
| 1099 | NoMad Residences Wynwood | Mixed-Use | Condo | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22021700001B001%27&outFields=*&f=pjson) |
| 1101 | 1190 NW 8 AV | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22022324001B001%27&outFields=*&f=pjson) |
| 1102 | Casa Bella Residences by B&B Italia | Mixed-Use | Condo | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22023542001B001%27&outFields=*&f=pjson) |
| 1106 | 3422 NW 5 AV | Mixed-Use | Condo | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22024640001B001%27&outFields=*&f=pjson) |
| 1107 | LOFTY Brickell (One Brickell Riverfront) | Mixed-Use | Condo | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22025768001B001%27&outFields=*&f=pjson) |
| 1109 | 758 NW 2 ST | Mixed-Use | Condo | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22026231001B001%27&outFields=*&f=pjson) |
| 1111 | 843 NW 12 ST | Mixed-Use | Condo | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22029203001B001%27&outFields=*&f=pjson) |
| 1113 | E11EVEN Club Residences Beyond | Mixed-Use | Condo | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22028869001B001%27&outFields=*&f=pjson) |
| 1115 | Mohawk at Wynwood | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22029540001B001%27&outFields=*&f=pjson) |
| 1116 | 420 NE 72 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22029810001B001%27&outFields=*&f=pjson) |
| 1117 | 90 NE 32 ST | Mixed-Use | Condo | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22029468001B001%27&outFields=*&f=pjson) |
| 1118 | 153 NW 25 ST | Mixed-Use | Office | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22030684001B001%27&outFields=*&f=pjson) |
| 1119 | 2000 Wynwood | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22030590001B001%27&outFields=*&f=pjson) |
| 1120 | FIVESIXTY Waterfront Residences + Club | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD22030713001B001%27&outFields=*&f=pjson) |
| 1121 | 2521 NW 35 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23000854001B001%27&outFields=*&f=pjson) |
| 1124 | 1698 NW 3 AV | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23002556001B001%27&outFields=*&f=pjson) |
| 1125 | 1900 SW 25 TER | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23001621001B001%27&outFields=*&f=pjson) |
| 1126 | Excel Miami | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23003086001B001%27&outFields=*&f=pjson) |
| 1128 | 1500 NW NORTH RIVER DR | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23003373001B001%27&outFields=*&f=pjson) |
| 1129 | 3631 NW 24 AV | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23002744001B001%27&outFields=*&f=pjson) |
| 1132 | 3332 NW 5 AV | Mixed-Use | Condo | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23004323001B001%27&outFields=*&f=pjson) |
| 1133 | The Residences at 1428 Brickell | Mixed-Use | Condo | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23005637001B001%27&outFields=*&f=pjson) |
| 1134 | 304 NW 32 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23007807001B001%27&outFields=*&f=pjson) |
| 1137 | 408 NW 32 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23008839001B001%27&outFields=*&f=pjson) |
| 1138 | Villa Miami | Mixed-Use | Condo | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23009076001B001%27&outFields=*&f=pjson) |
| 1139 | 234 NW 34 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23010055001B001%27&outFields=*&f=pjson) |
| 1142 | 8251 NE 1 PL | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23010452001B001%27&outFields=*&f=pjson) |
| 1144 | Downtown 6 | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23010187001B001%27&outFields=*&f=pjson) |
| 1145 | 97 NE 53 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23011035001B001%27&outFields=*&f=pjson) |
| 1147 | 319 NW 35 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23012087001B001%27&outFields=*&f=pjson) |
| 1149 | 1130 SW 22 ST | Mixed-Use | Hotel | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23012479001B001%27&outFields=*&f=pjson) |
| 1151 | 1700 NW 33 ST | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23013852001B001%27&outFields=*&f=pjson) |
| 1152 | 4301 NW 7 ST | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23013350001B001%27&outFields=*&f=pjson) |
| 1153 | 600 Miami Worldcenter | Mixed-Use | Condo | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23015044001B001%27&outFields=*&f=pjson) |
| 1154 | Domus Brickell Park | Mixed-Use | Hotel | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23016208001B001%27&outFields=*&f=pjson) |
| 1156 | Midtown 7 | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23016341001B001%27&outFields=*&f=pjson) |
| 1157 | ENSO | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23016524001B001%27&outFields=*&f=pjson) |
| 1158 | 154 SW 17 AV | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23017192001B001%27&outFields=*&f=pjson) |
| 1159 | 355 NW 31 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23017446001B001%27&outFields=*&f=pjson) |
| 1160 | 455 SW 16 AV | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23017963001B001%27&outFields=*&f=pjson) |
| 1164 | 33 NW 34 TER | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23020481001B001%27&outFields=*&f=pjson) |
| 1165 | 78 NW 32 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23020494001B001%27&outFields=*&f=pjson) |
| 1167 | The Perrin | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23020012001B001%27&outFields=*&f=pjson) |
| 1168 | The Rider Residences | Mixed-Use | Condo | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23020919001B001%27&outFields=*&f=pjson) |
| 1169 | 3407 ELIZABETH ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23021508001B001%27&outFields=*&f=pjson) |
| 1171 | 3600 NE 1 AV | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23023663001B001%27&outFields=*&f=pjson) |
| 1173 | 3021 NW 21 CT | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23024351001B001%27&outFields=*&f=pjson) |
| 1175 | River District 14 | Hotel | Condo | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23026274001B001%27&outFields=*&f=pjson) |
| 1176 | 7101 BISCAYNE BLVD | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23027473001B001%27&outFields=*&f=pjson) |
| 1177 | 222 Namdar Towers — Phase II | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23028187001B001%27&outFields=*&f=pjson) |
| 1178 | 7950 NE 3 AV | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23028583001B001%27&outFields=*&f=pjson) |
| 1181 | 3495 GRAND AV | Mixed-Use | Office | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23029093001B001%27&outFields=*&f=pjson) |
| 1185 | Miami Freedom Park — stadium | Retail | Other | — |
| 1187 | 5215 W FLAGLER ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23029763001B001%27&outFields=*&f=pjson) |
| 1188 | Residences in the Grove | Mixed-Use | Condo | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23029961001B001%27&outFields=*&f=pjson) |
| 1190 | 84 NW 34 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD23030081001B001%27&outFields=*&f=pjson) |
| 1192 | 1480 NW 8 AV | Mixed-Use | Office | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD24002248001B001%27&outFields=*&f=pjson) |
| 1193 | 38 West Eleventh Residences Miami | Mixed-Use | Condo | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD24003027001B001%27&outFields=*&f=pjson) |
| 1194 | 1900 SW 3 AV | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD24008946001B001%27&outFields=*&f=pjson) |
| 1195 | 2900 Terrace | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD24009935001B001%27&outFields=*&f=pjson) |
| 1196 | 259 NW 33 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD24012391001B001%27&outFields=*&f=pjson) |
| 1197 | 315 NW 32 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD24015181001B001%27&outFields=*&f=pjson) |
| 1198 | 2150 NW MIAMI CT | Mixed-Use | Office | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD24015343001B001%27&outFields=*&f=pjson) |
| 1202 | One West Twelve Residences | Mixed-Use | Condo | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD24018702001B001%27&outFields=*&f=pjson) |
| 1206 | 711 N MIAMI AV | Mixed-Use | Office | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD24024688001B001%27&outFields=*&f=pjson) |
| 1207 | 7140 NE Miami Court self-storage | Mixed-Use | Industrial | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD24025336001B001%27&outFields=*&f=pjson) |
| 1208 | 1401 Brickell by Santander | Mixed-Use | Office | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD24024742001B001%27&outFields=*&f=pjson) |
| 1209 | 112 NW 33 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD24027589001B001%27&outFields=*&f=pjson) |
| 1210 | 71 NW 33 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD24027590001B001%27&outFields=*&f=pjson) |
| 1211 | Metro 2 at Edgewater | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD24027768001B001%27&outFields=*&f=pjson) |
| 1212 | Dulce Vida Apartments | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD25000255001B001%27&outFields=*&f=pjson) |
| 1214 | 251 NW 33 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD25001185001B001%27&outFields=*&f=pjson) |
| 1218 | 1201 NW 63 ST | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD25005139001B001%27&outFields=*&f=pjson) |
| 1219 | Sense 22 | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD25007343001B001%27&outFields=*&f=pjson) |
| 1221 | 1310 NW 1 PL | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD25008427001B001%27&outFields=*&f=pjson) |
| 1223 | UniDos | Mixed-Use | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD25010853001B001%27&outFields=*&f=pjson) |
| 1224 | 1750 NW 15 STRD | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD25011839001B001%27&outFields=*&f=pjson) |
| 1225 | 320 NW 32 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD25012513001B001%27&outFields=*&f=pjson) |
| 1228 | 3121 MUNDY ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD25021447001B001%27&outFields=*&f=pjson) |
| 1231 | 400 NW 32 ST | Residential | Multifamily Rental | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD25026376001B001%27&outFields=*&f=pjson) |
| 1232 | Sweetbird North | Mixed-Use | Office | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD25028023001B001%27&outFields=*&f=pjson) |
| 1234 | The Aurelian | Residential | Multifamily Rental | [link](https://floridayimby.com/2026/10/empira-group-secures-81-million-construction-loan-for-the-aurelian-in-coral-gables.html) |
| 1235 | Ponce Park | Mixed-Use | Condo | [link](https://floridayimby.com/2025/12/construction-starts-on-ponce-park-at-3000-ponce-de-leon-blvd-coral-gables-florida.html) |
| 1244 | 2501 Galiano Street | Mixed-Use | Condo | [link](https://floridayimby.com/2024/10/13-story-traditional-condo-could-replace-closed-truist-bank-at-2501-galiano-st-coral-gables-fl.html) |
| 1245 | Alexan Crafts | Mixed-Use | Multifamily Rental | [link](https://floridayimby.com/2021/06/2255.html) |
| 1248 | Ponce 8 | Mixed-Use | Multifamily Rental | [link](https://floridayimby.com/2025/08/shoma-group-proposes-ponce-8-for-3850-sw-8th-st-coral-gables-fl.html) |
| 1288 | Sevilla at Downtown Doral | Residential | Multifamily Rental | [link](https://floridayimby.com/2026/03/codina-partners-tops-out-7-story-405-unit-sevilla-at-downtown-doral.html) |
| 1290 | Downtown Doral Parking Garages & Retail Expansion | Mixed-Use | Parking | [link](https://experience.arcgis.com/experience/f53641e40f6b40d88fa7721bf5ca38cb/) |
| 1301 | Doral Inn/ IMF Doral Residential Conversion Live L | Residential | Multifamily Rental | [link](https://experience.arcgis.com/experience/f53641e40f6b40d88fa7721bf5ca38cb/) |
| 1306 | Prime Square Doral | Mixed-Use | Retail | [link](https://floridayimby.com/2026/09/pre-construction-begins-on-prime-square-doral-at-7200-nw-102nd-avenue-in-doral.html) |
| 1311 | 824 Alton Road | Residential | Multifamily Rental | [link](https://floridayimby.com/2025/10/31-story-tower-with-workforce-housing-proposed-for-824-alton-rd-miami-beach-fl.html) |
| 1312 | Reserve at One Sunset | Residential | Multifamily Rental | [link](https://floridayimby.com/2026/09/new-york-based-developer-plans-20-story-reserve-at-one-sunset-for.html) |
| 1313 | 1575 Alton Road | Mixed-Use | Multifamily Rental | [link](https://floridayimby.com/2026/09/42111.html) |
| 1315 | 7410 Collins Avenue | Mixed-Use | Multifamily Rental | [link](https://floridayimby.com/2026/09/11-story-mixed-use-development-approved-at-7410-collins-avenue-in-miami-beach.html) |
| 1320 | 1826 Collins Avenue | Mixed-Use | Multifamily Rental | [link](https://floridayimby.com/2026/05/planning-board-approves-mechanical-parking-system-for-1826-collins-amid-neighbor-opposition-in-miami-beach.html) |
| 1326 | AVARA Residences | Residential | Multifamily Rental | [link](https://floridayimby.com/2025/12/avara-residences-debut-at-3900-alton-road-with-178-rental-homes-in-miami-beach.html) |
| 1327 | 1101 71st Street | Residential | Multifamily Rental | [link](https://floridayimby.com/2025/11/developer-proposes-affordable-housing-development-for-1101-71st-st-miami-beach-fl-33141.html) |
| 1333 | 1940 Park Avenue workforce housing | Residential | Multifamily Rental | [link](https://floridayimby.com/2026/06/shuttered-hotel-could-be-replaced-with-workforce-housing-at-1940-park-ave-miami-beach-florida.html) |
| 1334 | Southgate Towers Live Local redevelopment | Residential | Multifamily Rental | [link](https://therealdeal.com/miami/2026/10/30/miami-beach-axes-38-story-live-local-act-project-over-zoning/) |
| 1335 | 1960 Normandy Drive | Residential | Multifamily Rental | [link](https://www.miamibeachfl.gov/city-hall/planning/approved-live-local-act-applications/) |
| 1336 | 663 Alton Road | Residential | Multifamily Rental | [link](https://www.miamibeachfl.gov/city-hall/planning/approved-live-local-act-applications/) |
| 1337 | 7401-7425 Harding Avenue | Residential | Multifamily Rental | [link](https://www.miamibeachfl.gov/city-hall/planning/approved-live-local-act-applications/) |
| 1338 | 810 Alton Road | Residential | Multifamily Rental | [link](https://www.miamibeachfl.gov/city-hall/planning/live-local-act-applications-under-review/) |
| 1339 | 42 Collins Avenue | Residential | Multifamily Rental | [link](https://www.miamibeachfl.gov/city-hall/planning/live-local-act-applications-under-review/) |
| 1340 | 709 Alton Road | Residential | Multifamily Rental | [link](https://www.miamibeachfl.gov/city-hall/planning/live-local-act-applications-under-review/) |
| 1341 | 1030 6th Street | Residential | Multifamily Rental | [link](https://www.miamibeachfl.gov/city-hall/planning/live-local-act-applications-under-review/) |
| 1342 | 425 Ocean Drive | Residential | Mixed-Use | [link](https://www.miamibeachfl.gov/city-hall/planning/live-local-act-applications-under-review/) |
| 1350 | 120 South Dixie Highway | Mixed-Use | Multifamily Rental | [link](https://therealdeal.com/miami/2026/05/28/jeff-greene-plans-live-local-act-project-in-west-palm-beach/) |
| 1351 | 2085 North Flagler Drive | Residential | Condo | [link](https://floridayimby.com/2026/06/developers-plan-two-31-story-towers-for-2085-n-flagler-dr-west-palm-beach-fl.html) |
| 1358 | The Sound Apartments | Mixed-Use | Multifamily Rental | [link](https://floridayimby.com/2026/03/construction-nears-completion-on-the-sound-apartments-at-8111-south-dixie-highway-in-west-palm-beach.html) |
| 1361 | 534 Datura | Residential | Multifamily Rental | [link](https://floridayimby.com/2026/09/two-developers-plan-25-story-534-datura-for-534-datura-st-west-palm-beach-fl.html) |
| 1362 | 2001 North Flagler Drive | Residential | Condo | [link](https://floridayimby.com/2026/01/developers-propose-luxury-27-story-for-2001-n-flagler-dr-west-palm-beach-fl.html) |
| 1364 | 209 North Sapodilla Avenue | Residential | Multifamily Rental | [link](https://floridayimby.com/2026/07/related-ross-revises-plans-for-affordable-housing-venture-at-209-n-sapodilla-ave-west-palm-beach-fl.html) |
| 1365 | 501 Monceaux Road | Mixed-Use | Multifamily Rental | [link](https://floridayimby.com/2026/05/pebb-capital-proposes-eight-story-mixed-use-venture-for-501-monceaux-road-west-palm-beach-florida.html) |
| 1366 | 611 27th Street | Residential | Multifamily Rental | [link](https://floridayimby.com/2026/04/seven-story-affordable-housing-project-proposed-for-611-27th-street-west-palm-beach-florida.html) |
| 1368 | Soleste Palm Station Two | Residential | Multifamily Rental | [link](https://floridayimby.com/2026/05/15-story-soleste-palm-station-two-proposed-for-411-w-railroad-ave-west-palm-beach-fl.html) |
| 1369 | Deco Northwood | Residential | Multifamily Rental | [link](https://floridayimby.com/2025/10/deco-northwood-proposed-for-2900-broadway-west-palm-beach-florida.html) |

Reasons:

- **155 rows** — residential only
- **5 rows** — single major programme: hotel
- **id 961** — residential plus hotel, a real second programme
- **8 rows** — single major programme: office
- **16 rows** — residential with an office token only; the permit feed cannot tell a leasing office or bank branch from an office programme, so this stays residential
- **id 1044, id 1087, id 1207** — single major programme: industrial
- **id 1185** — a stadium is not retail: the permit's work items are STADIUM plus RECREATIONAL ESTABLISHMENT plus a retail token for the concourse retail, and the retail token is what had classified it
- **id 1208** — pinned: 1401 Brickell by Santander has no residential component at all. Its master permit names office, retail, restaurant and recreation, so the automatic rule already yields Office; pinned because the review called it out and the value should not drift.
- **id 1234** — described as a Class A multifamily rental building of 160 residences with no ground-floor retail, so there is not even a minor second use
- **id 1235** — a mixed-use condominium of 58 residences over 22,000 sq ft of ground-floor retail; retail under a tower is not a second programme
- **id 1244** — 93 units of which 85 are condominiums and eight are lodging units; eight keys out of 93 units is not a second programme, so the dominant use is for-sale residential
- **id 1245** — 263 rental apartments and 22 live-work units over retail
- **id 1248** — 201 apartments, 40 per cent of them affordable to qualify for Live Local Act incentives, plus commercial space
- **id 1288** — described as a seven-storey rental building with a central garage, delivered in two phases, within the Downtown Doral community
- **id 1290** — no residential component at all: two parking garage expansions adding 487 spaces, with 10,967 sq ft of retail. The garages are the project -- they are its name -- so parking is the dominant use and the retail the minor one, which is the reverse of the order that holds for a Miami tower
- **id 1301** — a conversion of an existing hotel to multifamily residential for people aged 55 and up; age-restricted multifamily of this kind is rental, and the City's own parking calculation treats it as age-restricted multi-family housing under its rental standard
- **id 1306** — no residential component: a 58,000 sq ft commercial condominium of which approximately 40,000 sq ft is retail and restaurant and 18,000 sq ft office, so retail is the dominant use
- **8 rows** — the project is a Live Local Act application, and the Act (ss. 125.01055(7) and 166.04151(7), Fla. Stat.) grants its relief only to developments reserving at least 40 per cent of residential units as RENTAL units affordable at or below 120 per cent of AMI, so what it applied under establishes the tenure
- **id 1313** — the project is a Live Local Act application, and the Act (ss. 125.01055(7) and 166.04151(7), Fla. Stat.) grants its relief only to developments reserving at least 40 per cent of residential units as RENTAL units affordable at or below 120 per cent of AMI, so what it applied under establishes the tenure. Its commercial space is retail and not a second programme
- **id 1315** — 50 rental residences over 8,535 sq ft of ground-floor retail, and the project is a Live Local Act application, and the Act (ss. 125.01055(7) and 166.04151(7), Fla. Stat.) grants its relief only to developments reserving at least 40 per cent of residential units as RENTAL units affordable at or below 120 per cent of AMI, so what it applied under establishes the tenure
- **id 1320** — 29 residential units including 12 workforce housing apartments, and the project is a Live Local Act application, and the Act (ss. 125.01055(7) and 166.04151(7), Fla. Stat.) grants its relief only to developments reserving at least 40 per cent of residential units as RENTAL units affordable at or below 120 per cent of AMI, so what it applied under establishes the tenure
- **id 1326** — described as 178 rental homes
- **id 1327** — an affordable housing development, and the project is a Live Local Act application, and the Act (ss. 125.01055(7) and 166.04151(7), Fla. Stat.) grants its relief only to developments reserving at least 40 per cent of residential units as RENTAL units affordable at or below 120 per cent of AMI, so what it applied under establishes the tenure
- **id 1333** — a workforce housing development, and the project is a Live Local Act application, and the Act (ss. 125.01055(7) and 166.04151(7), Fla. Stat.) grants its relief only to developments reserving at least 40 per cent of residential units as RENTAL units affordable at or below 120 per cent of AMI, so what it applied under establishes the tenure
- **id 1334** — the project is a Live Local Act application, and the Act (ss. 125.01055(7) and 166.04151(7), Fla. Stat.) grants its relief only to developments reserving at least 40 per cent of residential units as RENTAL units affordable at or below 120 per cent of AMI, so what it applied under establishes the tenure. It replaces two 14-storey, 495-unit apartment buildings
- **id 1341** — the City's plan record describes 72 elderly workforce housing units with ground-floor retail, and the project is a Live Local Act application, and the Act (ss. 125.01055(7) and 166.04151(7), Fla. Stat.) grants its relief only to developments reserving at least 40 per cent of residential units as RENTAL units affordable at or below 120 per cent of AMI, so what it applied under establishes the tenure
- **id 1342** — 90 condominiums AND 60 workforce rental apartments, which is both tenures in one project -- the brief's own test for a real second programme
- **id 1350** — 399 mixed-income apartments with roughly 7,550 sq ft of retail; about 160 of the apartments are for households at or below 120 per cent of AMI
- **id 1351** — the City's coverage states the plans call for 281 condos
- **id 1358** — 358 residential units including 90 workforce housing apartments, with 4,000 sq ft of divisible retail
- **id 1361** — a workforce housing tower of 261 residences, 40 per cent of them designated workforce housing
- **id 1362** — the coverage describes 90 condos with an average unit of 3,382 sq ft, and eight lower-level townhouses
- **id 1364** — a two-building affordable housing development with units reserved for tenants earning 30, 50, 60 or 80 per cent of AMI, which are rental tiers
- **id 1365** — 350 apartments with retail
- **id 1366** — a seven-storey affordable housing project of 151 apartments
- **id 1368** — described as 310 apartments
- **id 1369** — 167 units, 40 per cent of them workforce housing, proposed under the Live Local Act. the project is a Live Local Act application, and the Act (ss. 125.01055(7) and 166.04151(7), Fla. Stat.) grants its relief only to developments reserving at least 40 per cent of residential units as RENTAL units affordable at or below 120 per cent of AMI, so what it applied under establishes the tenure

### lender — 32 change(s)

| id | Project | Old | New | Source |
| --: | --- | --- | --- | --- |
| 947 | The Arbor Residences | — | Trez Capital | [link](https://www.bisnow.com/news/south-florida/multifamily/coconut-grove-condo-project-trades-to-stave-off-foreclosure-121486) |
| 966 | Una Residences | — | Bank OZK | [link](https://commercialobserver.com/2021/04/oko-group-cain-international-secure-128m-loan-for-miami-luxury-condo-tower/) |
| 1009 | Miami World Tower 1 | — | Bank of New York Mellon | [link](https://commercialobserver.com/2022/06/lalezarian-miami-worldcenter-tower-bank-of-new-york-mellon/) |
| 1010 | Fourteen Allapattah Residences | — | Churchill Real Estate | [link](https://commercialobserver.com/2022/08/neology-scores-58m-loan-for-allapattah-housing-development/) |
| 1022 | The River District — Phase 1 | — | Madison Realty Capital | [link](https://commercialobserver.com/2024/05/madison-realty-capital-chetrit-group-galaxy/) |
| 1024 | Forma Miami | — | Blackstone Real Estate | [link](https://floridayimby.com/2022/11/construction-rises-on-first-phase-of-nema-miami-at-2900-biscayne-boulevard-in-edgewater-miami.html) |
| 1025 | Society Wynwood | — | Pacific Western Bank; Square Mile Capital Management | [link](https://commercialobserver.com/2021/08/pmg-greybrook-142m-society-wynwood/) |
| 1039 | WNWD21 | — | Bank OZK | [link](https://www.bisnow.com/news/south-florida/deal-sheet/this-weeks-south-florida-deal-sheet-116924) |
| 1040 | Wynwood Diamedix — east building | — | Bank OZK | [link](https://www.bisnow.com/news/south-florida/deal-sheet/this-weeks-south-florida-deal-sheet-116924) |
| 1046 | District 225 | — | Madison Realty Capital | [link](https://floridayimby.com/2022/07/developers-secure-76-million-in-construction-funding-for-37-story-district-225-in-downtown-miami.html) |
| 1054 | 501 First Residences | — | Bank OZK | [link](https://floridayimby.com/2023/01/developers-get-building-permits-for-440-foot-tall-501-first-residences-in-downtown-miami.html) |
| 1057 | Amara Wynwood | — | CIBC Bank; Republic Bank; First Bank Chicago | [link](https://commercialobserver.com/2023/03/fifield-pgim-wynwood-station/) |
| 1060 | Wynhouse Miami | — | JPMorgan; Canyon Partners | [link](https://commercialobserver.com/2023/07/fisher-brothers-wynwood-jpm-canyon-wd/) |
| 1082 | Rhapsody at Rainbow Village | — | KeyBank Real Estate Capital; J.P. Morgan Chase | [link](https://commercialobserver.com/2025/02/miami-housing-trust-htg-jp-morgan-keybank-construction/) |
| 1083 | River Rapids | — | Kayne Anderson | [link](https://commercialobserver.com/2024/05/kayne-anderson-rovr-development-river-rapids/) |
| 1114 | 2200 Brickell | — | Bank OZK | [link](https://floridayimby.com/2023/10/developers-secure-77m-construction-loan-from-bank-ozk-for-2200-brickell.html) |
| 1126 | Excel Miami | — | Centennial Bank; Southern Realty Trust | [link](https://floridayimby.com/2024/11/94-5m-construction-loan-secured-for-24-story-multifamily-tower-excel-miami-in-miamis-arts-entertainment-district.html) |
| 1138 | Villa Miami | — | Tyko Capital | [link](https://floridayimby.com/2024/12/villa-miami-secures-285-million-in-financing-following-groundbreaking-in-edgewater.html) |
| 1154 | Domus Brickell Park | — | Forman Capital | [link](https://floridayimby.com/2024/04/forman-capital-funds-54-56-million-construction-loan-for-condominium-hotel-development-in-miamis-financial-district.html) |
| 1167 | The Perrin | — | Goldman Sachs | [link](https://floridayimby.com/2025/05/111-million-construction-loan-secured-for-26-story-multifamily-tower-the-perrin-at-244-sw-9th-street-in-brickell-miami.html) |
| 1185 | Miami Freedom Park — stadium | — | J.P. Morgan Chase | [link](https://commercialobserver.com/2025/04/lionel-messi-inter-miami-freedom-park-j-p-morgan-chase/) |
| 1188 | Residences in the Grove | — | Tyko Capital | [link](https://commercialobserver.com/2026/01/tyko-terra-well-coconut-grove-miami/) |
| 1195 | 2900 Terrace | — | Bank OZK | [link](https://floridayimby.com/2025/06/210-5-million-construction-financing-secured-for-2900-terrace-in-edgewater-miami.html) |
| 1202 | One West Twelve Residences | — | Maxim Capital Group | [link](https://commercialobserver.com/2025/12/pmg-one-west-twelve-residences-overtown-miami/) |
| 1212 | Dulce Vida Apartments | — | Citibank | [link](https://floridayimby.com/2025/12/dulce-vida-advances-toward-groundbreaking-following-35-million-financing.html) |
| 1232 | Sweetbird North | — | Crédit Agricole | [link](https://commercialobserver.com/2026/08/miami-design-district-snohetta-sweetbird-north/) |
| 1264 | Doral City Place 7 | — | Santander Bank | [link](https://floridayimby.com/2023/09/construction-tops-out-on-doral-city-place-7-mixed-use-project-at-8450-nw-85th-court-in-doral.html) |
| 1289 | Bridge Point Doral 826 | — | Associated Bank | [link](https://commercialobserver.com/2026/02/bridge-office-industrial-doral/) |
| 1349 | Alba Palm Beach | — | Trez Capital | [link](https://commercialobserver.com/2022/04/alba-palm-beach-bgi-companies-trez-capital/) |
| 1352 | The Ritz-Carlton Residences, West Palm Beach | — | Madison Realty Capital | [link](https://commercialobserver.com/2026/03/related-bh-west-palm-beach-ritz-carlton-madison-realty-flagler/) |
| 1367 | Soleste Palm Station | — | Synovus Bank | [link](https://commercialobserver.com/2024/07/estates-cos-soleste-palm-station-synovus-bank/) |
| 1370 | Rybovich Marina redevelopment | — | Tyko Capital | [link](https://commercialobserver.com/2026/08/tyko-integra-west-palm/) |

### lender_status — 18 change(s)

| id | Project | Old | New | Source |
| --: | --- | --- | --- | --- |
| 928 | Missoni Baia | — | lender_unnamed | [link](https://therealdeal.com/miami/2019/11/01/vlad-doronin-partners-score-243m-loan-for-edgewater-tower/) |
| 976 | Okan Tower | — | no_loan_reported | [link](https://commercialobserver.com/2026/01/okan-tower-downtown-miami/) |
| 978 | Mr. C Residences Coconut Grove | — | lender_unnamed | [link](https://therealdeal.com/miami/2019/09/26/terra-closes-on-185m-construction-loan-for-latest-coconut-grove-project/) |
| 995 | Natiivo Miami | — | lender_unnamed | [link](https://therealdeal.com/miami/2024/08/20/galbuts-pay-off-213m-construction-loan-for-miami-tower/) |
| 1004 | View 29 | — | no_loan_reported | [link](https://www.bisnow.com/south-florida/news/affordable-housing/affordable-housing-project-wynwood-44m-public-construction-financing-133392) |
| 1028 | Modera Riverside | — | lender_unnamed | — |
| 1207 | 7140 NE Miami Court self-storage | — | lender_unnamed | — |
| 1208 | 1401 Brickell by Santander | — | no_loan_reported | [link](https://floridayimby.com/2026/04/50-story-1401-brickell-by-santander-breaks-ground-in-miami.html) |
| 1220 | Miami Civic Center | — | no_loan_reported | [link](https://www.miami.gov/My-Government/Departments/Department-of-Real-Estate-Asset-Management-DREAM/New-City-of-Miami-Administration-Building) |
| 1223 | UniDos | — | no_loan_reported | [link](https://www.bisnow.com/news/south-florida/affordable-housing/nr-investments-groundbreaking-190-units-133494) |
| 1242 | Gables Village (University of Miami Housing Phase 3) | — | no_loan_reported | [link](https://www.miamidade.gov/govaction/legistarfiles/Matters/Y2024/241790.pdf) |
| 1296 | The Oasis at Doral | — | lender_unnamed | [link](https://floridayimby.com/2026/03/developers-secure-construction-loan-for-the-oasis-at-doral-at-4090-n-w-97th-ave-doral-florida.html) |
| 1324 | 71 NoBe | — | lender_unnamed | [link](https://floridayimby.com/2026/01/mixed-use-71-nobe-planned-for-2027-completion-at-400-71st-st-miami-beach-florida.html) |
| 1355 | The Berkeley Palm Beach | — | no_loan_reported | [link](https://floridayimby.com/2025/09/the-berkeley-palm-beach-secures-62-5-million-pre-construction-loan-from-jvp-management-in-downtown-west-palm-beach.html) |
| 1356 | Alida Residences and Tribute Portfolio by Marriott Hotel | — | no_loan_reported | [link](https://floridayimby.com/2026/07/construction-begins-on-alida-residences-and-tribute-portfolio-by-marriott-hotel-in-downtown-west-palm-beach.html) |
| 1358 | The Sound Apartments | — | lender_unnamed | — |
| 1359 | Palm Beach Atlantic University residence hall | — | no_loan_reported | [link](https://commercialobserver.com/2025/06/palm-beach-atlantic-university-bond-dorm-student-housing-gilbane-florida/) |
| 1371 | Cox Science Center Hall of Science expansion | — | no_loan_reported | — |

Reasons:

- **8 rows** — a construction loan is documented but no acceptable source names the lender
- **10 rows** — a source documents how the project is financed, so the empty lender is a finding rather than a gap

### developer — 5 change(s)

| id | Project | Old | New | Source |
| --: | --- | --- | --- | --- |
| 1004 | View 29 | — | New Urban Development | [link](https://www.bisnow.com/south-florida/news/affordable-housing/affordable-housing-project-wynwood-44m-public-construction-financing-133392) |
| 1057 | Amara Wynwood | — | Fifield Companies; PGIM Real Estate | [link](https://commercialobserver.com/2023/03/fifield-pgim-wynwood-station/) |
| 1083 | River Rapids | — | ROVR Development | [link](https://commercialobserver.com/2024/05/kayne-anderson-rovr-development-river-rapids/) |
| 1212 | Dulce Vida Apartments | — | Coral Rock Development Group | [link](https://floridayimby.com/2025/12/dulce-vida-advances-toward-groundbreaking-following-35-million-financing.html) |
| 1220 | Miami Civic Center | Miami Freedom Park, LLC | Adler Development | [link](https://www.miami.gov/Notices/Events-Activities/City-of-Miami-to-Celebrate-the-Topping-Out-of-the-New-Miami-Civic-Center) |

Reasons:

- **id 1220** — the City's own notice names Adler Development; the previous value, Miami Freedom Park LLC, is the ground lessee of the OTHER parcels

### name — 4 change(s)

| id | Project | Old | New | Source |
| --: | --- | --- | --- | --- |
| 1107 | LOFTY Brickell (One Brickell Riverfront) | LOFTY Brickell (One Brickell Riverfront) | LOFTY Brickell | [link](https://floridayimby.com/2023/12/newgard-development-group-appoints-g-t-mcdonald-enterprises-inc-as-general-contractor-for-44-story-lofty-brickell.html) |
| 1188 | Residences in the Grove | Residences in the Grove | The WELL Coconut Grove | [link](https://commercialobserver.com/2026/01/tyko-terra-well-coconut-grove-miami/) |
| 1203 | Miami Freedom Park — park | Miami Freedom Park — park | Miami Freedom Park — stadium parking garage | [link](https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services/Building_Permits_Since_2014/FeatureServer/0/query?where=PermitNumber%3D%27BD24019509001B001%27&outFields=*&f=pjson) |
| 1220 | Miami Freedom Park — commercial component | Miami Freedom Park — commercial component | Miami Civic Center | [link](https://www.miami.gov/Notices/Events-Activities/City-of-Miami-to-Celebrate-the-Topping-Out-of-the-New-Miami-Civic-Center) |

Reasons:

- **id 1107** — the parenthetical '(One Brickell Riverfront)' merged two projects into one name
- **id 1188** — the 148-unit scheme this row was recorded with was scrapped; the same sponsors at the same address are building a redesigned 194-unit project under a new name
- **id 1203** — the permit's work items are a parking garage and its contractor is a precast parking-structure builder; the actual park is a different permit that no row tracks
- **id 1220** — the row's permit, address, floor area and contractor all describe the City's administration building, not Miami Freedom Park LLC's commercial space

### architect — 3 change(s)

| id | Project | Old | New | Source |
| --: | --- | --- | --- | --- |
| 1162 | The St. Regis Residences, Miami | Robert A.M. Stern Architects | Cohen Freedman Encinosa & Associates Architects | [link](https://floridayimby.com/2023/06/developers-seek-udrb-approval-for-48-story-st-regis-residences-miami.html) |
| 1185 | Miami Freedom Park — stadium | Arquitectonica | — | [link](https://floridayimby.com/2025/01/city-of-miami-to-break-ground-on-eight-story-administration-building-at-miami-freedom-park.html) |
| 1203 | Miami Freedom Park — stadium parking garage | Arquitectonica | — | [link](https://floridayimby.com/2025/01/city-of-miami-to-break-ground-on-eight-story-administration-building-at-miami-freedom-park.html) |

Reasons:

- **id 1162** — the review is right and this follows the project's existing architect-of-record rule: the source names Robert A.M. Stern Architects as the designer and Cohen Freedman Encinosa as the architect OF RECORD, and the field holds the architect of record
- **id 1185, id 1203** — Architect CLEARED. The only source ever cited for it on this row is Florida YIMBY's coverage of the eight-storey ADMINISTRATION BUILDING, as the row's own note recorded, so Arquitectonica was this site's Civic Center architect attributed to the wrong building. No acceptable source names an architect for this structure.

### residential_units — 3 change(s)

| id | Project | Old | New | Source |
| --: | --- | --- | --- | --- |
| 976 | Okan Tower | — | 399 | [link](https://floridayimby.com/2026/04/construction-reaches-50th-floor-on-okan-tower-at-555-north-miami-avenue-in-downtown-miami.html) |
| 1107 | LOFTY Brickell | 363 | 362 | [link](https://floridayimby.com/2023/12/newgard-development-group-appoints-g-t-mcdonald-enterprises-inc-as-general-contractor-for-44-story-lofty-brickell.html) |
| 1188 | The WELL Coconut Grove | 148 | 194 | [link](https://commercialobserver.com/2026/01/tyko-terra-well-coconut-grove-miami/) |

Reasons:

- **id 976** — the review's rule is to count every residential unit including condo-hotel units, so the 163 Sky Residences and the 236 condo-hotel residences are both counted; the 316 Hilton keys are not units and stay in the hotel field
- **id 1107** — the review's 362 is confirmed by the developer's own general-contractor announcement; the stored 363 came from a two-project round-up
- **id 1188** — 194 in the redesigned scheme, superseding the 148 of the abandoned design

### building_height_ft — 2 change(s)

| id | Project | Old | New | Source |
| --: | --- | --- | --- | --- |
| 1107 | LOFTY Brickell | — | 574 | [link](https://floridayimby.com/2022/07/faa-permits-filed-for-44-story-lofty-brickell-tower-at-99-sw-7th-street-in-miami-170-million-construction-loan-secured.html) |
| 1144 | Downtown 6 | 576 | 574 | [link](https://floridayimby.com/2025/04/vertical-construction-underway-on-58-story-downtown-6th-in-miami.html) |

Reasons:

- **id 1107** — 574 ft above ground from the FAA filing, against 579 above sea level
- **id 1144** — the review's 574 ft is confirmed: the source states 574 feet ABOVE GROUND against 587 feet above sea level, and the stored 576 matched neither

### num_stories — 1 change(s)

| id | Project | Old | New | Source |
| --: | --- | --- | --- | --- |
| 1220 | Miami Civic Center | — | 8 | [link](https://www.miami.gov/My-Government/Departments/Department-of-Real-Estate-Asset-Management-DREAM/New-City-of-Miami-Administration-Building) |

Reasons:

- **id 1220** — the City's project page and Florida YIMBY both say eight storeys

### parking_spaces — 1 change(s)

| id | Project | Old | New | Source |
| --: | --- | --- | --- | --- |
| 1220 | Miami Civic Center | — | 889 | [link](https://floridayimby.com/2025/01/city-of-miami-to-break-ground-on-eight-story-administration-building-at-miami-freedom-park.html) |

Reasons:

- **id 1220** — an attached garage for 889 vehicles; the City's page says up to 900, and the specific figure is stored

### record created — 1 change(s)

| id | Project | Old | New | Source |
| --: | --- | --- | --- | --- |
| 1377 | Miami Freedom Park — One Heron (Phase 1 office) | — | Miami Freedom Park — One Heron (Phase 1 office) | [link](https://therealdeal.com/miami/2026/04/10/miami-freedom-park-retail-construction-kicks-off/) |

Reasons:

- **id 1377** — split out of the row that is now Miami Civic Center, which held this project's name and developer against the Civic Center's permit, address, floor area and contractor

### general_contractor — 1 change(s)

| id | Project | Old | New | Source |
| --: | --- | --- | --- | --- |
| 1107 | LOFTY Brickell | GT CONSTRUCTION & DEVELOPMENT INC | G.T. McDonald Enterprises, Inc. | [link](https://floridayimby.com/2023/12/newgard-development-group-appoints-g-t-mcdonald-enterprises-inc-as-general-contractor-for-44-story-lofty-brickell.html) |

Reasons:

- **id 1107** — the review's G.T. McDonald Enterprises is confirmed by a dedicated announcement of the appointment. The master permit on this row names GT Construction & Development Inc, but that permit belongs to a DIFFERENT tower — see the note

### neighborhood — 1 change(s)

| id | Project | Old | New | Source |
| --: | --- | --- | --- | --- |
| 1180 | The Residences at Mandarin Oriental, Miami — South Tower | — | Brickell Key | [link](https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/PaParcelView_gdb/FeatureServer/0/query?where=FOLIO%3D%270142070030020%27&outFields=*&f=pjson) |

Reasons:

- **id 1180** — the tower sits on Brickell Key, folio 0142070030020, and its sibling North Tower on the same island already carried the value; the field was blank
