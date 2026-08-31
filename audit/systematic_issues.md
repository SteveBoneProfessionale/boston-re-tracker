# Systematic Issues — Findings 6 and 8

Traced 2026-08-31. **No database write has been made.** Every page figure quoted
here was retrieved from bostonplans.org during this session and cached to
`audit/_bpda_pages_20260831.json`.

---

# FINDING 8 — The published GSF field

## The hypothesis was close, but the mechanism is different — and better

Your hypothesis was that a parent PDA value was being written down onto child
project rows. **It is not.** The three On the Dot rows each carry a *different*
`processed_filing_url`, so they were not fed from one shared parent page:

| id | processed_filing_url | extraction |
|---|---|---|
| 168 | `bpda.box.com/s/2yxinxfkzckiudotbcyptla10vux1qpp` | claude-haiku-4-5, 2026-04-30 23:57 |
| 170 | `bpda.box.com/s/7x36gig2a1h02g3xasi8zy7ixblj5rox` | claude-haiku-4-5, 2026-04-30 23:59 |
| 172 | `bpda.box.com/s/dwyod2mj63j6yv7dgj6iraw5k1ic5peb` | claude-haiku-4-5, 2026-05-01 00:01 |

Three separate **BPDA Board memos**, three separate LLM extraction runs, all
three landing on 1,386,500. That is the phase-one total, and each memo describes
the whole phase, so an extractor asked for "gross square feet of entire project"
returns the phase figure from every one of them. The convergence is not a bug in
the join; it is three correct readings of three documents that each describe the
same larger thing.

## Q1 — How was total_gsf populated for ids 168, 170, 172?

`scraper/extract_projects.py`. The prompt at line 54 asks for:

```
"total_gsf": integer gross square feet of entire project,
```

and line 229 writes it:

```python
proj.total_gsf = _to_int(data.get("total_gsf"))
```

The input is the BPDA Board memo PDF, not the project page. `total_gsf_source`
is then stamped `"filing"`, which is accurate — it *did* come from a filing.
The wording **"entire project"** is the defect: on a component parcel page in a
phased development, the entire project is the phase.

## Q2 — Does the ingest read the "Gross Floor Area" field from BPDA pages?

**Yes. It always has, and it stores it correctly — in a different column.**

`scraper/bpda_scraper.py` lines 259–261:

```python
elif "gross floor area" in key or "floor area" in key:
    detail["bpda_gsf"] = parse_gsf(value)
elif "land sq" in key or "land square" in key:
    detail["land_sqft"] = parse_gsf(value)
```

and line 441 persists the first of them:

```python
project.bpda_gsf = detail_data.get("bpda_gsf")
```

**The correct values were in the database the entire time.** For the three rows
you verified by hand:

| id | `bpda_gsf` (page) | `total_gsf` (app uses this) | your verified figure |
|---|---|---|---|
| 168 | **487,400** | 1,386,500 | 487,400 ✓ |
| 170 | **510,900** | 1,386,500 | 510,900 ✓ |
| 172 | **388,200** | 1,386,500 | 388,200 ✓ |

Nothing overwrote the page value. Two columns exist, they disagree, and the
application reads the wrong one.

### The land field is parsed and then dropped on the floor

`detail["land_sqft"]` is assigned at line 261 and **never written to the model**.
There is no `project.land_sq_ft = ...` anywhere, and no such column exists. Every
scrape since the project began has parsed Land Sq. Feet out of the HTML and
discarded it in the same function. That is why your schema request is cheap: the
parser already works, only the column and one assignment are missing.

## Q3 — Is a parent PDA value being written onto child rows?

No. See above — three distinct source documents. **There is no parent-PDA
propagation bug to hunt.** The `475-511-dorchester-avenue-on-the-dot-pda` page
you identified is not in the ingest path for these rows at all.

---

## The spot check — and it changes the audit's scope

You asked for five random Boston rows. Because `bpda_gsf` already holds the page
figure, I ran the comparison across **every** Boston row in scope, then fetched
pages live to confirm the stored values were not stale.

**Five random rows (seed 20260831), fetched live:**

| id | project | live page GFA | `bpda_gsf` | `total_gsf` | verdict |
|---|---|---|---|---|---|
| 175 | South Boston Innovation Campus | 837,000 | 837,000 | 837,000 | **match** |
| 339 | Allston Green | 259,000 | 259,000 | 356,000 | mismatch |
| 238 | Residences at Readville Station | 316,315 | 316,315 | 348,395 | mismatch |
| 326 | 135 Dudley Street | 289,073 | 289,073 | 282,382 | mismatch |
| 182 | 495 Dorchester Avenue | 309,604 | 309,604 | 358,258 | mismatch |

**They do not match — 4 of 5.** By your own criterion the Dorchester case is not
an edge case and the audit plan does need rethinking.

But the scraper is exact: **live page GFA equals stored `bpda_gsf` on every row
tested, without exception.** The failure is entirely in which column the
application consumes.

### Population-level result — every page fetched live, 0 failures

All **78** Boston in-scope rows with a bostonplans.org URL were retrieved this
session and cached to `audit/_bpda_pages_20260831.json`:

| | rows |
|---|---|
| pages carrying **Gross Floor Area** | 77 |
| pages carrying **Land Sq. Feet** | 71 |
| **live page GFA == stored `bpda_gsf`** | **77 / 77** |
| live page GFA == stored `total_gsf` | 29 / 78 |
| **mismatch** | **48** |

**The scraper is exact on every single row.** Not one stored `bpda_gsf` had
drifted from what the page publishes today. The entire defect is that the
application reads `total_gsf`.

Across those 48 rows, `total_gsf` sums to **29,054,192** against a page total of
**23,055,514** — the pipeline is **overstated by 5,998,678 GSF** on these rows
alone, before any of the dead-project removals in Findings 3–5.

The error is not one-directional: **34 rows overstated, 14 understated** (id 246
stores 700,000 against a page 810,000; id 213 stores 253,288 against 341,911).
It cannot be fixed by a blanket "take the smaller figure" rule, which is why each
of the 48 is proposed as its own row with its own URL.

### Two independent corroborations that the page column is the right one

1. **On the Dot.** 487,400 + 510,900 + 388,200 = **1,386,500 exactly.** The three
   page values reconstruct the phase total to the square foot. That is not a
   coincidence available to a wrong column.
2. **1000 Boylston (id 363).** `bpda_gsf` = **439,500**. Banker & Tradesman, which
   you cite in Finding 5, describes a **440,000 SF** condo tower. The page column
   agrees with the press figure; `total_gsf` (689,000) does not. **This resolves
   the GSF discrepancy you flagged in Finding 5** — 689,000 is an earlier or
   larger program version, and 439,500 is what BPDA currently publishes.

### What I am NOT claiming

That the page figure is right on all 48. It is *what BPDA publishes today*, which
under your hierarchy is Tier 1 and is the most recent authoritative figure. But
the versioning rule matters: on some rows `total_gsf` may be an as-proposed
figure from a live filing and the page may lag it, or vice versa. Each of the 48
is proposed as an individual row in `structural_corrections.csv` with its own
URL, not as a bulk column swap.

Confidence in the CSV reflects that distinction. **5 rows are `confirmed`** —
the three On the Dot rows (arithmetic proof plus your verification), 495
Dorchester (live fetch, and it sits outside a phase total that already
reconstructs exactly), and 1000 Boylston (page agrees with Banker & Tradesman
independently). The other **43 are `probable`**: Tier 1, retrieved live, but with
no second source confirming the page supersedes the filing figure.

## Recommended remedies, in order

1. **Do not repair rows one at a time first.** Fix the read path: the application
   should prefer `bpda_gsf` where present and fall back to `total_gsf`. That
   corrects 45 rows at once and is reversible.
2. **Retire the phrase "entire project" from the extraction prompt.** It is the
   root cause. It should ask for the GSF *of the building at this address*, and
   should be instructed to return null rather than a phase or master-plan total
   when the document describes more than one building.
3. **Add `land_sq_ft`** and assign the already-parsed `detail["land_sqft"]`. One
   line in `bpda_scraper.py` plus a migration.
4. **Add computed FAR** = `total_gsf / land_sq_ft`. Note it will be a *gross*
   FAR against the page's land figure, which may not equal the zoning lot; label
   it as such rather than as a zoning FAR.
5. **Re-extract, do not hand-patch.** 45 rows is too many to correct by hand and
   the same prompt will reintroduce the error on the next run.

---

# FINDING 6 — Permit issuance driving construction status

## Q1 — How was 380 Stuart Street's status set? Two paths, both wrong

**Path A — `completion_stage`, from an open building permit.**

```
completion_stage      Under Construction
completion_basis      permit_active
completion_date       2024-12-19
completion_source_url https://data.boston.gov/dataset/approved-building-permits
completion_evidence   Boston Approved Building Permits: ERT1415488
                      (Erect/New Construction), status Open, issued 2024-12-19
                      at 366-380 Stuart ST.
```

`permit_active` is a defined basis in `app/data.py:290`
(`{"label": "Permit active", "mark": "△", "rank": 2}`). An **open** permit — a
permit that has been issued and not yet closed — is being read as evidence of
construction. It is evidence of *permission*, which is what your Globe and
Skanska citations demonstrate: the permit was pulled on 2024-12-19 and there was
still no start date announced a year later.

**Path B — `status`, hardcoded.** `bpda_url` on this row is
`manual:380-stuart-st-boston`, i.e. it came from
`scraper/insert_manual_projects.py`, which sets `"status": "Under Construction"`
as a literal at line 185. That value was true-by-assumption when the row was
hand-entered and nothing has revisited it.

So the two fields agree by coincidence, from two unrelated mechanisms, and
neither consulted a source that says work started.

## Q2 — Blast radius

**`permit_active` is the basis on 13 rows totalling 1,617,732 GSF.** Three are in
the ≥250k audit scope, carrying **1,225,050 GSF**:

| id | project | GSF | permit issued | `status` field |
|---|---|---|---|---|
| 385 | 380 Stuart Street | 625,000 | 2024-12-19 | Under Construction |
| 358 | 10 Stack Street | 343,800 | 2020-08-06 | Under Construction |
| 126 | 250 Everett Street | 256,250 | 2026-06-02 | Under Construction |

And ten below the audit threshold:

| id | project | GSF | permit issued | `status` field |
|---|---|---|---|---|
| 225 | 1270 Commonwealth Avenue | 189,000 | 2026-05-26 | Under Construction |
| 390 | 55 India Street | 35,000 | 2025-04-28 | Under Construction |
| 106 | 279 Maverick Street | 34,000 | 2026-06-18 | Under Construction |
| 111 | 1318 River Street | 0 | 2026-05-05 | Under Construction |
| 165 | 3458 Washington Street | 0 | 2026-07-16 | Under Construction |
| 329 | 14 Gardner Street | 41,981 | 2025-07-03 | Board Approved |
| 350 | 706 Dudley Street | 36,799 | 2023-10-26 | Under Review |
| 307 | 363 E Street | 26,647 | 2026-03-18 | Board Approved |
| 299 | 226 Magnolia St | 18,655 | 2025-12-12 | Board Approved |
| 114 | 259 Allandale Street | 10,600 | 2026-06-02 | Board Approved |

**id 358, 10 Stack Street, deserves attention first.** Its permit issued
2020-08-06 — nearly six years ago. Either it completed long ago and nothing
closed the record, or it never started. Both are wrong, and neither is
"Under Construction".

The five rows whose `status` disagrees with `completion_stage` (329, 350, 307,
299, 114) are a second, separate inconsistency: the app now holds two conflicting
stage answers for the same project.

**Only one other row reaches Under Construction by any route:** id 391, Bunker
Hill Housing Redevelopment, basis `human_set`. That one is deliberate.

## Q3 — A status vocabulary that distinguishes the two states

Not implemented, as instructed. Proposed:

| value | means | evidence required |
|---|---|---|
| `Board Approved` | approved, no permit | BPDA board action |
| **`Permitted — Not Started`** | **building permit issued, no evidence of work** | **permit issued and open, nothing more** |
| `Under Construction` | work observably begun | a start announcement, GC mobilisation, a site observation, an inspection record against the permit, or a *phased/partial* permit sequence |
| `Complete` | finished | CO issued or assessor confirmation |

The corresponding `completion_basis` split:

- `permit_active` → keep, but it must map to **`Permitted — Not Started`**, never
  to Under Construction.
- add `construction_observed` → the only basis that may set Under Construction
  from evidence short of a CO.

**A cheap discriminator exists and is not being used.** The Boston permits
dataset carries inspection records against a permit number. A permit with issued
status and *no* inspection activity is almost certainly unstarted; a permit with
framing or foundation inspections is certainly started. That distinction is
available from the same `data.boston.gov` source already being read.

## Q4 — 380 Stuart

Set to `Permitted — Not Started` once the value exists. **GSF of 625,000 is
confirmed and is not proposed for change** — note that this row has no
`bpda_gsf` (it is a manual row with no BPDA page), so Finding 8 does not touch it.

---

# ALSO FOUND WHILE TRACING

**1. The name-annotation check from Finding 4 comes back clean.** Only **one** row
of the 893 carries status language in its name — id 246 itself. Ten other BPDA
rows have parentheticals, and all ten are legitimate identifiers (`Fenway Corners
(West)`, `Parcel X (310 Northern Avenue)`, `Tremont Crossing (P-3)`, `505
Dorchester Avenue (Dot Ave)`, `Harvard Enterprise Research Campus (ERC) Phase B`,
and similar). **The ingest is not systematically absorbing BPDA status
annotations into names.** id 246 is a one-off.

**2. `495 Dorchester Avenue` (id 182) sits OUTSIDE the phase-one figure.** The
three phase-one pages sum to 1,386,500 exactly, leaving no room for a fourth
building. Its own page publishes **309,604 GSF** against a stored 358,258 — a
separate error of the same Finding 8 class, not a phase-boundary question.

**3. That page's Address field reads "495 South Boston Avenue"**, which is not a
real street name — BPDA appears to have mangled "495 Dorchester Avenue, South
Boston". Cosmetic, but it will defeat any address-matching join.

**4. `phase_group` is populated on Cambridge rows only** (10 groups, all
Cambridge). Every Boston multi-phase site — Suffolk Downs, Seaport Square,
Allston Yards, 776 Summer Street — is ungrouped. Finding 2 addresses this.

**5. Two in-scope sites have component rows carrying `total_gsf = 0`**: Allston
Yards Building B (id 270) and 776 Summer Street Phase 2 (id 115). They are
invisible to any ≥250k query while belonging to in-scope sites, so any phase-sum
reconciliation will understate those two sites until they are filled.

**6. A scheduled BPDA scrape ran at 17:32 UTC today, immediately before this
audit began, and it moved more than timestamps.** Diffed against the last
committed database:

| column | rows changed | example |
|---|---|---|
| `last_checked_date` | 366 | id 1: 2026-07-10 → 2026-08-31 |
| **`status`** | **22** | id 4: `Under Review` → `Board Approved` |
| **`bpda_gsf`** | **9** | id 4: 120,186 → 120,262 |
| `description` | 7 | text refresh |

Plus 8 new rows (ids 903–910), all with null GSF and city.

**Two consequences.** First, the status verification in this audit is being done
against values that moved hours before it started — 22 of them. Second, this is
positive evidence that `bpda_gsf` tracks the pages: it updated on 9 rows in that
run, and my live fetch afterwards matched the stored value on 77 of 77.

**Recommend pausing the scheduled scrape while the audit is in flight**, so the
approved changes are applied against a stable baseline rather than a moving one.

**This audit itself wrote nothing.** Every difference above is attributable to
the scraper by its timestamp, and no column this audit proposes to change
(`total_gsf`, `developer`, `excluded`, `phase_group`, `residential_units`) was
altered.
