# GSF Verification Audit — Projects ≥ 250,000 GSF

**Database:** `data/boston_re.db`, table `projects`
**Scope query:** `coalesce(total_gsf,0) >= 250000`
**Count in scope:** **101** projects (0 excluded), 11 batches of 10
**Database state:** READ ONLY. Nothing in this audit has been written to the database.
**Structural checks run:** 2026-08-31, before any per-project verification.

---

# PART 0 — STRUCTURAL CHECKS

These were run first, against the database only. No external source has been
retrieved yet, so nothing below is a verification finding — every item is a
question raised by the data's own internal consistency, for you to direct.

---

## Check 1 — Identical GSF values on two or more rows

Three clusters, seven rows. One is serious; two are almost certainly coincidence.

### 1A. `1,386,500` GSF on three rows — MATERIAL DOUBLE-COUNT RISK

| id | name | address | status | developer |
|---|---|---|---|---|
| 168 | 505 Dorchester Avenue (Dot Ave) | 505 Dorchester Avenue | Board Approved | On the Dot LLC c/o Core Investments Inc. |
| 170 | 65 Ellery Street | 65 Ellery Street Street | Board Approved | On the Dot LLC c/o Core Investments Inc. |
| 172 | 75 Ellery Street | 75 Ellery Street Street | Board Approved | On the Dot LLC c/o Core Investments, Inc. |

These three rows are identical on **every** substantive field, not just GSF:

- `total_gsf` = 1,386,500 on all three
- `commercial_gsf` = 1,286,100 on all three
- `residential_units` = 0 on all three
- same developer (differing only by a comma before "Inc.")
- same city, neighborhood (South Boston), status, and `total_gsf_source` = `filing`
- all three first seen in the same second on 2026-04-30

They differ only in name, address, and BPDA URL — three distinct project pages:
`/505-dorchester-avenue`, `/65-ellery-street`, `/75-ellery-street`.

**The shape of the problem.** 505 Dorchester Avenue, 65 Ellery Street and 75
Ellery Street are contiguous parcels in the On the Dot assemblage in South
Boston. The reading that fits the data is that one master-plan total was written
onto three component parcel pages. If so, the pipeline currently overstates this
site by **2,773,000 GSF** — the figure is counted three times where it should be
counted once.

The competing reading is that the BPDA genuinely published the same
whole-development figure on each of three parcel filings, in which case the
database faithfully reflects three filings and the fix belongs in how the app
aggregates, not in the rows.

**Not resolved here, per instruction.** Deciding between those requires reading
all three BPDA filings, which is Tier 1 work and belongs in the row-by-row pass.
Flagged only.

### 1B. `455,000` GSF on two rows — probably coincidence

| id | name | city | developer |
|---|---|---|---|
| 167 | 232 A Street | Boston (South Boston Waterfront) | Parcel 3 Owner, L.L.C. |
| 433 | Cambridgeside Redevelopment – 80 First Street | Cambridge (East Cambridge) | New England Development |

Different cities, different developers, different asset classes, unrelated
sites. No shared fields beyond the number. Low concern; verify independently in
the normal pass.

### 1C. `311,000` GSF on two rows — probably coincidence

| id | name | city | developer |
|---|---|---|---|
| 164 | 22-24 Pratt Street | Boston (Allston) | Hines/Calare (HUSPP 22 Pratt…) |
| 261 | 41 Berkeley Street | Boston (South End) | Appleton Berkeley Propco… |

Different neighborhoods, developers and asset classes. Low concern.

**Cross-check:** none of these three values appears on any row *below* 250,000
GSF, so no cluster extends outside the audit scope.

---

## Check 2 — Master-plan remainder rows vs component buildings

Three remainder rows exist, **all in Cambridge**, all with status
`Approved PUD/Master Plan Development Remaining`. No Boston row uses this
pattern.

| id | name | GSF | phase_group |
|---|---|---|---|
| 408 | Cambridge Crossing (North Point) – Remaining Master Plan | 1,955,595 | Cambridge Crossing (North Point) |
| 422 | Kendall Common – Development Remaining | 2,151,529 | Kendall Common |
| 468 | MXD Infill – 105 Broadway (Building E) | 302,400 | MXD Infill |

**Inclusive or exclusive is not determinable from the database.** The status
string says "Remaining", which reads as exclusive — the balance of the approved
envelope after the named buildings are subtracted. But nothing in the row
records the approved master-plan total it was subtracted from, so the arithmetic
cannot be checked internally. Each needs its PUD/Master Plan Special Permit read
(Tier 1, Cambridge CDD).

Note id=468 is inconsistent with the other two: it carries a **specific building
name and street address** (105 Broadway, Building E) while holding the
"Development Remaining" status. That is either a mislabelled component or a
remainder row that was given a building's identity. Flagged.

---

## Check 3 — Multi-phase sites: phases summed vs approved master total

**No site could be confirmed as over its approved total, because no approved
master-plan total is stored anywhere in the database.** This check cannot be
completed without Tier 1 filings. What follows is the exposure map.

### Sites carrying a `phase_group`

| phase_group | rows | components | remainder | total if exclusive |
|---|---|---|---|---|
| Kendall Common | 3 | 851,801 | 2,151,529 | 3,003,330 |
| Cambridge Crossing (North Point) | 3 | 304,820 | 1,955,595 | 2,260,415 |
| MXD Infill | 4 | 1,375,180 | 302,400 | 1,677,580 |
| Cambridgeside Redevelopment | 3 | 955,000 | — | 955,000 |
| Cambridge Research Park | 1 | 500,000 | — | 500,000 |
| MIT Kendall Square | 2 | 336,480 | — | 336,480 |
| Alewife Park | 3 | 242,100 | — | 242,100 |
| Harvard | 1 | 107,545 | — | 107,545 |
| First Street PUD | 1 | 84,298 | — | 84,298 |
| Alexandria PUD | 1 | 30,087 | — | 30,087 |

### Multi-phase sites with NO `phase_group` — the bigger gap

`phase_group` is populated on Cambridge rows only. Every Boston multi-phase site
is ungrouped, so nothing in the schema ties its phases together:

| site | rows | in scope | summed GSF |
|---|---|---|---|
| **Suffolk Downs** | 5 (Phases 1–5) | 5 | **10,520,000** |
| Seaport Square | 4 (Blocks D, F, G, L3&L6) | 3 | 1,943,400 |
| Allston Yards | 4 (Buildings B, C, D, E) | 2 | 916,000 |
| 776 Summer Street | 2 (Phases 1–2) | 1 | 754,500 |

**Suffolk Downs is the single largest exposure in the audit** — 10.52M GSF across
five rows, 3 of them in the top 10 by size. The site is a single Article 80
master plan (Boston portion) plus a Revere portion. Whether these five phase
rows sum to the approved total, overlap it, or omit part of it cannot be
answered from the database.

Two rows carry `total_gsf = 0` (Allston Yards Building B id=270; 776 Summer
Street Phase 2 id=115) and so fall outside this audit's ≥250k scope while
plainly being part of an in-scope site.

---

## Check 4 — "Abandoned / withdrawn / dead" language on rows still in an active stage

A naive keyword sweep returned 32 rows, but **29 were false positives**: they
matched a boilerplate staleness note that says the *opposite* of what was being
searched for —

> "STALE: no recorded filing or hearing for N months (last YYYY-MM-DD). This is
> NOT a claim that the project was built or **abandoned** — no confirming source
> was found either way."

After removing that boilerplate, **4 genuine rows** remain:

| id | name | GSF | status | the contradiction |
|---|---|---|---|---|
| 363 | 1000 Boylston Street | 689,000 | **Board Approved** | notes: "WITHDRAWN/DEAD … (scrapped 2019). Operator spot-check." |
| 372 | Tremont Crossing (P-3) | 1,746,908 | **Board Approved** | notes: "WITHDRAWN/DEAD … (designation expired January 2026). Operator spot-check." |
| 246 | 1170-1190 Soldiers Field Road **(abandoned)** | 700,000 | **Board Approved** | the word is in the project NAME |
| 385 | 380 Stuart Street | 625,000 | **Under Construction** | description: "Board approved but **on hold** pending market conditions" |

Three of these are internally contradictory in the strict sense: a row cannot
simultaneously be withdrawn and Board Approved. **2,435,908 GSF** sits on rows
1000 Boylston + Tremont Crossing alone.

id=385 is a different contradiction — the *status* says Under Construction while
the *description* says approved-but-on-hold. One of the two is stale.

Note the provenance of the flags themselves: "Operator spot-check" is the
recorded basis on ids 363 and 372, i.e. someone's manual note, not a filing.
Under this audit's hierarchy that is not Tier 1 and cannot by itself change a
status either.

---

## Check 5 — Null or blank city / neighborhood

- **Blank city: 0 rows.**
- **Blank neighborhood: 2 rows**, and both are outside Boston and Cambridge, so
  the blank is arguably correct rather than missing — neither municipality has a
  neighborhood vocabulary loaded in this database:

| id | name | city | GSF |
|---|---|---|---|
| 382 | Beachmont Square at Suffolk Downs | Revere | 1,700,000 |
| 392 | 75 Reed Road Industrial | Hudson | 950,000 |

---

## Check 6 — Municipalities outside Boston and Cambridge

| city | rows | GSF |
|---|---|---|
| Boston | 82 | 60,646,936 |
| Cambridge | 16 | 14,004,884 |
| **Revere** | **1** | **1,700,000** |
| **Hudson** | **1** | **950,000** |
| **Providence** | **1** | **256,000** |

The three outliers:

- **id=382 — Beachmont Square at Suffolk Downs, Revere, 1,700,000 GSF, Under
  Construction, HYM Investment Group.** Not a stray: Suffolk Downs straddles the
  Boston/Revere line and HYM is the master developer of the whole site. This row
  is the Revere portion of the same development as ids 331–335. It interacts
  directly with Check 3 — the true site total may be 10.52M + 1.70M = **12.22M
  GSF across six rows in two municipalities**, or these may overlap.
- **id=392 — 75 Reed Road Industrial, Hudson, 950,000 GSF, National
  Development.** Hudson MA is ~30 miles west of Boston with no relationship to
  either in-scope city. This reads as out-of-scope data that entered the
  pipeline; no Tier 1 source in this audit's hierarchy covers Hudson.
- **id=779 — Emblem 125 (125 Clifford Street), Providence RI, 256,000 GSF, EQT
  Exeter.** Belongs to the separate Rhode Island pipeline this database also
  carries (the app has a distinct RI overview tab). Its own note already
  concedes the GSF is **web-sourced**, which is Tier 3 and unverifiable under
  this hierarchy. It should probably be scoped out of a Boston/Cambridge audit
  rather than marked unverified.

---

# PART 1 — PER-PROJECT VERIFICATION

Not started. Awaiting your direction on the structural findings.

Field completeness across the 101 in-scope rows, as a preview of how much of the
row-by-row work is filling blanks rather than checking values:

| field | populated | |
|---|---|---|
| total_gsf | 101 / 101 | 100.0% |
| status | 101 / 101 | 100.0% |
| city | 101 / 101 | 100.0% |
| asset_class | 101 / 101 | 100.0% |
| bpda_url | 101 / 101 | 100.0% |
| developer | 99 / 101 | 98.0% |
| neighborhood | 99 / 101 | 98.0% |
| general_contractor | 82 / 101 | 81.2% |
| architect | 79 / 101 | 78.2% |
| **civil_engineer** | **62 / 101** | **61.4%** |

Civil engineer is blank on 39 of 101 rows and will be the hardest field to
source — it is rarely named outside the filed drawings themselves.
