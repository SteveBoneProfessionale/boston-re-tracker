# Telling "permitted" from "under construction" from "finished"

Proposal only. **Nothing here is implemented.** The rule below was run in
read-only mode against all 13 `permit_active` rows to measure it; the status
changes that resulted were applied separately and are listed at the end.

---

## The headline: there is no inspections dataset, and you do not need one

I went looking for the inspection records and **Boston does not publish them**.
A search of data.boston.gov for building inspections returns Building and
Property Violations, After Hours Construction, RentSmart and the ZBA tracker —
no inspection history keyed to a building permit.

**The discriminator is inside the permits dataset you already query.** Boston
issues a *Certificate of Occupancy as its own permit record*, and it issues a
separate permit for every trade. So one table answers all three questions.

## Dataset and fields

**Approved Building Permits**, resource id `6ddcd912-32a0-43df-9908-63574f8c7e77`,
queryable through the CKAN `datastore_search_sql` endpoint the pipeline already
uses for `completion_basis = permit_active`.

The four fields that matter:

| field | use |
|---|---|
| `address` | the join key, and the weak point — see below |
| `worktype` | the trade. `ERECT`, `ELECTRICAL`, `PLUMBING`, `FA`, `LVOLT`, `RAZE`, `SPCEVE` … |
| `permittypedescr` | carries the literal string **`Certificate of Occupancy`** |
| `issued_date` | dates the transition |

**Enumerate `worktype` from the data, never from memory.** The values are
`ELECTRICAL` and `PLUMBING`, not `ELECTRIC`/`PLUMB`. My first draft of this rule
used `ELECTRIC`, matched nothing, and reported 10 Stack Street as never started
when three electrical permits were sitting at the address. The full vocabulary
is ~40 values; the ten that indicate physical work are listed in
`audit/_permit_rule.py`.

## The proposed rule

Against permits at the project's **exact street number and street body**:

```
if a permit with permittypedescr = 'Certificate of Occupancy' exists
        -> Complete            (basis: co_issued, date = its issued_date)

elif >=1 permit whose worktype is a TRADE
     (ELECTRICAL, PLUMBING, GAS, FA, LVOLT, SPRINK, INTREN, INTEXT, SRVCHG, TMPSER)
     issued after the ERECT permit
        -> Under Construction  (basis: construction_observed)

elif an ERECT permit exists and nothing else
        -> Permitted - Not Started   (basis: permit_active)

else
        -> no claim. Leave the status alone.
```

The reasoning is that a trade permit is bought by a contractor who is about to
do the work, whereas an erect permit is bought by an owner who has won the right
to. **`permit_active` stops meaning "under construction" and starts meaning
"permitted, not started"** — which is what it always literally described.

## How reliably do the records map to permits? This is the weak part

**The join is on an address string and it is not trustworthy.** Two failures
turned up while testing 13 rows:

**1. The near miss at Stack Street.** 10 Stack Street's $116.9M erect permit has
been open since August 2020. A loose match on `%STACK%` returns 22 permits, one
of which is `COO1217020`, a Certificate of Occupancy issued 2021-07-14 — **at 6
Stack ST, a different building on the same street.** A rule matching on street
name alone would have recorded a stalled tower as finished. Exact street-number
matching is mandatory, and even then the matched address strings should be
recorded so the join can be audited.

**2. Three of 13 rows have no permit at their exact number at all** — 380
Stuart, 55 India Street, 279 Maverick Street — although all three were flagged
`permit_active` originally. **The original flags came from a looser match than
the one used here**, which means an unknown number of the existing
`permit_active` flags across the database are attached to the wrong building.

A production version needs a better key than the address string. `parcel_id` and
`property_id` are both columns in the permits dataset and both are candidates,
but neither has been tested against the projects table and I would not build on
the address string alone.

## What it resolved, on the 13 rows

| verdict | rows | ids |
|---|---|---|
| **Under Construction** (trade permits found) | 5 | 358, 126, 225, 307, 299, 111 |
| **Permitted - Not Started** (erect only) | 2 | 350, 165 |
| **Complete** (CO found) | 1 | 114 |
| **No erect permit at this address** | 1 | 329 |
| **No permit at all at this address** | 3 | 385, 390, 106 |

**10 of 13 got a defensible answer; 4 exposed a bad join rather than a bad
status**, which is arguably the more valuable result.

Two rows were flatly wrong in a way nobody would have caught: **259 Allandale
Street finished in January 2023** (CO issued 2023-01-23) and was still listed as
Board Approved; **363 E Street and 226 Magnolia** were Board Approved with four
and five trade permits on site.

### 10 Stack Street specifically — it did not finish, and it may never have started

You asked whether six years of an open permit meant completion or paralysis.
**Neither is provable, but completion is ruled out.**

At `10 Stack ST` exactly: the $116.9M `ERT1017169` (2020-08-06, still Open), three
ELECTRICAL permits ($150k in 2022, $40k and $2.5k in 2024), two Electrical
Temporary Service permits (2024, 2026), **two special-event permits (July 2024)**
and **a RAZE permit (2025-06-06)**.

There is **no plumbing permit, no fire-alarm permit and no Certificate of
Occupancy**. A 343,800 SF building that had been finished would have all three.
Temporary electrical service, special events and a demolition are the signature
of a site being *used and cleared*, not a tower being topped out. The honest
answer is that the erect permit was never meaningfully acted on — but the permit
record cannot prove a negative, so the row is left as it stands and flagged.

## Effect across all 893 rows

**Not measurable from here, and I am not going to estimate it.** The rule was
run against 13 rows. Extending it needs one query per address; at ~0.4s that is
roughly six minutes for the whole table, which is cheap — but the *result* would
be unreliable until the join key is fixed, because 4 of 13 already
mis-joined.

What can be said:

- **42 rows** currently carry `status = 'Under Construction'` and **14** carry
  `completion_stage = 'Under Construction'`. Only one of those had a basis
  stronger than an open permit before this audit.
- Boston's office and lab market means permitted-but-unstarted is a *growing*
  class, so the error is systematically biased toward overstating activity.
- The same rule would find **completions nobody recorded**. 259 Allandale is one
  in a sample of 13; the CO test is cheap and would likely retire a number of
  rows sitting in the pipeline years after they opened.

## Recommended order

1. **Fix the join** — test `parcel_id` / `property_id` from the permits dataset
   against the projects table, and fall back to exact address only where those
   are absent. Nothing else is safe to build until this is done.
2. **Run the CO test alone across all 893 rows.** It is the highest-confidence
   half of the rule and it finds finished buildings, which is a different and
   currently invisible error.
3. **Then the trade-permit test**, and re-point `permit_active` at
   `Permitted - Not Started` in the vocabulary.
4. **Re-audit every existing `permit_active` flag**, since they were set with a
   looser match than the one proposed here.
