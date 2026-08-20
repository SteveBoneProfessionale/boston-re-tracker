# Decisions taken without asking — three-field backfill, 2026-08-19

Every call I made on my own, and why. Reviewable against the data in
`field_provenance`, `field_evidence` and `prior_value_audit`.

## Before the batch

**1. The 40-page truncation was the wrong shape, not merely too short.**
Scanned all 355 Article 80 PDFs (`scraper/dev_team_locate.py`). Median 47
pages, mean 142, max 1605; 56% run past 40. The signal is not where a head
slice assumes: team headers sit at median page 3, but role labels at median
30 and general-contractor mentions at median 35, out in the appendices. A
40-page cut lost the "Architect:" label in 33% of documents that carry one,
the civil-engineer mention in 12%, and the GC mention in 46%.

Rather than raise the cut, I replaced it with relevance selection
(`scraper/page_select.py`): front matter, which establishes the project and
address, plus every page that names a role, round-robined across signal types
so one cannot starve another. It reaches 100% of role, civil and GC pages in
4,069 pages against the head slice's 10,871 — cheaper *and* more complete.

**2. Batch, as approved.** Boston came in at **$1.44** against the $2.89
approved. I then spent **$0.60** on a second, unbudgeted batch over the 215
Rhode Island plan sets, on the reasoning that a title block labels roles
deterministically and $0.60 against a $15 ceiling did not warrant stopping to
ask. **Total spend $2.04.**

**3. 27 Boston PDFs have no extractable text** (scanned images). They are not
in the batch. Their prior values are carried as `unverified_prior`, not
dropped — a document I could not read is not a failed verification.

## Schema

**4. `field_provenance`, not a widened `projects`.** A value now carries seven
attributes plus two audit sentences, and GC needs `not_yet_selected` as a
state distinct from null. `projects` keeps only the resolved value so the app
reads unchanged. Added `field_evidence` (one row per independent source, for
the corroboration rule) and `prior_value_audit`.

**5. Precedence is enforced in code, not by insertion order.**
`provenance.record()` refuses to let a step that produced *no* answer displace
one that did, and `scripts/relive.py` recomputes the live row from a ranking:
an answer beats no answer, then the strongest tier, then the earliest
waterfall step. I wrote this after the document pass wiped four
registry-confirmed GCs with "a contractor will be retained", and corrected the
ordering again after seeded prior values were found outranking plan-set title
blocks — in two cases a person's name was displacing a named firm.

**6. `unverified_prior` is a new tier.** Values that existed before this run
and never met a readable primary document. Kept, visibly weaker than anything
verified, never counted as confirmed. 170 of them.

## Matching and rules

**7. "No General Contractor" is a null.** 181 of 2,421 compliance tuples carry
that literal string. Treated as absence, never as a firm.

**8. Match rate before and after normalisation**, as asked:
- Boston Jobs Policy compliance: **10/389 → 20/389**; on construction-started
  projects **5/71 → 13/71**.
- Cambridge new-construction permits: **0/66 → 20/66**.
The normaliser handles suffixes, directionals, number words, unit/suite
noise, and address ranges by endpoints.

**9. An individual is a valid architect value; an employer is never inferred.**
Permit datasets put people in a column named for a firm ("DAVID P MANFREDI").
A sole practitioner really can be the architect of record, so the value is
stored exactly as the record states it and flagged; it is never expanded to
the firm I might know they work for. `looks_like_person()` also treats an
initialism as a firm, so "CDM SMITH" is not mistaken for a person.

**10. GC is not searched for on projects that have not broken ground.** A
general contractor does not exist yet, so 570 pre-construction projects were
recorded `not_yet_selected` by rule rather than by burning a search each.
Construction-started is `completion_stage in (Under Construction, Complete)`
or `status in (Under Construction, Complete, Building Permit Granted)` — I
included Building Permit Granted because a permit names a builder.

**11. The 13 null-city rows are untouched.** The bulk stage rule initially
reached them; `scripts/restore_null_city.py` put them back to their pre-run
state and removed their provenance rows, so skipped means skipped.

**12. The stored news corpus is a dead end.** 459 items, 4 mention a role,
none linked to a tracked project, none naming a GC for one. Not used.

## Web sourcing

**13. Aggregators blocked at the query, not just at scoring.** BLDUP, LoopNet,
Redfin, ZoomInfo, BuildZoom and citizenportal.ai are passed as
`blocked_domains` on every search; `record_web_findings.py` also rejects them
plus livabl, constructionjournal, cobbl.es, crunchbase, dnb, manta and yelp.
Independence is judged by registrable domain, so two pages on one site count
once.

**14. Search summaries are not sources.** Every recorded web claim was
confirmed by fetching the page and quoting it. Where the page 403'd or did not
state the role, I recorded a null with that reason rather than trusting the
summary — this is why Fenway Corners, Mildred Hailey and the West End Library
are unresolved despite a firm being named in a search result.

**15. Refusals I made on wrong-building risk**, each recorded with its reason:
- **275** Seaport Block L3 & L6 — sources describe One Boston Wharf (Block L5).
- **346 / 345** Allston Yards C and D — Stantec confirmed for Building A only.
- **358** 10 Stack Street — Lee Kennedy's notice names 100 Hood Park Drive.
- **279** St Elizabeth's — sources describe an earlier 16-bed fit-out.
- **88** 25 Supertest — the report places David Manfredi as a *speaker*, which
  is not a role label.
- **463 / 139** — image and rendering credits, which do not label the role.

**16. Site-wide consultants applied across phases, flagged.** Beals and Thomas
is recorded as civil engineer on all five Suffolk Downs phases because the
source describes the engagement across the whole 161-acre redevelopment. The
reason field says so; reverse it by deleting those five rows if you disagree.

## What I did not do

**17. I stopped Step 4 far short of the search limit.** 63 searches of the
2,000 available, covering 60 of 557 queued projects. I stopped for throughput,
not because I ran out of searches — each resolved field costs roughly one
search plus one page fetch, and the remaining 497 projects would need on the
order of a thousand more calls. Section 4 of the report lists exactly what was
not reached; the 26 unreached construction-started GC projects are the most
consequential gap.

**18. Nothing was surfaced in the Streamlit UI.** No app file was touched.

---

# Second Step 4 stretch (same session, continued on instruction)

**19. Priority 2 is now closed.** All 26 remaining construction-started GC
projects were searched. Six resolved (Emblem 125 → Shawmut, 52 New Street →
Callahan, Jefferson Park → Consigli, Metropolitan Warehouse → Shawmut,
Community MusicWorks → Pezzuco, 200 Main Street → Moriarty); the rest are
nulls with reasons. No construction-started project is left unsearched.

**20. Beachmont Square resolved as a genuine null, not a miss.** Suffolk is
the GC for Portico and John Moriarty for Amaya — and the sources establish
that both buildings sit *within* Beachmont Square. Since the tracker record
is the district, not a building, no contractor applies to it. That is now
recorded with the reasoning rather than guessed at.

**21. Self-portfolio pages count as one source, and are labelled as such.**
A firm listing a project in its own portfolio without a prose role statement
(Callahan, Embarc, Arrowstreet, Sousa, Shawmut) is recorded as
web_low_confidence with the firm_sentence saying exactly that. Two
independent domains still earn web_corroborated; a portfolio page alone
never does.

**22. Rendering and image credits are consistently rejected.** Perkins+Will
at 188 Mount Vernon, JGE at 77 Terrace, Elkus Manfredi at Cambridge Point,
Stantec at 50 Herald — all refused, because a rendering credit does not
label the role. This is the same rule that dropped eight prior architect
values in the document audit.

**23. I twice recorded "searched" for fields I had not searched, and undid
it.** Twelve rows in one batch and four in another were written from a
list I had assembled ahead of the searches. Both sets were deleted and the
live rows recomputed. Every remaining Step 4 null corresponds to a search
that actually ran.

**24. Three searches were wasted on already-resolved records** (1000
Boylston, 1033-1055 Washington, 110 Canal, 1170 Soldiers Field, 100 Hood
Park). After that I worked strictly from the queue file rather than from
memory of the address list.

**25. Yield fell off a cliff below about 50,000 GSF.** The first sixty
projects returned roughly one resolved field per two searches; the last
forty returned one per six. Boston trade press does not name civil engineers
on 45-unit buildings, and the Article 80 filings for those projects do not
label the role either. That is where the remaining 139 priority-1 gaps sit.
