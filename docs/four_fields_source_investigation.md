# Step 1 — where the four fields actually live (verified 2026-08-19)

Every identifier below was fetched live. Nothing here is recalled.

## Scope correction
885 project rows; 727 not excluded. Boston 389, RI 256, Cambridge 66, 13 null-city, 3 stray MA.
Not 600. Backfill scope needs confirming: 727 non-excluded, or all 885.

## Verified endpoints

### Boston
- Approved Building Permits: CKAN `data.boston.gov`, package `approved-building-permits`,
  datastore resource `6ddcd912-32a0-43df-9908-63574f8c7e77`, 660,419 rows, licence ODC-PDDL.
  25 columns. NO contractor column. `applicant` is a person 80%+ of the time.
  Useful: `parcel_id`, `property_id`, `declared_valuation`, `worktype='NEWCON'`.
- Property Assessment: package `property-assessment`, 23 datastore resources, licence ODC-PDDL.
  Has `AV_LAND`, `LAND_SF`, `PID`. No sale price, no sale date, no book/page. Confirmed.
- MassGIS Property Tax Parcels FeatureServer layer 4 (`GISDATA.L3_ASSESS`):
  https://arcgisserver.digital.mass.gov/arcgisserver/rest/services/AGOL/MassachusettsPropertyTaxParcels/FeatureServer/4
  Fields include LS_PRICE, LS_DATE, LS_BOOK, LS_PAGE, LAND_VAL, PROP_ID, TOWN_ID.
  Boston (TOWN_ID=35): 180,445 parcels, FY2023 only. 100,031 with LS_PRICE>1000 (55%).
  Join verified: Boston assessing PID minus trailing "_" == MassGIS PROP_ID.
- Article 80 corpus: 355 PDFs in data/pdfs, 352 linked to projects.
  14-PDF sample: team section 10/14, civil engineer label 4/14, GC named 0/14
  (one prospective mention: "the Proponent will select a General Contractor").

### Cambridge
- Building Permits: New Construction `9qm7-wbdc`, 362 rows, ODC-PDDL, daily.
  architect_firm 187/362 (52%), architect_name 187 (52%), engineer_name 0 (0%),
  licensed_name 362 (100%), mbl 332 (92%), total_cost_of_construction 362 (100%).
- Building Permits: Addition/Alteration `qu2z-8suj`, 14,187 rows. No architect/engineer columns.
- Cambridge Property Database FY2016-FY2026 `eey2-rv59`, 323,748 rows, licence not stated,
  attribution "Cambridge Assessing Dept", annual. saleprice 292,468; saledate 291,932;
  book_page 292,468; landvalue 292,468. Keys gisid / map_lot. FY2016-FY2026 all present.
  Portal states it "is not the official information source for assessment records."

### Rhode Island
- OpenGov/ViewPoint permit portals, one per city, all confirmed by general_settings orgName:
  providenceri, cranstonri, warwickri, pawtucketri, newportri (api-east.viewpointcloud.com).
  Building permit records carry "Architect/Engineer Details" (A/E name, company, RI licence
  number, License Description = Architect or Professional Engineer) and "General Contractor
  Details" (contractor name, company, registration number). SPA; no public REST search.
- Plan sets: 215 cached in data/ri_plansets. Title blocks label roles deterministically.
- Minutes: 990 minutes documents cached across the five cities' boards (data/ri_agenda_corpus.json).
- Assessors: Vision (gis.vgsi.com) serves cranstonri, warwickri, pawtucketri — parcel pages
  carry Sale Price, Sale Date, Book & Page AND an Ownership History table. 404 for Providence
  and Newport. Providence open data has tax rolls + PVD_Parcels (`nyp3-msmz`) with ass_land /
  ass_total but NO sale price. Newport uses Patriot Properties + MapGeo, both SPA/ASP.

## Tax stamp rates — verified against the issuing authority
- Massachusetts: Suffolk County Registry of Deeds states "The effective tax rate is $2.28 per
  $500 or fraction thereof of taxable value. There is no excise tax due where the consideration
  stated is less than $100.00."  CONFIRMS $2.28. Note "or fraction thereof" — the excise is a
  ceiling function, so back-computation yields a $500-wide RANGE, not a point value.
- Rhode Island: RIGL 44-25-1(a) — "$3.75 for each five hundred dollars ($500), or fractional
  part of it", consideration over $100. 44-25-1(b) — additional $3.75 per $500 on residential
  "of the consideration in excess of eight hundred thousand dollars ($800,000)"; threshold
  CPI-adjusted for tax years from 1 Jan 2026. Most recent amendment effective 1 Oct 2025
  (P.L. 2025, ch. 278, art. 5, § 10). CONFIRMS $3.75 and the 1 Oct 2025 changeover date, and
  confirms the Tier 2 surcharge applies only to the excess above $800,000.
- NOT verified: the pre-1-Oct-2025 rate of $2.30 per $500. Statute shows current text only and
  the RI Division of Taxation pages returned 403/404. Session web-search budget is exhausted.

## Terms of use
- data.boston.gov: ODC-PDDL (public domain dedication). Automated access via documented CKAN API.
- Cambridge permits: ODC-PDDL. Cambridge property database: no licence stated; attribution
  requested; disclaimer that it is not the official record.
- Socrata: public API, app token recommended for rate limits, not required.
- MassGIS: no licenceInfo on the item; state open-data terms with accuracy disclaimer.
- Vision (gis.vgsi.com): no API. HTML parcel pages. Terms page returns 404; no explicit
  automated-access grant found. Treat as scrape-with-restraint, not licensed data.
- OpenGov permit portals: public record pages, no published API terms; SPA only.
- opengov.sos.ri.gov: RI Secretary of State open-meetings public records.
