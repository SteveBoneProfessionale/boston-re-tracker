"""Asset class for the four South Florida cities outside Miami.

Miami's rows carry the City's own use tokens, so `scraper/sofl_asset_class.py`
reads them mechanically. These 139 rows do not: their class came from the
record files, where the evidence is a sentence of description rather than a
closed vocabulary. So the decision is pinned per row, each with the sentence or
the statute it rests on -- the same approach the Doral intake already uses,
and for the same reason: a keyword classifier got eleven of seventy-five wrong
and iterating on it was the spiral this project is meant to avoid.

THE LIVE LOCAL ACT SETTLES SIXTEEN ROWS ON STATUTORY GROUNDS, not on a guess.
Florida's Live Local Act (ss. 125.01055(7) and 166.04151(7), Fla. Stat.)
grants its zoning and height relief only to a development that sets aside at
least 40 per cent of its RESIDENTIAL UNITS as RENTAL units affordable at or
below 120 per cent of area median income. A project pursuing Live Local
approval is therefore a rental development as a matter of what it applied
under, which is stronger evidence than a press adjective. The one exception is
425 Ocean Drive, which pairs 90 condominiums with 60 workforce rental
apartments -- both tenures in one project, which the brief counts as Mixed-Use.

WHAT IS DELIBERATELY LEFT AS "Residential"
The brief says "Residential" is not a valid value. For ten rows no acceptable
source states the tenure, and the choice is between an honest vague value and
an invented precise one. They stay "Residential" and are listed by name in
UNSTATED below, so the gap is visible rather than papered over. Regency Parc's
own note already recorded this: "the coverage does not state whether the
residences are rental or for sale, so the project is stored under Residential
rather than assigned to Condo on an assumption."
"""

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

LIVE_LOCAL = ("the project is a Live Local Act application, and the Act "
              "(ss. 125.01055(7) and 166.04151(7), Fla. Stat.) grants its relief only "
              "to developments reserving at least 40 per cent of residential units as "
              "RENTAL units affordable at or below 120 per cent of AMI, so what it "
              "applied under establishes the tenure")

# id -> (class, reason). Nothing here is inferred from a brand name or a
# developer's usual product.
DECISIONS = {
    # ── Coral Gables ────────────────────────────────────────────────────
    1234: ("Multifamily Rental",
           "described as a Class A multifamily rental building of 160 residences with "
           "no ground-floor retail, so there is not even a minor second use"),
    1235: ("Condo",
           "a mixed-use condominium of 58 residences over 22,000 sq ft of ground-floor "
           "retail; retail under a tower is not a second programme"),
    1236: ("Mixed-Use",
           "74 residences plus retail AND office space, so the office is a real "
           "second programme"),
    1237: ("Mixed-Use",
           "74 residences with 13,000 sq ft of office and nearly 18,000 sq ft of "
           "retail; the office area is stated, so the programme is real"),
    1244: ("Condo",
           "93 units of which 85 are condominiums and eight are lodging units; eight "
           "keys out of 93 units is not a second programme, so the dominant use is "
           "for-sale residential"),
    1245: ("Multifamily Rental",
           "263 rental apartments and 22 live-work units over retail"),
    1248: ("Multifamily Rental",
           "201 apartments, 40 per cent of them affordable to qualify for Live Local "
           "Act incentives, plus commercial space"),

    # ── Doral ───────────────────────────────────────────────────────────
    1287: ("Mixed-Use",
           "1,498 new residential units alongside 470 existing hotel units and "
           "141,694 sq ft of commercial space; the hotel is a real second programme"),
    1290: ("Parking",
           "no residential component at all: two parking garage expansions adding 487 "
           "spaces, with 10,967 sq ft of retail. The garages are the project -- they "
           "are its name -- so parking is the dominant use and the retail the minor "
           "one, which is the reverse of the order that holds for a Miami tower"),
    1288: ("Multifamily Rental",
           "described as a seven-storey rental building with a central garage, "
           "delivered in two phases, within the Downtown Doral community"),
    1292: ("Mixed-Use",
           "380 dwelling units with approximately 106,849 sq ft of office space, a "
           "stated office programme"),
    1296: ("Mixed-Use",
           "up to 623 units with approximately 301,591 sq ft of commercial and retail "
           "including 262,816 sq ft of existing office space"),
    1301: ("Multifamily Rental",
           "a conversion of an existing hotel to multifamily residential for people "
           "aged 55 and up; age-restricted multifamily of this kind is rental, and "
           "the City's own parking calculation treats it as age-restricted "
           "multi-family housing under its rental standard"),
    1306: ("Retail",
           "no residential component: a 58,000 sq ft commercial condominium of which "
           "approximately 40,000 sq ft is retail and restaurant and 18,000 sq ft "
           "office, so retail is the dominant use"),

    # ── Miami Beach ─────────────────────────────────────────────────────
    1309: ("Mixed-Use",
           "142 apartments with 45,313 sq ft of retail and 51,194 sq ft of office "
           "over five storeys; the office area is stated, so it is a real programme"),
    1311: ("Multifamily Rental", LIVE_LOCAL),
    1312: ("Multifamily Rental", LIVE_LOCAL),
    1313: ("Multifamily Rental",
           f"{LIVE_LOCAL}. Its commercial space is retail and not a second programme"),
    1314: ("Mixed-Use",
           "restoration of the 1939 art deco hotel with a new tower of 124 residences "
           "plus hotel keys; the hotel is a real second programme"),
    1315: ("Multifamily Rental",
           f"50 rental residences over 8,535 sq ft of ground-floor retail, and {LIVE_LOCAL}"),
    1320: ("Multifamily Rental",
           f"29 residential units including 12 workforce housing apartments, and {LIVE_LOCAL}"),
    1321: ("Mixed-Use",
           "59 units split 23 condominiums and 36 hotel keys, so the hotel is a real "
           "second programme"),
    1325: ("Mixed-Use",
           "76 units split 52 private residences and 24 hotel suites, so the hotel is "
           "a real second programme"),
    1326: ("Multifamily Rental", "described as 178 rental homes"),
    1327: ("Multifamily Rental",
           f"an affordable housing development, and {LIVE_LOCAL}"),
    1333: ("Multifamily Rental",
           f"a workforce housing development, and {LIVE_LOCAL}"),
    1334: ("Multifamily Rental",
           f"{LIVE_LOCAL}. It replaces two 14-storey, 495-unit apartment buildings"),
    1335: ("Multifamily Rental", LIVE_LOCAL),
    1336: ("Multifamily Rental", LIVE_LOCAL),
    1337: ("Multifamily Rental", LIVE_LOCAL),
    1338: ("Multifamily Rental", LIVE_LOCAL),
    1339: ("Multifamily Rental", LIVE_LOCAL),
    1340: ("Multifamily Rental", LIVE_LOCAL),
    1341: ("Multifamily Rental",
           f"the City's plan record describes 72 elderly workforce housing units with "
           f"ground-floor retail, and {LIVE_LOCAL}"),
    1342: ("Mixed-Use",
           "90 condominiums AND 60 workforce rental apartments, which is both tenures "
           "in one project -- the brief's own test for a real second programme"),

    # ── West Palm Beach ─────────────────────────────────────────────────
    1348: ("Mixed-Use",
           "110 hotel suites and 146 branded residences, so the hotel is a real "
           "second programme"),
    1350: ("Multifamily Rental",
           "399 mixed-income apartments with roughly 7,550 sq ft of retail; about 160 "
           "of the apartments are for households at or below 120 per cent of AMI"),
    1351: ("Condo",
           "the City's coverage states the plans call for 281 condos"),
    1356: ("Mixed-Use",
           "181 rental units and a separate 112-key Tribute Portfolio by Marriott "
           "hotel, so the hotel is a real second programme"),
    1358: ("Multifamily Rental",
           "358 residential units including 90 workforce housing apartments, with "
           "4,000 sq ft of divisible retail"),
    1361: ("Multifamily Rental",
           "a workforce housing tower of 261 residences, 40 per cent of them "
           "designated workforce housing"),
    1362: ("Condo",
           "the coverage describes 90 condos with an average unit of 3,382 sq ft, and "
           "eight lower-level townhouses"),
    1364: ("Multifamily Rental",
           "a two-building affordable housing development with units reserved for "
           "tenants earning 30, 50, 60 or 80 per cent of AMI, which are rental tiers"),
    1365: ("Multifamily Rental", "350 apartments with retail"),
    1366: ("Multifamily Rental", "a seven-storey affordable housing project of 151 apartments"),
    1368: ("Multifamily Rental", "described as 310 apartments"),
    1369: ("Multifamily Rental",
           "167 units, 40 per cent of them workforce housing, proposed under the Live "
           f"Local Act. {LIVE_LOCAL}"),
    1370: ("Mixed-Use",
           "660 residential units across four towers WITH office, commercial and "
           "recreational space, so the office is a real second programme"),
}

# No acceptable source states rental versus for-sale for these. They keep
# "Residential", which is the honest value, and are reported by name.
UNSTATED = {
    1238: "Regency Parc, Coral Gables — 126 residences of ~1,700 sq ft and upwards",
    1243: "The Mark, Coral Gables — 393 residential units",
    1246: "Crystal Residences, Coral Gables — 184 residences",
    1249: "6100 Caballero Boulevard, Coral Gables — 251 luxury units",
    1264: "Doral City Place 7 — approximately 391 residential units",
    1269: "Legacy at Doral (EWE Warehouse) — 185 dwelling units",
    1273: "Midtown PUD Phase II Modification, Doral — 734 dwelling units",
    1286: "Doral Parc (Midtown Doral Phases IV–VI) — 552 units",
    1305: "Miami International Mall redevelopment, Doral — approximately 900 units",
    1346: "Residences at 464 Fern Street, West Palm Beach — 194 residences",
    1367: "Soleste Palm Station, West Palm Beach — 321 units",
}
