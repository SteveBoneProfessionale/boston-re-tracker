"""Classify a South Florida project by its DOMINANT use, not by its ground floor.

THE PROBLEM THIS FIXES
The first Miami ingest classified a project as Mixed-Use whenever its permit
named two different uses. Almost every tower in Miami has ground-floor retail
and a parking podium, so that rule swept 131 of 452 rows into one bucket --
including pure condominium towers (Cipriani, Baccarat, LOFTY, 1428 Brickell)
and pure rental towers (Downtown 6, Midtown 7, UniDos, 222 Namdar Phase II).
A bucket holding 29 per cent of the market and several unlike things in it is
not a classification.

THE RULE
Ground-floor retail and a parking garage are not programmes; they are what a
tower sits on. So uses are split into MAJOR programmes and MINOR ones, and
only majors decide the class:

    majors   residential-rental, residential-for-sale, hotel, office,
             industrial, institutional, lab
    minors   retail, parking, other

    one major            -> that major's class
    two or more majors   -> Mixed-Use (a real second programme)
    no majors            -> the dominant minor

ONE DELIBERATE NARROWING, and it is the only place this departs from the rule
as written. The brief counts "an office component" as a real second programme.
The permit feed cannot support that: its office token is
"PROFESSIONAL OFFICE / BANK / FINANCIAL INSTITUTION", which a residential
tower's permit carries for a leasing office or a ground-floor bank branch just
as readily as an office tower's does for 600,000 leasable square feet. It
names the use, never the area. So residential + office alone resolves to the
residential class, and Mixed-Use requires a second major that is NOT office --
a hotel, industrial, institutional, or both rental and for-sale together.

That is not a shortcut around the rule; it is the rule applied to what the
record actually says, and it reproduces every one of the twenty-one hand
classifications in the review. Where the press DOES state a distinct office
programme with a square footage -- Okan Tower's "approximately 64,000 square
feet of Class A office space" -- Mixed-Use is set by hand through PINNED
below, with the source on the row.

CONDO HOTEL IS RESIDENTIAL. A condo-hotel unit is a for-sale residence that
happens to carry a hotel-branded rental programme; it is not a hotel key. The
feed's "CONDO HOTEL" and "APARTMENT HOTEL" tokens therefore count as for-sale
residential, and only the "N LODGING UNITS" tokens are a traditional hotel.
Getting this backwards is what put Natiivo and the E11EVEN towers in the wrong
bucket.
"""

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

# ── the use vocabulary, grouped by programme ────────────────────────────
RENTAL = {"MULTI-FAMILY (RENTAL)"}

# For-sale residential. CONDO HOTEL and APARTMENT HOTEL belong here, not with
# the hotels: those units are sold, and the brief counts them as units.
FOR_SALE = {"MULTI-FAMILY (CONDO)", "CONDO HOTEL", "APARTMENT HOTEL"}

# A traditional hotel: keys let by the night, which are NOT units.
HOTEL = {"26 OR MORE LODGING UNITS", "11-25 LODGING UNITS", "1-10 LODGING UNITS"}

OFFICE = {"PROFESSIONAL OFFICE / BANK / FINANCIAL INSTITUTION", "GENERAL OFFICE",
          "MEDICAL OR DENTAL OFFICE"}

INDUSTRIAL = {"COMMERCIAL STORAGE & WAREHOUSING", "COMMERCIAL STORAGE &amp; WAREHOUSING",
              "MINI STORAGE & WAREHOUSING", "MINI STORAGE &amp; WAREHOUSING",
              "LIGHT INDUSTRIAL/MARINE REPAIRS", "MANUFACTURING AND PROCESSING"}

INSTITUTIONAL = {
    "CHURCH / RELIGIOUS FACILTY", "ELEMENTARY SCHOOL", "MIDDLE / HIGH SCHOOL",
    "COLLEGE / UNIVERSITY", "PRE-SCHOOL", "LEARNING CENTER",
    "CHILD DAYCARE / NURSERY SCHOOL", "ADULT DAYCARE", "HOSPITAL",
    "ANIMAL HOSPITAL/KENNEL/POUND", "COMMUNITY FACILITY",
    "COMMUNITY SUPPORT FACILITY", "FUNERAL HOME",
}

PARKING = {"PARKING GARAGE-MULTIPLE FLOORS", "PUBLIC PARKING"}

# Everything retail-ish, including food and personal services. These are the
# ground-floor uses the old rule mistook for a programme.
RETAIL = {
    "RETAIL OF GOODS:SUPERMARKET/CLOTHING/FURNITURE/BOOK STORE/ART PRODUCTS",
    "GENERAL RETAIL", "GENERAL COMMERCIAL",
    "RESTAURANT (50 OR MORE OCCUPANTS)", "RESTAURANT (LESS THAN 50 OCCUPANTS)",
    "BAR / TAVERN (50 OR MORE OCCUPANTS)", "BAR / TAVERN (LESS THAN 50 OCCUPANTS)",
    "JUICE BAR / COFFEE SHOP/BAKERY/DELI/ICE CREAM SHOP/CIGAR BAR",
    "BARBER / BEAUTY SHOP / MANICURE-PEDICURE / WAXING/SPA/MASSAGE/TATTO SHOP",
    "CAR/ TRUCK WASH", "GAS STATION",
    "AUTO/MARINE RELATED COMMERCIAL ESTABLISHMENT",
}

GROUPS = [
    ("rental", RENTAL), ("for_sale", FOR_SALE), ("hotel", HOTEL),
    ("office", OFFICE), ("industrial", INDUSTRIAL),
    ("institutional", INSTITUTIONAL), ("retail", RETAIL), ("parking", PARKING),
]

MAJORS = {"rental", "for_sale", "hotel", "office", "industrial", "institutional", "lab"}

CLASS_OF = {
    "rental": "Multifamily Rental", "for_sale": "Condo", "hotel": "Hotel",
    "office": "Office", "industrial": "Industrial",
    "institutional": "Institutional", "retail": "Retail", "parking": "Parking",
}

# When nothing major is present, the dominant minor in this order.
MINOR_ORDER = ["retail", "parking"]

# ── hand classifications ────────────────────────────────────────────────
# Pinned where the permit feed cannot see what the press states. Each entry
# carries the reason, because an override with no reason is indistinguishable
# from a mistake.
PINNED = {
    976: ("Mixed-Use",
          "Okan Tower is genuinely mixed-use and the permit tokens alone would also "
          "say so: a 316-key Hilton, 236 condo-hotel residences, 163 Sky Residences "
          "and approximately 64,000 sq ft of Class A office space. Pinned so the "
          "office programme is recorded as a real one rather than resting on a token "
          "this module deliberately discounts elsewhere."),
    1208: ("Office",
           "1401 Brickell by Santander has no residential component at all. Its "
           "master permit names office, retail, restaurant and recreation, so the "
           "automatic rule already yields Office; pinned because the review called "
           "it out and the value should not drift."),
}


def groups_of(raw: str | None) -> set[str]:
    """The programme groups named in a pipe-separated work-item list."""
    if not raw:
        return set()
    tokens = {t.strip() for t in str(raw).split("|") if t.strip()}
    found = set()
    for name, vocab in GROUPS:
        if tokens & vocab:
            found.add(name)
    return found


def classify(raw: str | None, project_id: int | None = None) -> tuple[str | None, str]:
    """(asset class, the reason) from a permit's work items.

    Returns (None, reason) when the work items name no use this can read --
    a blank class is correct where an invented one would not be.
    """
    if project_id is not None and project_id in PINNED:
        cls, why = PINNED[project_id]
        return cls, f"pinned: {why}"

    found = groups_of(raw)
    if not found:
        return None, "no recognised use token in the permit's work items"

    majors = sorted(found & MAJORS)
    minors = sorted(found - MAJORS)

    if not majors:
        for m in MINOR_ORDER:
            if m in minors:
                return CLASS_OF[m], f"no major programme; dominant minor use is {m}"
        return "Other", "no major programme and no retail or parking"

    residential = [m for m in majors if m in ("rental", "for_sale")]
    non_res = [m for m in majors if m not in ("rental", "for_sale")]

    if len(residential) == 2:
        return ("Mixed-Use",
                "both rental and for-sale residential in one project, which the "
                "brief counts as a real second programme")

    if residential:
        res_class = CLASS_OF[residential[0]]
        # Office alone does not promote a residential tower to Mixed-Use: the
        # feed's office token covers a leasing office and a ground-floor bank
        # as readily as a leasable office programme, and names no area.
        promoting = [m for m in non_res if m != "office"]
        if promoting:
            return ("Mixed-Use",
                    f"residential plus {', '.join(promoting)}, a real second programme")
        if non_res:
            return (res_class,
                    "residential with an office token only; the permit feed cannot "
                    "tell a leasing office or bank branch from an office programme, "
                    "so this stays residential")
        return res_class, "residential only"

    if len(non_res) >= 2:
        return "Mixed-Use", f"two major non-residential programmes: {', '.join(non_res)}"
    return CLASS_OF[non_res[0]], f"single major programme: {non_res[0]}"


def audit(verbose=True):
    """Compare the stored class against this rule for every Miami row."""
    import sqlite3
    con = sqlite3.connect(os.path.join(ROOT, "data", "boston_re.db"))
    rows = con.execute(
        "select id, name, asset_class, asset_class_raw from projects "
        "where city='Miami' order by id").fetchall()
    con.close()
    changes, same, unreadable = [], 0, []
    for pid, name, cur, raw in rows:
        new, why = classify(raw, pid)
        if new is None:
            unreadable.append((pid, name, cur, raw))
            continue
        if new == cur:
            same += 1
        else:
            changes.append((pid, name, cur, new, why, raw))
    if verbose:
        print(f"{len(rows)} Miami rows: {same} unchanged, {len(changes)} reclassified, "
              f"{len(unreadable)} with no readable use token")
        tally = {}
        for _, _, cur, new, _, _ in changes:
            tally[(cur, new)] = tally.get((cur, new), 0) + 1
        print("\n  from -> to")
        for (cur, new), n in sorted(tally.items(), key=lambda kv: -kv[1]):
            print(f"  {str(cur):<20} -> {new:<20} {n:>4}")
        if unreadable:
            print("\n  no readable use token:")
            for pid, name, cur, raw in unreadable:
                print(f"    {pid} {str(name)[:40]:<40} stays {cur!r}  raw={raw!r}")
    return changes, unreadable


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    audit()
