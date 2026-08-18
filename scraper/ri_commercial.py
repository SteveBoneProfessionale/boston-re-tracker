r"""
Restrict the Rhode Island pipeline to COMMERCIAL development, matching the
scope already used for Boston and Cambridge.

The scope is taken from the existing Boston/Cambridge corpus rather than
invented here. Those 457 records are:

    Mixed-Use 212, Residential 155, Hotel 18, Office/R&D 13, Institutional 11,
    Lab 16, Industrial 5, Retail 3, Parking 3
    units: median 50, minimum 4      gsf: median 86,012, minimum 7,721

So "Residential" there means MULTIFAMILY -- income-producing property, four
units and up. Nothing owner-occupied, nothing single-family, and no minor
zoning relief.

Rhode Island's boards hear a much wider mix than Boston's Article 80 process
does. A Providence Zoning Board of Review agenda is mostly homeowners seeking
a fence height, a deck setback or an in-law apartment; a planning commission
also hears single-family subdivisions. None of that is commercial pipeline,
and including it buries the real projects.

Decision order, first rule that fires wins:

  1. EXCLUDE minor relief -- fence, sign, deck, pool, shed, driveway, garage,
     accessory/in-law unit, lot-line adjustment with no construction.
  2. EXCLUDE explicitly small residential -- single-family, one- and two-family
     dwellings, and subdivisions creating single-family house lots.
  3. INCLUDE a stated commercial use -- retail, office, lab, industrial,
     warehouse, hotel, restaurant, institutional, mixed-use, parking structure.
  4. INCLUDE residential at four units or more, matching the Boston floor.
  5. Otherwise UNDECIDED. Undecided records are kept out of the commercial
     pipeline but retained in the database, so the call is reversible and
     visible rather than a silent deletion.
"""

import re

# 1 -- minor relief. Never commercial pipeline, whatever else the text says.
MINOR_RELIEF = re.compile(
    r"\b(?:fence|sign(?:age)?\b|billboard|awning|banner|shed\b|deck\b|patio|"
    r"swimming\s+pool|pool\b|driveway|curb\s+cut|garage\b|carport|"
    r"in-?law|accessory\s+(?:dwelling|family|apartment|structure|building)|"
    r"\bADU\b|solar\s+panel|antenna|generator|dumpster|"
    r"tree\s+removal|retaining\s+wall|porch\b)\b", re.I)

# A lot-line or merger action with no construction proposed.
LOT_ONLY = re.compile(
    r"\b(?:lot\s*line\s*(?:adjustment|revision|relocation)|reconfigur\w+|"
    r"merg(?:er|ing)?\s+of\s+(?:A\.?P\.?|Plat|lots?)|administrative\s+subdivision|"
    # The administrative-approvals run-on writes these as bare parcel actions:
    #   "Subdivision of AP 83 Lot 91 at 535 Academy Ave"
    #   "Merging of AP 20 Lots 62, 189 ... at 88 Dorrance Street"
    # They are paper transactions on a parcel, not development projects.
    r"subdivision\s+of\s+A\.?P\.?\s*\d)", re.I)
CONSTRUCTION = re.compile(
    r"\b(?:construct|erect|build|redevelop|renovat|convert|adaptive\s+reuse|"
    r"addition|demolish|new\s+building)\w*\b", re.I)

# 2 -- explicitly small residential.
SMALL_RESI = re.compile(
    r"\b(?:single[- ]family|one[- ]family|two[- ]family|1[- ]family|2[- ]family|"
    r"duplex|single\s+family\s+(?:home|house|dwelling|residence))\b", re.I)

# 3 -- a stated commercial use.
COMMERCIAL_USE = re.compile(
    r"\b(?:retail|restaurant|storefront|shopping|office|laborator|life\s+scien|"
    r"research\s+and\s+development|\bR&D\b|warehouse|distribution\s+center|"
    r"manufactur\w*|industrial|self[- ]storage|hotel|lodging|guest\s*rooms?|"
    r"inn\b|mixed[- ]use|commercial|parking\s+(?:garage|structure|facility)|"
    r"school|church|hospital|museum|library|community\s+center|"
    r"place\s+of\s+worship|dispensary|brewery|medical\s+office|"
    r"multi-?family|apartment|mill\s+building)\b", re.I)

# 4 -- residential scale floor, matching Boston/Cambridge's minimum of 4 units.
UNIT_FLOOR = 4

# A "land development project" is a defined term under RIGL 45-23: a
# development other than a standard subdivision, which is what Article 80 is to
# Boston. A case carrying that classification, or a Development Plan Review /
# Unified Development Review case, IS a development project -- the filing does
# not need to spell out a use for that to be true. Without this, real pipeline
# was being dropped for saying "subdivide the lot and construct" rather than
# naming a use: 14 Cargill Street, 386 Ives Street, 859 Broad Street.
LAND_DEV_CASE = re.compile(
    r"\b(?:land\s+development\s+project|development\s+plan\s+review|"
    r"unified\s+development\s+review)\b", re.I)
# Providence encodes the same thing in the case suffix.
LAND_DEV_SUFFIX = re.compile(r"\b\d{2,4}-\d{2,4}\s*(MIL|MAL|MA|MI|UDR|DPR)\b", re.I)


def classify(description, units=None, asset_class=None, review_scale=None):
    """(is_commercial, reason) for one project record."""
    text = description or ""

    if MINOR_RELIEF.search(text):
        return False, "minor zoning relief, not a development project"

    if LOT_ONLY.search(text) and not CONSTRUCTION.search(text):
        return False, "lot-line action with no construction proposed"

    # Small residential is out UNLESS the filing also states a commercial use
    # or enough units to be income-producing -- "7 duplexes totaling 14 dwelling
    # units" is a multifamily development even though it says "duplex".
    if SMALL_RESI.search(text):
        if not (COMMERCIAL_USE.search(text) or (units or 0) >= UNIT_FLOOR):
            return False, "single/two-family residential"

    if COMMERCIAL_USE.search(text):
        return True, "states a commercial use"

    if review_scale in ("Major", "Minor") or LAND_DEV_CASE.search(text)             or LAND_DEV_SUFFIX.search(text):
        return True, "land development project / development plan review case"

    if (units or 0) >= UNIT_FLOOR:
        return True, f"{units} residential units (>= {UNIT_FLOOR})"

    if asset_class in {"Retail", "Office", "Industrial", "Hotel", "Lab/Research",
                       "Mixed-Use", "Institutional", "Parking"}:
        return True, f"asset class {asset_class}"

    return None, "use not stated in the filing"


# Reviewing bodies whose docket is predominantly minor residential relief.
# They still contribute stage events and context; they just do not, on their
# own, establish that something is a commercial development.
VARIANCE_BODIES = re.compile(r"zoning\s+board\s+of\s+review|board\s+of\s+appeals",
                             re.I)


def is_variance_body(name) -> bool:
    return bool(VARIANCE_BODIES.search(name or ""))
