r"""Normalise assessor use codes to one CRE asset taxonomy.

THE PROBLEM. `property_type` carries three vocabularies that do not talk to each
other. Boston writes the state class code with its label -- "Ret/Whsl/Service
(320)", "Office Cls B+ (346)". Cambridge writes its own truncated strings --
"GEN-OFFICE", "HIGH-TECH", "RETAIL-OFFIC". A handful of press-sourced rows
already carry clean labels -- "Office", "Multifamily". 115 distinct values, and
no way to compare a Boston office trade with a Cambridge one.

THE TAXONOMY is the one the development tracker already uses:

    Office . Lab/Research . Industrial . Retail . Multifamily . Mixed-Use
    Hotel . Institutional . Parking . Land

THE RULE ON THINGS THAT DO NOT FIT. Anything that cannot be mapped with
confidence is left UNMAPPED and rendered with a warning marker carrying its raw
code, not swept into an "Other" bucket that would read as a real asset class.
Four values are unmapped for that reason and they are listed in UNMAPPED below.

JUDGMENT CALLS ARE RECORDED, NOT HIDDEN. Where a code could defensibly go two
ways, the choice and the reason are written in JUDGMENTS so the decision is
reviewable rather than buried in a dict. The raw code stays on every row and is
shown on the detail view, so nothing here destroys information.
"""

OFFICE = "Office"
LAB = "Lab/Research"
INDUSTRIAL = "Industrial"
RETAIL = "Retail"
MULTIFAMILY = "Multifamily"
MIXED = "Mixed-Use"
HOTEL = "Hotel"
INSTITUTIONAL = "Institutional"
PARKING = "Parking"
LAND = "Land"

CATEGORIES = [OFFICE, LAB, INDUSTRIAL, RETAIL, MULTIFAMILY, MIXED,
              HOTEL, INSTITUTIONAL, PARKING, LAND]

# Codes deliberately left unmapped. Each is a real use that the ten-category
# taxonomy has no honest home for; bucketing them anywhere would invent a fact.
UNMAPPED = {
    "Cell Carrier (437)":
        "Telecom infrastructure on a rooftop or mast. Not a building type, and "
        "not one of the ten classes.",
    "Com Billboard (465)":
        "Signage rights. Three rows totalling $160M -- large enough that "
        "dropping it into Retail or Land would visibly distort both.",
    "Comm Condo (356)":
        "Commercial condominium states the OWNERSHIP FORM and no use at all. "
        "Seven rows, $113M. Could be office, retail or industrial.",
    "Commercial":
        "A single Cambridge row whose type is the word Commercial. Carries no "
        "use information.",
}

# (raw value) -> category. Boston writes "Label (nnn)"; Cambridge writes SHOUTY
# truncations; press rows already carry clean labels.
MAP = {
    # ---------------- Office ----------------
    "Office 1-2 Story (343)": OFFICE,
    "Office 3-9 Story (344)": OFFICE,
    "Office Cls B (345)": OFFICE,
    "Office Cls B+ (346)": OFFICE,
    "Office Cls A- (347)": OFFICE,
    "Office Cls A (348)": OFFICE,
    "Office Condo (358)": OFFICE,
    "Office (ATTACHED) (340)": OFFICE,
    "Medical Office (342)": OFFICE,
    "Bank Building (341)": OFFICE,
    "GEN-OFFICE": OFFICE,
    "INV-OFFICE": OFFICE,
    "MEDICAL-OFFC": OFFICE,
    "BANK": OFFICE,
    "Office": OFFICE,

    # ---------------- Lab / Research ----------------
    "Laboratory (306)": LAB,
    "HIGH-TECH": LAB,
    "MXD HIGH-TECH": LAB,
    "RES-&-DEV-FC": LAB,
    "RESRCH IND CND": LAB,
    "CLEAN-MANUF": LAB,
    "Light Mfg/ R & D (404)": LAB,
    "Lab/Office": LAB,
    "Office/Lab": LAB,

    # ---------------- Industrial ----------------
    "Warehouse /Distrib (316)": INDUSTRIAL,
    "Old WHSE, Garage (317)": INDUSTRIAL,
    "Cold Storage Whse (318)": INDUSTRIAL,
    "MINI-STORAGE Whse (312)": INDUSTRIAL,
    "WHSE: Industrial (401)": INDUSTRIAL,
    "OFFICE: Industrial (402)": INDUSTRIAL,
    "Old Manufacturing (400)": INDUSTRIAL,
    "New Manufacturing (403)": INDUSTRIAL,
    "Industrial Loft (405)": INDUSTRIAL,
    "Industr Condo (450)": INDUSTRIAL,
    "Machine Shop (small) (407)": INDUSTRIAL,
    "Metal Processing (412)": INDUSTRIAL,
    "Food Process Plant (414)": INDUSTRIAL,
    "Bottling Plant (415)": INDUSTRIAL,
    "Truck Terminal (314)": INDUSTRIAL,
    "Air Freight Terminal (395)": INDUSTRIAL,
    "Repair Garage (332)": INDUSTRIAL,
    "Com Utility Bldg /Shed (394)": INDUSTRIAL,
    "WAREHOUSE": INDUSTRIAL,
    "MANUFACTURNG": INDUSTRIAL,
    "Industrial": INDUSTRIAL,

    # ---------------- Retail ----------------
    "Ret/Whsl/Service (320)": RETAIL,
    "Strip Ctr Stores (319)": RETAIL,
    "Retail Condo (357)": RETAIL,
    "Retail Store Detach (325)": RETAIL,
    "Restaurant/Cafeteria (326)": RETAIL,
    "Restaurant/Lounge (327)": RETAIL,
    "Fast Food Restaurant (328)": RETAIL,
    "Bar/Tavern/Pub (329)": RETAIL,
    "Night Club (361)": RETAIL,
    "Discount Store (321)": RETAIL,
    "Department Store (322)": RETAIL,
    "Supermarket (324)": RETAIL,
    "Showroom (330)": RETAIL,
    "Auto Supply/Service (331)": RETAIL,
    "Service Center/Retail (334)": RETAIL,
    "SELF-SERV Station (333)": RETAIL,
    "Laundromat /Cleaner (311)": RETAIL,
    "RETAIL-STORE": RETAIL,
    "EATING-ESTBL": RETAIL,
    "GAS-STATION": RETAIL,
    "AUTO-SALES": RETAIL,
    "SUPERMARKET": RETAIL,
    "SH-CNTR/MALL": RETAIL,
    "Retail": RETAIL,

    # ---------------- Multifamily ----------------
    "Multifamily": MULTIFAMILY,
    "Residential": MULTIFAMILY,
    "Student housing": MULTIFAMILY,

    # ---------------- Mixed-Use ----------------
    "RETAIL-OFFIC": MIXED,
    "MXD GEN-OFFICE": MIXED,
    "Mixed-use": MIXED,

    # ---------------- Hotel ----------------
    "Hotel (300)": HOTEL,
    "Motel (301)": HOTEL,
    "INN, Resort (302)": HOTEL,
    "INN-RESORT": HOTEL,
    "HOTEL": HOTEL,
    "Hotel": HOTEL,

    # ---------------- Institutional ----------------
    "Nursing /Conv Home (304)": INSTITUTIONAL,
    "Private Hospital (305)": INSTITUTIONAL,
    "Training /Priv Educ (351)": INSTITUTIONAL,
    "Day Care (352)": INSTITUTIONAL,
    "Social Club (353)": INSTITUTIONAL,
    "CHURCH, Synagogue (379)": INSTITUTIONAL,
    "Funeral Home (355)": INSTITUTIONAL,
    "Postal Service (350)": INSTITUTIONAL,
    "Gym /Athletic Bldg (376)": INSTITUTIONAL,
    "Recreation Bldg (377)": INSTITUTIONAL,
    "Boat House /Marina (384)": INSTITUTIONAL,
    "Artist Studio (369)": INSTITUTIONAL,

    # ---------------- Parking ----------------
    "Parking Lot (337)": PARKING,
    "Pay Parking Lot (387)": PARKING,
    "Comm Pkg Garage (336)": PARKING,
    "Subterranean Garage (338)": PARKING,
    "Parking High Vol (339)": PARKING,
    "Condo Parking (COM) (359)": PARKING,
    "PARKING-LOT": PARKING,
    "Parking garage": PARKING,

    # ---------------- Land ----------------
    "Commercial Land (390)": LAND,
    "Com Land (Secondary) (391)": LAND,
    "Industrial Land (440)": LAND,
    "Ind Land (SECONDARY) (441)": LAND,
    "Air Rights Property (388)": LAND,
    "COM-DEV-LAND": LAND,
    "COM-PDV-LAND": LAND,
    "COM-UDV-LAND": LAND,
    "IND-DEV-LAND": LAND,
    "Development site": LAND,
}

# Every mapping that could defensibly have gone another way, with the reason.
# Read this before trusting a category, not after.
JUDGMENTS = {
    "HIGH-TECH": (
        LAB, "$3.32B across 17 rows and by far the largest single call here. "
        "Cambridge assesses lab and R&D buildings as HIGH-TECH -- the code "
        "predates the term life science -- so the Kendall Square lab stock sits "
        "under it. Mapping it to Office would understate Cambridge lab volume "
        "by more than three billion dollars and overstate office by the same. "
        "MXD HIGH-TECH ($810M) follows it."),
    "CLEAN-MANUF": (
        LAB, "Clean manufacturing is the cleanroom code. In Cambridge that is "
        "biotech and pharma production, not general industry. Two rows, $86M. "
        "Industrial is the arguable alternative."),
    "Light Mfg/ R & D (404)": (
        LAB, "The code pairs light manufacturing WITH R&D and cannot separate "
        "them. Three rows, $53M. Placed with research on the Cambridge "
        "precedent; Industrial is defensible."),
    "Repair Garage (332)": (
        INDUSTRIAL, "25 rows and $324M, the largest industrial call. An auto "
        "repair garage is service retail by frontage and industrial by "
        "structure and tenant. Placed in Industrial because these parcels trade "
        "to industrial buyers for industrial redevelopment across this table. "
        "Retail is the arguable alternative."),
    "Bank Building (341)": (
        OFFICE, "A branch is retail; a bank building is an office building. "
        "State class 341 describes the STRUCTURE, and seven rows at $47M "
        "average under $7M, which reads as small downtown office. Cambridge "
        "BANK follows it."),
    "OFFICE: Industrial (402)": (
        INDUSTRIAL, "Class 402 is office space ON AN INDUSTRIAL PARCEL -- flex. "
        "The industrial land is what is being bought. Five rows, $40M."),
    "MINI-STORAGE Whse (312)": (
        INDUSTRIAL, "Self-storage is its own institutional asset class and this "
        "taxonomy has no bucket for it. Nine rows, $365M, all to storage "
        "operators. Industrial is where it sits in the absence of a class."),
    "Student housing": (
        MULTIFAMILY, "Its own class in institutional capital markets, but one "
        "row ($169M) does not justify an eleventh category."),
    "Gym /Athletic Bldg (376)": (
        INSTITUTIONAL, "A commercial gym is retail; a university athletic "
        "building is institutional. Three rows, $39M."),
    "Recreation Bldg (377)": (INSTITUTIONAL, "Same call as Gym. Three rows, $35M."),
    "Boat House /Marina (384)": (INSTITUTIONAL, "One row, $7M. No better home."),
    "Artist Studio (369)": (INSTITUTIONAL, "One row, $3M."),
    "RETAIL-OFFIC": (
        MIXED, "The Cambridge code for retail at grade under office above, "
        "which IS mixed-use. 19 rows, $503M -- the reason Mixed-Use is "
        "populated at all on the Cambridge side."),
    "Postal Service (350)": (
        INSTITUTIONAL, "Government occupancy rather than a commercial class. "
        "Two rows, $12M. Industrial is arguable for a sorting facility."),
    "Air Rights Property (388)": (
        LAND, "Development rights over an existing structure. Not land in the "
        "literal sense, but it is bought and priced as a development site. Two "
        "rows, $34M."),
}


def classify(raw):
    """Return the taxonomy category, or None where the code does not map."""
    if raw is None:
        return None
    return MAP.get(str(raw).strip())


def is_unmapped(raw):
    return raw is not None and str(raw).strip() not in MAP
