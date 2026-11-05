"""Formatting for the project detail panel.

Pure functions, no Streamlit, so each one can be exercised directly. The panel
itself lives in app/tabs/project_table.py and composes these.

The panel's job is to make a permit record readable by a person. A permit
record is not written for people: it says
MULTI-FAMILY (CONDO)|BAR / TAVERN (50 OR MORE OCCUPANTS)|BUILDING OFFICIAL (TO
BE REMOVED BEFORE TCO/CO), it says JOHN MORIARTY AND ASSOCIATES OF FLORIDA INC,
and it says 249 NE 3 ST. All three are correct and none is readable. Everything
here turns one of those into something a person can scan, WITHOUT losing the
original -- the raw description keeps its own collapsed section and the legal
entity name stays in the field sources.
"""

import re

# ── title case ──────────────────────────────────────────────────────────
# Tokens that must never be title-cased, because lowercase or mixed case
# would be wrong rather than merely ugly.
_KEEP_UPPER = {
    # legal suffixes
    "LLC", "L.L.C.", "LP", "L.P.", "LLP", "PA", "PLLC", "NV", "SA", "AG",
    # directionals and street abbreviations that are read as letters
    "NE", "NW", "SE", "SW", "N", "S", "E", "W",
    # roman numerals up to a sensible depth
    "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X",
    # the ones that would look silly lowercased
    "USA", "US", "UK", "JV", "AIA", "BBQ", "TV", "HQ", "CRA", "HUD", "FAA",
    "UM", "AMI", "PMG", "DACRA", "ODP", "HOK", "RAMSA", "BHI", "OZK", "CIBC",
    "JPMorgan",
    # Initialised firm names met in this data. A two- or three-letter
    # initialism cannot be detected reliably -- "THE" and "NEW" are the same
    # shape -- so the ones that actually occur are listed rather than guessed
    # at by length. An unlisted one renders title-cased, which is a cosmetic
    # miss rather than a wrong value.
    "GT", "JM", "BI", "MG", "LD", "S2", "BDI", "ACD", "CFE", "CMC", "BH",
    "TGSV", "SROA", "DFRE", "MKDA", "PGIM", "AMLI", "KAST", "LNDMRK", "MWC",
    "HTG", "AHF", "UNCG", "DTI", "EWE", "ICP", "MDPLS", "SST", "BP", "PDC",
}

# Lowercase inside a name, never at the start.
# Suffixes that READ as words rather than initialisms. "INC" shouted in a
# title-cased name looks like a defect; "LLC" lowercased looks like a typo.
_FORCE_CASE = {"INC": "Inc", "INC.": "Inc.", "CORP": "Corp", "CORP.": "Corp.",
               "CO": "Co", "CO.": "Co.", "LTD": "Ltd", "LTD.": "Ltd.",
               "GMBH": "GmbH", "DBA": "dba"}

_SMALL = {"of", "and", "the", "at", "in", "for", "on", "a", "an", "to", "by",
          "de", "del", "da", "von", "van"}

# Street-type abbreviations as the permit writes them -> how to show them.
_STREET_TYPE = {
    "ST": "St", "AV": "Ave", "AVE": "Ave", "BLVD": "Blvd", "RD": "Rd",
    "DR": "Dr", "CT": "Ct", "PL": "Pl", "TER": "Ter", "TERR": "Ter",
    "LN": "Ln", "WAY": "Way", "CIR": "Cir", "PKWY": "Pkwy", "HWY": "Hwy",
    "CSWY": "Cswy", "PLZ": "Plz", "SQ": "Sq", "TRL": "Trl",
    # Miami writes "15 STRD" for Fifteenth Street Road.
    "STRD": "St Rd",
}

_ORDINAL_SUFFIX = {1: "st", 2: "nd", 3: "rd"}


def _ordinal(n: int) -> str:
    """3 -> 3rd, 11 -> 11th, 22 -> 22nd."""
    if 10 <= n % 100 <= 20:
        return f"{n}th"
    return f"{n}{_ORDINAL_SUFFIX.get(n % 10, 'th')}"


def title_case(value, *, address: bool = False) -> str:
    """Readable case, preserving acronyms and legal suffixes.

    `address=True` additionally turns the permit's bare street numbers into
    ordinals and expands its street-type abbreviations: "249 NE 3 ST" reads as
    "249 NE 3rd St". That conversion is mechanical and safe because it only
    fires on a bare number IMMEDIATELY FOLLOWED by a street type, and never on
    the house number, which is the first token.
    """
    if value in (None, ""):
        return ""
    text = " ".join(str(value).split())
    # Input that is already cased by a person is left alone -- "100 Federal
    # Street" and "Sieger Suarez Architects" need nothing done, and re-casing
    # risks breaking a name that was already right. But a PARTLY shouting name
    # does need fixing: the City writes "COASTAL CONSTRUCTION OF Miami Dade Inc
    # dba COASTAL CONDOMINIUMS II", which is neither all-caps nor readable. The
    # test is therefore "does it contain a shouted word", not "is it all caps".
    if not address:
        shouting = any(len(t.strip(".,;:")) >= 3
                       and t.strip(".,;:").isupper()
                       and t.strip(".,;:") not in _KEEP_UPPER
                       and t.strip(".,;:") not in _FORCE_CASE
                       for t in text.split(" "))
        if not shouting:
            return text

    tokens = text.split(" ")
    out = []
    for i, tok in enumerate(tokens):
        nxt = tokens[i + 1] if i + 1 < len(tokens) else ""
        bare = tok.strip(".,;:")
        upper = bare.upper()

        if address and i > 0 and bare.isdigit() and nxt.strip(".,;:").upper() in _STREET_TYPE:
            out.append(_ordinal(int(bare)))
            continue
        if address and upper in _STREET_TYPE:
            out.append(_STREET_TYPE[upper])
            continue
        if upper in _FORCE_CASE:
            out.append(_FORCE_CASE[upper])
            continue
        if upper in _KEEP_UPPER:
            out.append(upper if upper != "JPMorgan".upper() else "JPMorgan")
            continue
        if tok == "&" or bare.isdigit():
            out.append(tok)
            continue
        low = bare.lower()
        if i > 0 and low in _SMALL:
            out.append(low)
            continue
        # Hyphenated and slashed names capitalise each part: "MANICURE-PEDICURE".
        out.append(re.sub(r"[A-Za-z]+", lambda m: m.group(0).capitalize(), tok.lower()))
    return " ".join(out)


# ── programme chips ─────────────────────────────────────────────────────
# A permit's work-item list is a closed vocabulary written for inspectors.
# These are the human labels. Anything not listed falls through to a cleaned
# version of the token itself, so a use the City adds later still renders.
_CHIP = [
    ("MULTI-FAMILY (CONDO)", "Condo"),
    ("MULTI-FAMILY (RENTAL)", "Apartments"),
    ("CONDO HOTEL", "Condo-hotel"),
    ("APARTMENT HOTEL", "Condo-hotel"),
    ("LODGING UNITS", "Hotel"),
    ("PROFESSIONAL OFFICE", "Office"),
    ("GENERAL OFFICE", "Office"),
    ("MEDICAL OR DENTAL OFFICE", "Medical office"),
    ("RETAIL OF GOODS", "Retail"),
    ("GENERAL RETAIL", "Retail"),
    ("GENERAL COMMERCIAL", "Retail"),
    ("RESTAURANT", "Restaurant"),
    ("BAR / TAVERN", "Bar"),
    ("JUICE BAR", "Café"),
    ("BARBER", "Spa/Salon"),
    ("PARKING GARAGE", "Parking"),
    ("PUBLIC PARKING", "Parking"),
    ("STORAGE", "Storage"),
    ("WAREHOUS", "Storage"),
    ("LIGHT INDUSTRIAL", "Industrial"),
    ("MANUFACTURING", "Industrial"),
    ("CHURCH", "Religious"),
    ("SCHOOL", "School"),
    ("COLLEGE", "School"),
    ("PRE-SCHOOL", "School"),
    ("LEARNING CENTER", "School"),
    ("DAYCARE", "Daycare"),
    ("NURSERY SCHOOL", "Daycare"),
    ("HOSPITAL", "Hospital"),
    ("COMMUNITY FACILITY", "Community"),
    ("COMMUNITY SUPPORT", "Community"),
    ("FUNERAL HOME", "Funeral home"),
    ("THEATER", "Theatre"),
    ("CONCERT HALL", "Theatre"),
    ("NIGHTCLUB", "Nightclub"),
    ("SUPPERCLUB", "Nightclub"),
    ("BANQUET", "Event hall"),
    ("RECREATIONAL", "Recreation"),
    ("STADIUM", "Stadium"),
    ("NEW PARK ONLY", "Park"),
    ("GAS STATION", "Petrol station"),
    ("CAR/ TRUCK WASH", "Car wash"),
    ("AUTO/MARINE", "Auto"),
    ("DOCK", "Dock"),
]

# Work items that describe paperwork or a component rather than a use. These
# are dropped from the chips entirely -- "BUILDING OFFICIAL (TO BE REMOVED
# BEFORE TCO/CO)" is an instruction to an inspector, not something the
# building contains.
_CHIP_DROP = re.compile(
    r"BUILDING OFFICIAL|TO BE REMOVED|SHORING|RE-SHORING|POST TENSION|"
    r"CURTAIN WALL|EXCAVATION|PILES|SITE AND SOIL|TRUSSES|RAILING|"
    r"WATERPROOFING|STRUCTURAL SUPPORT|SAUNA|POOL|ELEVATOR|SCAFFOLD|"
    r"DEMOLITION|FENCE|BOLLARD|GENERATOR|INFRASTRUCTURE AND UTILITIES|"
    r"CONSTRUCTION TRAILER|WINDOWS|DOORS|SKYLIGHT|CONCRETE|PLUMBING|"
    r"ELECTRICAL|MECHANICAL|FIRE SPRINKLER|PHASED", re.I)

# Occupancy qualifiers the permit adds for code purposes. A reader does not
# need "(50 OR MORE OCCUPANTS)" to understand "Restaurant".
_OCCUPANCY = re.compile(r"\s*\((?:LESS THAN\s*)?\d+\s*(?:OR MORE\s*)?OCCUPANTS?\)", re.I)


def programme_chips(raw) -> list[str]:
    """Short, de-duplicated use labels from a pipe-separated work-item list."""
    if raw in (None, ""):
        return []
    chips: list[str] = []
    for token in str(raw).split("|"):
        token = _OCCUPANCY.sub("", token).strip()
        if not token or _CHIP_DROP.search(token):
            continue
        label = None
        upper = token.upper()
        for needle, pretty in _CHIP:
            if needle in upper:
                label = pretty
                break
        if label is None:
            # Unknown use: show it, cleaned, rather than dropping a real fact.
            label = title_case(token.split(":")[0].split("/")[0].strip())
        if label and label not in chips:
            chips.append(label)
    # Ordered the way a person describes a building -- what it IS first, then
    # what is in its base. The permit lists work items in filing order, which
    # put "Spa/Salon" ahead of "Condo" on the Waldorf.
    rank = {c: i for i, c in enumerate(_LEAD_ORDER + _TAIL_ORDER)}
    return sorted(chips, key=lambda c: rank.get(c, len(rank)))


# _LEAD_ORDER and _TAIL_ORDER are defined below, after the chip table; the
# sort above runs at call time, so the forward reference is fine.


# ── one-line summary ────────────────────────────────────────────────────
_NOUN = {
    "Condo": "condo",
    "Multifamily Rental": "apartment",
    "Residential": "residential",
    "Hotel": "hotel",
    "Office": "office",
    "Retail": "retail",
    "Industrial": "industrial",
    "Institutional": "institutional",
    "Parking": "parking",
    "Lab/Research": "lab",
}
# Chips that are a PROGRAMME worth naming in the lead phrase, in the order a
# reader would say them.
_LEAD_ORDER = ["Condo", "Apartments", "Condo-hotel", "Hotel", "Office",
               "Medical office", "Industrial", "Storage", "School", "Hospital"]
# The lead reads as an adjective, so it is singular: "apartment building",
# not "apartments building".
_LEAD_SINGULAR = {"Apartments": "apartment", "Condo": "condo",
                  "Condo-hotel": "condo-hotel", "Hotel": "hotel",
                  "Office": "office", "Medical office": "medical office",
                  "Industrial": "industrial", "Storage": "storage",
                  "School": "school", "Hospital": "hospital"}
# Chips that are ancillary -- listed after the colon, never in the lead.
_TAIL_ORDER = ["Retail", "Restaurant", "Bar", "Café", "Spa/Salon", "Recreation",
               "Event hall", "Nightclub", "Theatre", "Parking", "Dock", "Park"]


def _join(words: list[str]) -> str:
    if not words:
        return ""
    if len(words) == 1:
        return words[0]
    return ", ".join(words[:-1]) + " and " + words[-1]


def summary_line(p) -> str:
    """One plain-English sentence describing the building, built from fields.

    Deliberately built from the STRUCTURED fields rather than from the permit
    description, so it says the same kind of thing on every row and never
    leaks permit wording into the header.
    """
    def num(key):
        v = p.get(key)
        try:
            if v is None or v != v:          # NaN
                return None
            return int(float(v))
        except (TypeError, ValueError):
            return None

    stories = num("num_stories")
    units = num("residential_units")
    keys = num("hotel_rooms")
    chips = programme_chips(p.get("asset_class_raw"))
    asset = p.get("asset_class") or ""

    lead_chips = [c for c in _LEAD_ORDER if c in chips]
    # A tower with both for-sale and rental, or a hotel over condos, reads as
    # "condo and hotel" -- the lead names the major programmes, in order.
    if lead_chips:
        lead = _join([_LEAD_SINGULAR.get(c, c.lower()) for c in lead_chips[:3]])
    elif asset in _NOUN:
        lead = _NOUN[asset]
    elif asset:
        lead = asset.lower()
    else:
        lead = "building"

    # "tower" only when the building really is one. Twelve storeys is the
    # line: below it "tower" overstates a mid-rise.
    kind = "tower" if (stories or 0) >= 12 else "building"
    if lead == "building":
        head = f"{stories}-story building" if stories else "Building"
    else:
        head = (f"{stories}-story {lead} {kind}" if stories
                else f"{lead.capitalize()} {kind}")

    tail = []
    if units:
        noun = ("condos" if "Condo" in lead_chips or asset == "Condo"
                else "apartments" if "Apartments" in lead_chips or asset == "Multifamily Rental"
                else "residences")
        tail.append(f"{units:,} {noun}")
    if keys:
        tail.append(f"{keys:,} hotel keys")
    # A tail chip that merely repeats the lead says nothing: a retail
    # building whose only use is retail should not read "retail: retail".
    lead_words = set(lead.replace(" and ", " ").replace(",", " ").split())
    tail += [c.lower() for c in _TAIL_ORDER
             if c in chips and c.lower() not in lead_words]

    if not tail:
        gsf = num("total_gsf")
        if gsf:
            tail.append(f"{gsf:,} sq ft")
    sentence = head[0].upper() + head[1:]
    return f"{sentence}: {_join(tail)}." if tail else f"{sentence}."


# ── confidence dot ──────────────────────────────────────────────────────
# The panel used to print the internal codes -- "DOCUMENT ONLY ·
# WEB_LOW_CONFIDENCE" -- which tell a reader nothing and look like a defect.
# One dot, one plain sentence on hover.
CONFIDENCE_DOT = {
    "human_set":     ("#F5821E", "Set by hand after review."),
    "confirmed":     ("#22c55e", "Confirmed by permit and press."),
    "document_only": ("#eab308", "From the permit only."),
    "conflicted":    ("#ef4444", "Sources disagree on this name."),
    "unattributed":  ("#64748b", "Carried over from earlier work; no source recorded."),
}
# A method that is more specific than its confidence tier gets its own wording.
METHOD_TOOLTIP = {
    "web_low_confidence": "Low confidence: one web source.",
    "web_corroborated":   "Confirmed by permit and press.",
    "registry_self":      "From a licence or company registry.",
    "registry_confirmed": "Confirmed in a licence or company registry.",
    "document_only":      "From the permit only.",
    "human_set":          "Set by hand after review.",
}


def confidence_dot(confidence: str, method: str = "") -> str:
    """A small coloured dot with a plain-English tooltip, or ''."""
    if not confidence:
        return ""
    color, tip = CONFIDENCE_DOT.get(confidence, ("#64748b", ""))
    tip = METHOD_TOOLTIP.get((method or "").strip(), tip)
    if not tip:
        return ""
    return (f'<span title="{tip}" style="display:inline-block;width:7px;height:7px;'
            f'border-radius:50%;background:{color};margin-right:7px;'
            f'vertical-align:middle;cursor:help"></span>')


# ── source link label ───────────────────────────────────────────────────
# The panel labelled every source button "BPDA PAGE", which is the Boston
# agency, so a Miami permit record was offered as a BPDA page.
_SOURCE_LABEL = {
    "Miami": "City of Miami permit record",
    "Miami Beach": "City of Miami Beach record",
    "Coral Gables": "City of Coral Gables record",
    "Doral": "City of Doral record",
    "West Palm Beach": "City of West Palm Beach record",
    "Boston": "BPDA page",
    "Cambridge": "Cambridge development log",
    "Providence": "Providence city record",
    "Warwick": "Warwick city record",
    "Cranston": "Cranston city record",
    "Pawtucket": "Pawtucket city record",
    "Newport": "Newport city record",
}


def source_label(city: str, url: str = "") -> str:
    """What to call the row's source link, by market."""
    label = _SOURCE_LABEL.get(city or "")
    if label:
        return label
    if url and "arcgis" in url.lower():
        return "Permit record"
    return "Source record"


# ── addresses ───────────────────────────────────────────────────────────
def addresses(p) -> tuple[str, str]:
    """(address to show, permit address) -- the second only when it differs.

    A permit-derived row's `address` is the City's DeliveryAddress, which is
    the frontage the permit was filed under and often not how the building is
    known: the Waldorf Astoria is marketed as 300 Biscayne Blvd and permitted
    as 249 NE 3 ST. Where an alternative address was recorded by research, it
    is the marketed one and leads.
    """
    permit_addr = (p.get("address") or "").strip()
    alts = [a.strip() for a in str(p.get("alt_addresses") or "").split(",") if a.strip()]
    if p.get("permit_number") and alts:
        return title_case(alts[0], address=True), title_case(permit_addr, address=True)
    return title_case(permit_addr, address=True), ""


# ── numbers ─────────────────────────────────────────────────────────────
# Placeholders various pipelines write when a value is not yet known. They are
# data, not display text: "not_yet_selected" on a contractor means the same as
# an empty field and must read the same way.
SENTINELS = {"not_yet_selected", "unknown", "unknown - review needed", "tbd",
             "n/a", "na", "none", "null", "-", "—", "not found", "not applicable"}


def clean(value):
    """The value, or None when it is a placeholder rather than a fact."""
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in SENTINELS:
        return None
    return text


def money_millions(value) -> str:
    """426000000 -> '$426M'. Under a million keeps its digits."""
    try:
        n = float(value)
    except (TypeError, ValueError):
        return ""
    # NaN survives float() and fails every comparison, so without this guard a
    # missing cost rendered "$nan" -- which is how it reached a Boston panel.
    if n != n or n <= 0:
        return ""
    if n >= 1_000_000:
        m = n / 1_000_000
        return f"${m:,.0f}M" if m >= 10 else f"${m:,.1f}M"
    return f"${n:,.0f}"
