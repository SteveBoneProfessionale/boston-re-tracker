"""Find the press coverage of a project that is only known by its permit address.

The City of Miami's permit feed names a project by its address of record and
nothing else, so filling the developer, architect, unit count and height for
those rows means finding the article that is about that address. The first
attempt at this matched badly, and the reason is worth writing down because it
is the whole problem:

  the permit says        the press writes
  ---------------        ----------------
  1401 BRICKELL AV       "1401 Brickell"            -- no suffix
  90 NE 11 ST            "E11EVEN Club Residences"  -- a name, no address
  249 NE 3 ST            "300 Biscayne Boulevard"   -- the other frontage
  1 ISLAND DR            "on Brickell Key"          -- a district
  555 N MIAMI AV         "555 North Miami Avenue"   -- direction spelled out
  46 NE 6 ST             "Northeast 6th Street"     -- ordinal added

The first version required the permit's address string to appear verbatim in a
cue sentence. That is true of almost none of these, so it found 16 of 34
projects and reported the rest as having no coverage, when they were all
covered.

So matching here is by PATTERN over the whole article rather than by string
over one sentence: the street number, an optional direction in either spelling,
and the street name with an optional ordinal suffix. "1401 Brickell Avenue",
"1401 Brickell" and "1401 Brickell Ave" all match one expression; "1410
Brickell" does not, because the number is anchored.

A match is still only a candidate. The caller reads the cue sentences and
decides -- an article about one tower routinely names the one next door.
"""

import re

DIR_LONG = {"N": "North", "S": "South", "E": "East", "W": "West",
            "NE": "Northeast", "NW": "Northwest",
            "SE": "Southeast", "SW": "Southwest"}
DIR_ABBR = {v.upper(): k for k, v in DIR_LONG.items()}

SUFFIX_LONG = {"ST": "Street", "AV": "Avenue", "AVE": "Avenue", "BLVD": "Boulevard",
               "BLWY": "Boulevard", "TER": "Terrace", "CT": "Court", "DR": "Drive",
               "PL": "Place", "LN": "Lane", "RD": "Road", "CIR": "Circle",
               "WAY": "Way", "HWY": "Highway", "PKWY": "Parkway"}
SUFFIXES = set(SUFFIX_LONG) | {v.upper() for v in SUFFIX_LONG.values()}


def _ordinal(n: int) -> str:
    if 10 <= n % 100 <= 20:
        return f"{n}th"
    return f"{n}{ {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th') }"


def parse(address: str):
    """(number, direction, street core, suffix) from a City of Miami address."""
    toks = [t for t in re.split(r"\s+", str(address or "").strip()) if t]
    if not toks or not toks[0].isdigit():
        return None
    num, rest = toks[0], toks[1:]
    direction = ""
    if rest and rest[0].upper() in DIR_LONG:
        direction = rest.pop(0).upper()
    elif rest and rest[0].upper() in DIR_ABBR:
        direction = DIR_ABBR[rest.pop(0).upper()]
    suffix = ""
    if rest and rest[-1].upper() in SUFFIXES:
        suffix = rest.pop().upper()
    return num, direction, " ".join(rest), suffix


def variants(address: str) -> list[str]:
    """Search strings for one address, most specific first.

    These go to the search API, which is forgiving. The regex below is what
    actually decides whether a result is about this address.
    """
    p = parse(address)
    if not p:
        return [str(address)]
    num, direction, core, suffix = p
    core_disp = _ordinal(int(core)) if core.isdigit() else core.title()
    suf_disp = SUFFIX_LONG.get(suffix, suffix.title())
    out = []
    for d in ([direction, DIR_LONG.get(direction, direction)] if direction else [""]):
        base = " ".join(x for x in (num, d, core_disp) if x)
        out.append(f"{base} {suf_disp}".strip())
        out.append(base)                      # "1401 Brickell" -- the common form
    seen, uniq = set(), []
    for v in out:
        if v and v not in seen:
            seen.add(v)
            uniq.append(v)
    return uniq


def pattern(address: str):
    """A regex that matches this address however the press spells it.

    The street number is anchored on both sides so 1410 Brickell never matches
    1401 Brickell, which is the failure that would quietly attach the wrong
    developer to a tower.
    """
    p = parse(address)
    if not p:
        return None
    num, direction, core, _ = p
    if direction:
        d_alt = f"(?:{direction}|{DIR_LONG[direction]})\\.?\\s+"
    else:
        d_alt = ""
    if core.isdigit():
        core_alt = re.escape(core) + r"(?:st|nd|rd|th)?"
    else:
        core_alt = r"\s+".join(re.escape(w) for w in core.split())
    return re.compile(rf"(?<!\d){re.escape(num)}(?!\d)\s+{d_alt}{core_alt}\b",
                      re.IGNORECASE)


def entity_terms(owner: str) -> list[str]:
    """Distinctive words in an owner-of-record name.

    A single-purpose entity is often named after the project -- MOWYN MOHAWK
    LLC, 710 EDGE PROPERTY LLC, SWIRE JADECO LLC -- so its unusual words are a
    second way into the coverage when the address alone finds nothing. Legal
    suffixes and generic property words are stripped; what is left is searched.
    """
    if not owner:
        return []
    stop = {"LLC", "L.L.C.", "LP", "L.P.", "INC", "INC.", "CORP", "CORP.",
            "CO", "CO.", "LTD", "COMPANY", "PROPERTY", "PROPERTIES", "HOLDINGS",
            "HOLDING", "OWNER", "DEVELOPMENT", "DEVELOPERS", "DEVELOPER",
            "ACQUISITIONS", "ACQUISITION", "INVESTMENTS", "REALTY", "TRUST",
            "PARTNERS", "GROUP", "VENTURES", "ASSOCIATES", "OF", "THE", "AND",
            "FLORIDA", "MIAMI", "FL", "LLLP", "PLLC"}
    words = [w.strip(",.") for w in str(owner).upper().split()]
    return [w.title() for w in words
            if w and w not in stop and not w.isdigit() and len(w) > 2]
