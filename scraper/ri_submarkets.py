r"""
Submarket (neighborhood) sources per Rhode Island municipality.

Not every city publishes neighborhood boundaries, so the submarket DIMENSION
differs by city. That substitution must never be invisible: each entry carries
a `dimension_label` which the chart renders, so a Newport user sees they are
looking at zoning districts rather than neighborhoods.

Sources, verified 2026-08-17:

  Providence  City of Providence GIS Hub `Neighborhoods` FeatureServer, 25
              polygons. Primary source is actually the FILING -- CPC agendas
              state the neighborhood inline, e.g. "(AP 68 Lot 846, Smith Hill)"
              -- with the GIS layer used to validate and normalize the name.
  Pawtucket   City of Pawtucket DPW `neighborhoods` FeatureServer, 14 polygons.
  Warwick     RIGIS `PLAN_Villages` CLIPPED to the Warwick municipal polygon,
              21 villages. Clipping is required: an unclipped bounding-box query
              returns 32, including East Greenwich and Crompton, which are other
              municipalities.
  Cranston    "Neighborhoods Redistricting 2020" layer, 28 names off NBHD1.
              PROVISIONAL -- names are unmistakably Cranston (Edgewood, Garden
              City, Knightsville, Thornton) but it is published under a personal
              ArcGIS account with no city attribution. RIGIS Villages is not a
              substitute: it yields only 5 for Cranston. Pending confirmation
              from Cranston Planning; falls back to zoning district if the city
              confirms no official layer exists.
  Newport     NO boundary layer exists. Newport's neighborhoods are real and
              used administratively (The Point, Fifth Ward, Kerry Hill) but
              nobody publishes polygons. Falls back to zoning district.
              NOTE: ArcGIS returns a "Newport Neighborhoods" layer owned by
              NNVA_ADMIN. That is Newport News, VIRGINIA (maps.nnva.gov).
              It must not be used.
"""

import re

ZONING_FALLBACK = "Zoning District"
NEIGHBORHOOD = "Neighborhood"
VILLAGE = "Village"

SUBMARKET_SOURCES = {
    "Providence": {
        "dimension": "neighborhood",
        "dimension_label": NEIGHBORHOOD,
        "primary": "filing_inline",
        "service": "https://services6.arcgis.com/wv9mHoqblhTsnqdG/arcgis/rest/services/Neighborhoods/FeatureServer/0",
        "name_field": "lname",
        "expected_features": 25,
        "provisional": False,
        "clip_to_municipality": False,
    },
    "Pawtucket": {
        "dimension": "neighborhood",
        "dimension_label": NEIGHBORHOOD,
        "primary": "gis",
        "service": "https://services5.arcgis.com/KTVE1BfEqewed0ZV/arcgis/rest/services/neighborhoods/FeatureServer/0",
        "name_field": "Neighborho",     # truncated shapefile field name, as published
        "expected_features": 14,
        "provisional": False,
        "clip_to_municipality": False,
    },
    "Warwick": {
        "dimension": "village",
        "dimension_label": VILLAGE,
        "primary": "gis",
        "service": "https://services2.arcgis.com/S8zZg9pg23JUEexQ/arcgis/rest/services/PLAN_Villages/FeatureServer/0",
        "name_field": "NAME",
        "expected_features": 21,
        "provisional": False,
        "clip_to_municipality": True,   # REQUIRED -- see module docstring
    },
    "Cranston": {
        "dimension": "neighborhood",
        "dimension_label": NEIGHBORHOOD,
        "primary": "gis",
        "service": "https://services5.arcgis.com/bmiwyveTUuaT56jB/arcgis/rest/services/Neighborhoods/FeatureServer/0",
        "name_field": "NBHD1",
        "expected_features": 28,
        "provisional": True,
        "provisional_note": (
            "Unverified provenance: personal ArcGIS account, no city attribution. "
            "Confirmation requested from Cranston Planning. Falls back to zoning "
            "district if no official layer exists."
        ),
        "clip_to_municipality": False,
    },
    "Newport": {
        "dimension": "zoning_district",
        "dimension_label": ZONING_FALLBACK,
        "primary": "filing_zoning",
        "service": None,
        "name_field": None,
        "expected_features": None,
        "provisional": False,
        "fallback_reason": "No published neighborhood boundary layer exists for Newport, RI.",
        "clip_to_municipality": False,
    },
}

# Municipality -> the label the Projects-by-Submarket chart must display, so a
# dimension substitution is always visible to the reader.
def dimension_label(municipality: str) -> str:
    e = SUBMARKET_SOURCES.get(municipality)
    return e["dimension_label"] if e else NEIGHBORHOOD


def is_provisional(municipality: str) -> bool:
    e = SUBMARKET_SOURCES.get(municipality)
    return bool(e and e.get("provisional"))


# ── Name normalization ──────────────────────────────────────────────────

# Names that are correctly lowercase or mixed inside a title-cased string.
_SMALL_WORDS = {"of", "the", "and", "at", "on"}


def normalize_submarket_name(raw: str) -> str | None:
    """Title-case and clean a published submarket name.

    Published names are uppercase, sometimes with leading whitespace, and the
    RIGIS village layer contains at least one OCR-style typo ("0AKLAND BEACH",
    zero for the letter O). Returns None for an empty value rather than "".
    """
    if not raw or not raw.strip():
        return None
    n = re.sub(r"\s+", " ", raw.strip())
    n = _fix_digit_typos(n)
    parts = []
    for i, w in enumerate(n.split(" ")):
        lw = w.lower()
        if i > 0 and lw in _SMALL_WORDS:
            parts.append(lw)
        elif "'" in w:
            # O'Neill capitalizes after the apostrophe; a possessive like
            # Mary's does not. A single-letter prefix distinguishes them.
            a, b = w.split("'", 1)
            parts.append(a.capitalize() + "'" + (b.capitalize() if len(a) == 1 else b.lower()))
        elif "-" in w or "/" in w:          # Oak-Lawn, Oak Lawn/Brookfield
            sep = "-" if "-" in w else "/"
            parts.append(sep.join(p.capitalize() for p in w.split(sep)))
        else:
            parts.append(w.capitalize())
    return " ".join(parts)


# Digit-for-letter substitutions seen in published names. Applied only when the
# digit sits inside an otherwise-alphabetic token, so real numeric names are
# untouched.
_DIGIT_FIX = {"0": "O", "1": "I", "5": "S", "8": "B"}


def _fix_digit_typos(name: str) -> str:
    out = []
    for w in name.split(" "):
        if re.fullmatch(r"[A-Za-z0-9']+", w) and re.search(r"[A-Za-z]", w):
            letters = sum(ch.isalpha() for ch in w)
            digits = sum(ch.isdigit() for ch in w)
            # A token that is mostly letters with a stray digit is a typo;
            # something like "Route 5" or "Ward 3" is not.
            if digits and letters >= 3 and digits <= 1:
                w = "".join(_DIGIT_FIX.get(ch, ch) for ch in w)
        out.append(w)
    return " ".join(out)


def validate_submarket_name(raw: str) -> list[str]:
    """Problems with a published submarket name. Empty list means clean.

    A digit inside a name is flagged rather than silently corrected, because
    that typo class recurs in published GIS layers and a silent fix would hide
    a source-data problem that should be reported upstream.
    """
    problems = []
    if not raw or not raw.strip():
        return ["empty name"]
    if raw != raw.strip():
        problems.append("leading/trailing whitespace")
    if any(ch.isdigit() for ch in raw):
        problems.append(f"contains a digit ({raw!r}) — likely a letter/digit typo")
    if raw.isupper():
        problems.append("all-uppercase (needs title-casing)")
    if "  " in raw:
        problems.append("double space")
    return problems


if __name__ == "__main__":
    samples = ["0AKLAND BEACH", " APPONAUG", "CONIMICUT", "Oak Lawn/Brookfield",
               "WEST END", "Lower South Providence", "Route 5 Corridor"]
    print(f"{'RAW':26} {'NORMALIZED':26} FLAGS")
    for s in samples:
        print(f"{s!r:26} {str(normalize_submarket_name(s))!r:26} {validate_submarket_name(s)}")
    print()
    for m, e in SUBMARKET_SOURCES.items():
        flag = "  [PROVISIONAL]" if e.get("provisional") else ""
        print(f"  {m:11} dimension={e['dimension_label']:16} features={e['expected_features']}{flag}")
