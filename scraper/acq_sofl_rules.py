r"""Turn county sale records into acquisitions, under the brief's rules.

The counties publish PARCELS THAT SOLD. An acquisition is a DEED. Those are not
the same thing, and the gap between them is the single largest source of error
here, so it is handled first and explicitly:

    Miami, 2026, at or above $5M:  513 parcel rows  ->  189 deeds

The price is repeated IN FULL on every parcel a deed touches. One deed in that
set -- Brookfield's sale of the Mayfair House Hotel & Garden at 3000 Florida
Avenue, Coconut Grove --
covers 185 separate folios, each carrying $69,400,000. Treating rows as deals
would report that one transaction as $12.8 BILLION of volume. So a deed is
grouped once, its price taken once, and its parcels' areas, units and land
summed. Everything downstream counts deeds.

WHAT COUNTS AS COMMERCIAL
-------------------------
The brief's scope is multifamily of five units or more, office, retail,
industrial, hotel, mixed-use and land; single-family homes, individual condos,
and family or government transfers are out. Both counties state a Florida DOR
use code and both are followed rather than second-guessed -- with two places
where a code alone cannot decide:

1. A CONDO UNIT IS NOT A DEAL; A BUILDING OF THEM IS. 177 individual
   residential condos and 179 condo-hotel units cleared $5M in these four
   Miami-Dade cities this year, and every one is a home or a hotel room, not an
   acquisition. But when ONE DEED takes five or more at once, a building has
   changed hands -- the Mayfair House sale is exactly that, and so is El Ad
   buying 25
   units at 3265 Virginia Street for $45.5M. So the bulk test is on the deed,
   not the parcel, and BULK_MIN records where the line sits.

2. A VACANT RESIDENTIAL LOT MAY BE A DEVELOPMENT SITE OR A HOUSE LOT. Both
   clear $5M here, and ZONING is what separates them -- not size, which was the
   first thing tried and is simply wrong. Estate lots on North Bay Road and in
   Gables Estates run from a quarter-acre to nearly five, so any square-footage
   threshold either admits a $72M waterfront mansion site at 2740 North Bay
   Road or rejects a six-parcel Wynwood assembly at $6.2M. A 20,000 sq ft line
   admitted both.

   The county states each parcel's primary zone, and it divides them cleanly:
   30 of the 33 vacant-residential deeds in this window are zoned SINGLE
   FAMILY, SGL FAMILY or ESTATES -- house lots, whatever they cost -- and the
   three that are not are zoned for Wynwood pedestrian-oriented and liberal
   retail use, which is what a development site is. So SF_ONLY_ZONE decides it,
   and what is tested is what may be BUILT rather than how big the lot is.

ARM'S LENGTH IS THE COUNTY'S CALL, NOT MINE
-------------------------------------------
Miami-Dade publishes a qualification flag on every sale -- Q qualified, U
unqualified -- with a stated reason: affiliated parties, government, atypical
market exposure, non-market financing, bankruptcy trustees. Palm Beach exposes
the same determination as a search filter rather than a field, so a parcel
returned by the all-sales query but absent from the qualified-sales query is
recorded as disqualified by the county.

Unqualified COMMERCIAL deals are ingested with quarantined = 1 and the county's
own reason, matching how the Boston side already handles affiliated-party
transfers: they stay visible for review and enter no count, volume or ranking.
Rows that fail the SCOPE test -- a house, one condo -- are a different matter
and are never ingested at all; they are out of scope, not suspect.

ON DOCUMENT STAMPS, WHICH LOOK LIKE A PRICE CHECK AND ARE NOT
-------------------------------------------------------------
Florida records documentary stamp tax on every deed, and Miami-Dade publishes
the amount. At $0.60 per $100 a stamp figure ought to corroborate a stated
price independently -- which is exactly what the excise_stamp columns do on the
Boston side.

It does not here. All 834 sale records in this window carry DocumentStamps
equal to 0.600% of SalePrice, to the cent, without exception. The field is
computed FROM the price rather than recorded from the deed, so agreement
between them is arithmetic, not evidence. The columns are therefore left empty
for South Florida: a check that cannot fail would read as corroboration while
supplying none.

WHY A PERCENTAGE CHANGE IS OFTEN REFUSED
----------------------------------------
Prior sale comes from each parcel's full recorded history. A change in basis
between two sales only means anything if the two sales bought the same thing,
so NOT_COMPARABLE names each way that can fail -- an unqualified prior sale, a
prior price of zero or a merely nominal one, a building that did not exist at
the earlier date, or a parcel set that did not trade together. Where any
applies the prior sale is still recorded and the percentage is withheld.

Nominal consideration is the commonest of those after "no prior sale at all":
fifteen rows here have a previous deed recorded at $10 or $100, which states no
price. Dividing by it would turn a gift or a trust transfer into a gain of two
million per cent.
"""

import datetime as dt
import re

from app import asset_class as AC

# Five or more residential or hotel units on ONE deed is a building, not a
# collection of homes. Below it the deed is individual-unit sales. Five is the
# brief's own multifamily threshold, applied to the same question.
BULK_MIN = 5

# Zones permitting nothing but a detached house. A vacant parcel in one of
# these is a house lot and out of scope at any price; a vacant parcel in any
# other zone is a development site. Matched as a prefix against the counties'
# zone descriptions, which is where these come from: "SINGLE FAMILY - GENERAL",
# "SGL FAMILY - 1701-1900 SQ", "ESTATES - 15000 SQFT LOT".
SF_ONLY_ZONE = ("SINGLE FAMILY", "SGL FAMILY", "SINGLE-FAMILY", "ESTATES")


def is_house_lot_zone(zone):
    """True when a zone permits only a detached single-family house."""
    z = str(zone or "").strip().upper()
    return bool(z) and z.startswith(SF_ONLY_ZONE)


# SCOPE ONLY. The CATEGORY each class maps to lives in app/asset_class.py,
# beside the taxonomy it belongs to and the three other vocabularies that feed
# it, because the display layer needs exactly the same mapping -- South Florida
# rows rendered as unclassified until it was shared rather than duplicated here.
# What stays here is the part that is an INGEST question and meaningless to a
# reader: whether a class is in scope at all, and under what condition.
#
#   "in"    always in scope
#   "bulk"  in scope only when the deed takes BULK_MIN or more units
#   "site"  in scope only when zoning permits more than a detached house
#   "mf"    in scope only when the deed totals BULK_MIN or more dwelling units
#   "out"   never in scope; OUT_REASON says why
DOR_SCOPE = {
    "00": "site",                                  # vacant residential
    "01": "out", "02": "out",                      # houses, mobile homes
    "03": "in",                                    # multifamily, 10 or more
    "04": "bulk", "05": "bulk",                    # condominia, co-operatives
    "06": "in",                                    # retirement homes
    "07": "out", "09": "out",                      # misc residential, common area
    "08": "mf",                                    # multifamily under 10 units
    "10": "in", "40": "in",                        # vacant commercial, industrial
}
for _c in ("11", "12", "13", "14", "15", "16", "17", "18", "19", "21", "22",
           "23", "24", "25", "26", "27", "28", "29", "39", "41", "42", "43",
           "44", "45", "46", "47", "48", "49"):
    DOR_SCOPE[_c] = "in"
for _c in ("71", "72", "73", "74", "75", "76", "77", "78", "79",
           "80", "81", "82", "83", "84", "85", "86", "87", "88", "89"):
    DOR_SCOPE[_c] = "out"

OUT_REASON = {
    "01": "single-family home",
    "02": "mobile home",
    "07": "miscellaneous residential, outside the brief's asset classes",
    "09": "residential common area, not a separately tradable asset",
    "71": "place of worship, outside the brief's asset classes",
    "72": "private school, outside the brief's asset classes",
    "73": "private hospital, outside the brief's asset classes",
    "74": "home for the aged, outside the brief's asset classes",
    "75": "charitable use, outside the brief's asset classes",
    "76": "mortuary or cemetery, outside the brief's asset classes",
    "77": "club or lodge, outside the brief's asset classes",
    "78": "convalescent home, outside the brief's asset classes",
    "79": "cultural organisation, outside the brief's asset classes",
}
for _code in ("80", "81", "82", "83", "84", "85", "86", "87", "88", "89"):
    OUT_REASON[_code] = "government-held property"

# Every reason a basis change is withheld. Recorded on the row so the refusal
# is legible rather than looking like missing data.
# A deed recorded for a nominal sum conveys title without stating a price. Ten
# and a hundred dollars are the conventional amounts, and $2,600,000 against a
# prior "$100" is not a 2,599,900% gain -- it is a gift, a trust transfer or a
# correction. Anything at or below this is treated as no stated price, which is
# what it is. Zero is already caught; this catches the rest of the convention.
NOMINAL_MAX = 1_000

NOT_COMPARABLE = {
    "prior_nominal":
        "the earlier deed records only a nominal sum, which states no price",
    "prior_unqualified":
        "the county disqualified the earlier sale, so its price is not a "
        "market basis",
    "prior_zero":
        "the earlier sale records no price",
    "built_after":
        "the building postdates the earlier sale, which bought land",
    "parcel_set_differs":
        "the parcels did not trade together last time, so the two prices "
        "cover different property",
    "no_prior":
        "no earlier sale is recorded",
}


def dor2(code):
    """The two-digit statutory class from a county's four-digit use code."""
    digits = re.sub(r"[^0-9]", "", str(code or ""))
    return digits[:2] if len(digits) >= 2 else ""


def is_condo_form(desc, code):
    """True when the parcel is a condominium UNIT rather than a building.

    Two signals, because the counties use different ones. DOR classes 04 and 05
    are condominia and cooperatives outright. Beyond those, a commercial
    condominium keeps the DOR class of its USE -- a warehouse condo is 4804, an
    office condo 1800 -- and the only marker that it is one unit among many is
    the word CONDO in the description. Miami-Dade writes "COMMERCIAL - CONDO"
    and "COMMERCIAL - CONDO-HOTEL"; Palm Beach writes "WAREH/DIST TERM CONDO".
    Both are caught.
    """
    return dor2(code) in ("04", "05") or "CONDO" in str(desc or "").upper()


def use_scope(desc, code):
    """(category, scope) for a county use code, or (None, None) if unmapped.

    The category comes from the shared taxonomy and the scope from DOR_SCOPE, so
    a class the taxonomy does not know is unmapped here too rather than being
    silently admitted with no asset class.

    A condominium unit takes the ASSET CLASS its use code states -- a warehouse
    condo is industrial -- but the SCOPE of a bulk sale, because one unit is
    never an acquisition while five or more on one deed is a building. See note
    1 in the module docstring.
    """
    prefix = dor2(code)
    scope = DOR_SCOPE.get(prefix)
    if scope is None:
        return None, None
    if scope == "out":
        return None, "out"
    cat = AC.DOR_CATEGORY.get(prefix)
    if cat is None:
        return None, None
    if is_condo_form(desc, code):
        return cat, "bulk"
    return cat, scope


def out_reason(code):
    return OUT_REASON.get(dor2(code), "outside the brief's scope")


def property_type_label(desc, code):
    """The raw assessor value kept on the row, in Boston's "Label (code)" form.

    Keeping the county's own words AND its code on every row is the convention
    every other market here follows, and it is what makes a classification
    reviewable after the fact.
    """
    digits = re.sub(r"[^0-9]", "", str(code or ""))
    return f"{str(desc or '').strip()} ({digits})"


def to_date(text):
    """Miami-Dade writes M/D/YYYY, Palm Beach writes that and ISO."""
    t = str(text or "").strip()
    if not t:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
        try:
            return dt.datetime.strptime(t, fmt).date()
        except ValueError:
            continue
    return None


def deed_key(book, page, date, price=None, grantor=None, grantee=None):
    """What makes two parcel rows the same transaction.

    Book and page identify a recorded instrument, which is exactly one deed, so
    THE DATE IS NOT PART OF THIS KEY. It was at first, and that split a real
    transaction in two: Urbin Commodore's 30-parcel sale in Coconut Grove is one
    instrument, book 35139 page 1456 at $28,200,000, but the appraiser records
    25 of its parcels as sold on 27 January 2026 and the other five on the 28th.
    Keying on the date made that two deeds, and because the price repeats on
    every parcel it counted $28.2M twice -- the same failure the module
    docstring opens with, one step quieter. It was the only instrument in the
    window with split dates, which is exactly why a sample of ten rows was worth
    reading.

    The date still joins the key in the degenerate case, where book and page are
    zeros -- as old records carry -- because then there is no instrument
    identifier left and the parties and price are all that distinguish two
    sales.
    """
    book = str(book or "").strip()
    page = str(page or "").strip()
    date = str(date or "").strip()
    if not book or set(book) <= {"0"}:
        return ("nobook", date, price, str(grantor or "").strip(),
                str(grantee or "").strip())
    return ("bookpage", book.lstrip("0"), page.lstrip("0"))


def _groups(parcels):
    """Parcels bundled by the use they state, with their areas summed."""
    out = {}
    for p in parcels:
        key = (p["desc"], p["dor"])
        g = out.setdefault(key, {"n": 0, "bsf": 0, "lsf": 0, "units": 0,
                                 "parcels": []})
        g["n"] += 1
        g["bsf"] += p["building_sf"]
        g["lsf"] += p["land_sf"]
        g["units"] += p["units"]
        g["parcels"].append(p)
    return out


def _scope_ok(scope, group, parcels):
    """Whether a use on this deed is in scope, and why.

    Every conditional scope is tested AT THE DEED rather than at the parcel,
    because each is a question about the transaction -- how many units it took,
    what the land it bought may become. A single parcel cannot answer those.
    """
    if scope == "in":
        return True, None
    if scope == "out":
        return False, out_reason(group["parcels"][0]["dor"])

    if scope == "bulk":
        # Counted over every condominium parcel on the deed, not just this use,
        # because a deed taking three retail bays and three office suites in one
        # building is a bulk sale of six units. The building changed hands
        # either way and the sub-codes are not the question.
        n = sum(1 for p in parcels
                if use_scope(p["desc"], p["dor"])[1] == "bulk")
        if n < BULK_MIN:
            return False, (
                f"{n} condominium unit{'s' if n != 1 else ''} on the deed, "
                f"below the {BULK_MIN}-unit bulk threshold — an individual "
                f"unit sale, not a building")
        return True, f"{n} units on one deed, a bulk sale of the building"

    if scope == "mf":
        units = sum(p["units"] for p in parcels)
        if units < BULK_MIN:
            return False, (
                f"{units or len(parcels)} dwelling unit(s), below the brief's "
                f"{BULK_MIN}-unit multifamily floor")
        return True, f"{units} dwelling units"

    if scope == "site":
        # Zoning, never size. See note 2 in the module docstring.
        zones = [p["zone"] for p in parcels]
        houses = [z for z in zones if is_house_lot_zone(z)]
        if houses and len(houses) == len(zones):
            return False, (
                f"vacant land zoned {houses[0].strip()} — a house lot, which "
                f"permits only a detached home")
        other = next((z for z in zones if not is_house_lot_zone(z)), "")
        return True, (
            f"vacant land, a development site — "
            + (f"zoned {other.strip()}" if other.strip()
               else "the county states no zone for it, and it is not in "
                    "a house-lot zone"))

    return False, f"unrecognised scope {scope!r}"


def classify_deed(deed):
    """Decide a deed's asset class and whether it is in scope.

    Returns (category, raw_label, reason); category is None when the deed is
    out of scope, and reason always says why the decision went as it did.

    DOMINANCE IS DECIDED AMONG THE PARCELS THAT ARE IN SCOPE. Deciding it over
    every parcel first and testing scope afterwards throws real deals away: a
    $19M Miami Beach assembly of three former institutional buildings and one
    apartment block, all zoned MULTI-FAMILY 38-62 U/A, is dominated by area by
    the institutional parcels and would be dropped as institutional -- when
    what was bought is a multifamily development site. Three deeds were being
    lost that way before this was fixed.

    A deed where NOTHING is in scope can still be an acquisition: several
    parcels taken together under zoning permitting more than a detached house
    is a development site whatever happens to stand on them today. That is the
    vacant-land reasoning applied to an assembly carrying old buildings. A
    house plus a vacant lot, both zoned for houses, stays out -- the one case
    of that shape here, and it is a waterfront house lot.
    """
    parcels = deed["parcels"]
    groups = _groups(parcels)

    kept, refused = [], []
    for key, g in groups.items():
        desc, code = key
        cat, scope = use_scope(desc, code)
        if scope is None:
            refused.append((key, g, f"unrecognised use code {code!r} "
                                    f"({desc!r}) — not classified"))
            continue
        ok, why = _scope_ok(scope, g, parcels)
        (kept if ok else refused).append(
            (key, g, why or f"{desc} (DOR {dor2(code)})"))

    if kept:
        key, g, why = max(kept, key=lambda t: (t[1]["bsf"], t[1]["lsf"], t[1]["n"]))
        cat, _ = use_scope(*key)
        if len(groups) > 1:
            mix = ", ".join(
                f"{gg['n']}x {kk[0].title()}"
                for kk, gg, _ in sorted(kept + refused,
                                        key=lambda t: -t[1]["n"])[:4])
            why = f"{why}; deed also covers {mix}"
        return cat, property_type_label(*key), why

    # Nothing in scope on its own. An assembly in a zone permitting more than a
    # house is still a development site -- but ONLY if there is land in it.
    #
    # A CONDOMINIUM UNIT CONVEYS NO LAND, so a deed taking two condos cannot be
    # an assembly however the tower is zoned. Without that test this path
    # claimed four such deeds as development sites -- two units at 100 South
    # Pointe Drive, two at 1425 Brickell, two at 3901 South Flagler, two
    # warehouse condos on Clare Avenue -- because a high-rise multifamily zone
    # is not a single-family zone. They are individual unit sales that failed
    # the bulk test, and the bulk test's answer is the right one.
    zones = [p["zone"] for p in parcels]
    has_land = any(use_scope(p["desc"], p["dor"])[1] != "bulk"
                   and not is_condo_form(p["desc"], p["dor"])
                   for p in parcels)
    if (len(parcels) >= 2 and has_land
            and not all(is_house_lot_zone(z) for z in zones)):
        other = next((z for z in zones if not is_house_lot_zone(z)), "")
        key = max(groups.items(), key=lambda kv: (kv[1]["bsf"], kv[1]["lsf"]))[0]
        return AC.LAND, property_type_label(*key), (
            f"{len(parcels)} parcels assembled on one deed — a development "
            f"site; the existing buildings are not what was bought"
            + (f", zoned {other.strip()}" if other.strip() else ""))

    key, g, why = max(refused, key=lambda t: (t[1]["bsf"], t[1]["lsf"], t[1]["n"]))
    return None, property_type_label(*key), why


def arms_length_dade(sale):
    """(is_arms_length, reason) from Miami-Dade's own qualification flag."""
    flag = str(sale.get("QualifiedFlag") or "").strip().upper()
    why = str(sale.get("QualificationDescription") or "").strip()
    if flag == "Q":
        return True, None
    if flag == "U":
        return False, why or "disqualified by the county, reason not stated"
    return None, None


def metrics(category, price, building_sf, land_sf, units, keys):
    """Price per square foot, per unit and per key, under the brief's rules.

    Per square foot is on BUILDING area, except on land, where there is no
    building and the denominator is land area -- labelled as land SF so the two
    can never be read as the same measure. Hotels price per key.
    """
    out = {"price_per_sf": None, "psf_basis": None,
           "price_per_unit": None, "ppu_basis": None}
    if not price:
        return out

    if category == AC.LAND:
        if land_sf:
            out["price_per_sf"] = round(price / land_sf, 2)
            out["psf_basis"] = "land SF"
    elif building_sf:
        out["price_per_sf"] = round(price / building_sf, 2)
        out["psf_basis"] = "building SF"

    if category == AC.HOTEL and keys:
        out["price_per_unit"] = round(price / keys, 2)
        out["ppu_basis"] = "per key"
    elif category == AC.MULTIFAMILY and units:
        out["price_per_unit"] = round(price / units, 2)
        out["ppu_basis"] = "per unit"
    return out


def prior_sale(history, current_date, current_price, parcel_count, year_built):
    """The previous recorded sale, and whether a basis change may be computed.

    history is a list of (date, price, qualified_flag, seller) tuples, the
    shape both counties reduce to. Returns a dict so a caller cannot confuse
    the positional fields.
    """
    earlier = [h for h in history or [] if h[0] and h[0] < current_date]
    if not earlier:
        return {"date": None, "price": None, "flag": None, "seller": None,
                "pct": None, "not_comparable": "no_prior"}

    when, price, flag, seller = max(earlier, key=lambda h: h[0])
    price = int(price or 0)
    flag = str(flag or "").strip().upper()
    base = {"date": when, "price": price, "flag": flag, "seller": seller,
            "pct": None, "not_comparable": None}

    if price <= 0:
        base["not_comparable"] = "prior_zero"
        return base
    if price <= NOMINAL_MAX:
        base["not_comparable"] = "prior_nominal"
        return base
    if flag == "U":
        base["not_comparable"] = "prior_unqualified"
        return base

    # A building postdating the earlier sale means that sale bought land.
    # YearBuilt is "MULTI." on parcels carrying several structures, which says
    # nothing about a date and is left alone.
    m = re.fullmatch(r"(\d{4})", str(year_built or "").strip())
    if m and int(m.group(1)) > when.year:
        base["not_comparable"] = "built_after"
        return base

    # A deed assembling several parcels can only be compared with an earlier
    # sale if those parcels traded together then too. Establishing that needs
    # every parcel's history to agree, which the set as a whole does not
    # supply, so the change is withheld rather than guessed.
    if parcel_count > 1:
        base["not_comparable"] = "parcel_set_differs"
        return base

    base["pct"] = round((current_price - price) / price * 100.0, 1)
    return base
