r"""Primary-record clients for South Florida acquisitions.

Two counties, two systems, both free and both first-party. Neither is a
documented API; both are the backends of the counties' own public search
applications, found by reading those applications' JavaScript. The exact
request shapes are recorded here because they are not guessable and because
each one cost a round of probing to establish.

MIAMI-DADE  (Miami, Miami Beach, Coral Gables, Doral)
-----------------------------------------------------
The Property Appraiser's Comparable Sales application posts JSON to

    apps.miamidadepa.gov/PAComparableSalesProxy/ComparableSales.ashx
        ?Operation=ComparableSalesFolioMatch&clientAppName=ComparableSalesSearch

Four things about it are load-bearing and none are obvious:

1. UNSET NUMERICS ARE -1, NOT "" OR null. The application's SearchParameter
   constructor initialises every numeric bound to -1. Sending an empty string
   instead makes the upstream service return HTTP 400, which the proxy relays
   as an HTML error page under HTTP 200 -- so a naive client sees success and
   some HTML. _post_json() detects that and raises.

2. SelectedFolio IS MANDATORY. The service answers "what sold near THIS
   property", so it validates a subject folio before anything else. Validation
   runs folio, then sale year, then area filter, which is why a request missing
   all three complains only about the folio. ANCHOR below is an arbitrary real
   Miami folio present in every roll year of interest: a key to open the door,
   and no part of the results.

3. Cities IS ACCEPTED BUT THE APPLICATION NEVER SENDS IT. The service's own
   validation message names six area filters -- Cities, TRSs, Subdivisions,
   ZipCode, Land Use, Primary Zoning -- while the application only ever sends a
   geometry-derived folio list. Passing Cities turns a nearby-comps lookup into
   a municipality-wide sales query, which is what makes a city-by-city spine
   possible at all.

4. SalesYearMin/Max IS THE YEAR OF SALE, not the tax roll year. Verified: a
   request for 2026 returns only 2026 sale dates, and one for 2025 returns only
   2025 ones.

There is no result cap -- an unrestricted Coral Gables year returns 1,041 rows
-- but an unrestricted query against a large city times out upstream, so the
price floor is not merely a filter, it is what lets the request finish.

Per-folio sale HISTORY, which prior sale and basis change need, is absent from
those results: each row carries only the sale that matched. History comes from
the Property Appraiser's other proxy,

    apps.miamidadepa.gov/PApublicServiceProxy/PaServicesProxy.ashx
        ?Operation=GetPropertySearchByFolio&folioNumber=<13 digits>

which returns every recorded sale for a folio, each with its qualification
code.

PALM BEACH COUNTY  (West Palm Beach)
-------------------------------------
pbcpao.gov/AdvSearch/GetSalesSearch takes an ordinary form POST and answers
with an HTML page carrying its results as a JSON literal, var SearchResults =
[...].

The field that matters is PropertyType: R, C or V for the Residential,
Commercial and Vacant tabs. It is a hidden input the page sets from the tab, it
defaults to empty, and an empty value matches NOTHING -- a search correct in
every other respect comes back "No Results matched your search criteria" with
no error at all. All three tabs must be queried and merged, because a
commercial deal can sit under R (apartment buildings) or V (development land).

The page carries TWO forms. The one with the 43-entry SelectedCity code select
belongs to a different search; GetSalesSearch wants Municipality by NAME, and
silently ignores the code.

Palm Beach rows carry no qualification code and no grantor, so SaleFilter does
the arm's-length work instead: QS returns county-qualified sales only, AS every
sale. Both are fetched, and a parcel in AS but not in QS is recorded as
disqualified by the county.

RATE AND COST
-------------
Every request here is an unauthenticated GET or POST to a public government
endpoint. Nothing is paid for, no login is used, and no access control is
circumvented. Requests run sequentially with a pause between them, and raw
responses are cached on disk so a re-run does not re-ask the counties for bytes
they have already given.
"""

import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE = os.path.join(ROOT, "data", "cache", "acq_sofl")

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
PAUSE = 0.7

# ── Miami-Dade ──────────────────────────────────────────────────────────────

DADE_SALES = ("https://apps.miamidadepa.gov/PAComparableSalesProxy/ComparableSales.ashx"
              "?Operation=ComparableSalesFolioMatch&clientAppName=ComparableSalesSearch")
DADE_FOLIO = ("https://apps.miamidadepa.gov/PApublicServiceProxy/PaServicesProxy.ashx"
              "?Operation=GetPropertySearchByFolio&clientAppName=PropertySearch"
              "&folioNumber={folio}")
DADE_REFERER = "https://apps.miamidadepa.gov/ComparableSales/index.html"

# 50 NW 14 ST, Miami. See note 2 above: a door key, not a datum.
ANCHOR = "0131360090420"

# MUNICID from MD_ComparableSales/MapServer/2 (MunicipalityCoastal_poly).
DADE_CITIES = {"Miami": "01", "Miami Beach": "02",
               "Coral Gables": "03", "Doral": "35"}

PBC_SEARCH = "https://pbcpao.gov/AdvSearch/GetSalesSearch"
PBC_FORM = "https://pbcpao.gov/AdvSearch/SalesSearch"
PBC_CITIES = {"West Palm Beach": "West Palm Beach"}


def _cache_path(key):
    os.makedirs(CACHE, exist_ok=True)
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", key)
    return os.path.join(CACHE, safe + ".json")


def cached(key, fn, refresh=False):
    """Run fn() once and keep its result on disk under key."""
    path = _cache_path(key)
    if not refresh and os.path.exists(path):
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    value = fn()
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(value, fh)
    os.replace(tmp, path)
    time.sleep(PAUSE)
    return value


def search_params(**over):
    """The full SearchParameter the Comparable Sales service expects.

    Every key is present and every unset numeric is -1, because a partial or
    empty-string payload is rejected upstream with a 400 that arrives disguised
    as an HTML page. See note 1 in the module docstring.
    """
    p = {"ClientAppName": "ComparableSalesSearch",
         "SelectedFolio": ANCHOR, "Folios": [],
         "SalesYearMin": None, "SalesYearMax": None,
         "SalesAmountMin": -1, "SalesAmountMax": -1,
         "BedroomMin": -1, "BedroomMax": -1, "BathMin": -1, "BathMax": -1,
         "LotSizeMin": -1, "LotSizeMax": -1,
         "AreaActualMin": -1, "AreaActualMax": -1,
         "AreaLivingMin": -1, "AreaLivingMax": -1,
         "AreaAdjustedMin": -1, "AreaAdjustedMax": -1,
         "YearBuiltMin": -1, "YearBuiltMax": -1,
         "DORDescription": None, "SalesQualified": None,
         "SalesQualifiedDesc": None, "Improved": None, "ZipCode": None,
         "Distance": "1320", "DistanceCode": "1/4 mile",
         "ZipCodeFlag": False, "MunicipalityFlag": False,
         "PrimaryZones": None, "ZoneCode": None}
    p.update(over)
    return {"comparableSalesFolioMatchInput": p}


def _post_json(url, payload, referer, timeout=300):
    req = urllib.request.Request(
        url, data=json.dumps(payload).encode(), method="POST",
        headers={"User-Agent": UA, "Referer": referer,
                 "Content-Type": "application/json; charset=utf-8"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read().decode("utf-8", "replace")
    if raw.lstrip().startswith("<"):
        # The proxy answers an upstream failure with an HTML auto-submitting
        # form carrying the message, under HTTP 200. Surface it as an error
        # rather than let a caller parse nothing out of it.
        m = re.search(r"value='([^']*Exception[^']*)'", raw)
        raise RuntimeError(m.group(1) if m else raw[:200])
    return json.loads(raw)


def dade_city_sales(city, year, min_price, refresh=False):
    """Every recorded sale at or above min_price in one city in one year."""
    code = DADE_CITIES[city]

    def fetch():
        payload = search_params(Cities=[code], SalesYearMin=str(year),
                                SalesYearMax=str(year),
                                SalesAmountMin=int(min_price))
        j = _post_json(DADE_SALES, payload, DADE_REFERER)
        res = j.get("ComparableSalesFolioMatchResult") or {}
        if not res.get("Completed"):
            raise RuntimeError(f"{city} {year}: {res.get('Message')!r}")
        return res.get("ComparableSalesFolioMatch") or []

    return cached(f"dade_{code}_{year}_{int(min_price)}", fetch, refresh)


def dade_folio_detail(folio, refresh=False):
    """Full record for one folio, including its entire sale history."""
    folio = re.sub(r"\D", "", str(folio))

    def fetch():
        req = urllib.request.Request(
            DADE_FOLIO.format(folio=folio),
            headers={"User-Agent": UA, "Referer": DADE_REFERER})
        with urllib.request.urlopen(req, timeout=120) as r:
            return json.loads(r.read().decode("utf-8", "replace"))

    return cached(f"dade_folio_{folio}", fetch, refresh)


# ── Palm Beach County ───────────────────────────────────────────────────────

_PBC_OPENER = None


def _pbc_opener():
    import http.cookiejar
    cj = http.cookiejar.CookieJar()
    op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    op.addheaders = [("User-Agent", UA), ("Referer", PBC_FORM)]
    op.open(PBC_FORM, timeout=60).read()        # pick up the session cookies
    return op


def pbc_city_sales(city, prop_type, sale_filter, date_from, date_to,
                   min_price, refresh=False):
    """One tab of the Palm Beach sales search.

    prop_type is R, C or V -- see the module docstring; an empty value matches
    nothing at all. sale_filter is QS (county-qualified sales only) or AS
    (every sale).
    """

    def fetch():
        global _PBC_OPENER
        if _PBC_OPENER is None:
            _PBC_OPENER = _pbc_opener()
        form = {"PropertyType": prop_type, "SaleSrchType": "MUNI",
                "Municipality": PBC_CITIES[city],
                "SubDivision": "", "StreetName": "", "Zip": "",
                "Range": "", "Township": "", "Section": "",
                "SelectedSaleDate": "Custom Data Range",
                "SaleDateFrom": date_from, "SaleDateTo": date_to,
                "SalePriceFrom": str(int(min_price)), "salePriceTo": "",
                "SqftFrom": "", "SqftTo": "", "UseType": "",
                "SaleFilter": sale_filter}
        req = urllib.request.Request(
            PBC_SEARCH, data=urllib.parse.urlencode(form).encode(),
            headers={"Content-Type": "application/x-www-form-urlencoded"})
        html = _PBC_OPENER.open(req, timeout=300).read().decode("utf-8", "replace")
        if "No Results matched" in html:
            return []
        m = re.search(r"var\s+SearchResults\s*=\s*(\[.*?\]);", html, re.S)
        if not m:
            raise RuntimeError(f"{city} {prop_type}/{sale_filter}: no result array")
        return json.loads(m.group(1))

    key = (f"pbc_{city}_{prop_type}_{sale_filter}_{date_from}_{date_to}"
           f"_{int(min_price)}")
    return cached(key, fetch, refresh)


PBC_DETAIL = ("https://pbcpao.gov/MasterSearch/MasterSearch"
              "?searchvalue=&proptype=RE&pcn={pcn}")


def pbc_parcel_detail(pcn, refresh=False):
    """Full record for one Palm Beach parcel, from its detail page.

    The page carries everything as one JSON literal, `var model = {...}`:
    propertyDetail (including UseCode, which is the same four-digit Florida DOR
    code Miami-Dade publishes, plus zoning and units), salesInfo (the complete
    recorded sale history), structuralDetails and landDetails.

    salesInfo is where the SELLER comes from. Palm Beach publishes no grantor
    field at all, but each sale record carries the OwnerName it created -- so
    the owner named on the immediately preceding sale is the party that sold.
    """
    pcn = re.sub(r"\D", "", str(pcn))

    def fetch():
        req = urllib.request.Request(
            PBC_DETAIL.format(pcn=pcn),
            headers={"User-Agent": UA, "Referer": PBC_FORM})
        with urllib.request.urlopen(req, timeout=120) as r:
            html = r.read().decode("utf-8", "replace")
        marker = "var model = "
        if marker not in html:
            raise RuntimeError(f"{pcn}: no model block on the detail page")
        i = html.index(marker) + len(marker)
        depth = 0
        for j in range(i, len(html)):
            if html[j] == "{":
                depth += 1
            elif html[j] == "}":
                depth -= 1
                if depth == 0:
                    return json.loads(html[i:j + 1])
        raise RuntimeError(f"{pcn}: unterminated model block")

    return cached(f"pbc_parcel_{pcn}", fetch, refresh)
