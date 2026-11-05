r"""Build South Florida acquisition rows from county records.

    python -m scraper.acq_sofl_build --report
    python -m scraper.acq_sofl_build --write

Reads the two county sources, groups parcel rows into deeds, applies the
brief's scope rules, computes the brief's measures, and emits one row per
transaction ready for the transactions table. --report prints what it found and
writes nothing.

THE TWO COUNTIES HAND BACK DIFFERENT SHAPES, so each has its own reader and
both reduce to the same deed dict before any rule is applied:

    {"key", "date", "price", "book", "page", "buyer", "seller",
     "arms_length", "non_arms_length_reason", "parcels": [...]}

Miami-Dade gives everything in one response: grantor, grantee, book, page, the
qualification flag and its reason. Palm Beach gives a stripped search result --
price, date, buyer, parcel, area -- and the rest has to be assembled:

  * BOOK AND PAGE are not in the search result at all. They come from the
    parcel's own sale history, matched on the sale date, and they are what
    makes deed grouping possible on that side.

  * THE SELLER IS NOT PUBLISHED. Palm Beach has no grantor field anywhere. But
    its sale history records the OwnerName each sale created, so the owner named
    on the immediately PRECEDING sale is the party that sold. That is a
    derivation, not a quotation, so it is recorded with its own basis --
    "prior owner of record" -- and never presented as a stated grantor.

  * ARM'S LENGTH is a filter rather than a field: the county's qualified-sales
    query returns a subset of its all-sales query, and a parcel missing from
    that subset has been disqualified. The reason is not published, so the row
    says only that the county disqualified it.

WHAT IS NOT ATTEMPTED HERE, AND WHY
-----------------------------------
The brief puts county records first as the spine, with trade press second for
the names behind single-purpose LLCs and for partial stakes. This module builds
the spine ONLY. Nearly every buyer and seller here is an LLC formed for the
transaction -- "545 WYN OWNER LLC", "DEAN MCG PROPCO LLC" -- so the sponsor
behind the entity is usually not in the public record, and no amount of
re-reading the record will produce it.

So buyer and seller are recorded exactly as the deed names them, with
confidence "county_record" and resolution basis "deed", and the canonical
columns are LEFT EMPTY rather than filled with a guess. Filling them from
pattern-matching entity names would manufacture attributions the record does
not support. The partial-stake columns are empty for the same reason: neither
county publishes a conveyed percentage, and inferring one from a price would be
inventing it.
"""

import argparse
import collections
import datetime as dt
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from app import asset_class as AC                     # noqa: E402
from scraper import acq_sofl_rules as R               # noqa: E402
from scraper import acq_sofl_sources as S             # noqa: E402

WINDOW_FROM = dt.date(2026, 1, 1)
WINDOW_TO = dt.date(2026, 11, 5)
PRICE_FLOOR = 5_000_000
YEAR = 2026

SOURCE_DADE = "miamidade_pa"
SOURCE_PBC = "pbcpao"

# PALM BEACH DEEP-LINKS; MIAMI-DADE DOES NOT. Both were tested rather than
# assumed, because a status code says nothing about whether a single-page app
# loaded the record you asked for.
#
# Palm Beach serves MasterSearch?pcn=... from the server with the parcel's data
# in the page -- that is how this module reads sale history at all -- so the
# link lands on the record. Verified by fetching it and finding the address.
#
# Miami-Dade has no working folio link. apps.miamidadepa.gov/PAPropertySearch
# is a 404. Its live Property Search app answers 200 at two addresses, but its
# Angular router declares exactly two routes, "" and "**" -- there is no folio
# route, so a #/?folio=... fragment opens an empty search no matter what
# follows it. The one county app that DOES route on a folio, Comparable Sales
# at #/comparablesales/<folio>, forwards it by POST to
# gisims2.miamidade.gov/Myneighborhood/salesmap.asp, and that host no longer
# resolves -- the county's own deep link is broken.
#
# So Miami-Dade rows link to the search app itself and carry the folio in
# parcel_id for the reader to paste. A fabricated deep link that silently opens
# the wrong page is worse than an honest two-step.
DADE_URL = "https://www.miamidade.gov/Apps/PA/propertysearch/"
PBC_URL = ("https://pbcpao.gov/MasterSearch/MasterSearch"
           "?searchvalue=&proptype=RE&pcn={pcn}")


# ── Miami-Dade ──────────────────────────────────────────────────────────────

def deeds_dade(city, verbose=True):
    """Every deed at or above the floor in one Miami-Dade city, in window."""
    rows = S.dade_city_sales(city, YEAR, PRICE_FLOOR)
    deeds = {}
    for row in rows:
        info = row.get("PropertyInfo") or {}
        addrs = row.get("SiteAddress") or []
        for sale in (row.get("SalesInfo") or []):
            when = R.to_date(sale.get("DateOfSale"))
            if when is None or when < WINDOW_FROM or when > WINDOW_TO:
                continue
            key = R.deed_key(sale.get("OfficialRecordBook"),
                             sale.get("OfficialRecordPage"),
                             sale.get("DateOfSale"), sale.get("SalePrice"),
                             sale.get("GrantorName1"), sale.get("GranteeName1"))
            al, why = R.arms_length_dade(sale)
            deed = deeds.setdefault(key, {
                "key": key, "city": city, "county": "Miami-Dade",
                "source": SOURCE_DADE, "date": when,
                "price": int(sale.get("SalePrice") or 0),
                "book": str(sale.get("OfficialRecordBook") or "").strip() or None,
                "page": str(sale.get("OfficialRecordPage") or "").strip() or None,
                "doc_type": (sale.get("SaleInstrument") or "").strip() or None,
                "buyer": _names(sale.get("GranteeName1"), sale.get("GranteeName2")),
                "seller": _names(sale.get("GrantorName1"), sale.get("GrantorName2")),
                "seller_basis": "deed",
                "arms_length": al, "non_arms_length_reason": why,
                "parcels": [],
            })
            # One instrument recorded against two dates takes the earlier of
            # them: the recording is a single event and the first date is when
            # it happened. See deed_key on why this can occur at all.
            if when < deed["date"]:
                deed["date"] = when
            deed["parcels"].append({
                "id": _digits(info.get("FolioNumber")),
                "id_fmt": info.get("FolioNumber"),
                "desc": (info.get("DORDescription") or "").strip(),
                "dor": str(info.get("DORCode") or ""),
                "building_sf": _int(info.get("BuildingActualArea")),
                "land_sf": _int(info.get("LotSize")),
                "units": _int(info.get("UnitCount")),
                "year_built": str(info.get("YearBuilt") or "").strip(),
                "zone": (info.get("PrimaryZoneDescription") or "").strip(),
                "address": (addrs[0].get("Address") if addrs else None),
                "neighborhood": (info.get("NeighborhoodDescription") or "").strip(),
            })
    if verbose:
        print(f"  {city}: {len(rows)} parcel rows -> {len(deeds)} deeds",
              file=sys.stderr)
    return list(deeds.values())


def dade_history(folio):
    """(date, price, flag, seller) for every recorded sale of one folio."""
    try:
        detail = S.dade_folio_detail(folio)
    except Exception as exc:                            # noqa: BLE001
        print(f"  ! folio {folio}: {type(exc).__name__}: {exc}", file=sys.stderr)
        return []
    out = []
    for s in (detail.get("SalesInfos") or []):
        out.append((R.to_date(s.get("DateOfSale")), s.get("SalePrice"),
                    s.get("QualifiedFlag"),
                    _names(s.get("GrantorName1"), s.get("GrantorName2"))))
    return out


# ── Palm Beach County ───────────────────────────────────────────────────────

def deeds_pbc(city, verbose=True):
    """Every deed at or above the floor in West Palm Beach, in window.

    Three property-type tabs are merged because a commercial deal can sit under
    any of them, and the qualified-sales query is fetched alongside the
    all-sales query purely to learn which parcels the county disqualified.
    """
    frm = WINDOW_FROM.strftime("%m/%d/%Y")
    to = WINDOW_TO.strftime("%m/%d/%Y")

    allsales, qualified = {}, set()
    for ptype in ("C", "V", "R"):
        for row in S.pbc_city_sales(city, ptype, "AS", frm, to, PRICE_FLOOR):
            allsales.setdefault((row["ParcelId"], row["SaleDate"],
                                 row["SalePrice"]), row)
        for row in S.pbc_city_sales(city, ptype, "QS", frm, to, PRICE_FLOOR):
            qualified.add((row["ParcelId"], row["SaleDate"], row["SalePrice"]))

    deeds = {}
    for ident, row in allsales.items():
        pcn, date_text, price_text = ident
        when = R.to_date(date_text)
        if when is None or when < WINDOW_FROM or when > WINDOW_TO:
            continue
        try:
            model = S.pbc_parcel_detail(pcn)
        except Exception as exc:                        # noqa: BLE001
            print(f"  ! pcn {pcn}: {type(exc).__name__}: {exc}", file=sys.stderr)
            continue
        detail = model.get("propertyDetail") or {}
        history = _pbc_history(model)

        # Book and page are absent from the search result; they come from the
        # parcel's own history, matched on this sale's date.
        book = page = None
        for h in model.get("salesInfo") or []:
            if R.to_date(h.get("SaleDate")) == when:
                book = str(h.get("Book") or "").strip() or None
                page = str(h.get("Page") or "").strip() or None
                break

        price = _int(price_text)
        key = R.deed_key(book, page, date_text, price, None, row.get("OwnerName"))
        prior = R.prior_sale(history, when, price, 1, None)
        deed = deeds.setdefault(key, {
            "key": key, "city": city, "county": "Palm Beach",
            "source": SOURCE_PBC, "date": when, "price": price,
            "book": book, "page": page,
            "doc_type": _pbc_doc_type(model, when),
            "buyer": (row.get("OwnerName") or "").strip() or None,
            # Palm Beach publishes no grantor. The owner the previous sale
            # created is the party that sold; recorded as a derivation.
            "seller": prior.get("seller"),
            "seller_basis": "prior owner of record",
            "arms_length": ident in qualified,
            "non_arms_length_reason": (
                None if ident in qualified
                else "the county's qualified-sales query excludes this sale; "
                     "Palm Beach does not publish the reason"),
            "parcels": [],
        })
        if when < deed["date"]:
            deed["date"] = when
        deed["parcels"].append({
            "id": _digits(pcn),
            "id_fmt": detail.get("FormattedPCN") or pcn,
            "desc": (detail.get("UseCodeDesc") or "").strip(),
            "dor": str(detail.get("UseCode") or ""),
            "building_sf": _int(row.get("SQFT")) or _int(detail.get("SqFt")),
            "land_sf": _pbc_land_sf(row, detail),
            "units": _int(detail.get("Units")),
            "year_built": _pbc_year_built(model),
            "zone": (detail.get("ZoningDesc") or "").strip(),
            "address": (row.get("Location") or "").strip() or None,
            "neighborhood": (detail.get("Subdivision") or "").strip(),
            # Carried on the parcel so the row builder does not have to fetch
            # the page again. Miami-Dade parcels have no history here and the
            # builder fetches theirs on demand.
            "history": history,
        })
    if verbose:
        print(f"  {city}: {len(allsales)} parcel rows -> {len(deeds)} deeds "
              f"({len(qualified)} county-qualified)", file=sys.stderr)
    return list(deeds.values())


def _pbc_history(model):
    out = []
    for s in model.get("salesInfo") or []:
        out.append((R.to_date(s.get("SaleDate")), s.get("Price"), None,
                    (s.get("OwnerName") or "").strip() or None))
    # Each record names the owner it CREATED, so the seller of a given sale is
    # the owner named on the one before it. Shift the names back by one, in
    # date order, so prior_sale() reads a seller rather than a buyer.
    out.sort(key=lambda h: (h[0] or dt.date.min))
    shifted = []
    for i, (when, price, flag, owner) in enumerate(out):
        seller = out[i - 1][3] if i else None
        shifted.append((when, price, flag, seller))
    return shifted


def _pbc_doc_type(model, when):
    for s in model.get("salesInfo") or []:
        if R.to_date(s.get("SaleDate")) == when:
            return (s.get("SaleType") or "").strip() or None
    return None


def _pbc_year_built(model):
    for e in model.get("extraDetails") or []:
        if e.get("YrBuilt"):
            return str(e["YrBuilt"]).strip()
    struct = (model.get("structuralDetails") or {}).get("StructuralElements") or []
    for e in struct:
        if e.get("ElementName") == "Year Built" and e.get("ElementValue"):
            return str(e["ElementValue"]).strip()
    return None


def _pbc_land_sf(row, detail):
    """Palm Beach reports a lot size, or acres, or neither."""
    lot = _int(row.get("LotSize"))
    if lot:
        return lot
    acres = row.get("Acres") or detail.get("Acres")
    try:
        return int(round(float(str(acres).replace(",", "")) * 43_560))
    except (TypeError, ValueError):
        return 0


# ── shared helpers ──────────────────────────────────────────────────────────

def _int(value):
    try:
        return int(float(str(value).replace(",", "").strip()))
    except (TypeError, ValueError):
        return 0


def _digits(value):
    return "".join(c for c in str(value or "") if c.isdigit())


def _names(*parts):
    seen = [str(p).strip() for p in parts if p and str(p).strip()]
    uniq = []
    for n in seen:
        if n not in uniq:
            uniq.append(n)
    return " & ".join(uniq) or None


def _street(raw):
    """A county address line, written the way a person writes an address.

    Three things the county's form gets wrong for a reader, all fixed here by
    the SAME formatter the Projects tab uses, so one address reads identically
    in both places:

    1. DIRECTIONALS AND ORDINALS. "545 NW 26 ST" is not a street anyone names.
       Plain .title() made it "545 Nw 26 St" -- the compass point lower-cased
       into a word and the street left as a bare number. title_case(address=
       True) gives "545 NW 26th St", because it knows NW is an abbreviation and
       that a bare number before a street type is an ordinal.

    2. A UNIT NUMBER IS NOT PART OF THE STREET. The appraiser appends the
       unit to the line -- "3265 VIRGINIA ST 17", "14 NE 1 AVE 100H", "3000
       FLORIDA AVE HOTEL" -- and on a deed covering 25 or 185 units naming one
       of them is worse than naming none. Everything after the LAST street-type
       token is dropped, which removes the unit without touching an address
       whose street type is genuinely final.

    3. Only the street line is kept; city, state and ZIP repeat the CITY column.
    """
    from app.detail_bits import _STREET_TYPE, title_case

    line = str(raw or "").split(",")[0].strip()
    if not line:
        return ""
    tokens = line.split()
    types = [i for i, t in enumerate(tokens)
             if t.strip(".,").upper() in _STREET_TYPE]
    if types and types[-1] < len(tokens) - 1:
        tokens = tokens[:types[-1] + 1]
    return title_case(" ".join(tokens), address=True)


def _place(raw):
    """A county neighbourhood name, trimmed to something a person would say.

    The appraiser's neighbourhood field is a surveyor's description rather
    than a name -- "Lincoln Rd-Washington Ave to Alton Rd-Front" marks out a
    block face. Everything from " to " onward is the far boundary, which says
    nothing about where the parcel is, so it goes.
    """
    from app.detail_bits import title_case

    text = " ".join(str(raw or "").split())
    text = re.split(r"\s+to\s+", text, maxsplit=1, flags=re.I)[0]
    text = re.sub(r"\s*-\s*", "-", text).strip(" -,")
    if not text or text.upper() == "UNCLASSIFIED NEIGHBORHOOD":
        return ""
    return title_case(text)


# A valuation block face, not a market area: "41 St Nw-Doral-(826) To
# Trnpk-Front", "2Nd Ave Ne-54 St To Ne Design Blvd-Back". The appraiser's
# NeighborhoodDescription mixes real neighbourhood names with these survey
# strings, which describe the stretch of street a parcel is valued against.
_BLOCK_FACE = re.compile(r"-(front|back|other)$|\bto\b", re.I)


def _submarket(raw):
    """The neighbourhood, but only when it is a PLACE and not a block face.

    SUBMARKET claims to be a market area -- the column help says BPDA
    neighbourhood in Boston, CDD neighbourhood in Cambridge. Miami-Dade's
    equivalent field is a VALUATION neighbourhood, and most of its values are
    a surveyor's description of a stretch of street rather than anywhere a
    person would name. Printing "8 St Sw-37 Ave To 76 Ct-Front" under that
    header asserts something false about what the column holds.

    So the named ones are kept -- Downtown North, Miami CBD, Wynwood -- and the
    block faces are left EMPTY, which says "no submarket recorded" instead of
    dressing a survey line up as one.
    """
    # TESTED ON THE RAW VALUE, not on the cleaned one. _place trims everything
    # from " to " onward, which is exactly the half that identifies a block
    # face -- so cleaning first destroys the evidence and "41 St Nw-Doral-(826)
    # To Trnpk-Front" sails through as "41 St Nw-Doral-(826)".
    if not str(raw or "").strip() or _BLOCK_FACE.search(str(raw)):
        return None
    return _place(raw) or None


def _address(deed):
    """One address for the deed: the largest parcel's, plus a parcel count."""
    parcels = deed["parcels"]
    best = max(parcels, key=lambda p: (p["building_sf"], p["land_sf"]))
    addr = _street(best.get("address"))
    if not addr:
        addr = next((_street(p.get("address")) for p in parcels
                     if _street(p.get("address"))), "")

    if not addr:
        # THREE DEEDS IN THIS WINDOW HAVE NO ADDRESS ANYWHERE. Not missing from
        # the sales feed -- absent from the property record too: the county's
        # SiteAddress for these folios is an empty string, because they are
        # vacant parcels it has never assigned a number to.
        #
        # A FOLIO IS NOT AN ADDRESS. It is Miami-Dade's parcel identifier, the
        # local equivalent of an APN, and printing "01-3137-036-0060" in a
        # column of street addresses asks the reader to decode a filing system
        # to learn where a building is. The neighbourhood answers the question
        # the column is actually asking, and the folio keeps its own home in
        # parcel_id, which the row detail prints under "parcel".
        hood = _place(next((p.get("neighborhood") for p in parcels
                            if p.get("neighborhood")), ""))
        addr = f"Vacant land, {hood}" if hood else "Vacant land, address not assigned"

    if len(parcels) > 1:
        addr = f"{addr} + {len(parcels) - 1} more parcel" + \
               ("s" if len(parcels) > 2 else "")
    return addr


def _source_url(deed):
    """Where a reader can see this record. See the note on DADE_URL."""
    if deed["source"] == SOURCE_DADE:
        return DADE_URL
    return PBC_URL.format(pcn=deed["parcels"][0]["id"])


# ── assembly ────────────────────────────────────────────────────────────────

def build(verbose=True):
    """Every in-scope transaction, plus the audit of what was excluded."""
    deeds = []
    if verbose:
        print("Miami-Dade:", file=sys.stderr)
    for city in S.DADE_CITIES:
        deeds += deeds_dade(city, verbose)
    if verbose:
        print("Palm Beach:", file=sys.stderr)
    for city in S.PBC_CITIES:
        deeds += deeds_pbc(city, verbose)

    rows, dropped = [], []
    for deed in deeds:
        cat, label, reason = R.classify_deed(deed)
        if not cat:
            dropped.append((deed, label, reason))
            continue
        rows.append(_row(deed, cat, label, reason, verbose))
    rows.sort(key=lambda r: (-(r["price"] or 0), r["sale_date"]))
    return rows, dropped


def _row(deed, cat, label, reason, verbose=True):
    parcels = deed["parcels"]
    n = len(parcels)
    building_sf = sum(p["building_sf"] for p in parcels)
    land_sf = sum(p["land_sf"] for p in parcels)
    stated_units = sum(p["units"] for p in parcels)

    # On a bulk condominium sale each folio IS one unit, and the county leaves
    # UnitCount at zero on most commercial condominiums -- so the parcel count
    # is the unit count there, and the stated count is used everywhere else.
    condo_units = sum(1 for p in parcels
                      if R.use_scope(p["desc"], p["dor"])[1] == "bulk")
    units = stated_units or (condo_units if condo_units >= R.BULK_MIN else 0)
    keys = units if cat == AC.HOTEL else 0

    big = max(parcels, key=lambda p: (p["building_sf"], p["land_sf"]))
    m = R.metrics(cat, deed["price"], building_sf, land_sf, units, keys)

    # Prior sale comes from the largest parcel's own recorded history. Palm
    # Beach parcels carry theirs already, read from the detail page while the
    # deed was assembled; Miami-Dade needs one extra request per folio, and it
    # is only made for single-parcel deeds, because a multi-parcel deed cannot
    # be compared with an earlier sale anyway -- see prior_sale.
    history = big.get("history")
    if history is None:
        history = dade_history(big["id"]) if n == 1 else []
    prior = R.prior_sale(history, deed["date"], deed["price"], n,
                         big["year_built"])

    notes = [reason]
    if m["psf_basis"] == "land SF":
        notes.append("Price per square foot is on LAND area, not building area: "
                     "this is a land sale.")
    if m["ppu_basis"] == "per key":
        notes.append(f"{keys} hotel keys; the per-unit figure is per key.")
    if prior["not_comparable"]:
        notes.append("Change in basis withheld: "
                     + R.NOT_COMPARABLE[prior["not_comparable"]] + ".")
    if n > 1:
        notes.append(f"One deed over {n} parcels. The price is the deed's, "
                     f"taken once; areas and units are summed across them.")
    if deed["seller_basis"] != "deed" and deed["seller"]:
        notes.append("The seller is not published by Palm Beach County; this "
                     "is the owner of record immediately before the sale.")
    notes.append("Documentary stamps are omitted: the county computes them "
                 "from the price, so they corroborate nothing.")
    if deed["source"] == SOURCE_DADE:
        notes.append(f"Miami-Dade publishes no working folio deep link, so the "
                     f"source link opens the Property Appraiser's search; "
                     f"search folio {big['id_fmt']}.")

    return {
        "address": _address(deed),
        "parcel_id": ";".join(p["id_fmt"] or p["id"] for p in parcels[:12]),
        "city": deed["city"],
        "transaction_type": "asset_sale",
        "sale_date": deed["date"].isoformat(),
        "sale_date_precision": "day",
        "sale_date_basis": "recorded deed date",
        "price": deed["price"],
        "buyer": deed["buyer"],
        "buyer_confidence": "county_record",
        "buyer_resolution_basis": "deed",
        "seller": deed["seller"],
        "seller_confidence": "county_record",
        "seller_resolution_basis": deed["seller_basis"],
        "property_type": label,
        # The decided class, stored because on an assembly the raw code
        # describes what still stands rather than what was bought. See
        # db/database.py on the asset_class column.
        "asset_class": cat,
        "building_sf": building_sf or None,
        "unit_count": units or None,
        "land_sf": land_sf or None,
        "price_per_sf": m["price_per_sf"],
        "price_per_unit": m["price_per_unit"],
        # NOT set on land sales. psf_unreliable means the recorded area is the
        # parcel's rather than the asset's, so the quotient describes nothing --
        # and the tab masks the figure and warns on it. On a land sale the
        # parcel IS the asset and dollars per land foot is how land trades, so
        # flagging it would hide a sound number behind a false warning. The
        # basis is distinguished by label instead; see land_sf below.
        "psf_unreliable": 0,
        "deed_book": deed["book"],
        "deed_page": deed["page"],
        "doc_type": deed["doc_type"],
        "arms_length": (None if deed["arms_length"] is None
                        else int(bool(deed["arms_length"]))),
        "non_arms_length_reason": deed["non_arms_length_reason"],
        "quarantined": 0 if deed["arms_length"] else 1,
        "quarantine_reason": (None if deed["arms_length"]
                              else deed["non_arms_length_reason"]),
        "prior_sale_date": prior["date"].isoformat() if prior["date"] else None,
        "prior_sale_price": prior["price"] or None,
        "prior_sale_source": ("county sale history"
                              if prior["date"] else None),
        "basis_change_pct": prior["pct"],
        "source": deed["source"],
        "source_url": _source_url(deed),
        "source_name": ("Miami-Dade County Property Appraiser"
                        if deed["source"] == SOURCE_DADE
                        else "Palm Beach County Property Appraiser"),
        "source_date": deed["date"].isoformat(),
        "confidence": "primary_record",
        "submarket": _submarket(big.get("neighborhood")),
        "notes": " ".join(notes),
        "_asset_class": cat,
        "_parcels": n,
    }


def report(rows, dropped):
    by_city = collections.Counter(r["city"] for r in rows)
    by_class = collections.Counter(r["_asset_class"] for r in rows)
    live = [r for r in rows if not r["quarantined"]]
    quar = [r for r in rows if r["quarantined"]]

    print(f"\n{'=' * 66}\nSOUTH FLORIDA ACQUISITIONS  "
          f"{WINDOW_FROM} to {WINDOW_TO}  floor ${PRICE_FLOOR:,}\n{'=' * 66}")
    print(f"\n{len(rows)} in-scope transactions "
          f"({len(live)} arm's-length, {len(quar)} quarantined)")
    print(f"{len(dropped)} deeds excluded as out of scope\n")

    print(f"{'CITY':<18}{'DEALS':>7}{'ARMS':>7}{'VOLUME':>18}")
    for city, n in by_city.most_common():
        vol = sum(r["price"] for r in live if r["city"] == city)
        arms = sum(1 for r in live if r["city"] == city)
        print(f"{city:<18}{n:>7}{arms:>7}{'$' + format(vol, ','):>18}")
    print(f"{'TOTAL':<18}{len(rows):>7}{len(live):>7}"
          f"{'$' + format(sum(r['price'] for r in live), ','):>18}")

    print("\nBY ASSET CLASS")
    for cls, n in by_class.most_common():
        vol = sum(r["price"] for r in live if r["_asset_class"] == cls)
        print(f"  {cls:<14}{n:>5} deals   ${vol:>15,}")

    print("\nBY PRICE BAND (arm's-length only)")
    bands = [(5_000_000, 10_000_000, "$5M – $10M"),
             (10_000_000, 25_000_000, "$10M – $25M"),
             (25_000_000, 50_000_000, "$25M – $50M"),
             (50_000_000, 100_000_000, "$50M – $100M"),
             (100_000_000, 10 ** 12, "$100M and above")]
    for lo, hi, lbl in bands:
        sel = [r for r in live if lo <= r["price"] < hi]
        print(f"  {lbl:<18}{len(sel):>5} deals   "
              f"${sum(r['price'] for r in sel):>15,}")

    print("\nEXCLUDED, BY REASON")
    why = collections.Counter(r.split("—")[0].strip()[:58] for _, _, r in dropped)
    for reason, n in why.most_common(12):
        print(f"  {n:>5}  {reason}")

    print("\nBASIS CHANGE")
    with_pct = [r for r in rows if r["basis_change_pct"] is not None]
    print(f"  {len(with_pct)} of {len(rows)} carry a comparable prior sale")
    held = collections.Counter()
    for r in rows:
        if r["basis_change_pct"] is None and "withheld" in (r["notes"] or ""):
            for k, text in R.NOT_COMPARABLE.items():
                if text in r["notes"]:
                    held[k] += 1
    for k, n in held.most_common():
        print(f"  {n:>5}  not comparable: {k}")
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", action="store_true")
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    rows, dropped = build(verbose=not args.quiet)
    if args.report or not args.write:
        report(rows, dropped)
    if args.write:
        from scraper import acq_sofl_load
        acq_sofl_load.write(rows)
    return 0


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.exit(main())
