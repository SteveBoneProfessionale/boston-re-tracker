r"""
Rhode Island project identity: parcel parsing, matching and deduplication.

A project comes before a board repeatedly -- Master Plan one year, Preliminary
the next, then extension requests, then Final Plan. One project record per
agenda item would multiply the pipeline count and inflate total SF by the same
factor, so identity has to collapse those appearances onto one record.

IDENTITY KEY
------------
Normalized Assessor's Plat + Lot(s) plus municipality. Address is a SECONDARY
matcher, because address strings vary too much between filings to be primary.
Both of these are real pairs from the harvested corpus, and both are one project:

    AP 89 Lots 380, 381 and 383 at 165 Alger Ave
    AP 89 Lots 380, 381 and 383 at 99 Dixon Street     <- corner parcel, two frontages

    AP 95 Lots 631 and 668 at 80 Erastus Street
    AP 95 Lots 631 and 668 at 80 and 88 Erastus Street <- address grew, parcel didn't

Address-primary matching would split both. Plat+lot merges them.

MATCHING IS BY LOT OVERLAP, NOT LOT EQUALITY
--------------------------------------------
A filing often names a subset of the project's lots, and lots legitimately
change mid-project through consolidation and subdivision. Requiring equal lot
sets would split a multi-lot project the moment one filing mentioned one lot.
These corpus pairs share a plat and overlap on lots, and are the same project:

    AP 68 Lots 1 and 131 at 383 Smith Street
    AP 68 Lots 1 and 31  at 383 Smith St and 52 No...   <- 131/31, overlap {1}

WARWICK IS INVERTED
-------------------
Warwick agendas carry plat/lot on only 19% of development items, against
85-100% elsewhere. There, address is the PRIMARY key and plat is secondary, and
every Warwick match is flagged for manual review at a higher rate because
address matching is inherently weaker.
"""

import re
import sys
import logging
from pathlib import Path
from dataclasses import dataclass, field

sys.path.insert(0, str(Path(__file__).parent.parent))

log = logging.getLogger(__name__)

# Plat notation differs by municipality and shares no common token:
#   Providence  "AP 68 Lot 846"
#   Pawtucket   "Tax Assessors Plat 44, Lot 561"
#   Newport     "TAP 34, Lot 13"
#   Cranston    "Plat 12, Lot 3"
#   Warwick     "Plat 288 Lot 485"
_PLAT = re.compile(
    r"\b(?:TAP|A\.?P\.?|(?:tax\s*)?assessor'?s?\s*plat|plat)\s*\.?\s*#?\s*(\d+)",
    re.I)
# Lots run as "Lot 5", "Lots 291 & 309", "Lots 380, 381 and 383".
# Matching a number, then only continuing across a separator when another
# NUMBER follows, is what distinguishes a lot list from a trailing clause:
# "Lot 846, Smith Hill" must yield {846}, not swallow the neighborhood.
_LOTS = re.compile(
    r"\blots?\s*\.?\s*#?\s*(\d+(?:\s*(?:,|&|and)\s*\d+)*)", re.I)
_LOT_NUM = re.compile(r"\d+")

# Address normalization: enough to compare, not to display.
_ADDR_SUFFIX = {
    "STREET": "ST", "AVENUE": "AVE", "ROAD": "RD", "DRIVE": "DR", "LANE": "LN",
    "BOULEVARD": "BLVD", "PLACE": "PL", "COURT": "CT", "TERRACE": "TER",
    "HIGHWAY": "HWY", "SQUARE": "SQ", "PARKWAY": "PKWY", "CIRCLE": "CIR",
}
_ADDR_NOISE = re.compile(
    r"\b(unit|apt|suite|ste|#|floor|fl|building|bldg|rear|the)\b\.?", re.I)

# Municipalities where address is the primary key rather than plat/lot.
ADDRESS_PRIMARY = {"Warwick"}


@dataclass(frozen=True)
class ParcelId:
    municipality: str
    plat: str | None
    lots: frozenset = field(default_factory=frozenset)
    address_key: str = ""
    raw: str = ""

    @property
    def has_parcel(self) -> bool:
        return bool(self.plat and self.lots)


def parse_plat_lots(text: str) -> tuple[str | None, set[str]]:
    """Extract (plat, {lots}) from agenda wording.

    Returns (None, set()) when the text carries no parcel reference, rather
    than guessing -- a missing parcel is a real and common state, especially in
    Warwick.
    """
    if not text:
        return None, set()
    pm = _PLAT.search(text)
    plat = pm.group(1).lstrip("0") or "0" if pm else None

    lots: set[str] = set()
    # Search after the plat where possible, so a street number is not mistaken
    # for a lot number.
    tail = text[pm.end():] if pm else text
    lm = _LOTS.search(tail)
    if lm:
        for n in _LOT_NUM.findall(lm.group(1)):
            lots.add(n.lstrip("0") or "0")
    return plat, lots


def normalize_address(address: str) -> str:
    """Comparison key for an address. Not for display."""
    if not address:
        return ""
    a = address.upper()
    a = re.sub(r"\(.*?\)", " ", a)
    a = _ADDR_NOISE.sub(" ", a)
    a = re.sub(r"[.,]", " ", a)
    # "337 to 341 Douglas" / "22-24 Desoto" -> keep the first number only, so a
    # range and its first address compare equal. re.I matters: the string has
    # already been upper-cased, so a lowercase "to" in the pattern never fires.
    a = re.sub(r"^(\s*\d+)\s*(?:-|–|TO)\s*\d+", r"\1", a, flags=re.I)
    # "80 and 88 Erastus Street" -> "80 Erastus Street". A filing that lists two
    # street numbers for one parcel must compare equal to one naming only the
    # first, which is how the same Erastus Street project appears twice.
    a = re.sub(r"^(\s*\d+)\s*(?:AND|&)\s*\d+\b", r"\1", a, flags=re.I)
    words = []
    for w in a.split():
        words.append(_ADDR_SUFFIX.get(w, w))
    a = " ".join(words)
    return re.sub(r"\s+", " ", a).strip()


def parcel_id(municipality: str, plat_lot_text: str, address: str) -> ParcelId:
    plat, lots = parse_plat_lots(plat_lot_text or "")
    return ParcelId(
        municipality=(municipality or "").strip(),
        plat=plat,
        lots=frozenset(lots),
        address_key=normalize_address(address),
        raw=(plat_lot_text or "").strip(),
    )


def same_project(a: ParcelId, b: ParcelId) -> tuple[bool, str, bool]:
    """Do two parcel identities describe the same project?

    Returns (match, reason, needs_review).
    """
    if a.municipality != b.municipality or not a.municipality:
        return False, "different municipality", False

    address_primary = a.municipality in ADDRESS_PRIMARY

    plat_match = bool(a.plat and b.plat and a.plat == b.plat)
    lot_overlap = a.lots & b.lots
    addr_match = bool(a.address_key and a.address_key == b.address_key)

    if address_primary:
        # Warwick: address leads, plat corroborates. Every match here is
        # flagged, because address matching is weaker than parcel matching.
        if addr_match and plat_match:
            return True, "address match, plat corroborates", True
        if addr_match:
            return True, "address match (no plat on one or both filings)", True
        if plat_match and lot_overlap:
            return True, f"plat {a.plat} with lot overlap {sorted(lot_overlap)}", True
        return False, "no address or parcel match", False

    # Everywhere else: parcel leads.
    if plat_match and lot_overlap:
        exact = a.lots == b.lots
        return True, (
            f"plat {a.plat}, lots {'identical' if exact else 'overlap'} "
            f"{sorted(lot_overlap)}"), not exact
    if plat_match and (not a.lots or not b.lots):
        # Same plat, one filing gave no lots. Plausible but not proven.
        if addr_match:
            return True, f"plat {a.plat} and matching address, lots absent on one filing", True
        return False, f"plat {a.plat} but no lots to compare and addresses differ", False
    if addr_match and not (a.plat and b.plat):
        return True, "address match, no parcel on one or both filings", True
    if plat_match and a.lots and b.lots and not lot_overlap:
        # Same plat, disjoint lots: adjacent parcels, or a subdivision. Not a
        # match, but worth surfacing rather than silently separating.
        return False, f"plat {a.plat} but disjoint lots — possible subdivision", True
    return False, "no parcel or address match", False


def collapse(items: list[dict]) -> list[dict]:
    """Group raw agenda items into projects.

    `items` carry municipality, plat_lot_text, address, and whatever else the
    caller wants preserved. Returns one group per project, each with the member
    items and the match reasons that joined them.
    """
    groups: list[dict] = []
    for item in items:
        pid = parcel_id(item.get("municipality", ""),
                        item.get("plat_lot_text", ""),
                        item.get("address", ""))
        placed = False
        for g in groups:
            # Guard against transitive over-merging. Matching is pairwise, so
            # without this a chain A-B-C can pull two genuinely different
            # parcels into one group through a weak middle link -- observed in
            # Warwick, where plat 215 lot 33 and plat 311 lot 191 shared an
            # address string and merged. A group may never contain two
            # different plats.
            group_plats = {p.plat for p in g["parcels"] if p.plat}
            if pid.plat and group_plats and pid.plat not in group_plats:
                continue

            for member in g["parcels"]:
                ok, reason, review = same_project(pid, member)
                if ok:
                    g["items"].append(item)
                    g["parcels"].append(pid)
                    g["reasons"].append(reason)
                    g["needs_review"] = g["needs_review"] or review
                    placed = True
                    break
            if placed:
                break
        if not placed:
            groups.append({"items": [item], "parcels": [pid],
                           "reasons": [], "needs_review": False,
                           "municipality": pid.municipality})
    return groups
