r"""Populate transactions.submarket from official neighborhood boundaries.

FOUR WAYS A ROW GETS A POINT, in order of preference:

    1. latitude / longitude   Cambridge rows geocoded at load. 143 of 152.
    2. MassGIS LOC_ID         The parcel centroid encoded in parcel_id. Every
                              Boston row loaded from MassGIS L3 -- 610 of 641 --
                              carries one. See app/geo_massgis.py.
    3. Cambridge maplot       The Cambridge Master Address List (vup6-kpwv)
                              keyed on the map-lot parcel id, for the Socrata
                              rows that were never geocoded.
    4. Address point lookup   City of Boston SAM (Boston) or the same Cambridge
                              Master Address List (Cambridge), by street number
                              and street body, for press rows with no parcel.

CONSENSUS, BECAUSE BOSTON REPEATS ITS STREET NAMES. The first version of this
script took the first SAM row for an address and placed three high-value rows in
the wrong neighborhood: 505 Washington Street to Brighton, 18 Tremont Street to
Charlestown, 1000 Washington Street to Mattapan. SAM holds a 505 Washington
Street in Boston 02111, in Brighton 02135 AND in Dorchester 02124, and nothing
in the address string chooses between them.

    So an address lookup now fetches EVERY match, puts each through the polygon,
    and accepts the answer only if they all agree. Where they disagree the row
    is left null and reported as ambiguous.

    Where a row names more than one address -- "1000 Washington Street / 321
    Harrison Avenue" -- each component is resolved separately and an
    unambiguous component decides. That is what recovers the South End here,
    since Harrison Avenue occurs once and Washington Street occurs everywhere.

    SAM's own MAILING_NEIGHBORHOOD is deliberately NOT used. It is postal
    geography and disagrees with the BPDA layer the development side speaks --
    it files 1000 Washington Street under Roxbury where the BPDA polygon puts
    it in the South End. The coordinate is the source of truth; the polygon
    names it.

WHAT DOES NOT GET A POINT. A row whose address is a portfolio rather than a
place -- "Dorchester Avenue warehouse portfolio (10 sites)" -- has no single
point and is left null on purpose. So is any address the City's file does not
carry, and any address whose candidates disagree. Nothing here infers a
neighborhood from a street name.

    python scripts/derive_submarkets.py            # dry run, reports only
    python scripts/derive_submarkets.py --apply
"""

import argparse
import logging
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import requests
from sqlalchemy import text

from app.geo_massgis import state_plane_to_latlon
from app.submarket import (_BOSTON, _CAMBRIDGE, _load, _lookup, point_for,
                           submarket_for)
from db.database import engine

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

SAM_RESOURCE = "6d6cfc99-6f26-4974-bbb3-17b5dbad49a9"
SAM_URL = "https://data.boston.gov/api/3/action/datastore_search_sql"
CAM_URL = "https://data.cambridgema.gov/resource/vup6-kpwv.json"

# Addresses that name a portfolio rather than a place. Explicit, so the skip is
# a decision rather than a silent parse failure.
PORTFOLIO = re.compile(r"portfolio|\(\d+\s*sites?\)|and \d+ other", re.I)

# Rows the address file cannot decide, which the row's OWN BUILDING NAME does.
# These are not guesses from a street name -- each row carries the name of a
# specific, locatable building, and the override records which of the competing
# candidates that building is. Anything not decidable this way stays null.
OVERRIDE = {
    177: ("South Boston Waterfront",
          "One Marina Park Drive is Fan Pier, the Seaport tower; the address "
          "file carries the street but the parser needed the building name to "
          "reach it."),
    627: ("Downtown",
          "The Kensington, 665 Washington Street at Kneeland, Boston 02111. "
          "SAM also holds a 665 Washington Street in Brighton 02135; the "
          "Kensington is the downtown one."),
    890: ("Downtown",
          "The Godfrey Hotel, 505 Washington Street in Downtown Crossing, "
          "Boston 02111. SAM holds a 505 Washington Street in Brighton 02135 "
          "and another in Dorchester 02124."),
    181: ("Back Bay",
          "31 St. James Avenue is the Park Square office building beside the "
          "Public Garden, Boston 02116. SAM's other Saint James is in Roxbury."),
    178: ("Downtown",
          "18 Tremont Street is the office building opposite King's Chapel, "
          "Boston 02108 -- the row's $102.75M 2019 basis and $29.5M resale are "
          "a downtown office trade. SAM holds a Tremont Street in Charlestown "
          "as well."),
    189: ("East Cambridge",
          "Twenty20 is the residential tower at Cambridge Crossing, the former "
          "NorthPoint site in East Cambridge."),
}

_SUFFIX = {
    "street", "st", "avenue", "av", "ave", "road", "rd", "drive", "dr",
    "boulevard", "blvd", "place", "pl", "square", "sq", "lane", "ln",
    "court", "ct", "way", "terrace", "ter", "row", "wharf", "circle",
    "cir", "highway", "hwy", "park", "plaza", "pz",
}
_WORD_NUM = {"one": "1", "two": "2", "three": "3", "four": "4", "five": "5",
             "six": "6", "seven": "7", "eight": "8", "nine": "9", "ten": "10"}


def parse_components(addr):
    """Address string -> list of (number, [body candidates, longest first]).

    Splits on ' and ' and '/' so a row naming two addresses yields both; an
    unambiguous one can then decide for the row.

    SEVERAL BODY CANDIDATES PER COMPONENT, because the street body is not
    recoverable by rule. "One Marina Park Drive" has the body "Marina Park" and
    the suffix "Dr", but Park is itself a suffix word, so any single rule that
    stops at the first suffix token yields "Marina" and misses. Candidates are
    tried longest first and the first that the city file knows wins.
    """
    if not addr:
        return []
    a = re.sub(r"\(.*?\)", " ", addr)              # drop building names
    parts = re.split(r"\s+and\s+|/|;", a, flags=re.I)
    out = []
    for p in parts:
        toks = [t.strip(".,") for t in p.strip().strip(",").split() if t.strip(".,")]
        if not toks:
            continue
        head = _WORD_NUM.get(toks[0].lower(), toks[0].lower())
        m = re.match(r"^(\d+)", head)
        if not m:
            continue
        num = m.group(1)
        rest = toks[1:]
        # Drop a trailing suffix word ("Street", "Drive") if anything precedes it.
        if len(rest) > 1 and rest[-1].lower() in _SUFFIX:
            rest = rest[:-1]
        rest = rest[:4]
        bodies = [" ".join(rest[:k]) for k in range(len(rest), 0, -1)]
        if bodies:
            out.append((num, bodies))
    return out


def _body_variants(body):
    """SAM and Cambridge spell 'St James' as 'Saint James'."""
    v = [body]
    if re.match(r"^St\.?\s+", body, re.I):
        v.append(re.sub(r"^St\.?\s+", "Saint ", body, flags=re.I))
    if re.match(r"^Saint\s+", body, re.I):
        v.append(re.sub(r"^Saint\s+", "St ", body, flags=re.I))
    return v


def sam_points(num, body):
    """Every Boston SAM coordinate for an address."""
    pts = []
    for b in _body_variants(body):
        safe = b.replace("'", "''")
        sql = (f'SELECT DISTINCT "X_COORD","Y_COORD" FROM "{SAM_RESOURCE}" '
               f"WHERE \"STREET_NUMBER\" = '{num}' "
               f"AND upper(\"STREET_BODY\") = upper('{safe}')")
        try:
            r = requests.get(SAM_URL, params={"sql": sql}, timeout=60)
            recs = r.json().get("result", {}).get("records", [])
        except Exception as exc:                            # noqa: BLE001
            log.warning("    SAM error %s %s: %s", num, b, exc)
            continue
        for rec in recs:
            try:
                pts.append(state_plane_to_latlon(float(rec["X_COORD"]),
                                                 float(rec["Y_COORD"])))
            except (TypeError, ValueError, KeyError):
                pass
        if pts:
            break
    return pts


def cambridge_points(num=None, body=None, maplot=None):
    """Every Cambridge Master Address List coordinate for an address or maplot.

    Cambridge writes `stname` WITH the suffix attached -- "Hampshire St", not
    "Hampshire" -- so the street match is a prefix, not an equality. Maplot
    lookups also try the first two components, because a transaction may carry
    a condominium sub-parcel ('8-91-20') where the address file holds the
    parent ('8-91').
    """
    pts = []
    if maplot:
        keys = [maplot]
        bits = maplot.split("-")
        if len(bits) > 2:
            keys.append("-".join(bits[:2]))
        for k in keys:
            try:
                r = requests.get(CAM_URL, params={
                    "$limit": 200, "$select": "latitude,longitude", "maplot": k},
                    timeout=60)
                recs = r.json() if r.status_code == 200 else []
            except Exception as exc:                        # noqa: BLE001
                log.warning("    Cambridge error maplot %s: %s", k, exc)
                continue
            for rec in recs:
                try:
                    pts.append((float(rec["latitude"]), float(rec["longitude"])))
                except (TypeError, ValueError, KeyError):
                    pass
            if pts:
                break
        return pts

    for b in _body_variants(body):
        safe = b.replace("'", "''")
        try:
            r = requests.get(CAM_URL, params={
                "$limit": 200, "$select": "latitude,longitude",
                "street_number": num,
                "$where": (f"starts_with(upper(stname), upper('{safe}'))")},
                timeout=60)
            recs = r.json() if r.status_code == 200 else []
        except Exception as exc:                            # noqa: BLE001
            log.warning("    Cambridge error %s %s: %s", num, b, exc)
            continue
        for rec in recs:
            try:
                pts.append((float(rec["latitude"]), float(rec["longitude"])))
            except (TypeError, ValueError, KeyError):
                pass
        if pts:
            break
    return pts


def consensus(points, shapes):
    """Neighborhoods for a set of points -> the single agreed name, or a set."""
    names = {n for n in (_lookup(shapes, la, lo) for la, lo in points) if n}
    return names


def resolve_by_address(city, addr):
    """(submarket, how) or (None, reason)."""
    comps = parse_components(addr)
    if not comps:
        return None, "address not parseable"
    boston = (city or "").strip().lower() != "cambridge"
    shapes = _load(_BOSTON if boston else _CAMBRIDGE, "name")
    found, ambiguous = [], []
    for num, bodies in comps:
        pts, used = [], None
        for body in bodies:              # longest candidate first
            pts = sam_points(num, body) if boston else cambridge_points(num, body)
            time.sleep(0.12)
            if pts:
                used = body
                break
        if not pts:
            continue
        names = consensus(pts, shapes)
        if len(names) == 1:
            found.append((names.pop(), f"{num} {used}"))
        elif len(names) > 1:
            ambiguous.append((sorted(names), f"{num} {used}"))
    if found:
        agreed = {n for n, _ in found}
        if len(agreed) == 1:
            return found[0][0], f"address:{found[0][1]}"
        return None, f"components disagree: {sorted(agreed)}"
    if ambiguous:
        names, comp = ambiguous[0]
        return None, f"ambiguous street name {comp} -> {names}"
    return None, "address not in city file"


def main(dry_run):
    conn = engine.connect()
    rows = list(conn.execute(text(
        "select id, city, address, latitude, longitude, parcel_id, price "
        "from transactions where coalesce(quarantined,0) = 0 order by price desc")))

    placed, unresolved = {}, []
    n_geom = n_maplot = n_addr = 0
    n_over = 0
    for rid, city, addr, lat, lon, pid, price in rows:
        sm = submarket_for(city, lat, lon, pid)
        if sm:
            placed[rid] = (sm, "geometry")
            n_geom += 1
            continue
        if rid in OVERRIDE:
            placed[rid] = (OVERRIDE[rid][0], "building name")
            n_over += 1
            log.info("  NAMED  id=%-5s %-42s -> %s", rid, (addr or "")[:42],
                     OVERRIDE[rid][0])
            continue

        cam = (city or "").strip().lower() == "cambridge"
        # Cambridge rows carry a map-lot parcel id the address file also keys on.
        if cam and pid and not re.match(r"^[A-Z]_\d+_\d+$", str(pid).upper()):
            pts = cambridge_points(maplot=str(pid).strip())
            time.sleep(0.12)
            names = consensus(pts, _load(_CAMBRIDGE, "name"))
            if len(names) == 1:
                placed[rid] = (names.pop(), "maplot")
                n_maplot += 1
                log.info("  MAPLOT id=%-5s %-42s -> %s", rid,
                         (addr or "")[:42], placed[rid][0])
                continue

        if point_for(city, lat, lon, pid) is not None:
            unresolved.append((rid, city, addr, price, "point outside all polygons"))
            continue
        if PORTFOLIO.search(addr or ""):
            unresolved.append((rid, city, addr, price, "portfolio address"))
            continue

        sm, why = resolve_by_address(city, addr)
        if sm:
            placed[rid] = (sm, why)
            n_addr += 1
            log.info("  ADDR   id=%-5s %-42s -> %-24s [%s]", rid,
                     (addr or "")[:42], sm, why)
        else:
            unresolved.append((rid, city, addr, price, why))

    log.info("\nPLACED %d of %d (%.1f%%)", len(placed), len(rows),
             len(placed) / len(rows) * 100)
    log.info("  by parcel/point geometry : %d", n_geom)
    log.info("  by Cambridge maplot      : %d", n_maplot)
    log.info("  by address lookup        : %d", n_addr)
    log.info("  by building name         : %d", n_over)

    log.info("\nUNRESOLVED: %d", len(unresolved))
    for rid, city, addr, price, why in unresolved:
        log.info("  id=%-5s %-10s $%-13s %-46s %s", rid, city,
                 f"{price or 0:,.0f}", (addr or "")[:46], why)

    if not dry_run:
        for rid, (sm, _how) in placed.items():
            conn.execute(text(
                "update transactions set submarket = :s where id = :id"),
                {"s": sm, "id": rid})
        conn.commit()
        log.info("\n%d rows written", len(placed))
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(dry_run=not ap.parse_args().apply)
