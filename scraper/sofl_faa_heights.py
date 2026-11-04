"""Height above ground from the FAA's Digital Obstacle File, with calibration.

Height is the thinnest field in this market after lender: the press prints
storey counts and prints feet only at a board-approval stage. The brief allows
an FAA filing as a height source, and the FAA publishes the Digital Obstacle
File as a queryable feature service carrying, per obstacle, AGL (above ground
level) and AMSL (above mean sea level) -- the exact distinction the brief draws,
with AGL being the wanted number and AMSL the one never to use.

WHY THIS NEEDS CALIBRATING BEFORE IT IS TRUSTED
The DOF is a file of EXISTING verified obstacles, keyed on coordinates rather
than on an address or a project. Three ways it can lie about a project:

  * the obstacle at those coordinates is the building being REPLACED, not the
    one being built -- a redevelopment site's DOF entry can predate the project
    by a decade;
  * it is a crane, an antenna or a stack rather than the building (Type_Code);
  * it is the building next door, because a 50 m radius in Brickell contains
    several towers.

So `calibrate()` runs first and scores the method against the projects whose
height is already known from the press. If the nearest DOF building does not
reproduce those, the method is not fit to fill the unknown ones, and the
honest answer is to say so rather than to load 400 plausible numbers.

Nothing here writes to the database. It reports, and a reviewed subset is
applied through scraper/sofl_apply_findings.py like any other finding.

THE CALIBRATION RESULT: DO NOT USE THIS FOR BULK FILLING
Run on 4 November 2026 against the 42 South Florida projects whose height was
already known from the press, at a 60 m radius:

    17 of 42 had a DOF building within the radius
     9 of those 17 agreed with the published height

A 53 per cent hit rate is a coin flip, and the misses are not small. The
failures are precisely the two modes predicted above:

    Waldorf Astoria   press 1,049 ft   DOF   395 ft   surveyed 2018
    130 Biscayne      press   955 ft   DOF   362 ft   surveyed 2017
    Baccarat          press   848 ft   DOF   550 ft   surveyed 2025
    Downtown 6        press   576 ft   DOF   355 ft   surveyed 2026
    Excel Miami       press   275 ft   DOF   414 ft   surveyed 2019

The 2017 and 2018 surveys are the buildings that were demolished to make room
for the towers now on those parcels. The 2025 and 2026 ones are the tower next
door: in Brickell and Downtown a 60 m circle holds several high-rises, and the
tallest obstacle in it is routinely not the project.

Tightening the radius does not rescue it, because the near misses and the far
hits are not separable by distance -- Casa Bella agrees at 54 m while Baccarat
is wrong at 34 m. The ones that agree are the cases where the project happens
to be the tallest thing nearby, which is not knowable in advance.

So this module stays as a calibration harness and a lookup for checking a
single project by hand. Height in this market is genuinely sparse because the
press prints storeys and prints feet only at a board approval, and 49 of 452
is the honest number rather than 400 plausible ones.
"""

import math
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scraper import sofl_common as sc        # noqa: E402

DOF = ("https://services6.arcgis.com/ssFJjBXIUyZDrSYZ/arcgis/rest/services/"
       "Digital_Obstacle_File/FeatureServer/0")

# The five cities' bounding box, generously drawn.
BBOX = "-80.45,25.60,-80.00,26.85"

# Obstacle types that are a building. A crane is the thing most likely to sit
# on a project site and is deliberately excluded: its height is not the
# building's.
BUILDING_TYPES = {"BLDG", "BLDG-TWR", "BUILDING"}


def fetch(sess=None, verbose=True):
    """Every building obstacle in the South Florida box."""
    s = sess or sc.session()
    out, offset = [], 0
    while True:
        for attempt in range(5):
            try:
                data = sc._get_json(s, DOF + "/query", {
                    "geometry": BBOX, "geometryType": "esriGeometryEnvelope",
                    "inSR": 4326, "spatialRel": "esriSpatialRelIntersects",
                    "outFields": "OAS_Number,City,Lat_DD,Long_DD,Type_Code,AGL,AMSL,Verified,Date",
                    "f": "json", "returnGeometry": "false",
                    "resultOffset": offset, "resultRecordCount": 1000,
                    "orderByFields": "OBJECTID ASC"})
            except RuntimeError:
                time.sleep(20)
                continue
            if "error" in data:
                # The service is a shared public one and rate-limits hard.
                time.sleep(25)
                continue
            break
        else:
            raise RuntimeError("FAA DOF unavailable after retries")
        feats = [f["attributes"] for f in data.get("features", [])]
        out.extend(feats)
        if verbose:
            print(f"  fetched {len(out)}", file=sys.stderr)
        if len(feats) < 1000:
            break
        offset += 1000
        time.sleep(2)
    cleaned = []
    for a in out:
        t = (a.get("Type_Code") or "").strip().upper()
        if t not in BUILDING_TYPES:
            continue
        try:
            cleaned.append({"oas": (a.get("OAS_Number") or "").strip(),
                            "lat": float(a["Lat_DD"]), "lon": float(a["Long_DD"]),
                            "agl": int(a["AGL"]), "amsl": int(a["AMSL"]),
                            "verified": (a.get("Verified") or "").strip(),
                            "date": str(a.get("Date") or "")})
        except (TypeError, ValueError, KeyError):
            continue
    return cleaned


def _metres(lat1, lon1, lat2, lon2):
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def nearest(obstacles, lat, lon, radius_m=60.0, top=3):
    """Building obstacles near a point, tallest first within the radius."""
    hits = []
    for o in obstacles:
        d = _metres(lat, lon, o["lat"], o["lon"])
        if d <= radius_m:
            hits.append({**o, "distance_m": round(d, 1)})
    return sorted(hits, key=lambda h: -h["agl"])[:top]


def dof_year(date_str):
    """The DOF date is YYYYDDD -- year plus day-of-year."""
    try:
        return int(str(date_str)[:4])
    except (TypeError, ValueError):
        return 0


def calibrate(radius_m=60.0, verbose=True):
    """Score the method against projects whose height is already known.

    Prints one line per project with a known height: the press figure, the
    nearest DOF building's AGL, the gap, and the DOF survey year. The decision
    to use this source at all should rest on what this prints.
    """
    import sqlite3
    db = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                      "data", "boston_re.db")
    con = sqlite3.connect(db)
    rows = con.execute(
        "select id, name, city, latitude, longitude, building_height_ft, num_stories "
        "from projects where city in "
        "('Miami','Miami Beach','West Palm Beach','Coral Gables','Doral') "
        "and building_height_ft is not null and latitude is not null").fetchall()
    con.close()

    obstacles = fetch(verbose=verbose)
    if verbose:
        print(f"\n{len(obstacles)} building obstacles in the box\n")
        print(f"{'project':<42}{'press':>7}{'FAA AGL':>9}{'gap':>7}{'dist':>7}{'yr':>6}")
    scored = []
    for pid, name, city, lat, lon, press_ft, storeys in rows:
        hits = nearest(obstacles, lat, lon, radius_m)
        if not hits:
            scored.append((name, press_ft, None, None, None, None))
            if verbose:
                print(f"{name[:42]:<42}{press_ft:>7.0f}{'-':>9}{'-':>7}{'-':>7}{'-':>6}")
            continue
        h = hits[0]
        gap = h["agl"] - press_ft
        scored.append((name, press_ft, h["agl"], gap, h["distance_m"], dof_year(h["date"])))
        if verbose:
            print(f"{name[:42]:<42}{press_ft:>7.0f}{h['agl']:>9}{gap:>+7.0f}"
                  f"{h['distance_m']:>7.0f}{dof_year(h['date']):>6}")
    matched = [s for s in scored if s[2] is not None]
    close = [s for s in matched if abs(s[3]) <= max(25, 0.05 * s[1])]
    if verbose:
        print(f"\n{len(matched)} of {len(scored)} known-height projects have a DOF building "
              f"within {radius_m:.0f} m")
        if matched:
            print(f"{len(close)} of those agree with the published height "
                  f"(within 25 ft or 5 per cent)")
    return scored


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    calibrate()
