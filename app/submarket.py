r"""Derive a submarket for an acquisition row, the same way the development side does.

SUBMARKET IS THE FIRST THING ANYONE FILTERS ON and the acquisitions table had no
such column. The development tracker carries `neighborhood` on every project and
uses the official municipal vocabularies; this module reproduces those exactly,
so a Seaport comp and a Seaport project name the same place.

THE TWO SOURCES, both official and both cached under data/boundaries:

    Boston      BPDA Neighborhood Boundaries, data.boston.gov dataset
                `bpda-neighborhood-boundaries`, 26 polygons. This is the layer
                the BPDA itself files against, which is why the project side
                already speaks it.
    Cambridge   CDD Neighborhoods, data.cambridgema.gov geospatial export
                k3pi-9823, 13 polygons.

HOW A ROW GETS A POINT. Cambridge rows were geocoded at load and carry latitude
and longitude. NO BOSTON ROW DOES -- all 641 are blank. Their coordinate comes
instead out of the MassGIS parcel id, which encodes the parcel centroid; see
app/geo_massgis.py for why that works and what it cost to avoid pyproj.

WHERE A ROW GETS NOTHING. A row with neither a coordinate nor a LOC_ID gets a
null submarket, and a point that lands outside every polygon gets a null too.
Both are left blank rather than guessed at from the street name, on the same
rule the sponsor columns run on: a blank is correct, a wrong one poisons the
filter. 31 press-sourced Boston rows and 9 Cambridge rows have no geometry.

Point-in-polygon is ray casting, with holes subtracted. It is exact for the
polygon as published; the only error it can introduce is at a boundary line,
where a parcel centroid within a few feet of a neighborhood edge could fall
either side. Boston's neighborhoods meet along streets, so a centroid is
essentially never that close.
"""

import json
import logging
from pathlib import Path

from app.geo_massgis import loc_id_to_latlon

log = logging.getLogger(__name__)

_DIR = Path(__file__).parent.parent / "data" / "boundaries"
_BOSTON = _DIR / "boston_neighborhoods.geojson"
_CAMBRIDGE = _DIR / "cambridge_neighborhoods.geojson"

_cache = {}


def _rings(geom):
    """GeoJSON geometry -> list of polygons, each a list of rings."""
    t = geom.get("type")
    if t == "Polygon":
        return [geom["coordinates"]]
    if t == "MultiPolygon":
        return list(geom["coordinates"])
    return []


def _in_ring(x, y, ring):
    """Ray casting. ring is [[lon, lat], ...]."""
    inside = False
    n = len(ring)
    j = n - 1
    for i in range(n):
        xi, yi = ring[i][0], ring[i][1]
        xj, yj = ring[j][0], ring[j][1]
        if (yi > y) != (yj > y):
            if x < (xj - xi) * (y - yi) / (yj - yi) + xi:
                inside = not inside
        j = i
    return inside


def _in_polygon(x, y, poly):
    """poly is [outer_ring, hole, hole, ...]."""
    if not poly or not _in_ring(x, y, poly[0]):
        return False
    return not any(_in_ring(x, y, h) for h in poly[1:])


def _load(path, name_key):
    if path in _cache:
        return _cache[path]
    shapes = []
    if path.exists():
        data = json.loads(path.read_text(encoding="utf-8"))
        for f in data.get("features", []):
            nm = f.get("properties", {}).get(name_key)
            if not nm:
                continue
            for poly in _rings(f.get("geometry") or {}):
                xs = [p[0] for p in poly[0]]
                ys = [p[1] for p in poly[0]]
                # Bounding box first -- 26 polygons times ~2,000 vertices times
                # 793 rows is enough work to be worth the cheap rejection.
                shapes.append((str(nm), min(xs), min(ys), max(xs), max(ys), poly))
    else:
        log.warning("boundary file missing: %s", path)
    _cache[path] = shapes
    return shapes


def _lookup(shapes, lat, lon):
    for nm, x0, y0, x1, y1, poly in shapes:
        if x0 <= lon <= x1 and y0 <= lat <= y1 and _in_polygon(lon, lat, poly):
            return nm
    return None


def point_for(city, latitude, longitude, parcel_id):
    """Best available (lat, lon) for a row, or None."""
    if latitude is not None and longitude is not None:
        try:
            return float(latitude), float(longitude)
        except (TypeError, ValueError):
            pass
    return loc_id_to_latlon(parcel_id)


def submarket_for(city, latitude=None, longitude=None, parcel_id=None):
    """Official neighborhood name for a row, or None where it cannot be placed."""
    pt = point_for(city, latitude, longitude, parcel_id)
    if pt is None:
        return None
    lat, lon = pt
    c = (city or "").strip().lower()
    if c == "cambridge":
        return _lookup(_load(_CAMBRIDGE, "name"), lat, lon)
    if c == "boston":
        return _lookup(_load(_BOSTON, "name"), lat, lon)
    # Unknown city: try both, Boston first.
    return (_lookup(_load(_BOSTON, "name"), lat, lon)
            or _lookup(_load(_CAMBRIDGE, "name"), lat, lon))
