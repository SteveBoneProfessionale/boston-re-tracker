r"""Turn a MassGIS LOC_ID into latitude and longitude, with no dependencies.

WHY THIS EXISTS. 641 of the 793 acquisition rows are Boston, and NOT ONE of them
carries a latitude or longitude -- only Cambridge rows were ever geocoded. That
blocks any spatial derivation, including submarket, for four fifths of the table.

But the Boston rows carry a MassGIS parcel identifier of the form

    F_773623_2945114

which is not an opaque key. It is the parcel centroid written into the id:
easting 773623, northing 2945114, in NAD83 / Massachusetts Mainland (US survey
FEET), EPSG:2249. Every Boston row with a parcel_id therefore already contains
its own coordinate, and the only thing missing was the inverse projection.

Adding pyproj for this would be the obvious move and is the wrong one here:
requirements.txt is entirely unpinned, so every new dependency is another thing
that silently re-rolls its version on the next rebuild. The inverse Lambert
Conformal Conic is forty lines of closed-form arithmetic and it is written out
below rather than imported.

    EPSG:2249 parameters, from the EPSG registry:
        projection            Lambert Conformal Conic, 2 standard parallels
        ellipsoid             GRS 1980  (a = 6378137, 1/f = 298.257222101)
        latitude of origin    41 deg  0 min  0 sec  N
        central meridian      71 deg 30 min  0 sec  W
        standard parallel 1   42 deg 41 min  0 sec  N
        standard parallel 2   41 deg 43 min  0 sec  N
        false easting         200000 m  =  656166.6666667 ftUS
        false northing        750000 m  = 2460625.0000000 ftUS

    The US survey foot is 1200/3937 metres exactly, which is NOT the
    international foot. Using 0.3048 instead shifts a Boston parcel by roughly
    four feet -- immaterial for a neighborhood lookup, but the exact ratio costs
    nothing so it is used.
"""

import math
import re

_A = 6378137.0                      # GRS80 semi-major axis, metres
_F = 1.0 / 298.257222101            # GRS80 flattening
_E = math.sqrt(2 * _F - _F * _F)    # first eccentricity

_LAT0 = math.radians(41.0)
_LON0 = math.radians(-71.5)
_LAT1 = math.radians(42.0 + 41.0 / 60.0)
_LAT2 = math.radians(41.0 + 43.0 / 60.0)

_US_FOOT = 1200.0 / 3937.0          # exact
_FE = 200000.0 / _US_FOOT           # false easting in ftUS
_FN = 750000.0 / _US_FOOT           # false northing in ftUS

_LOC_ID = re.compile(r"^[A-Z]_(\d+)_(\d+)$")


def _m(lat):
    s = math.sin(lat)
    return math.cos(lat) / math.sqrt(1.0 - _E * _E * s * s)


def _t(lat):
    s = math.sin(lat)
    es = _E * s
    return (math.tan(math.pi / 4.0 - lat / 2.0)
            / ((1.0 - es) / (1.0 + es)) ** (_E / 2.0))


_M1, _M2 = _m(_LAT1), _m(_LAT2)
_T0, _T1, _T2 = _t(_LAT0), _t(_LAT1), _t(_LAT2)
_N = (math.log(_M1) - math.log(_M2)) / (math.log(_T1) - math.log(_T2))
_BIGF = _M1 / (_N * _T1 ** _N)
_RHO0 = _A * _BIGF * _T0 ** _N


def state_plane_to_latlon(easting_ft, northing_ft):
    """EPSG:2249 (ftUS) -> (lat, lon) in degrees."""
    x = easting_ft * _US_FOOT - 200000.0
    y = northing_ft * _US_FOOT - 750000.0

    rho = math.copysign(math.hypot(x, _RHO0 - y), _N)
    t = (rho / (_A * _BIGF)) ** (1.0 / _N)
    theta = math.atan2(x, _RHO0 - y)

    lon = theta / _N + _LON0
    # Iterate for latitude; converges in three or four passes at this latitude.
    lat = math.pi / 2.0 - 2.0 * math.atan(t)
    for _ in range(12):
        es = _E * math.sin(lat)
        prev = lat
        lat = (math.pi / 2.0
               - 2.0 * math.atan(t * ((1.0 - es) / (1.0 + es)) ** (_E / 2.0)))
        if abs(lat - prev) < 1e-12:
            break
    return math.degrees(lat), math.degrees(lon)


def loc_id_to_latlon(loc_id):
    """'F_773623_2945114' -> (lat, lon), or None if it is not a LOC_ID."""
    if not loc_id:
        return None
    mt = _LOC_ID.match(str(loc_id).strip().upper())
    if not mt:
        return None
    return state_plane_to_latlon(float(mt.group(1)), float(mt.group(2)))
