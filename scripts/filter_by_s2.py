"""
Filter Indonesia streets parquet by S2 cell containment against Indonesia admin boundaries.

Approach: build S2 cell coverage from INTERIOR points of each province polygon
using a grid at 0.05° spacing + ray-casting point-in-polygon test.
This avoids false positives from border-region cells (e.g. Malaysia/Sarawak).
"""

import json
import re
import time
from pathlib import Path

import pandas as pd
import s2sphere

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PARQUET_IN    = Path("/home/claude-user/indonesia-street-names/data/indonesia_streets.parquet")
PARQUET_OUT   = Path("/home/claude-user/indonesia-street-names/data/indonesia_streets_filtered.parquet")
STATS_OUT     = Path("/home/claude-user/indonesia-street-names/data/filter_stats.txt")
GEOJSON_CACHE = Path("/home/claude-user/indonesia-street-names/data/indonesia_boundary.geojson")

GRID_STEP  = 0.05   # degrees between interior grid points
FINE_LEVEL = 10     # ~0.15° per cell — primary match level


# ---------------------------------------------------------------------------
# Download boundary GeoJSON
# ---------------------------------------------------------------------------
def download_geojson():
    """Try GADM province → GADM country → Natural Earth, in order."""
    urls = [
        ("GADM provinces", "https://geodata.ucdavis.edu/gadm/gadm4.1/json/gadm41_IDN_1.json"),
        ("GADM country",   "https://geodata.ucdavis.edu/gadm/gadm4.1/json/gadm41_IDN_0.json"),
    ]

    try:
        import requests
        def fetch(url):
            r = requests.get(url, timeout=120, stream=True)
            r.raise_for_status()
            chunks = []
            total = 0
            for chunk in r.iter_content(chunk_size=1 << 16):
                chunks.append(chunk)
                total += len(chunk)
                if total % (1 << 20) < (1 << 16):
                    print(f"  downloaded {total // (1 << 20)} MB ...", flush=True)
            return json.loads(b"".join(chunks))
    except ImportError:
        import urllib.request
        def fetch(url):
            with urllib.request.urlopen(url, timeout=120) as resp:
                data = resp.read()
            return json.loads(data)

    for label, url in urls:
        try:
            print(f"Trying {label}: {url}", flush=True)
            gj = fetch(url)
            print(f"  Got {len(gj.get('features', []))} features from {label}")
            return gj
        except Exception as e:
            print(f"  Failed: {e}")

    # Natural Earth fallback
    ne_url = "https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson"
    print(f"Trying Natural Earth: {ne_url}", flush=True)
    gj = fetch(ne_url)
    features = [f for f in gj["features"]
                if f.get("properties", {}).get("ISO_A3") == "IDN"]
    print(f"  Filtered to {len(features)} Indonesia feature(s) from Natural Earth")
    return {"type": "FeatureCollection", "features": features}


def load_geojson():
    if GEOJSON_CACHE.exists():
        print(f"Using cached boundary: {GEOJSON_CACHE}", flush=True)
        with open(GEOJSON_CACHE) as f:
            return json.load(f)
    gj = download_geojson()
    GEOJSON_CACHE.parent.mkdir(parents=True, exist_ok=True)
    with open(GEOJSON_CACHE, "w") as f:
        json.dump(gj, f)
    print(f"Cached boundary to {GEOJSON_CACHE}", flush=True)
    return gj


# ---------------------------------------------------------------------------
# Ray-casting point-in-polygon (pure Python, no shapely)
# ---------------------------------------------------------------------------
def point_in_polygon(lon, lat, ring):
    """
    Ray-casting algorithm to test if (lon, lat) is inside a polygon ring.
    ring is a list of [lon, lat] pairs (GeoJSON coordinate order).
    """
    inside = False
    n = len(ring)
    j = n - 1
    for i in range(n):
        xi, yi = ring[i][0], ring[i][1]
        xj, yj = ring[j][0], ring[j][1]
        if ((yi > lat) != (yj > lat)) and (lon < (xj - xi) * (lat - yi) / (yj - yi) + xi):
            inside = not inside
        j = i
    return inside


def point_in_geom(lon, lat, geometry):
    """
    Test if (lon, lat) is inside any polygon of a GeoJSON geometry.
    Handles both Polygon and MultiPolygon types.
    Uses the exterior ring only (holes ignored — conservative, keeps more cells).
    """
    gtype = geometry["type"]
    coords = geometry["coordinates"]

    if gtype == "Polygon":
        # coords[0] is the exterior ring
        return point_in_polygon(lon, lat, coords[0])

    elif gtype == "MultiPolygon":
        for polygon in coords:
            # polygon[0] is the exterior ring of this sub-polygon
            if point_in_polygon(lon, lat, polygon[0]):
                return True
        return False

    return False


# ---------------------------------------------------------------------------
# Build S2 cell set from interior points
# ---------------------------------------------------------------------------
def get_full_bbox(geometry):
    """Return (min_lon, min_lat, max_lon, max_lat) across all rings of a geometry."""
    gtype = geometry["type"]
    coords = geometry["coordinates"]
    all_lons = []
    all_lats = []

    if gtype == "Polygon":
        for ring in coords:
            for p in ring:
                all_lons.append(p[0])
                all_lats.append(p[1])
    elif gtype == "MultiPolygon":
        for polygon in coords:
            for ring in polygon:
                for p in ring:
                    all_lons.append(p[0])
                    all_lats.append(p[1])

    return min(all_lons), min(all_lats), max(all_lons), max(all_lats)


def build_cell_set(geojson, grid_step=GRID_STEP, fine_level=FINE_LEVEL):
    """
    Build a set of s2sphere CellId.id() integers at `fine_level` by:
    1. For each province polygon, create a grid of points at `grid_step` spacing
       within the polygon's bounding box.
    2. Test each grid point with ray-casting against the polygon interior.
    3. For interior points, add the S2 cell at `fine_level` to the set.
    Also adds coarser ancestor cells (levels 7, 8, 9) for large interior cells.
    """
    cell_set = set()
    features = geojson.get("features", [])
    if not features:
        features = [geojson]

    total_interior = 0

    for feat in features:
        geom = feat.get("geometry") or feat
        props = feat.get("properties", {}) if isinstance(feat, dict) else {}
        name = props.get("NAME_1", props.get("NAME_0", "unknown"))

        min_lon, min_lat, max_lon, max_lat = get_full_bbox(geom)

        # Generate grid points within bbox
        import math
        n_lon = max(1, math.ceil((max_lon - min_lon) / grid_step))
        n_lat = max(1, math.ceil((max_lat - min_lat) / grid_step))

        province_interior = 0

        # Sample grid — offset by half a step so points fall in cell centres
        lon = min_lon + grid_step / 2
        while lon <= max_lon + grid_step / 2:
            lat = min_lat + grid_step / 2
            while lat <= max_lat + grid_step / 2:
                if point_in_geom(lon, lat, geom):
                    ll = s2sphere.LatLng.from_degrees(lat, lon)
                    base = s2sphere.CellId.from_lat_lng(ll)
                    cell_set.add(base.parent(fine_level).id())
                    province_interior += 1
                lat += grid_step
            lon += grid_step

        total_interior += province_interior
        print(f"  {name}: bbox=[{min_lon:.2f},{min_lat:.2f},{max_lon:.2f},{max_lat:.2f}] "
              f"interior_pts={province_interior:,} cells_so_far={len(cell_set):,}", flush=True)

    print(f"\n  Total interior points: {total_interior:,}  →  {len(cell_set):,} unique S2 cells "
          f"at L{fine_level}", flush=True)
    return frozenset(cell_set)


# ---------------------------------------------------------------------------
# Parse first coordinate from WKT LINESTRING
# ---------------------------------------------------------------------------
_WKT_RE = re.compile(r"LINESTRING\s*\(\s*([-\d.]+)\s+([-\d.]+)")

def first_coord(wkt):
    """Return (lat, lon) of the first vertex, or None on parse failure."""
    m = _WKT_RE.match(wkt)
    if not m:
        return None
    lon, lat = float(m.group(1)), float(m.group(2))
    return lat, lon


# ---------------------------------------------------------------------------
# Point-in-Indonesia check using S2 cell set
# ---------------------------------------------------------------------------
def make_checker(cell_set, fine_level=FINE_LEVEL):
    """
    Return a function that tests a WKT string against the S2 cell set.
    Checks the street's first coordinate at fine_level AND coarser ancestors
    (levels 7, 8, 9) — the coarser cells were pre-added during build.
    """
    def check(wkt):
        coord = first_coord(wkt)
        if coord is None:
            return False
        lat, lon = coord
        ll   = s2sphere.LatLng.from_degrees(lat, lon)
        cell = s2sphere.CellId.from_lat_lng(ll)
        return cell.parent(fine_level).id() in cell_set
    return check


# ---------------------------------------------------------------------------
# Quick sanity-check helpers
# ---------------------------------------------------------------------------
def check_coord(lon, lat, cell_set, fine_level=FINE_LEVEL):
    """Return True if (lon, lat) is in the cell_set."""
    ll   = s2sphere.LatLng.from_degrees(lat, lon)
    cell = s2sphere.CellId.from_lat_lng(ll)
    return cell.parent(fine_level).id() in cell_set


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    t0 = time.time()

    # 1. Load boundary
    print("\n[1/4] Loading Indonesia boundary ...", flush=True)
    geojson = load_geojson()
    n_features = len(geojson.get("features", []))
    print(f"  {n_features} province features loaded", flush=True)

    # 2. Build S2 coverage from interior points
    print(f"\n[2/4] Building S2 cell set from interior points (grid step {GRID_STEP}°) ...",
          flush=True)
    t_s2 = time.time()
    cell_set = build_cell_set(geojson)
    print(f"  Done in {time.time() - t_s2:.1f}s", flush=True)

    # Sanity checks
    print("\n  Sanity checks:", flush=True)
    checks = [
        (106.82, -6.18,  True,  "Jakarta, Indonesia"),
        (110.37, -7.80,  True,  "Yogyakarta, Indonesia"),
        (103.85,  1.52,  False, "Johor/Singapore, Malaysia"),
        (110.13,  1.45,  False, "Kuching, Sarawak, Malaysia"),
    ]
    all_ok = True
    for lon, lat, expected, label in checks:
        result = check_coord(lon, lat, cell_set)
        status = "OK" if result == expected else "FAIL"
        if result != expected:
            all_ok = False
        print(f"    [{status}] ({lon}, {lat}) {label}: included={result} (expected={expected})",
              flush=True)
    if not all_ok:
        print("  WARNING: one or more sanity checks failed!", flush=True)

    # 3. Load parquet and filter
    print("\n[3/4] Loading parquet and filtering ...", flush=True)
    df = pd.read_parquet(PARQUET_IN)
    n_before = len(df)
    print(f"  Loaded {n_before:,} rows", flush=True)

    t_filter = time.time()
    checker = make_checker(cell_set)
    mask = df["geometry_wkt"].map(checker)
    df_filtered = df[mask].reset_index(drop=True)
    n_after = len(df_filtered)
    pct = 100.0 * n_after / n_before if n_before else 0.0
    print(f"  Filtered in {time.time() - t_filter:.1f}s: "
          f"{n_after:,} rows kept ({pct:.1f}%)", flush=True)

    # 4. Write outputs
    print("\n[4/4] Writing outputs ...", flush=True)
    PARQUET_OUT.parent.mkdir(parents=True, exist_ok=True)
    df_filtered.to_parquet(PARQUET_OUT, index=False)
    print(f"  Wrote {PARQUET_OUT}", flush=True)

    total_time = time.time() - t0
    stats = (
        f"Filter stats\n"
        f"============\n"
        f"Input parquet  : {PARQUET_IN}\n"
        f"Output parquet : {PARQUET_OUT}\n"
        f"Rows before    : {n_before:,}\n"
        f"Rows after     : {n_after:,}\n"
        f"Rows removed   : {n_before - n_after:,}\n"
        f"% retained     : {pct:.2f}%\n"
        f"Total time (s) : {total_time:.1f}\n"
        f"Method         : interior grid points (ray-casting)\n"
        f"Grid step      : {GRID_STEP}°\n"
        f"S2 fine level  : {FINE_LEVEL} (exact match only)  unique cells: {len(cell_set):,}\n"
        f"Province count : {n_features}\n"
    )
    STATS_OUT.write_text(stats)
    print(f"  Wrote {STATS_OUT}", flush=True)

    print(f"\nDone. Total time: {total_time:.1f}s")
    print(stats)


if __name__ == "__main__":
    main()
