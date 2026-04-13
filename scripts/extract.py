import duckdb
import click
import json
import math
import os
import re
import time
from pathlib import Path

import pandas as pd
import s2sphere


@click.group()
def cli():
    """Indonesia street names dataset tools."""
    pass


@cli.command()
@click.option("--release", default="2026-03-18.0", show_default=True, help="Overture Maps release version")
@click.option("--output", default="data/indonesia_streets.parquet", show_default=True, help="Output parquet path")
@click.option("--sample", default="data/sample.csv", show_default=True, help="Sample CSV path")
@click.option("--sample-size", default=100, show_default=True, help="Number of rows in sample")
def extract(release, output, sample, sample_size):
    """Extract all named streets in Indonesia from Overture Maps.

    Uses Indonesia's bounding box (lon 95–141, lat -11–6) as the geographic filter.
    Fast and accurate — bbox is specific enough for Indonesia with minimal overlap
    from neighboring countries.
    """
    os.makedirs(os.path.dirname(output), exist_ok=True)

    click.echo("Connecting to DuckDB...")
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("SET s3_region='us-west-2';")
    con.execute("SET memory_limit='6GB';")
    con.execute("SET threads=4;")

    click.echo(f"Extracting from Overture release {release}...")
    con.execute(f"""
    COPY (
        SELECT DISTINCT ON (names.primary)
            names.primary AS street_name,
            regexp_replace(
                regexp_replace(sources[1].record_id, '^w', ''),
                '@[0-9]+$', ''
            ) AS osm_way_id,
            sources[1].dataset AS source_dataset,
            ST_AsText(geometry) AS geometry_wkt
        FROM read_parquet(
            's3://overturemaps-us-west-2/release/{release}/theme=transportation/type=segment/*',
            hive_partitioning=1
        )
        WHERE bbox.xmin BETWEEN 95 AND 141
          AND bbox.ymin BETWEEN -11 AND 6
          AND names.primary IS NOT NULL
          AND length(trim(names.primary)) > 1
          AND names.primary NOT LIKE '%*%'
          AND names.primary != ''
        ORDER BY street_name
    ) TO '{output}' (FORMAT parquet, COMPRESSION 'zstd');
    """)

    count = con.execute(f"SELECT count(*) FROM '{output}'").fetchone()[0]
    click.echo(f"Done! {count:,} rows written to {output}")

    click.echo(f"Writing {sample_size}-row sample to {sample}...")
    con.execute(f"""
    COPY (
        SELECT street_name, osm_way_id, source_dataset
        FROM '{output}'
        LIMIT {sample_size}
    ) TO '{sample}' (HEADER, DELIMITER ',');
    """)
    click.echo("Done.")


# ---------------------------------------------------------------------------
# S2 filter helpers (inlined from filter_by_s2.py)
# ---------------------------------------------------------------------------

GRID_STEP  = 0.05   # degrees between interior grid points
FINE_LEVEL = 10     # ~0.15° per cell — primary match level


def _download_geojson():
    """Try GADM province -> GADM country -> Natural Earth, in order."""
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
                    click.echo(f"  downloaded {total // (1 << 20)} MB ...", err=False)
            return json.loads(b"".join(chunks))
    except ImportError:
        import urllib.request
        def fetch(url):
            with urllib.request.urlopen(url, timeout=120) as resp:
                data = resp.read()
            return json.loads(data)

    for label, url in urls:
        try:
            click.echo(f"Trying {label}: {url}")
            gj = fetch(url)
            click.echo(f"  Got {len(gj.get('features', []))} features from {label}")
            return gj
        except Exception as e:
            click.echo(f"  Failed: {e}")

    ne_url = "https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson"
    click.echo(f"Trying Natural Earth: {ne_url}")
    gj = fetch(ne_url)
    features = [f for f in gj["features"]
                if f.get("properties", {}).get("ISO_A3") == "IDN"]
    click.echo(f"  Filtered to {len(features)} Indonesia feature(s) from Natural Earth")
    return {"type": "FeatureCollection", "features": features}


def _load_geojson(boundary_path):
    boundary = Path(boundary_path)
    if boundary.exists():
        click.echo(f"Using cached boundary: {boundary}")
        with open(boundary) as f:
            return json.load(f)
    gj = _download_geojson()
    boundary.parent.mkdir(parents=True, exist_ok=True)
    with open(boundary, "w") as f:
        json.dump(gj, f)
    click.echo(f"Cached boundary to {boundary}")
    return gj


def _point_in_polygon(lon, lat, ring):
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


def _point_in_geom(lon, lat, geometry):
    gtype = geometry["type"]
    coords = geometry["coordinates"]
    if gtype == "Polygon":
        return _point_in_polygon(lon, lat, coords[0])
    elif gtype == "MultiPolygon":
        for polygon in coords:
            if _point_in_polygon(lon, lat, polygon[0]):
                return True
        return False
    return False


def _get_full_bbox(geometry):
    gtype = geometry["type"]
    coords = geometry["coordinates"]
    all_lons, all_lats = [], []
    if gtype == "Polygon":
        for ring in coords:
            for p in ring:
                all_lons.append(p[0]); all_lats.append(p[1])
    elif gtype == "MultiPolygon":
        for polygon in coords:
            for ring in polygon:
                for p in ring:
                    all_lons.append(p[0]); all_lats.append(p[1])
    return min(all_lons), min(all_lats), max(all_lons), max(all_lats)


def _build_cell_set(geojson):
    cell_set = set()
    features = geojson.get("features", [])
    if not features:
        features = [geojson]
    total_interior = 0

    for feat in features:
        geom = feat.get("geometry") or feat
        props = feat.get("properties", {}) if isinstance(feat, dict) else {}
        name = props.get("NAME_1", props.get("NAME_0", "unknown"))

        min_lon, min_lat, max_lon, max_lat = _get_full_bbox(geom)

        province_interior = 0
        lon = min_lon + GRID_STEP / 2
        while lon <= max_lon + GRID_STEP / 2:
            lat = min_lat + GRID_STEP / 2
            while lat <= max_lat + GRID_STEP / 2:
                if _point_in_geom(lon, lat, geom):
                    ll = s2sphere.LatLng.from_degrees(lat, lon)
                    base = s2sphere.CellId.from_lat_lng(ll)
                    cell_set.add(base.parent(FINE_LEVEL).id())
                    province_interior += 1
                lat += GRID_STEP
            lon += GRID_STEP

        total_interior += province_interior
        click.echo(f"  {name}: bbox=[{min_lon:.2f},{min_lat:.2f},{max_lon:.2f},{max_lat:.2f}] "
                   f"interior_pts={province_interior:,} cells_so_far={len(cell_set):,}")

    click.echo(f"\n  Total interior points: {total_interior:,}  ->  {len(cell_set):,} unique S2 cells "
               f"at L{FINE_LEVEL}")
    return frozenset(cell_set)


_WKT_RE = re.compile(r"LINESTRING\s*\(\s*([-\d.]+)\s+([-\d.]+)")

def _first_coord(wkt):
    m = _WKT_RE.match(wkt)
    if not m:
        return None
    lon, lat = float(m.group(1)), float(m.group(2))
    return lat, lon


def _make_checker(cell_set):
    def check(wkt):
        coord = _first_coord(wkt)
        if coord is None:
            return False
        lat, lon = coord
        ll = s2sphere.LatLng.from_degrees(lat, lon)
        cell = s2sphere.CellId.from_lat_lng(ll)
        return cell.parent(FINE_LEVEL).id() in cell_set
    return check


def _check_coord(lon, lat, cell_set):
    ll = s2sphere.LatLng.from_degrees(lat, lon)
    cell = s2sphere.CellId.from_lat_lng(ll)
    return cell.parent(FINE_LEVEL).id() in cell_set


# ---------------------------------------------------------------------------
# filter command
# ---------------------------------------------------------------------------

@cli.command("filter")
@click.option("--input", "input_path", default="data/indonesia_streets.parquet", show_default=True, help="Input parquet path")
@click.option("--output", default="data/indonesia_streets_filtered.parquet", show_default=True, help="Output parquet path")
@click.option("--boundary", default="data/indonesia_boundary.geojson", show_default=True, help="Boundary GeoJSON path (cached or downloaded)")
def filter_cmd(input_path, output, boundary):
    """Filter streets to Indonesia only using S2 cell containment.

    Builds S2 cell coverage from interior points of each province polygon
    using a grid at 0.05 degrees spacing + ray-casting point-in-polygon test.
    This avoids false positives from border-region cells (e.g. Malaysia/Sarawak).
    """
    t0 = time.time()

    # 1. Load boundary
    click.echo("\n[1/4] Loading Indonesia boundary ...")
    geojson = _load_geojson(boundary)
    n_features = len(geojson.get("features", []))
    click.echo(f"  {n_features} province features loaded")

    # 2. Build S2 coverage
    click.echo(f"\n[2/4] Building S2 cell set from interior points (grid step {GRID_STEP} deg) ...")
    t_s2 = time.time()
    cell_set = _build_cell_set(geojson)
    click.echo(f"  Done in {time.time() - t_s2:.1f}s")

    # Sanity checks
    click.echo("\n  Sanity checks:")
    checks = [
        (106.82, -6.18,  True,  "Jakarta, Indonesia"),
        (110.37, -7.80,  True,  "Yogyakarta, Indonesia"),
        (103.85,  1.52,  False, "Johor/Singapore, Malaysia"),
        (110.13,  1.45,  False, "Kuching, Sarawak, Malaysia"),
    ]
    all_ok = True
    for lon, lat, expected, label in checks:
        result = _check_coord(lon, lat, cell_set)
        status = "OK" if result == expected else "FAIL"
        if result != expected:
            all_ok = False
        click.echo(f"    [{status}] ({lon}, {lat}) {label}: included={result} (expected={expected})")
    if not all_ok:
        click.echo("  WARNING: one or more sanity checks failed!")

    # 3. Load parquet and filter
    click.echo("\n[3/4] Loading parquet and filtering ...")
    df = pd.read_parquet(input_path)
    n_before = len(df)
    click.echo(f"  Loaded {n_before:,} rows")

    t_filter = time.time()
    checker = _make_checker(cell_set)
    mask = df["geometry_wkt"].map(checker)
    df_filtered = df[mask].reset_index(drop=True)
    n_after = len(df_filtered)
    pct = 100.0 * n_after / n_before if n_before else 0.0
    click.echo(f"  Filtered in {time.time() - t_filter:.1f}s: "
               f"{n_after:,} rows kept ({pct:.1f}%)")

    # 4. Write outputs
    click.echo("\n[4/4] Writing outputs ...")
    Path(output).parent.mkdir(parents=True, exist_ok=True)
    df_filtered.to_parquet(output, index=False)
    click.echo(f"  Wrote {output}")

    total_time = time.time() - t0
    stats_path = Path("data/filter_stats.txt")
    stats = (
        f"Filter stats\n"
        f"============\n"
        f"Input parquet  : {input_path}\n"
        f"Output parquet : {output}\n"
        f"Rows before    : {n_before:,}\n"
        f"Rows after     : {n_after:,}\n"
        f"Rows removed   : {n_before - n_after:,}\n"
        f"% retained     : {pct:.2f}%\n"
        f"Total time (s) : {total_time:.1f}\n"
        f"Method         : interior grid points (ray-casting)\n"
        f"Grid step      : {GRID_STEP} deg\n"
        f"S2 fine level  : {FINE_LEVEL} (exact match only)  unique cells: {len(cell_set):,}\n"
        f"Province count : {n_features}\n"
    )
    stats_path.write_text(stats)
    click.echo(f"  Wrote {stats_path}")

    click.echo(f"\nDone. Total time: {total_time:.1f}s")
    click.echo(stats)


@cli.command()
@click.option("--input", "input_path", default="data/indonesia_streets_filtered.parquet", show_default=True, help="Source parquet path")
@click.option("--output", default="data/sample.csv", show_default=True, help="Output CSV path")
@click.option("--size", default=100, show_default=True, help="Number of rows")
def sample(input_path, output, size):
    """Regenerate sample.csv from existing parquet."""
    con = duckdb.connect()
    count = con.execute(f"SELECT count(*) FROM '{input_path}'").fetchone()[0]
    click.echo(f"Source: {count:,} rows")
    con.execute(f"""
    COPY (
        SELECT street_name, osm_way_id, source_dataset
        FROM '{input_path}'
        LIMIT {size}
    ) TO '{output}' (HEADER, DELIMITER ',');
    """)
    click.echo(f"Written {size} rows to {output}")


if __name__ == "__main__":
    cli()
