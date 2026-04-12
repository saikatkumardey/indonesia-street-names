import duckdb
import urllib.request
import subprocess

print("Connecting...", flush=True)
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("INSTALL spatial; LOAD spatial;")
con.execute("SET s3_region='us-west-2';")

print("Fetching Indonesia boundary...", flush=True)
url = "https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson"
urllib.request.urlretrieve(url, "countries.geojson")
con.execute("""
    CREATE TABLE indonesia AS
    SELECT geometry
    FROM ST_Read('countries.geojson')
    WHERE properties->>'ADMIN' = 'Indonesia'
""")
print("Indonesia boundary loaded", flush=True)

print("Running extraction...", flush=True)
con.execute("""
COPY (
    SELECT DISTINCT ON (names.primary)
        names.primary AS street_name,
        regexp_replace(
            regexp_replace(sources[1].record_id, '^w', ''),
            '@[0-9]+$', ''
        ) AS osm_way_id,
        sources[1].dataset AS source_dataset,
        ST_AsText(s.geometry) AS geometry_wkt
    FROM read_parquet(
        's3://overturemaps-us-west-2/release/2026-03-18.0/theme=transportation/type=segment/*',
        hive_partitioning=1
    ) s, indonesia i
    WHERE s.bbox.xmin BETWEEN 95 AND 141
      AND s.bbox.ymin BETWEEN -11 AND 6
      AND names.primary IS NOT NULL
      AND length(trim(names.primary)) > 1
      AND names.primary NOT LIKE '%*%'
      AND names.primary != ''
      AND ST_Within(ST_Centroid(s.geometry), i.geometry)
    ORDER BY street_name
) TO 'data/indonesia_streets.parquet' (FORMAT parquet, COMPRESSION 'zstd');
""")

count = con.execute("SELECT count(*) FROM 'data/indonesia_streets.parquet'").fetchone()[0]
print(f"Done! {count:,} rows", flush=True)

print("Writing sample...", flush=True)
con.execute("""
COPY (
    SELECT street_name, osm_way_id, source_dataset
    FROM 'data/indonesia_streets.parquet'
    LIMIT 100
) TO 'data/sample.csv' (HEADER, DELIMITER ',');
""")
