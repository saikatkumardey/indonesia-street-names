import duckdb
import subprocess
import sys

print("Connecting...", flush=True)
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("SET s3_region='us-west-2';")

print("Running extraction...", flush=True)
con.execute("""
COPY (
    SELECT DISTINCT
        names.primary AS street_name,
        regexp_replace(sources[1].record_id, '@[0-9]+$', '') AS osm_way_id,
        sources[1].dataset AS source_dataset
    FROM read_parquet(
        's3://overturemaps-us-west-2/release/2026-03-18.0/theme=transportation/type=segment/*',
        hive_partitioning=1
    )
    WHERE bbox.xmin BETWEEN 95 AND 141
      AND bbox.ymin BETWEEN -11 AND 6
      AND names.primary IS NOT NULL
    ORDER BY street_name
) TO 'indonesia_streets.csv' (HEADER, DELIMITER ',');
""")

result = subprocess.run(['wc', '-l', 'indonesia_streets.csv'], capture_output=True, text=True)
print(f"Done! {result.stdout.strip()}", flush=True)
