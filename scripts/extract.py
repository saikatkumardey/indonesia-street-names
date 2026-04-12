import duckdb
import urllib.request
import click
import os


def get_indonesia_boundary(con):
    click.echo("Fetching Indonesia boundary...")
    url = "https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson"
    urllib.request.urlretrieve(url, "/tmp/countries.geojson")

    # Inspect columns from ST_Read to find the right name field
    cols = con.execute("DESCRIBE SELECT * FROM ST_Read('/tmp/countries.geojson') LIMIT 1").fetchall()
    col_names = [c[0] for c in cols]
    click.echo(f"GeoJSON columns: {col_names}")

    # Try known country name fields in order
    for field in ("ADMIN", "NAME", "name", "NAME_EN"):
        if field in col_names:
            # detect geometry column name (geom or geometry)
            geom_col = "geom" if "geom" in col_names else "geometry"
            con.execute(f"""
                CREATE OR REPLACE TABLE indonesia AS
                SELECT ST_Simplify({geom_col}, 0.01) AS geometry
                FROM ST_Read('/tmp/countries.geojson')
                WHERE {field} = 'Indonesia'
            """)
            click.echo(f"Indonesia boundary loaded (field: {field})")
            return

    raise RuntimeError(f"No country name field found in: {col_names}")


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
    """Extract all named streets in Indonesia from Overture Maps."""
    os.makedirs(os.path.dirname(output), exist_ok=True)

    click.echo("Connecting to DuckDB...")
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("SET s3_region='us-west-2';")
    con.execute("SET memory_limit='6GB';")
    con.execute("SET threads=4;")

    get_indonesia_boundary(con)

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
            ST_AsText(s.geometry) AS geometry_wkt
        FROM read_parquet(
            's3://overturemaps-us-west-2/release/{release}/theme=transportation/type=segment/*',
            hive_partitioning=1
        ) s, indonesia i
        WHERE s.bbox.xmin BETWEEN 95 AND 141
          AND s.bbox.ymin BETWEEN -11 AND 6
          AND names.primary IS NOT NULL
          AND length(trim(names.primary)) > 1
          AND names.primary NOT LIKE '%*%'
          AND names.primary != ''
          AND ST_Within(ST_Centroid(s.geometry), ST_SetCRS(i.geometry, 'OGC:CRS84'))
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


if __name__ == "__main__":
    cli()
