import duckdb
import click
import os


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


@cli.command()
@click.option("--input", "input_path", default="data/indonesia_streets.parquet", show_default=True, help="Source parquet path")
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
