import duckdb
import numpy as np
import pandas as pd
import datashader as ds
import datashader.transfer_functions as tf
import colorcet
from shapely import wkt
import click


@click.command()
@click.option("--input", "input_path", default="data/indonesia_streets_filtered.parquet", show_default=True)
@click.option("--output", "output_path", default="data/map.png", show_default=True)
@click.option("--width", default=4000, show_default=True, help="Output image width in pixels")
@click.option("--height", default=2333, show_default=True, help="Output image height in pixels")
def visualize(input_path, output_path, width, height):
    """Generate a street density map from the parquet dataset."""
    import os
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    click.echo(f"Loading geometries from {input_path}...")
    df = duckdb.query(f"SELECT geometry_wkt FROM '{input_path}' WHERE geometry_wkt IS NOT NULL").df()
    click.echo(f"{len(df):,} rows loaded")

    click.echo("Expanding to coordinate arrays...")
    xs, ys = [], []
    for geom_str in df.geometry_wkt:
        try:
            geom = wkt.loads(geom_str)
            if geom.geom_type == 'LineString':
                coords = list(geom.coords)
            elif geom.geom_type == 'MultiLineString':
                coords = [c for part in geom.geoms for c in list(part.coords)]
            else:
                continue
            for x, y in coords:
                xs.append(x)
                ys.append(y)
            xs.append(np.nan)
            ys.append(np.nan)
        except Exception:
            continue

    lines = pd.DataFrame({'x': xs, 'y': ys})
    click.echo(f"{len(lines):,} coordinate points")

    click.echo(f"Rendering {width}x{height} map...")
    cvs = ds.Canvas(plot_width=width, plot_height=height,
                    x_range=(95, 141), y_range=(-11, 6))
    agg = cvs.line(lines, 'x', 'y', ds.count())
    img = tf.shade(agg, cmap=colorcet.fire, how='log')
    img = tf.set_background(img, 'black')

    click.echo(f"Saving to {output_path}...")
    img.to_pil().save(output_path, dpi=(300, 300))
    click.echo("Done.")


if __name__ == "__main__":
    visualize()
