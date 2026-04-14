import json
import duckdb
import numpy as np
import pandas as pd
import datashader as ds
import datashader.transfer_functions as tf
from shapely import wkt
from PIL import Image, ImageDraw
import click

# Map extent
X_MIN, X_MAX = 95, 141
Y_MIN, Y_MAX = -11, 6

# Colors
OCEAN_BG    = '#f0f4f8'   # light blue-gray for ocean/background
LAND_FILL   = '#e8eae6'   # light warm gray for land
LAND_BORDER = '#c8ccc6'   # slightly darker gray for province borders
GREEN_CMAP  = ['#43a047', '#2e7d32', '#1b5e20']


def lon_to_px(lon, width):
    return int((lon - X_MIN) / (X_MAX - X_MIN) * width)

def lat_to_px(lat, height):
    return int((Y_MAX - lat) / (Y_MAX - Y_MIN) * height)

def draw_land(boundary_path, width, height):
    """Render province polygons onto a PIL image."""
    img = Image.new('RGBA', (width, height), OCEAN_BG)
    draw = ImageDraw.Draw(img)

    try:
        with open(boundary_path) as f:
            gj = json.load(f)
    except Exception:
        return img

    def draw_poly(coords):
        pts = [(lon_to_px(c[0], width), lat_to_px(c[1], height)) for c in coords]
        if len(pts) < 3:
            return
        draw.polygon(pts, fill=LAND_FILL, outline=LAND_BORDER)

    for feat in gj.get('features', []):
        geom = feat.get('geometry', {})
        gtype = geom.get('type', '')
        if gtype == 'Polygon':
            draw_poly(geom['coordinates'][0])
        elif gtype == 'MultiPolygon':
            for poly in geom['coordinates']:
                draw_poly(poly[0])

    return img


@click.command()
@click.option('--input', 'input_path', default='data/indonesia_streets_clean.parquet', show_default=True)
@click.option('--output', 'output_path', default='data/map.png', show_default=True)
@click.option('--boundary', default='data/indonesia_boundary.geojson', show_default=True)
@click.option('--width', default=4000, show_default=True)
@click.option('--height', default=1481, show_default=True, help='Matches Indonesia aspect ratio (~2.7:1)')
def visualize(input_path, output_path, boundary, width, height):
    """Generate a light-themed street map with land context."""
    import os
    os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)

    # --- Land layer ---
    click.echo('Rendering land polygons...')
    base = draw_land(boundary, width, height)

    # --- Street layer ---
    click.echo(f'Loading geometries from {input_path}...')
    df = duckdb.query(f"SELECT geometry_wkt FROM '{input_path}' WHERE geometry_wkt IS NOT NULL").df()
    click.echo(f'  {len(df):,} streets')

    xs, ys = [], []
    for geom_str in df.geometry_wkt:
        try:
            geom = wkt.loads(geom_str)
            if geom.geom_type == 'LineString':
                coords = list(geom.coords)
            elif geom.geom_type == 'MultiLineString':
                coords = [c for part in geom.geoms for c in part.coords]
            else:
                continue
            for x, y in coords:
                xs.append(x); ys.append(y)
            xs.append(np.nan); ys.append(np.nan)
        except Exception:
            continue

    lines = pd.DataFrame({'x': xs, 'y': ys})
    click.echo(f'  {len(lines):,} coordinate points')

    click.echo(f'Rendering {width}x{height} streets...')
    cvs = ds.Canvas(plot_width=width, plot_height=height,
                    x_range=(X_MIN, X_MAX), y_range=(Y_MIN, Y_MAX))
    agg = cvs.line(lines, 'x', 'y', ds.count())
    streets = tf.shade(agg, cmap=GREEN_CMAP, how='eq_hist')
    streets = tf.dynspread(streets, threshold=0.5, max_px=4)

    # --- Composite ---
    street_pil = streets.to_pil().convert('RGBA')
    # Replace white (background) pixels from datashader with transparent
    r, g, b, a = street_pil.split()
    arr = np.array(street_pil)
    # Pixels where datashader put background (white) → transparent
    white_mask = (arr[:, :, 0] > 250) & (arr[:, :, 1] > 250) & (arr[:, :, 2] > 250)
    arr[white_mask, 3] = 0
    street_pil = Image.fromarray(arr, 'RGBA')

    result = Image.alpha_composite(base.convert('RGBA'), street_pil)

    click.echo(f'Saving to {output_path}...')
    result.convert('RGB').save(output_path, dpi=(300, 300))
    click.echo('Done.')


if __name__ == '__main__':
    visualize()
