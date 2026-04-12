import duckdb
import numpy as np
import pandas as pd
import datashader as ds
import datashader.transfer_functions as tf
import colorcet
from shapely import wkt

print("Loading geometries...", flush=True)
df = duckdb.query("SELECT geometry_wkt FROM 'data/indonesia_streets.parquet' WHERE geometry_wkt IS NOT NULL").df()
print(f"{len(df):,} rows", flush=True)

print("Expanding to coordinate arrays...", flush=True)
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
print(f"{len(lines):,} coordinate points", flush=True)

print("Rendering...", flush=True)
cvs = ds.Canvas(plot_width=4000, plot_height=2333,
                x_range=(95, 141), y_range=(-11, 6))
agg = cvs.line(lines, 'x', 'y', ds.count())
img = tf.shade(agg, cmap=colorcet.fire, how='log')
img = tf.set_background(img, 'black')

print("Saving map.png...", flush=True)
img.to_pil().save('data/map.png', dpi=(300, 300))
print("Done", flush=True)
