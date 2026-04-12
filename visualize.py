import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.collections as mc
from shapely import wkt
import numpy as np
import sys

print("Loading parquet...", flush=True)
df = duckdb.query("SELECT geometry_wkt FROM 'indonesia_streets.parquet' WHERE geometry_wkt IS NOT NULL").df()
print(f"{len(df)} rows loaded", flush=True)

print("Parsing geometries...", flush=True)
lines = []
for geom_str in df['geometry_wkt']:
    try:
        geom = wkt.loads(geom_str)
        if geom.geom_type == 'LineString':
            lines.append(np.array(geom.coords))
        elif geom.geom_type == 'MultiLineString':
            for part in geom.geoms:
                lines.append(np.array(part.coords))
    except Exception:
        continue

print(f"{len(lines)} line segments parsed", flush=True)

print("Rendering map...", flush=True)
fig, ax = plt.subplots(figsize=(24, 14), facecolor='#0a0a0a')
ax.set_facecolor('#0a0a0a')

# Render as line collection for speed
lc = mc.LineCollection(lines, linewidths=0.08, colors='#c8a96e', alpha=0.4)
ax.add_collection(lc)

# Set bounds to Indonesia
ax.set_xlim(95, 141)
ax.set_ylim(-11, 6)
ax.set_aspect('equal')
ax.axis('off')

fig.text(0.02, 0.04, 'Every Named Street in Indonesia', color='#c8a96e',
         fontsize=14, fontfamily='monospace', alpha=0.8)
fig.text(0.02, 0.01, 'Source: Overture Maps Foundation', color='#666666',
         fontsize=8, fontfamily='monospace')

plt.savefig('map.png', dpi=150, bbox_inches='tight',
            facecolor='#0a0a0a', edgecolor='none')
print("map.png saved", flush=True)
