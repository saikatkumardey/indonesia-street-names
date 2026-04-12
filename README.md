# Indonesia Street Names

A dataset of every named street in Indonesia, extracted from [Overture Maps](https://overturemaps.org/).

## Dataset

`indonesia_streets.csv` — columns:
- `street_name` — unique street name (deduplicated)
- `osm_way_id` — source OSM way ID number (where applicable)
- `source_dataset` — data source (e.g. OpenStreetMap)
- `geometry_wkt` — road geometry as WKT LineString

## Updating

Run the workflow manually via GitHub Actions: **Actions → Extract Indonesia Street Names → Run workflow**

The CSV is committed back to this repo automatically.

## Source

Overture Maps Foundation transportation/segment layer, release `2026-03-18.0`.  
License: [ODbL 1.0](https://opendatacommons.org/licenses/odbl/)

Built by [Saikat Kumar Dey](https://saikatkumardey.com)
