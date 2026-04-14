# Indonesia Street Names

![Extract](https://github.com/saikatkumardey/indonesia-street-names/actions/workflows/extract.yml/badge.svg)
![Visualize](https://github.com/saikatkumardey/indonesia-street-names/actions/workflows/visualize.yml/badge.svg)

A dataset of every uniquely named street in Indonesia, extracted from [Overture Maps](https://overturemaps.org/).

**141,358 confirmed street names** across 34 provinces (Jalan/Gang/Lorong prefix required). S2-filtered to Indonesia, enriched with province metadata.

![Every named street in Indonesia](data/map.png)

## Dataset

| File | Description |
|------|-------------|
| `data/indonesia_streets_clean.parquet` | **Primary dataset**: confirmed street names (Jalan/Gang/Lorong prefix), province metadata (13MB) |
| `data/indonesia_streets_enriched.parquet` | All S2-filtered streets with province, including non-prefixed names (16MB) |
| `data/indonesia_streets_filtered.parquet` | S2-filtered only, no province enrichment (16MB) |
| `data/indonesia_streets.parquet` | Raw bbox extract, includes minor border overlap (18MB) |
| `data/sample.csv` | 100-row random preview |

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `street_name` | string | Unique street name |
| `osm_way_id` | string | Source OSM way ID |
| `source_dataset` | string | Data source (e.g. OpenStreetMap) |
| `geometry_wkt` | string | Road geometry as WKT LineString |
| `province` | string | Indonesian province (GADM L1, e.g. `JawaBarat`) |

### Coverage

Coverage reflects OpenStreetMap + Microsoft + TomTom contributions via Overture Maps. Java is well-mapped; eastern Indonesia is sparse.

| Province | Streets | Province | Streets |
|----------|---------|----------|---------|
| JawaBarat | 29,458 | KalimantanSelatan | 2,552 |
| JawaTimur | 23,401 | KalimantanBarat | 2,401 |
| JakartaRaya | 22,265 | KalimantanTengah | 2,324 |
| JawaTengah | 15,290 | SumateraSelatan | 1,511 |
| Banten | 13,165 | SumateraBarat | 1,128 |
| Bali | 5,861 | Aceh | 1,032 |
| SumateraUtara | 5,532 | NusaTenggaraTimur | 934 |
| SulawesiSelatan | 5,106 | Jambi | 905 |
| KalimantanTimur | 4,600 | KepulauanRiau | 794 |
| Riau | 4,365 | Papua | 469 |
| Yogyakarta | 3,680 | Maluku | 277 |
| NusaTenggaraBarat | 3,197 | MalukuUtara | 85 |
| Lampung | 2,769 | *(other provinces)* | ~800 |

Java + Banten + Jakarta = ~107K streets (68% of total). Coverage will improve as Overture Maps releases are updated.

## Usage

```python
import duckdb

# Query directly — no download needed
df = duckdb.query("""
    SELECT street_name, province, osm_way_id
    FROM 'https://github.com/saikatkumardey/indonesia-street-names/raw/main/data/indonesia_streets_clean.parquet'
    WHERE province = 'JakartaRaya'
      AND street_name ILIKE 'jalan sudirman%'
""").df()
```

## Fun Stats

| Stat | Value |
|------|-------|
| Total streets | 141,358 |
| Median name length | 18 chars |
| Shortest | `Gg.` (3 chars) |
| Longest | 89 chars — a full address crammed into the name field |
| Typical word count | 3 words (43% of streets) |
| Numbered streets | 63,751 (45%) — end in roman or arabic numeral |
| Named after people | ~1,019 — follow "Jalan H. / K.H. / Dr." pattern |

**Most common words** (after stripping Jalan/Gang/Lorong):

| Word | Count | Meaning |
|------|-------|---------|
| Raya | 6,072 | Main / grand road |
| Haji | 5,049 | Named after Hajj pilgrims |
| Timur / Barat / Utara / Selatan | 3,600 / 3,261 / 2,651 / 2,244 | Cardinal directions |
| Indah / Asri / Permai / Jaya | ~3,000 each | Beautiful / peaceful / prosperous — housing estate names |
| Taman / Bukit / Gunung | ~1,900 / 1,237 / 1,387 | Garden / hill / mountain |

## Updating

Workflows under **Actions**:

- **Extract Indonesia Street Names** — re-runs full extraction from Overture Maps, then filters → enriches → generates sample, commits all outputs
- **Generate Map** — regenerates `data/map.png` (also auto-triggers after extract)

## Source

Overture Maps Foundation, release `2026-03-18.0`, transportation/segment layer.
Province boundaries: [GADM 4.1](https://gadm.org/) (IDN_1).
License: [ODbL 1.0](https://opendatacommons.org/licenses/odbl/)
