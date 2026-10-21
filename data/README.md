# Data

The Parquet and CSV files are **not committed**. `node tools/build-parquet.mjs` (Node 22, `@duckdb/node-api`) downloads and writes them here; they are published at `https://data.latticegrid.dev/duckdb/lattice-grid-demo-duckdb-etl-311/` (the page's one data constant is `DATA_HOST` in `src/config.js`; `?data=local` reads this folder instead).

## Source

NYC Open Data, "311 Service Requests from 2020 to Present" (Socrata dataset `erm2-nwe9`), published by the City of New York:
<https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2020-to-Present/erm2-nwe9>

The build downloads **requests created in March 2025** through the Socrata CSV export, 17 of the dataset's 44 columns, ordered by `created_date, unique_key`.

## Licence and attribution

The dataset page states no licence. Use is subject to the NYC Open Data terms of use: <https://www1.nyc.gov/home/terms-of-use.page>. The page and this README credit the City of New York as the publisher and make no claim of endorsement.

## Files (built 2026-10-21; `facts.json` records the same figures)

| File | Rows (`SELECT count(*)`) | Size | Notes |
|---|---|---|---|
| `nyc311-2025-03-raw.parquet` | 281,221 | 9,492,714 bytes (9.05 MB) | every column VARCHAR, empty cells kept as empty strings, sorted by created_date, 3 row groups of up to 100,000 rows, zstd |
| `nyc311-2025-03-raw.csv` | 281,221 | 70,416,530 bytes (67.2 MB) | the same rows as text, for the "raw file" view and download |

After the page's six steps: 281,085 rows (136 rows closed before they were created are dropped; the month has no duplicate request ids).

The data is raw on purpose: blanks, placeholder values ("Unspecified", "UNKNOWN") and text-typed numbers and timestamps are what the cleaning steps work on. Counts change slightly if the City revises records, so a rebuild may differ from these figures; `facts.json` is rewritten each time.
