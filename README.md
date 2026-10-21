# lattice-grid-demo-duckdb-etl-311

An ETL pipeline, before and after, on a month of NYC 311 service requests, in the browser. Six SQL steps (load raw, cast types, parse timestamps, normalise borough and agency, drop duplicates and closed-before-created rows, derive resolution hours) each run as a DuckDB-Wasm view over the one before. [Lattice Grid](https://latticegrid.dev) shows the raw rows and the current step's output side by side, each with a per-column summary; a chart of requests by complaint type follows the output grid; "Download clean Parquet" exports the final step with `COPY ... TO`.

Plain JavaScript, no build step.

```
npm ci
node tools/build-parquet.mjs   # downloads March 2025 from NYC Open Data, writes data/*.parquet and *.csv
npm start                      # then open http://localhost:<port>/?data=local
npm run verify                 # headless Chrome (Node 22), needs the data built
```

By default the page reads its data from `https://data.latticegrid.dev/duckdb/lattice-grid-demo-duckdb-etl-311/` (one constant, `src/config.js`); `?data=local` reads `./data/`. See [data/README.md](data/README.md) for source, licence and sizes.

Data: NYC Open Data, City of New York. Subject to the [NYC Open Data terms of use](https://www1.nyc.gov/home/terms-of-use.page).
