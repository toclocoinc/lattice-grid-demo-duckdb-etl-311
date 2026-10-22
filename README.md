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
## Loading

The first visit has three slow steps: the query engine (about 34 MB of WebAssembly, roughly a fifth of that on the wire), starting it, and reading the Parquet file. The page says what it is doing from first paint (pattern copied from the SaaS demo; `src/loading.js`, `src/duckdb.js`):

1. A status line in `index.html`: spinner, one plain-English sentence, progress bar. "Downloading the query engine (33 MB, first visit only): n of m MB", "Starting the engine", "Reading the March 2025 311 requests (n of m MB)", then "Building the grids, summaries and chart".
2. Real byte counts, no second download: the engine reports its own download; the worker is started through a wrapper that reports each Parquet response size over a `BroadcastChannel`; the total is the file size from a one-byte range request.
3. Every panel (both grids, both column summaries, the chart) has a loading cover until its first answer, and again when a step switch rebuilds it.

Measured with Chrome DevTools throttling at 20 Mbps / 40 ms, cache disabled, hosted data: first paint about 0.4 to 0.6 s, first rows in the raw grid about 19 s, everything (both grids, summaries, chart) about 35 s.

