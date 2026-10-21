/**
 * Where the data lives, and what it is called. This is the only file that
 * names the data location.
 *
 * By default the page reads the Parquet from the data host, which answers
 * HTTP range requests (GitHub Pages cannot serve them for Parquet).
 * `?data=local` reads `./data/` from the same origin instead, for development
 * with `node tools/serve.mjs` after `node tools/build-parquet.mjs`.
 */

/** The month on show; `tools/build-parquet.mjs` writes the files under this name. */
export const MONTH = '2025-03';
export const MONTH_LABEL = 'March 2025';

export const DATA_HOST = 'https://data.latticegrid.dev/duckdb/lattice-grid-demo-duckdb-etl-311/';

export const PARQUET_NAME = `nyc311-${MONTH}-raw.parquet`;
export const CSV_NAME = `nyc311-${MONTH}-raw.csv`;

/** The base URL of the data files, with a trailing slash. */
export function dataBase() {
  const local = new URLSearchParams(location.search).get('data') === 'local';
  return local ? new URL('./data/', location.href).href : DATA_HOST;
}

export const PARQUET_URL = () => dataBase() + PARQUET_NAME;
export const CSV_URL = () => dataBase() + CSV_NAME;

export const SOURCE_URL = 'https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2020-to-Present/erm2-nwe9';
export const TERMS_URL = 'https://www1.nyc.gov/home/terms-of-use.page';
