/**
 * Build the demo's data files from NYC Open Data, public sources only.
 *
 * Source: "311 Service Requests from 2020 to Present" (Socrata dataset
 * erm2-nwe9) on NYC Open Data. This script downloads one month through the
 * Socrata CSV export and writes two files into data/:
 *
 *   nyc311-<month>-raw.parquet   every column VARCHAR, exactly as exported
 *                                (empty strings stay empty strings), sorted by
 *                                created_date and written in row groups of
 *                                100,000 rows with zstd, so the browser can
 *                                read byte ranges of it.
 *   nyc311-<month>-raw.csv       the same rows as the CSV the page offers for
 *                                the "raw file" view and as a download.
 *
 * The files are NOT committed (see .gitignore). Re-run this and upload the
 * output to https://data.latticegrid.dev/duckdb/lattice-grid-demo-duckdb-etl-311/
 *
 *   node tools/build-parquet.mjs                  # March 2025
 *   node tools/build-parquet.mjs --month 2025-04  # another calendar month
 *
 * Nothing is cleaned here. Cleaning is what the page demonstrates.
 */

import { createHash } from 'node:crypto';
import { createWriteStream } from 'node:fs';
import { access, mkdir, rename, stat, writeFile } from 'node:fs/promises';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const args = process.argv.slice(2);
const month = args.includes('--month') ? args[args.indexOf('--month') + 1] : '2025-03';
const match = /^(\d{4})-(\d{2})$/.exec(month);
if (!match || Number(match[2]) < 1 || Number(match[2]) > 12) throw new Error(`--month wants YYYY-MM, got "${month}"`);
const [year, mon] = [Number(match[1]), Number(match[2])];
const next = mon === 12 ? `${year + 1}-01` : `${year}-${String(mon + 1).padStart(2, '0')}`;

const cache = join(root, '.cache');
const dataDir = join(root, 'data');
await mkdir(cache, { recursive: true });
await mkdir(dataDir, { recursive: true });

/* The columns the demo shows. The export has 44; these are the ones a reader
   can follow, in the dataset's own names and order. */
const COLUMNS = [
  'unique_key', 'created_date', 'closed_date', 'agency', 'agency_name', 'complaint_type', 'descriptor',
  'location_type', 'incident_zip', 'incident_address', 'city', 'status', 'community_board', 'borough',
  'open_data_channel_type', 'latitude', 'longitude',
];

const API = 'https://data.cityofnewyork.us/resource/erm2-nwe9.csv';
const params = new URLSearchParams({
  $select: COLUMNS.join(','),
  $where: `created_date >= '${month}-01T00:00:00' AND created_date < '${next}-01T00:00:00'`,
  $order: 'created_date,unique_key',
  $limit: '500000',
});
const url = `${API}?${params}`;
const exported = join(cache, `export-${month}.csv`);

const exists = async (p) => { try { await access(p); return true; } catch { return false; } };
const mb = (n) => `${(n / 1048576).toFixed(2)} MB`;

if (await exists(exported)) {
  console.log(`Using the cached export ${exported}`);
} else {
  console.log(`Downloading ${url}`);
  const response = await fetch(url);
  if (!response.ok) throw new Error(`NYC Open Data answered ${response.status}`);
  const part = `${exported}.part`;
  await pipeline(Readable.fromWeb(response.body), createWriteStream(part));
  await rename(part, exported);
  console.log(`  ${mb((await stat(exported)).size)}`);
}

const { DuckDBInstance } = await import('@duckdb/node-api');
const conn = await (await DuckDBInstance.create(':memory:')).connect();
const q = (s) => s.replaceAll("'", "''");

/* every column VARCHAR, and nullstr set to something that never occurs so that
   empty cells stay '' instead of being turned into NULL: that is the raw state. */
await conn.run(`
  CREATE TABLE raw AS
  SELECT ${COLUMNS.map((c) => `"${c}"::VARCHAR AS "${c}"`).join(', ')}
  FROM read_csv('${q(exported)}', header = true, all_varchar = true, nullstr = '\u0001<none>\u0001', sample_size = -1)
  ORDER BY created_date, unique_key
`);

const stem = `nyc311-${month}-raw`;
const parquet = join(dataDir, `${stem}.parquet`);
const csv = join(dataDir, `${stem}.csv`);
await conn.run(`COPY raw TO '${q(parquet)}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)`);
await conn.run(`COPY raw TO '${q(csv)}' (FORMAT csv, HEADER true)`);

const one = async (sql) => (await conn.runAndReadAll(sql)).getRowObjects()[0];
const rows = Number((await one(`SELECT count(*) AS n FROM read_parquet('${q(parquet)}')`)).n);
const groups = Number((await one(`SELECT count(DISTINCT row_group_id) AS n FROM parquet_metadata('${q(parquet)}')`)).n);
const csvRows = Number((await one(`SELECT count(*) AS n FROM read_csv('${q(csv)}', header = true, all_varchar = true)`)).n);
if (rows !== csvRows) throw new Error(`Parquet has ${rows} rows but the CSV has ${csvRows}`);
const span = await one(`SELECT min(created_date) AS first, max(created_date) AS last FROM read_parquet('${q(parquet)}')`);

const sha = async (p) => {
  const h = createHash('sha256');
  await pipeline((await import('node:fs')).createReadStream(p), h);
  return h.digest('hex');
};
const facts = {
  month,
  rows,
  row_groups: groups,
  columns: COLUMNS.length,
  first_created: String(span.first),
  last_created: String(span.last),
  parquet: { file: `${stem}.parquet`, bytes: (await stat(parquet)).size, sha256: await sha(parquet) },
  csv: { file: `${stem}.csv`, bytes: (await stat(csv)).size },
  source: 'https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2020-to-Present/erm2-nwe9',
  export_url: url,
  built: new Date().toISOString().slice(0, 10),
};
await writeFile(join(dataDir, 'facts.json'), `${JSON.stringify(facts, null, 2)}\n`);
console.log(JSON.stringify(facts, null, 2));
