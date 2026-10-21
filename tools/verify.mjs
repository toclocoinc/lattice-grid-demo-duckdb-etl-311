/**
 * Load the page in headless Chrome against the local data and check that it
 * tells the truth. Every expected figure is recomputed here by a second DuckDB
 * (Node), with the cleaning rules written out again independently of
 * src/steps.js, so an error in the page's SQL cannot hide by being made twice.
 *
 * Interactions use real input events: clicks on the step buttons and on a
 * column header, typing in the search boxes, a click on the download button.
 *
 * Usage:  node tools/verify.mjs [--shots <dir>]
 * Needs data/ built first:  node tools/build-parquet.mjs
 */

import { mkdir, mkdtemp, readdir, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { startServer } from './serve.mjs';
import { openBrowser } from './browser.mjs';
import { PARQUET_NAME } from '../src/config.js';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const args = process.argv.slice(2);
const shotDir = args.includes('--shots') ? resolve(args[args.indexOf('--shots') + 1]) : null;

const failures = [];
const check = (ok, what, detail) => {
  const line = `${what}${detail ? ` (${detail})` : ''}`;
  if (!ok) failures.push(line);
  console.log(`  ${ok ? 'ok  ' : 'FAIL'} ${line}`);
};
const int = (n) => Number(n).toLocaleString('en-US');

/* ------------------------------------------------ the independent answers */

const TEXT_COLUMNS = 'unique_key, agency, agency_name, complaint_type, descriptor, location_type, incident_zip, incident_address, city, status, community_board, borough, open_data_channel_type, created_date, closed_date, latitude, longitude';

async function nodeAnswers() {
  const { DuckDBInstance } = await import('@duckdb/node-api');
  const conn = await (await DuckDBInstance.create(':memory:')).connect();
  const file = join(root, 'data', PARQUET_NAME).replaceAll("'", "''");
  const rows = async (sql) => (await conn.runAndReadAll(sql)).getRowObjects().map((r) => Object.fromEntries(
    Object.entries(r).map(([k, v]) => [k, typeof v === 'bigint' ? Number(v) : v])));
  const one = async (sql) => (await rows(sql))[0];

  await conn.run(`CREATE TABLE raw AS SELECT * FROM read_parquet('${file}')`);
  /* the cleaning, written again: typed, blanks to NULL, normalised, filtered */
  const nul = (c) => `NULLIF(trim(${c}), '')`;
  await conn.run(`
    CREATE TABLE typed AS SELECT
      TRY_CAST(${nul('unique_key')} AS BIGINT) AS unique_key,
      TRY_CAST(${nul('created_date')} AS TIMESTAMP) AS created_date,
      TRY_CAST(${nul('closed_date')} AS TIMESTAMP) AS closed_date,
      ${nul('complaint_type')} AS complaint_type,
      CASE WHEN upper(trim(borough)) IN ('', 'UNSPECIFIED') THEN NULL ELSE trim(borough) END AS borough,
      ${nul('descriptor')} AS descriptor, upper(trim(agency)) AS agency
    FROM raw`);
  await conn.run(`
    CREATE TABLE clean AS SELECT *, round(epoch(closed_date - created_date) / 3600.0, 2) AS resolution_hours
    FROM (SELECT DISTINCT ON (unique_key) * FROM typed
          WHERE unique_key IS NOT NULL AND (closed_date IS NULL OR closed_date >= created_date)
          ORDER BY unique_key)`);

  const counts = {
    raw: (await one('SELECT count(*) AS n FROM raw')).n,
    typed: (await one('SELECT count(*) AS n FROM typed')).n,
    clean: (await one('SELECT count(*) AS n FROM clean')).n,
    badDates: (await one('SELECT count(*) AS n FROM typed WHERE closed_date < created_date')).n,
  };
  const quickTerm = 'driveway';
  const out = {
    counts,
    quickTerm,
    /* the grid's quick search is "any column's text contains the term" */
    quickClean: (await one(`SELECT count(*) AS n FROM raw WHERE TRY_CAST(unique_key AS BIGINT) IN (SELECT unique_key FROM clean) AND concat_ws('|', ${TEXT_COLUMNS}) ILIKE '%${quickTerm}%'`)).n,
    quickRaw: (await one(`SELECT count(*) AS n FROM raw WHERE concat_ws('|', ${TEXT_COLUMNS}) ILIKE '%${quickTerm}%'`)).n,
    brooklynClean: (await one(`SELECT count(*) AS n FROM clean WHERE borough IS NOT NULL AND lower(borough) = 'brooklyn'`)).n,
    firstRaw: (await rows('SELECT unique_key FROM raw ORDER BY created_date, unique_key LIMIT 3')).map((r) => String(r.unique_key)),
    firstClean: (await rows('SELECT unique_key FROM clean ORDER BY unique_key LIMIT 3')).map((r) => r.unique_key),
    lastClean: (await rows('SELECT unique_key FROM clean ORDER BY unique_key DESC LIMIT 3')).map((r) => r.unique_key),
    topComplaints: await rows('SELECT complaint_type, count(*) AS n FROM clean GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 6'),
    closedNull: (await one('SELECT count(*) AS n FROM clean WHERE closed_date IS NULL')).n,
    rawClosedBlank: (await one(`SELECT count(*) AS n FROM raw WHERE trim(closed_date) = ''`)).n,
    boroughNullClean: (await one('SELECT count(*) AS n FROM clean WHERE borough IS NULL')).n,
    boroughDistinctClean: (await one('SELECT count(DISTINCT borough) AS n FROM clean')).n,
    agencyDistinctRaw: (await one('SELECT count(DISTINCT agency) AS n FROM raw')).n,
    resolutionSum: (await one('SELECT round(sum(resolution_hours), 2) AS s FROM clean')).s,
    resolutionNulls: (await one('SELECT count(*) AS n FROM clean WHERE resolution_hours IS NULL')).n,
  };
  return { out, conn, file };
}

/* ------------------------------------------------------------------ run */

const served = await startServer(0);
const origin = `http://127.0.0.1:${served.port}`;
const downloads = await mkdtemp(join(tmpdir(), 'etl311-dl-'));
let browser;
try {
  console.log(`Serving ${origin}`);
  const expected = await nodeAnswers();
  const E = expected.out;
  console.log(`Expected (Node DuckDB): raw ${int(E.counts.raw)}, clean ${int(E.counts.clean)}, closed-before-created ${E.counts.badDates}`);

  browser = await openBrowser({ width: 1440, height: 1000, downloadDir: downloads });
  const b = browser;
  const demo = (expr) => b.evaluate(`(() => { const d = window.__etlDemo; return ${expr}; })()`);
  const shoot = async (name) => {
    if (!shotDir) return;
    await mkdir(shotDir, { recursive: true });
    await writeFile(join(shotDir, `${name}.png`), Buffer.from(await b.screenshot(), 'base64'));
  };

  await b.call('Page.navigate', { url: `${origin}/?data=local` });
  await b.waitFor('!!window.__etlDemo && (window.__etlDemo.ready || window.__etlDemo.error)', 240000, 'the page to report in');
  check(await demo('d.ready === true'), 'the page reports ready', await demo('d.error || ""'));
  const paneReady = (pane, total) => `d.panes.${pane}.grid && d.panes.${pane}.total === ${total} && d.panes.${pane}.grid.rows.count() > 0 && d.panes.${pane}.summary.length > 0`;
  await b.waitFor(`(() => { const d = window.__etlDemo; return ${paneReady('left', E.counts.raw)} && ${paneReady('right', E.counts.clean)} && d.chartData; })()`, 120000, 'both grids, summaries and the chart');

  /* ---- the raw side */
  check(await demo('d.panes.left.total') === E.counts.raw, 'raw grid row count is the file\'s', `${int(E.counts.raw)}`);
  await b.waitFor('window.__etlDemo.panes.left.grid.rows.get(2)?.data?.unique_key', 60000, 'the raw rows');
  const leftFirst = await demo('[0, 1, 2].map((i) => String(d.panes.left.grid.rows.get(i).data.unique_key))');
  check(JSON.stringify(leftFirst) === JSON.stringify(E.firstRaw), 'raw grid first rows match the file', leftFirst.join(', '));
  check(await demo('d.panes.left.pane === undefined && d.panes.left.columns.every((c) => c.type === "VARCHAR")'), 'every raw column is VARCHAR');
  const rawFile = await demo('document.querySelector("[data-role=raw-file]").textContent');
  check(rawFile.startsWith('unique_key,created_date') && rawFile.split('\n').length >= 5, 'the raw file view shows the CSV header and rows');

  /* ---- each step, by clicking it */
  const stepCounts = { 1: E.counts.raw, 2: E.counts.raw, 3: E.counts.raw, 4: E.counts.raw, 5: E.counts.clean, 6: E.counts.clean };
  for (let id = 1; id <= 6; id += 1) {
    await b.click(`[data-step="${id}"]`);
    await b.waitFor(`(() => { const d = window.__etlDemo; return d.step === ${id} && !d.busy && ${paneReady('right', stepCounts[id])}; })()`, 120000, `step ${id}`);
    const shown = await demo(`({ total: d.panes.right.total, rows: d.panes.right.grid.rows.count(), sql: document.querySelector("[data-role=sql]").textContent, title: document.querySelector("[data-role=step-title]").textContent, types: d.panes.right.columns.map((c) => c.name + ":" + c.type).join(",") })`);
    check(shown.total === stepCounts[id] && shown.rows > 0, `step ${id}: output grid has ${int(stepCounts[id])} rows`, `showing ${shown.rows}`);
    check(shown.sql.includes(`step_${id}`) && shown.title.startsWith(`Step ${id}`), `step ${id}: its SQL is on show`);
    if (id === 2) check(/unique_key:BIGINT/.test(shown.types) && /latitude:DOUBLE/.test(shown.types), 'step 2: ids and coordinates are numbers');
    if (id === 3) check(/created_date:TIMESTAMP/.test(shown.types) && /created_day:DATE/.test(shown.types), 'step 3: timestamps and a day column');
    if (id === 5) check(await demo('d.rowCounts[4] - d.rowCounts[5]') === E.counts.raw - E.counts.clean, `step 5 drops ${E.counts.raw - E.counts.clean} rows`, `${E.counts.badDates} closed before created`);
    if (id === 6) check(/resolution_hours:DOUBLE/.test(shown.types), 'step 6: resolution_hours derived');
  }

  /* ---- summaries agree with Node */
  const summary = await demo('Object.fromEntries(d.panes.right.summary.map((s) => [s.column, s]))');
  const rawSummary = await demo('Object.fromEntries(d.panes.left.summary.map((s) => [s.column, s]))');
  check(summary.closed_date.nulls === E.closedNull && rawSummary.closed_date.nulls === E.rawClosedBlank, 'summary: closed_date blanks (raw) and nulls (clean)', `${E.rawClosedBlank} → ${E.closedNull}`);
  check(summary.borough.nulls === E.boroughNullClean && summary.borough.distinct === E.boroughDistinctClean, 'summary: borough nulls and distinct after normalising', `${summary.borough.distinct} distinct`);
  check(rawSummary.agency.distinct === E.agencyDistinctRaw, 'summary: raw agency distinct');
  check(summary.resolution_hours.nulls === E.resolutionNulls, 'summary: resolution_hours nulls');
  check(summary.unique_key.type === 'BIGINT' && rawSummary.unique_key.type === 'VARCHAR', 'summary: types differ before and after');

  /* ---- sort by a real click on the header, both ways */
  const header = 'js:[...document.querySelectorAll("[data-pane=out] .lat-header-cell")].find((e) => e.textContent.trim() === "unique_key")';
  await b.click(header, { edge: true });
  await b.waitFor(`d = window.__etlDemo, d.panes.right.grid.rows.get(0)?.data?.unique_key === ${E.firstClean[0]}`, 60000, 'ascending sort');
  check(JSON.stringify(await demo('[0, 1, 2].map((i) => d.panes.right.grid.rows.get(i).data.unique_key)')) === JSON.stringify(E.firstClean), 'sort ascending (header click): first rows', E.firstClean.join(', '));
  await b.click(header, { edge: true });
  await b.waitFor(`d = window.__etlDemo, d.panes.right.grid.rows.get(0)?.data?.unique_key === ${E.lastClean[0]}`, 60000, 'descending sort');
  check(JSON.stringify(await demo('[0, 1, 2].map((i) => d.panes.right.grid.rows.get(i).data.unique_key)')) === JSON.stringify(E.lastClean), 'sort descending (header click): first rows', E.lastClean.join(', '));

  /* ---- quick search by typing */
  await b.type('[data-pane=out] [data-role=search]', E.quickTerm);
  await b.waitFor(`window.__etlDemo.panes.right.total === ${E.quickClean}`, 60000, 'quick search on the output grid');
  check(true, `quick search "${E.quickTerm}" on the output grid`, `${int(E.quickClean)} rows`);
  await b.waitFor(`window.__etlDemo.chartData && window.__etlDemo.chartData.totalRequests === ${E.quickClean}`, 60000, 'the chart to follow the search');
  check(await demo('d.chartData.top[0]') === 'Blocked Driveway' && await demo('d.chartData.totalRequests') === E.quickClean, 'the chart follows the search', `${int(E.quickClean)} requests, led by Blocked Driveway`);
  await b.type('[data-pane=raw] [data-role=search]', E.quickTerm);
  await b.waitFor(`window.__etlDemo.panes.left.total === ${E.quickRaw}`, 60000, 'quick search on the raw grid');
  check(true, `quick search "${E.quickTerm}" on the raw grid`, `${int(E.quickRaw)} rows`);

  /* clear both searches with real key events */
  for (const pane of ['out', 'raw']) {
    await b.click(`[data-pane=${pane}] [data-role=search]`);
    for (let i = 0; i < E.quickTerm.length; i += 1) {
      await b.call('Input.dispatchKeyEvent', { type: 'keyDown', key: 'Backspace', code: 'Backspace', windowsVirtualKeyCode: 8 });
      await b.call('Input.dispatchKeyEvent', { type: 'keyUp', key: 'Backspace', code: 'Backspace', windowsVirtualKeyCode: 8 });
    }
  }
  await b.waitFor(`window.__etlDemo.panes.right.total === ${E.counts.clean} && window.__etlDemo.panes.left.total === ${E.counts.raw}`, 60000, 'searches cleared');
  check(true, 'clearing the search restores both counts');

  /* ---- a column filter through the grid's filter API */
  await demo(`d.panes.right.grid.filters.set({ col: "borough", op: "eq", value: "Brooklyn" })`);
  await b.waitFor(`window.__etlDemo.panes.right.total === ${E.brooklynClean}`, 60000, 'borough filter');
  check(true, 'filter borough = Brooklyn on the output grid', `${int(E.brooklynClean)} rows`);
  const plan = await demo('d.panes.right.plan && d.panes.right.plan.unpushed');
  check(Array.isArray(plan) && plan.length === 0, 'the whole filter and sort ran in DuckDB', 'nothing unpushed');
  await demo('d.panes.right.grid.filters.clear()');
  await b.waitFor(`window.__etlDemo.panes.right.total === ${E.counts.clean}`, 60000, 'filter cleared');

  /* ---- the chart */
  await b.waitFor(`window.__etlDemo.chartData && window.__etlDemo.chartData.top.length === 6 && window.__etlDemo.chartData.totalRequests === ${E.counts.clean}`, 120000, 'the full chart');
  const chart = await demo('({ top: d.chartData.top, rows: d.chartData.rows, marks: document.querySelectorAll("[data-role=chart] svg path, [data-role=chart] svg circle, [data-role=chart] svg rect").length })');
  check(JSON.stringify(chart.top) === JSON.stringify(E.topComplaints.map((t) => t.complaint_type)), 'chart: the six top complaint types match', chart.top[0]);
  const topSum = chart.rows.filter((r) => r.complaint_type === E.topComplaints[0].complaint_type).reduce((a, r) => a + r.requests, 0);
  check(topSum === E.topComplaints[0].n, 'chart: the top type\'s daily counts add up to Node\'s total', int(topSum));
  check(chart.marks >= chart.top.length, 'chart drew a mark per series', `${chart.marks}`);
  await b.click('[data-step="2"]');
  await b.waitFor('window.__etlDemo.step === 2 && !window.__etlDemo.busy && window.__etlDemo.chartData === null', 60000, 'chart at step 2');
  check(true, 'chart says it starts at step 3 when the step has no day column');
  await b.click('[data-step="6"]');
  await b.waitFor('window.__etlDemo.step === 6 && !window.__etlDemo.busy && window.__etlDemo.chartData && window.__etlDemo.panes.right.summary.length > 0', 60000, 'back to step 6');
  await sleep(500);
  await shoot('desktop-light');

  /* ---- the Parquet export, by clicking the button */
  await b.click('[data-role=export]');
  await b.waitFor('window.__etlDemo.exported', 120000, 'the export');
  const exported = await demo('d.exported');
  let file = null;
  for (let i = 0; i < 40 && !file; i += 1) {
    file = (await readdir(downloads)).find((n) => n.endsWith('.parquet'));
    if (!file) await sleep(250);
  }
  check(Boolean(file), 'the browser received a .parquet download', file);
  if (file) {
    const path = join(downloads, file);
    const size = (await stat(path)).size;
    check(size === exported.bytes && size > 1e6, 'downloaded file size matches what the page reported', `${(size / 1048576).toFixed(2)} MB`);
    const { conn } = expected;
    const q = path.replaceAll("'", "''");
    const got = (await conn.runAndReadAll(`SELECT count(*) AS n, count(DISTINCT unique_key) AS u, round(sum(resolution_hours), 2) AS s, count(*) FILTER (WHERE resolution_hours IS NULL) AS z FROM read_parquet('${q}')`)).getRowObjects()[0];
    check(Number(got.n) === E.counts.clean && Number(got.u) === E.counts.clean, 'clean Parquet: row count and unique ids', int(got.n));
    check(Number(got.s) === E.resolutionSum && Number(got.z) === E.resolutionNulls, 'clean Parquet: resolution_hours total and nulls match Node', `${got.s}`);
    const types = (await conn.runAndReadAll(`SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_parquet('${q}'))`)).getRowObjects();
    check(types.find((t) => t.column_name === 'unique_key')?.column_type === 'BIGINT' && types.find((t) => t.column_name === 'created_date')?.column_type.startsWith('TIMESTAMP'), 'clean Parquet is typed, not text');
  }

  /* ---- the page itself */
  const text = await demo('document.body.innerText');
  check(!/\bNaN\b|undefined|\[object/.test(text), 'no NaN / undefined / [object] text on the page');
  const watermark = await demo('(() => { const g = d.panes.right.grid; try { return g.licence.watermark(); } catch (e) { return "unreadable: " + e.message; } })()');
  check(watermark === false, 'no watermark', `licence state: ${watermark}`);
  const shownTiming = await demo('d.panes.right.ui.timing.textContent');
  check(/ms/.test(shownTiming) && /rows scanned/.test(shownTiming), 'live query timings are shown', shownTiming);
  check(b.consoleErrors.length === 0 && b.pageErrors.length === 0, '0 console errors', [...b.consoleErrors, ...b.pageErrors].join(' | ').slice(0, 300));
} catch (error) {
  failures.push(`the run stopped: ${error.message}`);
  console.log(`  FAIL the run stopped: ${error.message}`);
} finally {
  await browser?.close();
  served.server.close();
  await rm(downloads, { recursive: true, force: true }).catch(() => {});
}

console.log(failures.length ? `\n${failures.length} check(s) failed.` : '\nAll checks passed.');
process.exit(failures.length ? 1 : 0);
