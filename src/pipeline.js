/**
 * The page: a step list, two grids (the raw rows and the current step's
 * output), a per-column summary under each, a chart bound to the output and a
 * Parquet export. Every number on it comes from a DuckDB query.
 */

import { readAll, readOne, recordingConnection } from './duckdb.js';
import { STEPS, FINAL_STEP, viewName } from './steps.js';
import {
  CSV_NAME, CSV_URL, MONTH_LABEL, PARQUET_URL, SOURCE_URL, TERMS_URL,
} from './config.js';

const el = (tag, className, text) => {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text != null) node.textContent = text;
  return node;
};
const int = (n) => (n == null ? '—' : Number(n).toLocaleString('en-US'));
const pct = (n, of) => (of ? `${((100 * n) / of).toFixed(of > 1000 && n / of < 0.001 && n > 0 ? 2 : 1)}%` : '—');

/* Column presentation. Widths are for the data, not the title. */
const WIDTH = {
  unique_key: 108, created_date: 200, closed_date: 200, created_day: 116, agency: 90, agency_name: 260,
  complaint_type: 200, descriptor: 220, location_type: 170, incident_zip: 110, incident_address: 200,
  city: 140, status: 110, community_board: 150, borough: 130, open_data_channel_type: 190,
  latitude: 110, longitude: 110, resolution_hours: 150,
};
const SET_FILTER = new Set(['agency', 'borough', 'status', 'open_data_channel_type']);

function columnFor({ column_name: field, column_type: type }) {
  const column = { field, title: field, layout: { width: WIDTH[field] ?? 130 }, allowGroup: false };
  if (field === 'unique_key') column.layout.pin = 'start';
  if (type === 'BIGINT') Object.assign(column, { type: 'number', format: { pattern: '0' }, filter: { type: 'number' } });
  else if (type === 'DOUBLE' && field === 'resolution_hours') Object.assign(column, { type: 'number', format: { pattern: '#,##0.00' }, filter: { type: 'number' } });
  else if (type === 'DOUBLE') Object.assign(column, { type: 'number', format: { pattern: '0.0000' }, filter: { type: 'number' } });
  else if (type === 'DATE') Object.assign(column, { type: 'date', filter: { type: 'date' } });
  else if (type.startsWith('TIMESTAMP')) Object.assign(column, { type: 'datetime', filter: { type: 'date' } });
  else column.filter = { type: SET_FILTER.has(field) ? 'set' : 'text' };
  return column;
}

export function buildPipeline({ root, LG, createChart, connection, db, version }) {
  const built = {
    step: null, stepMs: {}, rowCounts: {}, panes: {}, chart: null, chartGrid: null,
    chartData: null, connection, ready: false, exported: null, duckdbVersion: version,
  };

  /* ---------------------------------------------------------------- shell */
  root.textContent = '';
  const app = el('div', 'app');
  root.append(app);

  const head = el('header', 'masthead');
  const headTop = el('div', 'masthead-top');
  headTop.append(el('h1', null, 'From raw 311 text to clean Parquet, in your browser'));
  const themeButton = el('button', 'btn ghost theme-toggle');
  themeButton.type = 'button';
  headTop.append(themeButton);
  head.append(headTop);
  head.append(el('p', 'lede',
    `${MONTH_LABEL}’s New York City 311 requests arrive as a text file where every column is a string. `
    + 'Walk the six cleaning steps below: each one is a SQL view over the one before, running in DuckDB inside this tab, '
    + 'with the raw rows on the left, the step’s output on the right, and the finished table one click from a Parquet download.'));
  const facts = el('p', 'facts');
  facts.dataset.role = 'facts';
  head.append(facts);
  app.append(head);

  const layout = el('div', 'layout');
  app.append(layout);

  /* ------------------------------------------------------------- steps */
  const side = el('aside', 'steps');
  side.setAttribute('aria-label', 'Pipeline steps');
  side.append(el('h2', null, 'Pipeline'));
  const list = el('ol', 'step-list');
  const stepButtons = {};
  for (const step of STEPS) {
    const item = el('li');
    const button = el('button', 'step');
    button.type = 'button';
    button.dataset.step = String(step.id);
    const num = el('span', 'step-num', String(step.id));
    const text = el('span', 'step-text');
    text.append(el('span', 'step-title', step.title), el('span', 'step-count', 'not run yet'));
    button.append(num, text);
    button.addEventListener('click', () => { void selectStep(step.id); });
    item.append(button);
    list.append(item);
    stepButtons[step.id] = button;
  }
  side.append(list);

  const exportBox = el('div', 'export');
  const exportButton = el('button', 'btn primary', 'Download clean Parquet');
  exportButton.type = 'button';
  exportButton.dataset.role = 'export';
  const exportNote = el('p', 'export-note', 'Writes the final step with COPY ... TO and hands you the file.');
  exportNote.setAttribute('aria-live', 'polite');
  exportNote.dataset.role = 'export-note';
  exportBox.append(exportButton, exportNote);
  side.append(exportBox);
  layout.append(side);

  const main = el('div', 'main');
  layout.append(main);

  /* the current step's SQL */
  const sqlPanel = el('section', 'panel sql-panel');
  sqlPanel.setAttribute('aria-label', 'Step SQL');
  const sqlHead = el('div', 'panel-head');
  const sqlTitle = el('h2', null, '');
  sqlTitle.dataset.role = 'step-title';
  const sqlMeta = el('span', 'meta');
  sqlMeta.dataset.role = 'step-meta';
  sqlHead.append(sqlTitle, sqlMeta);
  const sqlSummary = el('p', 'step-summary');
  const sqlPre = el('pre', 'sql');
  sqlPre.dataset.role = 'sql';
  sqlPre.tabIndex = 0;
  sqlPanel.append(sqlHead, sqlSummary, sqlPre);
  main.append(sqlPanel);

  /* the two grids */
  const compare = el('div', 'compare');
  main.append(compare);

  function makePanel(role, title, subtitle) {
    const panel = el('section', 'panel grid-panel');
    panel.dataset.pane = role;
    panel.setAttribute('aria-label', title);
    const h = el('div', 'panel-head');
    const heading = el('h2', null, title);
    heading.dataset.role = 'pane-title';
    h.append(heading);
    const sub = el('p', 'panel-sub', subtitle);
    const tools = el('div', 'pane-tools');
    const search = el('input', 'search');
    search.type = 'search';
    search.placeholder = 'Search every column';
    search.setAttribute('aria-label', `Search ${title}`);
    search.dataset.role = 'search';
    tools.append(search);
    const timing = el('p', 'timing');
    timing.dataset.role = 'timing';
    timing.setAttribute('aria-live', 'off');
    const gridHost = el('div', 'grid-host');
    gridHost.setAttribute('aria-label', `${title} rows`);
    const summaryTitle = el('h3', null, 'Column summary');
    const summaryHost = el('div', 'summary-host');
    summaryHost.setAttribute('aria-label', `${title} column summary`);
    panel.append(h, sub, tools, gridHost, timing, summaryTitle, summaryHost);
    compare.append(panel);
    return { panel, heading, sub, search, timing, gridHost, summaryHost };
  }
  const leftUi = makePanel('raw', 'Raw', 'Step 1 as loaded: all text, blanks still empty strings.');
  const rightUi = makePanel('out', 'Output', '');

  /* raw file */
  const rawFile = el('section', 'panel raw-file');
  rawFile.setAttribute('aria-label', 'The raw file');
  const rawHead = el('div', 'panel-head');
  rawHead.append(el('h2', null, 'The raw file'));
  const rawLink = el('a', 'btn ghost', `Download ${CSV_NAME}`);
  rawLink.href = CSV_URL();
  rawLink.setAttribute('download', CSV_NAME);
  rawHead.append(rawLink);
  const rawPre = el('pre', 'sql raw-lines');
  rawPre.dataset.role = 'raw-file';
  rawPre.tabIndex = 0;
  rawPre.textContent = 'Reading the first lines of the CSV...';
  rawFile.append(rawHead, el('p', 'panel-sub', 'The first lines of the export as text, read with an HTTP range request: this is what the cleaning starts from.'), rawPre);
  main.append(rawFile);

  /* chart */
  const chartPanel = el('section', 'panel chart-panel');
  chartPanel.setAttribute('aria-label', 'Requests by complaint type');
  const chartHead = el('div', 'panel-head');
  chartHead.append(el('h2', null, 'Requests by complaint type, by day'));
  const chartSub = el('p', 'panel-sub', '');
  chartSub.dataset.role = 'chart-note';
  const chartBox = el('div', 'chart-box');
  chartBox.dataset.role = 'chart';
  chartPanel.append(chartHead, chartSub, chartBox);
  main.append(chartPanel);

  const colophon = el('footer', 'colophon');
  colophon.innerHTML =
    `Data: <a href="${SOURCE_URL}">311 Service Requests from 2020 to Present</a>, NYC Open Data, `
    + 'published by the City of New York. The dataset page states no licence; use is subject to the '
    + `<a href="${TERMS_URL}">NYC Open Data terms of use</a>. This page shows one month of the city’s data and is not endorsed by or `
    + 'affiliated with the City. Built with <a href="https://www.latticegrid.dev/charts/">Lattice Grid</a> and '
    + '<a href="https://duckdb.org/docs/api/wasm/overview.html">DuckDB-Wasm</a>.';
  app.append(colophon);

  /* ---------------------------------------------------------------- theme */
  const rootEl = document.documentElement;
  const media = window.matchMedia('(prefers-color-scheme: dark)');
  const requested = new URLSearchParams(location.search).get('theme');
  let theme = requested === 'dark' || requested === 'light' ? requested : (media.matches ? 'dark' : 'light');
  const applyTheme = () => {
    rootEl.dataset.theme = theme;
    themeButton.textContent = theme === 'dark' ? 'Light theme' : 'Dark theme';
    themeButton.setAttribute('aria-label', `Switch to the ${theme === 'dark' ? 'light' : 'dark'} theme`);
  };
  themeButton.addEventListener('click', () => {
    theme = theme === 'dark' ? 'light' : 'dark';
    applyTheme();
    built.chart?.refresh?.();
  });
  applyTheme();

  /* ----------------------------------------------------------- the panes */
  function makePane(ui, role) {
    const pane = {
      role, ui, view: null, grid: null, summaryGrid: null, adapter: null, source: null, sequence: 0,
      lastPage: null, lastCount: null, total: null, columns: [], summary: [],
    };
    const wrapped = recordingConnection(connection, (entry) => {
      if (/\bLIMIT\b/.test(entry.sql) && !entry.sql.includes('__lattice_total')) pane.lastPage = entry;
      else if (entry.sql.includes('__lattice_total')) pane.lastCount = entry;
      renderTiming(pane);
    });
    pane.connection = wrapped;
    ui.search.addEventListener('input', () => {
      clearTimeout(pane.searchTimer);
      pane.searchTimer = setTimeout(() => pane.grid?.filters.quick(ui.search.value), 250);
    });
    return pane;
  }
  const left = makePane(leftUi, 'raw');
  const right = makePane(rightUi, 'out');
  built.panes = { left, right };

  function renderTiming(pane) {
    const parts = [];
    if (pane.total != null) parts.push(`${int(pane.total)} rows match`);
    if (pane.lastPage) parts.push(`page: ${pane.lastPage.ms} ms, ${int(pane.lastPage.rows)} rows returned`);
    if (pane.lastCount) parts.push(`count: ${pane.lastCount.ms} ms`);
    pane.ui.timing.textContent = parts.length ? `${parts.join(' · ')} · rows scanned: ${int(pane.viewRows)}` : '';
  }

  async function mountPane(pane, view, viewRows) {
    const sequence = ++pane.sequence;
    pane.grid?.destroy?.();
    pane.summaryGrid?.destroy?.();
    pane.ui.gridHost.textContent = '';
    pane.ui.summaryHost.textContent = '';
    pane.ui.search.value = '';
    pane.lastPage = null;
    pane.lastCount = null;
    pane.total = null;
    pane.view = view;
    pane.viewRows = viewRows;

    const described = await readAll(connection, `DESCRIBE ${view}`);
    if (sequence !== pane.sequence) return;
    const columns = described.map(columnFor);
    pane.columns = described.map((d) => ({ name: d.column_name, type: d.column_type }));

    pane.adapter = LG.duckdbAdapter({
      connection: pane.connection,
      from: view,
      fields: described.map((d) => d.column_name),
    });
    pane.source = LG.createPushdownSource({
      adapter: pane.adapter,
      compute: LG,
      pageSize: 200,
      aggregates: { default: 'engine-if-identical' },
    });
    pane.grid = LG.createGrid(pane.ui.gridHost, {
      rowKey: 'unique_key',
      columns,
      source: pane.source,
      toolPanel: { side: 'right', panels: ['filters', 'columns'] },
      showTotalInHeader: true,
    });

    const inner = pane.adapter.execute.bind(pane.adapter);
    let signature = '';
    pane.adapter.execute = async (query, request) => {
      const result = await inner(query, request);
      if (typeof result.total === 'number') pane.total = result.total;
      pane.plan = pane.source.lastPlan?.() ?? null;
      renderTiming(pane);
      if (pane === right) {
        const next = JSON.stringify([query.filters ?? null, query.quick ?? '']);
        if (next !== signature) {
          signature = next;
          void refreshChart(query.filters ?? null, query.quick ?? '');
        }
      }
      return result;
    };

    void summarise(pane, view, described, sequence);
  }

  /** One scan of the view: blanks/nulls and distinct values for every column. */
  async function summarise(pane, view, described, sequence) {
    const started = performance.now();
    const parts = described.map((d, i) => {
      const c = `"${d.column_name}"`;
      return `count(*) FILTER (WHERE ${c} IS NULL OR trim(${c}::VARCHAR) = '') AS b${i}, count(DISTINCT ${c}) AS d${i}`;
    });
    const row = await readOne(connection, `SELECT count(*) AS n, ${parts.join(', ')} FROM ${view}`);
    if (sequence !== pane.sequence) return;
    pane.summaryMs = Math.round(performance.now() - started);
    const total = row.n;
    pane.summary = described.map((d, i) => ({
      column: d.column_name,
      type: d.column_type,
      nulls: row[`b${i}`],
      nullShare: total ? row[`b${i}`] / total : 0,
      distinct: row[`d${i}`],
    }));
    pane.summaryGrid = LG.createGrid(pane.ui.summaryHost, {
      rowKey: 'column',
      rows: pane.summary,
      columns: [
        { field: 'column', title: 'Column', layout: { width: 128 } },
        { field: 'type', title: 'Type', layout: { width: 92 } },
        { field: 'nulls', title: 'Blank', type: 'number', format: { pattern: '#,##0' }, layout: { width: 72 } },
        { field: 'nullShare', title: 'Blank %', type: 'number', format: { pattern: '0.0%' }, layout: { width: 72 } },
        { field: 'distinct', title: 'Distinct', type: 'number', format: { pattern: '#,##0' }, layout: { width: 80 } },
      ],
    });
  }

  /* ------------------------------------------------------------- the steps */
  async function runStep(id) {
    const step = STEPS[id - 1];
    if (built.stepMs[id] != null) return;
    const started = performance.now();
    await connection.query(step.sql(PARQUET_URL()));
    const created = Math.round(performance.now() - started);
    const t = performance.now();
    const count = (await readOne(connection, `SELECT count(*) AS n FROM ${viewName(id)}`)).n;
    built.stepMs[id] = { create: created, count: Math.round(performance.now() - t) };
    built.rowCounts[id] = count;
    const label = stepButtons[id].querySelector('.step-count');
    const previous = built.rowCounts[id - 1];
    const delta = previous != null && previous !== count ? ` (${count - previous > 0 ? '+' : '−'}${int(Math.abs(count - previous))})` : '';
    label.textContent = `${int(count)} rows${delta}`;
  }

  async function selectStep(id) {
    if (built.busy) return;
    built.busy = true;
    try {
      for (let i = 1; i <= id; i += 1) await runStep(i);
      built.step = id;
      for (const [key, button] of Object.entries(stepButtons)) {
        button.classList.toggle('on', Number(key) === id);
        button.setAttribute('aria-current', Number(key) === id ? 'step' : 'false');
      }
      const step = STEPS[id - 1];
      sqlTitle.textContent = `Step ${id}: ${step.title}`;
      sqlSummary.textContent = step.summary;
      sqlPre.textContent = step.sql(PARQUET_URL());
      sqlMeta.textContent = `created in ${built.stepMs[id].create} ms · ${int(built.rowCounts[id])} rows in ${built.stepMs[id].count} ms`;
      rightUi.heading.textContent = `Output of step ${id}`;
      rightUi.sub.textContent = id === 1 ? 'The same view as the raw side: nothing has been cleaned yet.' : `${viewName(id)}, a view over ${viewName(id - 1)}.`;
      await mountPane(right, viewName(id), built.rowCounts[id]);
    } finally {
      built.busy = false;
    }
  }

  /* ---------------------------------------------------------------- chart */
  const TOP = 6;
  let chartSequence = 0;
  const dayOf = (value) => {
    if (value instanceof Date) return value.toISOString().slice(0, 10);
    if (typeof value === 'number') return new Date(value).toISOString().slice(0, 10);
    return String(value ?? '').slice(0, 10);
  };

  async function refreshChart(filters, quick) {
    const sequence = ++chartSequence;
    const hasDay = right.columns.some((c) => c.name === 'created_day');
    if (!hasDay) {
      chartSub.textContent = 'The chart bins requests by calendar day, which exists from step 3 (Parse timestamps) on.';
      built.chartData = null;
      built.chart?.destroy?.();
      built.chart = null;
      built.chartGrid = null;
      chartBox.textContent = '';
      chartBox.classList.add('empty');
      return;
    }
    chartBox.classList.remove('empty');
    const started = performance.now();
    const result = await right.source.aggregate(
      { filters, quick: quick || undefined, sort: [], range: null, groupBy: ['created_day', 'complaint_type'] },
      [{ id: 'n', col: 'unique_key', fn: 'count' }],
    );
    if (sequence !== chartSequence) return;
    const groups = (result.groups ?? []).filter((g) => !g.grouping?.[0] && !g.grouping?.[1]);
    const totals = new Map();
    for (const g of groups) totals.set(g.keys[1], (totals.get(g.keys[1]) ?? 0) + Number(g.values.n));
    const top = [...totals.entries()].sort((a, b) => b[1] - a[1]).slice(0, TOP).map(([name]) => name);
    const wanted = new Set(top);
    const rows = groups
      .filter((g) => wanted.has(g.keys[1]) && g.keys[0] != null)
      .map((g) => ({ day: dayOf(g.keys[0]), complaint_type: String(g.keys[1]), requests: Number(g.values.n) }))
      .sort((a, b) => (a.day < b.day ? -1 : a.day > b.day ? 1 : a.complaint_type.localeCompare(b.complaint_type)))
      .map((r) => ({ id: `${r.day}|${r.complaint_type}`, ...r, label: r.day.slice(5) }));
    const ms = Math.round(performance.now() - started);
    built.chartData = { rows, top, ms, totalRequests: [...totals.values()].reduce((a, b) => a + b, 0) };
    chartSub.textContent = rows.length
      ? `The ${top.length} most frequent complaint types among the ${int(built.chartData.totalRequests)} requests in the output grid, per day. Grouped by DuckDB in ${ms} ms and following the grid’s filters and search.`
      : 'No requests match the output grid’s filters.';

    if (built.chartGrid) {
      built.chartGrid.rows.load(rows);
      built.chart?.refresh?.();
      return;
    }
    built.chartGrid = LG.createHeadlessGrid({
      rowKey: 'id',
      columns: [{ field: 'id' }, { field: 'day' }, { field: 'label' }, { field: 'complaint_type' }, { field: 'requests', type: 'number' }],
      rows,
    });
    try {
      built.chart = createChart({
        grid: built.chartGrid, container: chartBox, type: 'line', x: 'label', y: 'requests', series: 'complaint_type',
        legend: true, axis: { x: { labels: true, every: 3 }, y: 'Requests' },
      });
    } catch (error) {
      chartBox.append(el('p', 'chart-error', `This chart could not be drawn: ${error.message}`));
      console.error('[etl demo] chart', error);
    }
  }

  /* --------------------------------------------------------------- export */
  exportButton.addEventListener('click', async () => {
    exportButton.disabled = true;
    exportNote.textContent = 'Writing Parquet...';
    const started = performance.now();
    try {
      const file = `nyc311-${viewName(FINAL_STEP)}-clean.parquet`;
      await runStep(FINAL_STEP);
      await connection.query(`COPY (SELECT * FROM ${viewName(FINAL_STEP)}) TO '${file}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)`);
      const bytes = await db.copyFileToBuffer(file);
      const url = URL.createObjectURL(new Blob([bytes], { type: 'application/vnd.apache.parquet' }));
      const link = el('a');
      link.href = url;
      link.download = 'nyc311-clean.parquet';
      document.body.append(link);
      link.click();
      link.remove();
      setTimeout(() => URL.revokeObjectURL(url), 30000);
      await db.dropFile(file);
      const ms = Math.round(performance.now() - started);
      built.exported = { bytes: bytes.byteLength, rows: built.rowCounts[FINAL_STEP], ms };
      exportNote.textContent = `nyc311-clean.parquet: ${int(built.rowCounts[FINAL_STEP])} rows, ${(bytes.byteLength / 1048576).toFixed(1)} MB, written in ${ms} ms.`;
    } catch (error) {
      console.error('[etl demo] export', error);
      exportNote.textContent = `The export failed: ${error?.message ?? error}`;
    } finally {
      exportButton.disabled = false;
    }
  });

  /* ------------------------------------------------------------- raw file */
  async function loadRawFile() {
    try {
      const response = await fetch(CSV_URL(), { headers: { Range: 'bytes=0-4095' } });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const text = (await response.text()).split('\n');
      rawPre.textContent = text.slice(0, 9).map((l) => (l.length > 400 ? `${l.slice(0, 400)}...` : l)).join('\n');
      const size = response.headers.get('content-range')?.split('/')[1];
      if (size) rawLink.textContent = `Download ${CSV_NAME} (${(Number(size) / 1048576).toFixed(0)} MB)`;
    } catch (error) {
      rawPre.textContent = `The raw file could not be read: ${error.message}`;
    }
  }

  /* ---------------------------------------------------------------- start */
  built.start = async () => {
    await runStep(1);
    await mountPane(left, viewName(1), built.rowCounts[1]);
    facts.textContent = `${int(built.rowCounts[1])} raw requests · DuckDB-Wasm ${version} · Parquet read in byte ranges from ${new URL(PARQUET_URL()).host}`;
    void loadRawFile();
    await selectStep(FINAL_STEP);
  };

  built.selectStep = selectStep;
  built.destroy = () => {
    for (const pane of [left, right]) { pane.grid?.destroy?.(); pane.summaryGrid?.destroy?.(); }
    built.chart?.destroy?.();
  };
  return built;
}
