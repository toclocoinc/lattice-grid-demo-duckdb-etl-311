/**
 * The entry point: start DuckDB, hand the connection to the pipeline page, and
 * get out of the way. Nothing is awaited at the top level, so the document
 * finishes loading while the engine starts.
 */

import * as LG from './node_modules/@toclocoinc/lattice-grid/lattice-grid.esm.min.js';
import { createChart } from './node_modules/@toclocoinc/lattice-grid/modules/charts.esm.min.js';
import { DEMO_LICENCE } from './src/licence.js';
import { startDuckDB } from './src/duckdb.js';
import { createStatus, megabytes } from './src/loading.js';
import { PARQUET_URL, MONTH_LABEL } from './src/config.js';
import { buildPipeline } from './src/pipeline.js';

/* Applied before anything is drawn: a grid keeps the licence in force when it was built. */
LG.setLicence(DEMO_LICENCE);

const root = document.querySelector('#app');

function showMessage(title, message, hint) {
  root.textContent = '';
  const panel = document.createElement('div');
  panel.className = 'loading';
  const h = document.createElement('h1');
  h.textContent = title;
  const p = document.createElement('p');
  p.className = 'loading-message';
  p.textContent = message;
  panel.append(h, p);
  if (hint) {
    const q = document.createElement('p');
    q.className = 'loading-message';
    q.textContent = hint;
    panel.append(q);
  }
  root.append(panel);
}

/** The Parquet file's size from a one-byte range request, or null: it gives the reading bar its total. */
async function parquetSize() {
  try {
    const response = await fetch(PARQUET_URL(), { headers: { Range: 'bytes=0-0' } });
    const size = Number(response.headers.get('content-range')?.split('/')[1]);
    return size > 0 ? size : null;
  } catch { return null; }
}

async function start() {
  const started = performance.now();
  /* The status line is in index.html, so it is on screen from first paint. */
  const status = createStatus(root.querySelector('.load-status'));
  const size = parquetSize();
  try {
    const { db, connection, version, close, reads } = await startDuckDB(
      (message) => status.set(message),
      ({ loaded, total }) => status.set(
        `Downloading the query engine (${megabytes(total)}, first visit only): ${megabytes(loaded)} of ${megabytes(total)}`,
        loaded / total,
      ),
    );
    status.set(`Opening the ${MONTH_LABEL} 311 file`);
    const built = buildPipeline({ root, LG, createChart, connection, db, version, reads, status, parquetBytes: await size });
    window.addEventListener('pagehide', () => { built.destroy(); close(); }, { once: true });
    window.__etlDemo = built;
    await built.start();
    status.hide();
    built.bootMs = Math.round(performance.now() - started);
    built.ready = true;
    console.log('[etl demo] ready', { ms: built.bootMs, duckdb: version });
  } catch (error) {
    window.__etlDemo = { ready: false, error: String(error?.message ?? error) };
    showMessage(
      'The data could not be loaded',
      String(error?.message ?? error),
      'This page runs DuckDB compiled to WebAssembly and reads a Parquet file with HTTP range requests. '
      + 'It needs a browser with WebAssembly and a data host that answers 206 Partial Content.',
    );
    console.error('[etl demo]', error);
  }
}

start();
