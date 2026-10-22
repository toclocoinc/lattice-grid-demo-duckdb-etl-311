/**
 * DuckDB-Wasm, started in the page.
 *
 * The grid ships no engine. `duckdbAdapter` takes a connection the host made,
 * so everything about DuckDB (which version, where it comes from, what
 * extensions are loaded) is decided here.
 *
 * `LOAD httpfs` makes DuckDB open the Parquet file by its footer and read byte
 * ranges. The absolute URL matters because DuckDB resolves the path itself,
 * inside a worker.
 */

/** Pinned. A CDN import with no version breaks on someone else's release day. */
export const DUCKDB_VERSION = '1.32.0';
export const DUCKDB_MODULE = `https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@${DUCKDB_VERSION}/+esm`;

/**
 * The engine's download size once decoded, which is what the progress callback
 * counts (the wire carries about a fifth of it, compressed). Used only when the
 * server does not give a usable total.
 */
export const ENGINE_BYTES = 34242586;

/**
 * Counts the bytes the engine reads from Parquet files.
 *
 * DuckDB reads over XMLHttpRequest inside its worker, so the page cannot see
 * those requests. The worker is therefore started through a small wrapper that
 * reports the size of every response to a BroadcastChannel before it loads the
 * engine's own worker script. Nothing is fetched twice: this only measures what
 * the engine already asked for.
 */
function createReadMeter() {
  const channelName = `duckdb-reads-${Math.random().toString(36).slice(2)}`;
  const channel = new BroadcastChannel(channelName);
  const listeners = new Set();
  const meter = {
    bytes: 0,
    onRead(fn) { listeners.add(fn); return () => listeners.delete(fn); },
    prelude: `const reads = new BroadcastChannel(${JSON.stringify(channelName)});
const send = XMLHttpRequest.prototype.send;
XMLHttpRequest.prototype.send = function (...args) {
  const report = () => {
    if (this.__counted) return;
    this.__counted = true;
    const body = this.response;
    reads.postMessage({ url: this.responseURL, bytes: body && body.byteLength ? body.byteLength : 0 });
  };
  this.addEventListener('loadend', report);
  const out = send.apply(this, args);
  if (this.readyState === 4) report();
  return out;
};
`,
    close() { channel.close(); },
  };
  channel.onmessage = ({ data }) => {
    if (!/\.parquet(\?|$)/.test(data.url)) return;
    meter.bytes += data.bytes;
    for (const fn of listeners) fn(meter.bytes);
  };
  return meter;
}

/**
 * @param {(message: string) => void} [onStatus] what is happening, in plain words, for the loading panel
 * @param {(progress: {loaded: number, total: number}) => void} [onDownload] bytes of the engine downloaded so far
 */
export async function startDuckDB(onStatus = () => {}, onDownload = () => {}) {
  onStatus('Loading the DuckDB library');
  const duckdb = await import(/* @vite-ignore */ DUCKDB_MODULE);
  const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());

  const reads = createReadMeter();
  const workerSource = `${reads.prelude}importScripts(${JSON.stringify(bundle.mainWorker)});`;
  const workerUrl = URL.createObjectURL(new Blob([workerSource], { type: 'text/javascript' }));
  const worker = new Worker(workerUrl);
  URL.revokeObjectURL(workerUrl);
  const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(duckdb.LogLevel.ERROR), worker);

  /* The engine downloads and compiles its own WebAssembly inside `instantiate`
     and reports the bytes it has fetched, so the file is downloaded once and
     the bar is the real download. */
  onStatus('Downloading the query engine');
  let compiling = false;
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker, (progress) => {
    const total = progress.bytesTotal >= progress.bytesLoaded && progress.bytesTotal > 0 ? progress.bytesTotal : ENGINE_BYTES;
    const loaded = Math.min(progress.bytesLoaded, total);
    onDownload({ loaded, total });
    if (loaded >= total && !compiling) { compiling = true; onStatus('Starting the engine'); }
  });

  const connection = await db.connect();
  await connection.query('LOAD httpfs;');
  const version = (await readOne(connection, 'SELECT version() AS v'))?.v ?? 'unknown';

  return {
    db,
    connection,
    reads,
    version: String(version),
    close() {
      try { connection.close?.(); } catch { /* already gone */ }
      try { worker.terminate?.(); } catch { /* already gone */ }
      reads.close();
    },
  };
}

/** Rows of a query as plain objects, BigInts turned into numbers. */
export async function readAll(connection, sql) {
  const result = await connection.query(sql);
  return result.toArray().map((row) => {
    const out = {};
    for (const [key, value] of Object.entries(row.toJSON())) {
      out[key] = typeof value === 'bigint' ? Number(value) : value;
    }
    return out;
  });
}

export async function readOne(connection, sql) {
  return (await readAll(connection, sql))[0] ?? null;
}

/**
 * Wrap a connection so every statement that passes through is recorded, with
 * its bound values, how long it took and how many rows came back. The adapter
 * is handed the wrapper, so the timings shown on the page are the engine's own
 * statements, not a reconstruction.
 */
export function recordingConnection(connection, record) {
  const note = (sql, params, started, result) => {
    const rows = typeof result?.numRows === 'number' ? result.numRows : 0;
    record({ sql, params, ms: Math.round(performance.now() - started), rows });
  };
  return {
    query: async (sql) => {
      const started = performance.now();
      const result = await connection.query(sql);
      note(sql, [], started, result);
      return result;
    },
    prepare: async (sql) => {
      const statement = await connection.prepare(sql);
      return {
        query: async (...params) => {
          const started = performance.now();
          const result = await statement.query(...params);
          note(sql, params, started, result);
          return result;
        },
        close: () => statement.close?.(),
      };
    },
  };
}
