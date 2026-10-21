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
 * @param {(message: string) => void} [onStatus] progress, for the loading panel
 */
export async function startDuckDB(onStatus = () => {}) {
  onStatus(`Loading DuckDB-Wasm ${DUCKDB_VERSION}...`);
  const duckdb = await import(/* @vite-ignore */ DUCKDB_MODULE);
  const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());

  onStatus('Starting the engine...');
  const worker = await duckdb.createWorker(bundle.mainWorker);
  const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(duckdb.LogLevel.ERROR), worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

  const connection = await db.connect();
  await connection.query('LOAD httpfs;');
  const version = (await readOne(connection, 'SELECT version() AS v'))?.v ?? 'unknown';

  return {
    db,
    connection,
    version: String(version),
    close() {
      try { connection.close?.(); } catch { /* already gone */ }
      try { worker.terminate?.(); } catch { /* already gone */ }
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
