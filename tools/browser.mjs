/**
 * A small Chrome driver over the DevTools protocol, used by tools/verify.mjs.
 * No dependency: Node 22's WebSocket and fetch are enough.
 */

import { spawn } from 'node:child_process';
import { access, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer } from 'node:net';
import { setTimeout as sleep } from 'node:timers/promises';

const CANDIDATES = [
  process.env.CHROME_PATH,
  '/usr/bin/google-chrome',
  '/usr/bin/google-chrome-stable',
  '/usr/bin/chromium',
  '/usr/bin/chromium-browser',
  '/snap/bin/chromium',
  '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
].filter(Boolean);

async function findChrome() {
  for (const path of CANDIDATES) {
    try { await access(path); return path; } catch { /* next */ }
  }
  throw new Error(`No browser found. Tried:\n  ${CANDIDATES.join('\n  ')}\nSet CHROME_PATH.`);
}

const freePort = () => new Promise((ok, fail) => {
  const probe = createServer();
  probe.on('error', fail);
  probe.listen(0, '127.0.0.1', () => {
    const { port } = probe.address();
    probe.close(() => ok(port));
  });
});

/** Start Chrome headless and attach to a fresh page. */
export async function openBrowser({ width = 1440, height = 1000, mobile = false, downloadDir = null } = {}) {
  if (typeof WebSocket === 'undefined') throw new Error(`This needs Node 22 or newer for its WebSocket. You are on ${process.version}.`);
  const chrome = await findChrome();
  const profile = await mkdtemp(join(tmpdir(), 'etl311-verify-'));
  const port = await freePort();
  const child = spawn(chrome, [
    '--headless=new', `--remote-debugging-port=${port}`, `--user-data-dir=${profile}`,
    '--no-first-run', '--no-default-browser-check', '--no-sandbox', '--disable-gpu',
    '--disable-dev-shm-usage', '--hide-scrollbars', `--window-size=${width},${height}`, 'about:blank',
  ], { stdio: ['ignore', 'pipe', 'pipe'], detached: true });
  child.stderr.on('data', () => {});

  let wsUrl;
  for (let i = 0; i < 150 && !wsUrl; i += 1) {
    try {
      const response = await fetch(`http://127.0.0.1:${port}/json/version`);
      if (response.ok) wsUrl = (await response.json()).webSocketDebuggerUrl;
    } catch { /* not up yet */ }
    if (!wsUrl) await sleep(200);
  }
  if (!wsUrl) throw new Error('the browser never opened its debugging port');

  const socket = new WebSocket(wsUrl);
  await new Promise((ok, fail) => { socket.onopen = ok; socket.onerror = () => fail(new Error('could not attach to the browser')); });

  let nextId = 0;
  const pending = new Map();
  const consoleErrors = [];
  const pageErrors = [];
  socket.onclose = () => {
    for (const { reject } of pending.values()) reject(new Error('the browser went away before it answered'));
    pending.clear();
  };
  socket.onmessage = (event) => {
    const message = JSON.parse(event.data);
    if (message.id != null && pending.has(message.id)) {
      const { resolve, reject } = pending.get(message.id);
      pending.delete(message.id);
      if (message.error) reject(new Error(JSON.stringify(message.error)));
      else resolve(message.result);
      return;
    }
    if (message.method === 'Runtime.consoleAPICalled' && message.params.type === 'error') {
      consoleErrors.push(message.params.args.map((a) => a.value ?? a.description ?? a.type).join(' '));
    }
    if (message.method === 'Runtime.exceptionThrown') {
      const d = message.params.exceptionDetails;
      pageErrors.push(d.exception?.description || d.text);
    }
    if (message.method === 'Log.entryAdded' && message.params.entry.level === 'error') {
      consoleErrors.push(message.params.entry.text);
    }
  };
  const send = (method, params = {}, sessionId) => new Promise((resolve, reject) => {
    const id = ++nextId;
    pending.set(id, { resolve, reject });
    socket.send(JSON.stringify({ id, method, params, sessionId }));
  });

  const { targetId } = await send('Target.createTarget', { url: 'about:blank' });
  const { sessionId } = await send('Target.attachToTarget', { targetId, flatten: true });
  const call = (method, params) => send(method, params, sessionId);
  await call('Page.enable');
  await call('Runtime.enable');
  await call('Log.enable');
  await call('Emulation.setDeviceMetricsOverride', { width, height, deviceScaleFactor: 1, mobile });
  if (downloadDir) await send('Browser.setDownloadBehavior', { behavior: 'allow', downloadPath: downloadDir, eventsEnabled: true });

  const evaluate = async (expression, ms = 30000) => {
    const result = await Promise.race([
      call('Runtime.evaluate', { expression, returnByValue: true, awaitPromise: true }),
      sleep(ms).then(() => { throw new Error('the page did not answer in time'); }),
    ]);
    if (result.exceptionDetails) throw new Error(`${result.exceptionDetails.text} ${result.exceptionDetails.exception?.description || ''}`);
    return result.result.value;
  };

  const waitFor = async (expression, timeout, what) => {
    const until = Date.now() + timeout;
    while (Date.now() < until) {
      try { if (await evaluate(expression, 15000)) return true; } catch { /* still busy */ }
      await sleep(400);
    }
    throw new Error(`timed out waiting for ${what}`);
  };

  /** A real mouse click at the centre of the first element matching a selector (or a JS expression returning an element). */
  const click = async (target, { edge = false } = {}) => {
    const expression = target.startsWith('js:') ? target.slice(3) : `document.querySelector(${JSON.stringify(target)})`;
    const box = await evaluate(`(() => {
      const el = ${expression};
      if (!el) return null;
      el.scrollIntoView({ block: 'center', inline: 'center' });
      const r = el.getBoundingClientRect();
      return { x: ${edge} ? r.x + 14 : r.x + r.width / 2, y: r.y + r.height / 2, w: r.width, h: r.height };
    })()`);
    if (!box || !box.w || !box.h) throw new Error(`nothing to click for ${target}`);
    await call('Input.dispatchMouseEvent', { type: 'mouseMoved', x: box.x, y: box.y });
    await call('Input.dispatchMouseEvent', { type: 'mousePressed', x: box.x, y: box.y, button: 'left', clickCount: 1 });
    await call('Input.dispatchMouseEvent', { type: 'mouseReleased', x: box.x, y: box.y, button: 'left', clickCount: 1 });
  };

  /** Focus an input with a real click, then type into it with key events. */
  const type = async (selector, text) => {
    await click(selector);
    for (const ch of text) {
      await call('Input.dispatchKeyEvent', { type: 'keyDown', text: ch, key: ch });
      await call('Input.dispatchKeyEvent', { type: 'keyUp', key: ch });
    }
  };

  const screenshot = async (full = true) => (await call('Page.captureScreenshot', { format: 'png', captureBeyondViewport: full })).data;

  const close = async () => {
    try { process.kill(-child.pid, 'SIGKILL'); } catch { /* gone */ }
    try { child.kill('SIGKILL'); } catch { /* gone */ }
    await rm(profile, { recursive: true, force: true }).catch(() => {});
  };

  return { call, evaluate, waitFor, click, type, screenshot, close, consoleErrors, pageErrors, send };
}
