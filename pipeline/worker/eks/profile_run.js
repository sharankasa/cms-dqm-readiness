#!/usr/bin/env node
/*
 * profile_run.js — run pipeline/s1/run_engine.js (CMS's engine, unmodified) under V8's sampling CPU
 * profiler and write a .cpuprofile when the engine finishes or after PROFILE_SECONDS, whichever comes
 * first (the equivalence run explanation of the whole-record runtime, exploratory). A CPU profile holds function names,
 * source files, line numbers and timings: no data values.
 *
 * The profiler is driven from a separate thread connected to the main thread's inspector: the engine
 * evaluates one patient-year as one long computation that never yields to timers, so a timer on the
 * main thread would not fire (first version, 2026-10-11). Inspector messages from another thread
 * interrupt running JavaScript, so the stop happens on time; the process is then ended from that thread.
 *
 *   node --max-old-space-size=<MB> profile_run.js <run_engine.js> <run_engine args...>
 *   env PROFILE_FILE (output path, required), PROFILE_SECONDS (default 3600), PROFILE_INTERVAL_US (default 1000)
 * Exit: 0 when the engine finished first; killed (SIGKILL) right after writing when the time limit hit.
 */
const { Worker } = require('worker_threads');
const path = require('path');

const [, , runEngine, ...rest] = process.argv;
const FILE = process.env.PROFILE_FILE;
if (!runEngine || !FILE) { console.error('usage: PROFILE_FILE=<out> node profile_run.js <run_engine.js> <args...>'); process.exit(1); }

const controller = `
const { parentPort, workerData } = require('worker_threads');
const inspector = require('inspector');
const fs = require('fs');
const { file, secs, interval } = workerData;
const s = new inspector.Session();
s.connectToMainThread();
const post = (m, p) => new Promise((res, rej) => s.post(m, p || {}, (e, r) => (e ? rej(e) : res(r))));
const t0 = Date.now();
const keepAlive = setInterval(() => {}, 1 << 30);   // pending inspector replies alone do not keep this thread alive
let stopping = false;
async function stop(why) {
  if (stopping) return; stopping = true;
  const { profile } = await post('Profiler.stop');
  fs.writeFileSync(file, JSON.stringify(profile));
  fs.writeFileSync(file.replace(/\\.cpuprofile$/, '') + '.run.json', JSON.stringify({ ended_by: why,
    profiled_seconds: (Date.now() - t0) / 1000, interval_us: interval, limit_seconds: secs }));
  console.log('[profile] ' + why + ' after ' + ((Date.now() - t0) / 1000).toFixed(0) + ' s; profile written');
  if (why === 'time limit') process.kill(process.pid, 'SIGKILL');   // the engine is still busy: end it here
  clearInterval(keepAlive);
  s.disconnect();
  parentPort.postMessage('written');
}
(async () => {
  await post('Profiler.enable');
  await post('Profiler.setSamplingInterval', { interval });
  await post('Profiler.start');
  parentPort.on('message', (m) => { if (m === 'stop') stop('engine finished'); });
  setTimeout(() => stop('time limit'), secs * 1000);
  parentPort.postMessage('started');
})().catch((e) => { console.error('[profile] ' + e.message); process.exit(1); });
`;
const w = new Worker(controller, { eval: true, workerData: {
  file: FILE, secs: Number(process.env.PROFILE_SECONDS || 3600), interval: Number(process.env.PROFILE_INTERVAL_US || 1000) } });
let asked = false;
w.on('message', (m) => {
  if (m === 'started') {
    w.unref();                       // so the main thread can reach 'beforeExit' when the engine is done
    process.on('beforeExit', () => { if (!asked) { asked = true; w.ref(); w.postMessage('stop'); } });
    process.argv = [process.argv[0], runEngine, ...rest];
    require(path.resolve(runEngine));
  } else if (m === 'written') {
    process.exit(0);
  }
});
w.on('error', (e) => { console.error(`[profile] controller failed: ${e.message}`); process.exit(1); });
