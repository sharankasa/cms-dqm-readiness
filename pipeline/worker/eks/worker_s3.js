#!/usr/bin/env node
/*
 * worker_s3.js — the pilot-the readiness inputs EKS worker: one Kubernetes Indexed Job pod = one shard.
 * Downloads its shard's input from S3, lays it out the way pipeline/s1/run_engine.js reads it, runs
 * CMS's reference engine (unmodified, via run_engine.js), and keeps its result file in S3 so a
 * replacement pod resumes where a killed one stopped (exactly once per (measure, stay)).
 *
 * Environment:
 *   MODE      sliced | reference (engine) | v (independent implementation, on the reference input)
 *             | compare-v (one pod: engine reference-side results vs V; env ENGINE_OUT, V_OUT, LABEL,
 *               NO_POP_COUNTS=1 for the pilot)
 *   SHARD     shard index (from the Job's completion index)
 *   SHARD_OFFSET  added to SHARD (default 0): a second job can run shards N, N+1, ... (the equivalence run: heavy
 *             shards with more memory in one job, the rest in another)
 *   INPUT     s3://bucket/prefix/export      (manifest/, slices/, shared/, reference/, shared_all/)
 *   ENGINE    s3://bucket/prefix/engine      (bundles/*.json, vsac-cache/*.json)
 *   OUT       s3://bucket/prefix/runs/<job>  (results/<mode>_<shard>.jsonl, status/...)
 *   HEAP_MB   engine heap (default 2560)
 *   BATCH     sliced side only: stays per engine call (default 1 = as in S1); BATCH_MAX_RES: slices with
 *             more resources are evaluated alone (default 400). See pipeline/s1/run_engine.js --batch.
 *   ENGINE_PREFIX  compare-v only: result files of the engine job to compare (default "reference_";
 *             "sliced_" compares V with a sliced-side job, e.g. V on the readiness cohort, where V reads
 *             each stay's slice as its record)
 *   SYNC_SECONDS  result upload interval (default 30)
 *   S3_LOCAL_ROOT  testing only: map s3://bucket/key to <root>/bucket/key on the local disk
 *   RUN_ENGINE     path of run_engine.js (default: next to this folder, as in the image)
 *   MODE=profile (exploratory, the equivalence run): re-runs ONE patient-year of the shard on the reference side under
 *             V8's CPU profiler (profile_run.js) and writes <OUT>/profile/{profile,summary}_<shard>.*
 *             (function names and timings only). The patient-year is picked from the shard's earlier
 *             results in RESULTS_FROM (s3://.../runs/<job>): the one whose recorded time is closest to
 *             PROFILE_TARGET_S (default 2400, at least PROFILE_MIN_S, default 60); the profile stops
 *             after PROFILE_SECONDS (default 3600) if the engine has not finished. No results are kept.
 * Input lines: manifest "unit_id,patient_id,mp_year,..."; slices "<unit_id>\t<json>";
 * reference "<patient_id>\t<json>"; shared and shared_all "<json>". All gzip text.
 */
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const { spawn } = require('child_process');

const env = (k, d) => (process.env[k] !== undefined && process.env[k] !== '' ? process.env[k] : d);
const SHARD = env('SHARD') === undefined ? undefined : String(Number(env('SHARD')) + Number(env('SHARD_OFFSET', '0')));
const MODE = env('MODE'), INPUT = env('INPUT'), ENGINE = env('ENGINE'), OUT = env('OUT');
const HEAP = env('HEAP_MB', '2560'), SYNC = Number(env('SYNC_SECONDS', '30')), LOCAL = env('S3_LOCAL_ROOT');
const WORK = env('WORK_DIR', '/work');
const V_DIR = env('V_DIR', path.join(__dirname, '..', 'v'));
const V_RULES = env('V_RULES', '--exclusion-rule first-test --diagnosis-rule linked --med-interval-rule strict --prevalence-rule qicore').split(/\s+/);
if (MODE === 'compare-v') {
  if (!env('ENGINE_OUT') || !env('V_OUT') || !OUT) { console.error('compare-v: set ENGINE_OUT, V_OUT, OUT'); process.exit(1); }
} else if (!['sliced', 'reference', 'v', 'profile'].includes(MODE) || SHARD === undefined || !INPUT || !ENGINE || !OUT) {
  console.error('set MODE (sliced|reference|v|profile|compare-v), SHARD, INPUT, ENGINE, OUT'); process.exit(1);
} else if (MODE === 'profile' && !env('RESULTS_FROM')) {
  console.error('profile: set RESULTS_FROM (the job output holding results/reference_<shard>.jsonl)'); process.exit(1);
}
const t0 = Date.now();
const log = (s) => console.log(`[worker ${MODE} ${SHARD ?? '-'} ${((Date.now() - t0) / 1000).toFixed(0)}s] ${s}`);
const split = (u) => { const m = /^s3:\/\/([^/]+)\/?(.*)$/.exec(u); if (!m) throw new Error(`not an s3 URL: ${u}`); return [m[1], m[2].replace(/\/$/, '')]; };

// ---- storage: S3, or a local folder standing in for S3 (tests)
let s3, S3C;
if (!LOCAL) { S3C = require('@aws-sdk/client-s3'); s3 = new S3C.S3Client({ region: env('AWS_REGION', 'us-west-2') }); }
async function list(url) {
  const [b, p] = split(url);
  if (LOCAL) {
    const dir = path.join(LOCAL, b, p);
    if (!fs.existsSync(dir)) return [];
    const outKeys = [];
    const walk = (d) => { for (const e of fs.readdirSync(d, { withFileTypes: true })) {
      const f = path.join(d, e.name); if (e.isDirectory()) walk(f); else outKeys.push(path.relative(path.join(LOCAL, b), f)); } };
    walk(dir); return outKeys.sort();
  }
  const keys = []; let token;
  do {
    const r = await s3.send(new S3C.ListObjectsV2Command({ Bucket: b, Prefix: p + '/', ContinuationToken: token }));
    for (const o of r.Contents || []) if (!o.Key.endsWith('/')) keys.push(o.Key);
    token = r.IsTruncated ? r.NextContinuationToken : undefined;
  } while (token);
  return keys.sort();
}
async function getBuffer(bucket, key) {
  if (LOCAL) { const f = path.join(LOCAL, bucket, key); return fs.existsSync(f) ? fs.readFileSync(f) : null; }
  try {
    const r = await s3.send(new S3C.GetObjectCommand({ Bucket: bucket, Key: key }));
    return Buffer.from(await r.Body.transformToByteArray());
  } catch (e) { if (e.name === 'NoSuchKey' || e.$metadata?.httpStatusCode === 404) return null; throw e; }
}
async function put(bucket, key, body) {
  if (LOCAL) { const f = path.join(LOCAL, bucket, key); fs.mkdirSync(path.dirname(f), { recursive: true }); fs.writeFileSync(f, body); return; }
  await s3.send(new S3C.PutObjectCommand({ Bucket: bucket, Key: key, Body: body }));
}
const maybeGunzip = (buf) => (buf.length > 1 && buf[0] === 0x1f && buf[1] === 0x8b ? zlib.gunzipSync(buf) : buf);
// every line of every object under a prefix (gzip or plain), passed to fn(line)
async function eachLine(url, fn) {
  const [b] = split(url);
  let n = 0, bytes = 0;
  for (const key of await list(url)) {
    const raw = await getBuffer(b, key); if (!raw) continue;
    bytes += raw.length;
    const buf = maybeGunzip(raw);
    let s = 0;
    while (s < buf.length) {
      let e = buf.indexOf(10, s); if (e < 0) e = buf.length;
      if (e > s) { const line = buf.toString('utf8', s, e); if (line.trim()) { fn(line); n++; } }
      s = e + 1;
    }
  }
  return { lines: n, bytes };
}

// ---- grouped writer: lines per key -> <dir>/<key>.ndjson.gz, flushing to plain files when buffers grow
function groupedWriter(dir) {
  fs.mkdirSync(dir, { recursive: true });
  const buf = new Map(); let size = 0;
  const flush = () => { for (const [k, arr] of buf) fs.appendFileSync(path.join(dir, `${k}.ndjson`), arr.join('\n') + '\n'); buf.clear(); size = 0; };
  return {
    add(k, line) { if (!buf.has(k)) buf.set(k, []); buf.get(k).push(line); size += line.length; if (size > 64e6) flush(); },
    finish() { flush(); let n = 0;
      for (const f of fs.readdirSync(dir)) if (f.endsWith('.ndjson')) {
        const p = path.join(dir, f); fs.writeFileSync(p + '.gz', zlib.gzipSync(fs.readFileSync(p))); fs.unlinkSync(p); n++; }
      return n; },
  };
}

async function compareV() {
  // one pod: the engine's reference-side results vs V's results, compared by pipeline/v/compare_v.py;
  // only the summary of counts is meant to leave AWS (pilot: without population counts)
  const dir = path.join(WORK, 'cmp'); for (const d of ['engine', 'v', 'out']) fs.mkdirSync(path.join(dir, d), { recursive: true });
  for (const [src, pre, sub] of [[env('ENGINE_OUT'), env('ENGINE_PREFIX', 'reference_'), 'engine'], [env('V_OUT'), 'v_', 'v']]) {
    const [b, p] = split(src);
    for (const key of await list(`${src.replace(/\/$/, '')}/results`)) {
      const name = path.basename(key); if (!name.startsWith(pre) || !name.endsWith('.jsonl')) continue;
      fs.writeFileSync(path.join(dir, sub, name), await getBuffer(b, key));
    }
  }
  const args = [path.join(V_DIR, 'compare_v.py'), '--engine', path.join(dir, 'engine', '*.jsonl'),
    '--reference', path.join(dir, 'v', '*.jsonl'), '--out', path.join(dir, 'out'), '--label', env('LABEL', 'run')];
  if (env('NO_POP_COUNTS', '') === '1') args.push('--no-population-counts');
  const code = await new Promise((resolve) => spawn('python3', args, { stdio: ['ignore', 'inherit', 'inherit'] })
    .on('exit', (c) => resolve(c ?? 1)));
  const [ob, op] = split(OUT);
  for (const f of fs.readdirSync(path.join(dir, 'out'))) await put(ob, `${op}/compare-v/${f}`, fs.readFileSync(path.join(dir, 'out', f)));
  log(`compare-v exit ${code}; outputs in ${OUT}/compare-v/`);
  cleanup(); process.exit(code);
}

async function runProfile({ prep, engine, outDir, bundles, status }) {
  const target = Number(env('PROFILE_TARGET_S', '2400')), minS = Number(env('PROFILE_MIN_S', '60'));
  const [rb, rp] = split(env('RESULTS_FROM'));
  const buf = await getBuffer(rb, `${rp}/results/reference_${SHARD}.jsonl`);
  if (!buf) throw new Error(`no reference results for shard ${SHARD} under ${env('RESULTS_FROM')}`);
  const groups = new Map();
  for (const l of buf.toString('utf8').split('\n')) {
    if (!l.trim()) continue;
    let r; try { r = JSON.parse(l); } catch { continue; }
    const k = `${r.patient_id}|${r.mp_year}`;
    if (!groups.has(k)) groups.set(k, { units: new Set(), sec: new Map() });
    const g = groups.get(k); g.units.add(r.unit_id);
    g.sec.set(r.measure, Math.max(g.sec.get(r.measure) || 0, Number(r.seconds) || 0));
  }
  const cand = [...groups.entries()]
    .map(([k, g]) => ({ k, units: [...g.units], s: [...g.sec.values()].reduce((a, b) => a + b, 0) }))
    .filter((g) => g.s >= minS && g.s > 0)
    .sort((a, b) => Math.abs(Math.log(a.s / target)) - Math.abs(Math.log(b.s / target)) || (a.k < b.k ? -1 : 1));
  if (!cand.length) throw new Error(`shard ${SHARD}: no recorded patient-year took at least ${minS} s`);
  const pick = cand[0];
  log(`profiling one patient-year: ${pick.units.length < 10 ? '<10' : pick.units.length} stay(s), recorded ${pick.s.toFixed(0)} s in the full run`);
  const profDir = path.join(WORK, 'prof'); fs.mkdirSync(profDir, { recursive: true });
  const profFile = path.join(profDir, `profile_${SHARD}.cpuprofile`);
  const runEngine = env('RUN_ENGINE', path.join(__dirname, '..', 'run_engine.js'));
  const wrapper = env('PROFILE_RUN', path.join(__dirname, 'profile_run.js'));
  const child = spawn(process.execPath, [`--max-old-space-size=${HEAP}`, wrapper, runEngine, '--mode', 'reference', '--prep', prep,
    '--out', path.join(outDir, `profile_${SHARD}_scratch.jsonl`), '--bundles', ...bundles.sort(),
    '--vs-cache', path.join(engine, 'vsac-cache'), '--only', pick.units.join(',')],
  { stdio: ['ignore', 'inherit', 'inherit'], env: { ...process.env, PROFILE_FILE: profFile } });
  process.on('SIGTERM', () => { child.kill('SIGTERM'); process.exit(143); });
  const code = await new Promise((resolve) => child.on('exit', (c, sig) => resolve(c ?? (sig ? 128 : 1))));
  // the wrapper ends the process itself (SIGKILL) when the time limit stops a still-busy engine: a written
  // profile and its run note are what count, not the exit code
  const runNote = profFile.replace(/\.cpuprofile$/, '') + '.run.json';
  if (!fs.existsSync(profFile) || !fs.existsSync(runNote)) throw new Error(`profiled run exited ${code} without a profile`);
  const { summarize } = require(path.join(__dirname, 'profile_summary.js'));
  const run = JSON.parse(fs.readFileSync(profFile.replace(/\.cpuprofile$/, '') + '.run.json', 'utf8'));
  const out = { shard: Number(SHARD), stays_in_patient_year: pick.units.length < 10 ? '<10' : pick.units.length,
    recorded_seconds_full_run: Math.round(pick.s), ...run, summary: summarize(JSON.parse(fs.readFileSync(profFile, 'utf8')), 40) };
  const [ob, op] = split(OUT);
  await put(ob, `${op}/profile/profile_${SHARD}.cpuprofile`, fs.readFileSync(profFile));
  await put(ob, `${op}/profile/summary_${SHARD}.json`, JSON.stringify(out, null, 2));
  status.exit_code = 0; status.ended_utc = new Date().toISOString(); status.wall_seconds = (Date.now() - t0) / 1000;
  await put(ob, `${op}/status/profile_${SHARD}.${Date.now()}.json`, JSON.stringify(status, null, 2));
  log(`profile done (${run.ended_by}, ${run.profiled_seconds.toFixed(0)} s): ${OUT}/profile/summary_${SHARD}.json`);
  cleanup(); process.exit(0);
}

// remove the downloaded input and engine files when the pod ends: a finished pod keeps its files on the
// node's disk until the pod object is deleted, and those leftovers caused disk-pressure evictions (2026-10-12)
function cleanup() {
  for (const d of ['prep', 'engine', 'prof', 'cmp']) { try { fs.rmSync(path.join(WORK, d), { recursive: true, force: true }); } catch { /* best effort */ } }
}

(async () => {
  if (MODE === 'compare-v') return compareV();
  const shardPrefix = (sub) => `${INPUT.replace(/\/$/, '')}/${sub}/shard=${SHARD}`;
  const prep = path.join(WORK, 'prep'), engine = path.join(WORK, 'engine'), outDir = path.join(WORK, 'out');
  for (const d of [prep, engine, outDir]) fs.mkdirSync(d, { recursive: true });
  const status = { mode: MODE, shard: Number(SHARD), pod: env('POD_NAME', ''), node: env('NODE_NAME', ''),
    input: INPUT, started_utc: new Date().toISOString() };

  // engine inputs: measure bundles + VSAC expansions
  const [eb] = split(ENGINE);
  const bundles = [];
  for (const key of await list(ENGINE)) {
    const rel = key.slice(split(ENGINE)[1].length + 1);
    const dest = path.join(engine, rel); fs.mkdirSync(path.dirname(dest), { recursive: true });
    fs.writeFileSync(dest, await getBuffer(eb, key));
    if (rel.startsWith('bundles/')) bundles.push(dest);
  }
  if (bundles.length !== 2) throw new Error(`expected 2 measure bundles under ${ENGINE}/bundles, found ${bundles.length}`);

  // manifest (first three columns are unit_id, patient_id, mp_year in every export)
  const rows = [];
  const m = await eachLine(shardPrefix('manifest'), (l) => { const c = l.split(','); rows.push(`${c[0]},${c[1]},${c[2]}`); });
  fs.writeFileSync(path.join(prep, 'manifest.csv'), 'unit_id,patient_id,mp_year\n' + rows.join('\n') + (rows.length ? '\n' : ''));
  status.stays_in_shard = rows.length;
  log(`manifest: ${rows.length} stays`);

  // result file kept in S3 by an earlier pod of this shard, if any (resume). Checked BEFORE downloading
  // the shard's input: if every (measure, stay) is already there, the pod ends at once (2026-10-12: finished
  // shards used to download several GB only to find nothing to do, and filled the node disks)
  const [ob, op] = split(OUT);
  const resKey = `${op}/results/${MODE}_${SHARD}.jsonl`, outFile = path.join(outDir, `${MODE}_${SHARD}.jsonl`);
  const prior = MODE === 'profile' ? null : await getBuffer(ob, resKey);
  if (prior && (MODE === 'sliced' || MODE === 'reference')) {
    const have = new Set();
    for (const l of prior.toString('utf8').split('\n')) { try { const r = JSON.parse(l); have.add(`${r.measure}|${r.unit_id}`); } catch { /* torn line */ } }
    const units = new Set(rows.map((r) => r.split(',')[0]));
    if (units.size > 0 && have.size >= units.size * bundles.length) {
      status.exit_code = 0; status.skipped_complete = true; status.resumed_lines = have.size;
      status.ended_utc = new Date().toISOString(); status.wall_seconds = (Date.now() - t0) / 1000;
      await put(ob, `${op}/status/${MODE}_${SHARD}.${Date.now()}.json`, JSON.stringify(status, null, 2));
      log(`already complete (${have.size} results for ${units.size} stays): nothing to do`);
      cleanup(); process.exit(0);
    }
  }

  // resources
  if (MODE === 'sliced') {
    const w = groupedWriter(path.join(prep, 'slices'));
    const r = await eachLine(shardPrefix('slices'), (l) => { const i = l.indexOf('\t'); w.add(l.slice(0, i), l.slice(i + 1)); });
    status.slice_files = w.finish(); status.input_lines = r.lines; status.input_bytes = r.bytes;
    const shared = []; await eachLine(`${INPUT.replace(/\/$/, '')}/shared`, (l) => shared.push(l));
    fs.writeFileSync(path.join(prep, 'shared.ndjson.gz'), zlib.gzipSync(shared.join('\n') + (shared.length ? '\n' : '')));
  } else {   // reference and v: each sampled patient's full record
    const w = groupedWriter(path.join(prep, 'patients'));
    const r = await eachLine(shardPrefix('reference'), (l) => { const i = l.indexOf('\t'); w.add(l.slice(0, i), l.slice(i + 1)); });
    status.patient_files = w.finish(); status.input_lines = r.lines; status.input_bytes = r.bytes;
    const shared = []; await eachLine(`${INPUT.replace(/\/$/, '')}/shared_all`, (l) => shared.push(l));
    fs.writeFileSync(path.join(prep, 'shared_all.ndjson.gz'), zlib.gzipSync(shared.join('\n') + (shared.length ? '\n' : '')));
  }
  status.input_ready_seconds = (Date.now() - t0) / 1000;
  log(`input ready (${status.input_lines} lines)`);
  if (MODE === 'profile') return runProfile({ prep, engine, outDir, bundles, status });

  if (prior) { fs.writeFileSync(outFile, prior); status.resumed_lines = prior.toString('utf8').split('\n').filter(Boolean).length; log(`resuming: ${status.resumed_lines} result lines already done`); }

  let lastSize = -1;
  const sync = async () => {
    if (!fs.existsSync(outFile)) return;
    const size = fs.statSync(outFile).size; if (size === lastSize) return;
    await put(ob, resKey, fs.readFileSync(outFile)); lastSize = size;
  };
  const timer = setInterval(() => sync().catch((e) => log(`sync failed: ${e.message}`)), SYNC * 1000);

  const runEngine = env('RUN_ENGINE', path.join(__dirname, '..', 'run_engine.js'));
  const child = MODE === 'v'
    // V (independent implementation, registration §4.6), with the readings that match CMS's logic; it rewrites
    // its output from scratch (fast), so no resume is needed
    ? spawn('python3', [path.join(V_DIR, 'reference_measures.py'), '--prep', prep, '--out', outFile,
        '--bundles', ...bundles.sort(), '--vs-cache', path.join(engine, 'vsac-cache'), ...V_RULES],
      { stdio: ['ignore', 'inherit', 'inherit'] })
    : spawn(process.execPath, [`--max-old-space-size=${HEAP}`, runEngine,
        '--mode', MODE, '--prep', prep, '--out', outFile, '--bundles', ...bundles.sort(), '--vs-cache', path.join(engine, 'vsac-cache'),
        ...(MODE === 'sliced' ? ['--batch', env('BATCH', '1'), '--batch-max-resources', env('BATCH_MAX_RES', '400')] : [])],
      { stdio: ['ignore', 'inherit', 'inherit'] });
  status.batch = MODE === 'sliced' ? { stays_per_call: Number(env('BATCH', '1')), max_resources_batched: Number(env('BATCH_MAX_RES', '400')) } : null;
  let stopping = false;
  process.on('SIGTERM', async () => {        // pod deleted: save what is done, then stop
    if (stopping) return; stopping = true; log('SIGTERM: saving results');
    child.kill('SIGTERM'); clearInterval(timer); try { await sync(); } catch (e) { log(`final sync failed: ${e.message}`); }
    cleanup(); process.exit(143);
  });
  const code = await new Promise((resolve) => child.on('exit', (c, sig) => resolve(c ?? (sig ? 128 : 1))));
  clearInterval(timer);
  await sync();
  status.exit_code = code; status.ended_utc = new Date().toISOString(); status.wall_seconds = (Date.now() - t0) / 1000;
  // container memory: peak and limit from the cgroup (v2, else v1), for sizing the next runs
  const readNum = (f) => { try { const v = fs.readFileSync(f, 'utf8').trim(); return /^\d+$/.test(v) ? Number(v) : null; } catch { return null; } };
  const peak = readNum('/sys/fs/cgroup/memory.peak') ?? readNum('/sys/fs/cgroup/memory/memory.max_usage_in_bytes');
  const lim = readNum('/sys/fs/cgroup/memory.max') ?? readNum('/sys/fs/cgroup/memory/memory.limit_in_bytes');
  status.memory_peak_mib = peak === null ? null : Math.round(peak / 1048576);
  status.memory_limit_mib = lim === null ? null : Math.round(lim / 1048576);
  // container CPU time (cgroup v2 cpu.stat usage_usec, else v1 cpuacct.usage in ns), for S2's CPU use
  try {
    const st = fs.readFileSync('/sys/fs/cgroup/cpu.stat', 'utf8').match(/^usage_usec (\d+)/m);
    status.cpu_seconds = st ? Number(st[1]) / 1e6 : null;
  } catch { const ns = readNum('/sys/fs/cgroup/cpuacct/cpuacct.usage'); status.cpu_seconds = ns === null ? null : ns / 1e9; }
  status.result_lines = fs.existsSync(outFile) ? fs.readFileSync(outFile, 'utf8').split('\n').filter(Boolean).length : 0;
  await put(ob, `${op}/status/${MODE}_${SHARD}.${Date.now()}.json`, JSON.stringify(status, null, 2));
  const meta = outFile.replace(/\.jsonl$/, '.meta.json');
  if (fs.existsSync(meta)) await put(ob, `${op}/status/${MODE}_${SHARD}.meta.json`, fs.readFileSync(meta));
  log(`exit ${code}; ${status.result_lines} result lines; ${status.wall_seconds.toFixed(0)} s; memory peak ${status.memory_peak_mib ?? '?'} MiB`);
  cleanup(); process.exit(code);
})().catch((e) => { console.error('FATAL', e); cleanup(); process.exit(1); });
