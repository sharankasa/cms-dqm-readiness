#!/usr/bin/env node
/*
 * run_engine.js — the demo equivalence check / S1: evaluate CMS816 and CMS871 with fqm-execution (CMS's reference engine,
 * unmodified) and write each stay's episode result, for the two sides of the equivalence check:
 *
 *   reference : UNSLICED, UNFILTERED. For every patient and every discharge year among their stays,
 *               all of the patient's resources (+ every resource without a patient) in one bundle,
 *               measurement period = that calendar year. The engine is only run in parallel across
 *               patients (shards), never changed (registration §4.3).
 *   sliced    : one bundle per stay = its slice (+ shared Location/Organization), same period.
 *
 *   node run_engine.js --mode reference|sliced --prep <slice_stays.py output dir> --out <file.jsonl> \
 *        --bundles <CMS816 bundle> <CMS871 bundle> --vs-cache <dir> [--shard i/n] [--period-year 2026] \
 *        [--only <id,id>] [--batch N] [--batch-max-resources R]
 *
 * Output: one JSON line per (measure, stay): populations by populationId (true/false), measure
 * observations (CMS871: eligible days, event days), error if any, seconds. Resumable: a rerun skips
 * what the output file already holds. Same measure preparation as testcases/run_fqm.js
 * (real VSAC value sets from the local cache; CMS871 cqfm-aggregateMethod "Sum" -> "sum").
 * Sliced lines also carry `defs` (added 2026-10-14, for R2's "measure components responsible"): for
 * each named expression (define) of the measure's main library, except Patient and SDE, the size of
 * its result in this stay's slice (list -> number of items, boolean -> true/false, null -> null,
 * other value -> 1). The engine computes these anyway; nothing in the evaluation changes.
 *
 * --batch N (sliced side only; default 1 = one engine call per stay, as in S1): up to N stays per
 * engine call, one patient bundle each (CMS's engine evaluates a list of patients per call; the
 * per-call set-up of measure and value sets is then paid once per batch, not once per stay). Rules:
 * stays in one call share the measurement year and have distinct patients (the engine keys its
 * results by patient); a slice with more than R resources (default 400) is evaluated alone; if a
 * batched call fails, or a stay's result cannot be matched to its patient, each stay of that call is
 * evaluated alone. Batched lines record batch_n and batch_seconds; `seconds` is then the call's time
 * divided by batch_n (a share, not a per-stay measurement). Results must be identical to --batch 1
 * (checked on the S1 sample before use, S2).
 */
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const { createRequire } = require('module');

let fqm;
try { fqm = require('fqm-execution'); } catch {
  fqm = createRequire(path.join(__dirname, '../../testcases/package.json'))('fqm-execution');
}
const { Calculator } = fqm;
const engineReq = (() => { try { return createRequire(require.resolve('fqm-execution')); } catch {
  return createRequire(path.join(__dirname, '../../testcases/package.json')); } })();
const ver = (p) => { try { return engineReq(`${p}/package.json`).version; } catch { return 'unknown'; } };

const args = process.argv.slice(2);
const opt = (k, d) => { const i = args.indexOf(k); return i >= 0 ? args[i + 1] : d; };
const multi = (k) => { const i = args.indexOf(k); if (i < 0) return []; const v = [];
  for (let j = i + 1; j < args.length && !args[j].startsWith('--'); j++) v.push(args[j]); return v; };
const mode = opt('--mode'), prep = opt('--prep'), out = opt('--out');
const bundlePaths = multi('--bundles'), vsCache = opt('--vs-cache');
const [shardI, shardN] = (opt('--shard', '0/1')).split('/').map(Number);
const periodYear = opt('--period-year');            // fixed year (CMS test cases: 2026)
const only = opt('--only') ? new Set(opt('--only').split(',')) : null;
const batchN = Math.max(1, Number(opt('--batch', '1')) || 1);
const batchMaxRes = Math.max(1, Number(opt('--batch-max-resources', '400')) || 400);
if (!['reference', 'sliced'].includes(mode) || !prep || !out || !bundlePaths.length) {
  console.error('usage: node run_engine.js --mode reference|sliced --prep <dir> --out <file.jsonl> --bundles <b1> <b2> --vs-cache <dir>');
  process.exit(1);
}

// ---- measures (same preparation as run_fqm.js)
function loadMeasure(p) {
  const bundle = JSON.parse(fs.readFileSync(p));
  const declared = new Set(), present = new Set();
  for (const { resource: r } of bundle.entry) {
    if (r.resourceType === 'ValueSet') present.add(r.url);
    if (r.resourceType === 'Library') for (const c of r.content || []) if (c.contentType === 'text/cql' && c.data)
      for (const m of Buffer.from(c.data, 'base64').toString('utf8').matchAll(/'(http:\/\/cts\.nlm\.nih\.gov\/fhir\/ValueSet\/[0-9.]+)'/g)) declared.add(m[1]);
  }
  const missing = [...declared].filter(u => !present.has(u));
  for (const u of missing) {
    const f = path.join(vsCache || '', `${u.split('/').pop()}.json`);
    if (!vsCache || !fs.existsSync(f)) { console.error(`missing value set ${u}: pass --vs-cache`); process.exit(1); }
    bundle.entry.push({ resource: JSON.parse(fs.readFileSync(f)) });
  }
  let patched = 0;
  const fix = (n) => { if (Array.isArray(n)) return n.forEach(fix); if (n && typeof n === 'object') {
    if (String(n.url || '').endsWith('cqfm-aggregateMethod') && 'valueString' in n) {
      n.valueCode = n.valueString.trim().toLowerCase(); delete n.valueString; patched++; }
    Object.values(n).forEach(fix); } };
  fix(bundle);
  const id = bundle.entry.find(e => e.resource.resourceType === 'Measure').resource.name ||
             path.basename(p).replace('-bundle.json', '');
  return { id, bundle, vsFromCache: missing.map(u => u.split('/').pop()), patched };
}
const measures = bundlePaths.map(loadMeasure);

// ---- inputs
// Parsed line by line from the decompressed bytes (no single giant string: a whole unfiltered patient
// record can exceed V8's maximum string length). Same resources, same order as before.
const readNdjsonGz = (f) => {
  const buf = zlib.gunzipSync(fs.readFileSync(f)), outArr = [];
  let s = 0;
  while (s < buf.length) {
    let e = buf.indexOf(10, s); if (e < 0) e = buf.length;
    if (e > s) { const line = buf.toString('utf8', s, e); if (line.trim()) outArr.push(JSON.parse(line)); }
    s = e + 1;
  }
  return outArr;
};
const manifest = fs.readFileSync(path.join(prep, 'manifest.csv'), 'utf8').trim().split(/\r?\n/);
const hdr = manifest.shift().split(',').map(s => s.trim());
const col = (n) => { const i = hdr.indexOf(n); if (i < 0) { console.error(`manifest.csv has no column ${n}`); process.exit(1); } return i; };
const units = manifest.map(l => l.split(',').map(s => s.trim())).map(c => ({ unit_id: c[col('unit_id')],
  patient_id: c[col('patient_id')], mp_year: periodYear || c[col('mp_year')] }));
const badYear = units.find(u => !/^\d{4}$/.test(String(u.mp_year)));
if (badYear) { console.error(`no valid measurement year for stay ${badYear.unit_id}: ${badYear.mp_year}`); process.exit(1); }
const fnv = (s) => { let h = 2166136261; for (const ch of s) { h ^= ch.charCodeAt(0); h = Math.imul(h, 16777619) >>> 0; } return h; };
const mine = units.filter(u => fnv(u.patient_id) % shardN === shardI)
  .filter(u => !only || only.has(u.unit_id) || only.has(u.patient_id));

// ---- resume
const done = new Set();
if (fs.existsSync(out)) for (const l of fs.readFileSync(out, 'utf8').split('\n').filter(Boolean)) {
  try { const r = JSON.parse(l); done.add(`${r.measure}|${r.unit_id}`); } catch { /* partial last line */ }
}
fs.mkdirSync(path.dirname(path.resolve(out)), { recursive: true });
// if a previous run was killed mid-line, start on a fresh line (the broken line is ignored on resume)
if (fs.existsSync(out) && fs.statSync(out).size > 0) {
  const fd = fs.openSync(out, 'r'); const b = Buffer.alloc(1);
  fs.readSync(fd, b, 0, 1, fs.statSync(out).size - 1); fs.closeSync(fd);
  if (b.toString() !== '\n') fs.appendFileSync(out, '\n');
}
const metaFile = out.replace(/\.jsonl$/, '') + '.meta.json';
if (!fs.existsSync(metaFile)) fs.writeFileSync(metaFile, JSON.stringify({
  mode, prep: path.resolve(prep), shard: `${shardI}/${shardN}`, started_utc: new Date().toISOString(),
  versions: { node: process.version, 'fqm-execution': ver('fqm-execution'), 'cql-execution': ver('cql-execution'),
    'cql-exec-fhir': ver('cql-exec-fhir') },
  measures: measures.map(m => ({ id: m.id, vsFromCache: m.vsFromCache, aggregatePatched: m.patched })),
  trustMetaProfile: 'engine default (true)', periodYear: periodYear || 'per stay: discharge year',
  batch: mode === 'sliced' ? { stays_per_call: batchN, max_resources_batched: batchMaxRes } : null }, null, 2));

// `er` = one patient's ExecutionResult (det.results[i])
const episodeOf = (er, unitId) => {
  const pr = er.detailedResults[0];
  const ep = (pr.episodeResults || []).find(e => e.episodeId === unitId);
  if (!ep) return { episode: false, pops: {}, obs: {} };
  const pops = {}, obs = {};
  for (const p of ep.populationResults) {
    pops[p.populationId || `${p.populationType}:${p.criteriaExpression}`] = !!p.result;
    if (p.populationType === 'measure-observation') obs[p.populationId] = p.observations ?? null;
  }
  return { episode: true, pops, obs };
};
// sizes of the main library's named expressions (R2 components); the main library is the one that
// defines "Initial Population"
const defsOf = (er) => {
  const sr = er.detailedResults[0].statementResults || [];
  const ip = sr.find(s => s.statementName === 'Initial Population' && !s.isFunction);
  if (!ip) return null;
  const out = {};
  for (const s of sr) {
    if (s.libraryName !== ip.libraryName || s.isFunction || s.statementName === 'Patient' || /^SDE /.test(s.statementName)) continue;
    const v = s.raw;
    out[s.statementName] = Array.isArray(v) ? v.length : typeof v === 'boolean' ? v : v === null || v === undefined ? null : 1;
  }
  return out;
};

async function evaluate(entries, year, which = measures) {
  const res = {};
  for (const m of which) {
    const t = Date.now();
    try {
      res[m.id] = { det: await Calculator.calculate(m.bundle, [{ resourceType: 'Bundle', type: 'collection', entry: entries }],
        { measurementPeriodStart: `${year}-01-01`, measurementPeriodEnd: `${year}-12-31` }), secs: (Date.now() - t) / 1000 };
    } catch (e) { res[m.id] = { error: String(e.message || e).replace(/\s+/g, ' ').slice(0, 400), secs: (Date.now() - t) / 1000 }; }
  }
  return res;
}
const write = (o) => fs.appendFileSync(out, JSON.stringify(o) + '\n');
const periodOf = (year) => ({ measurementPeriodStart: `${year}-01-01`, measurementPeriodEnd: `${year}-12-31` });
const asBundle = (entries) => ({ resourceType: 'Bundle', type: 'collection', entry: entries });
const slicedLine = (m, u, r, entries, extra = {}) => ({ mode, measure: m.id, unit_id: u.unit_id, patient_id: u.patient_id,
  mp_year: u.mp_year,
  ...(r.error ? { episode: null, pops: null, obs: null, error: r.error }
    : { ...episodeOf(r.er, u.unit_id), defs: defsOf(r.er), error: null }),
  seconds: r.secs, n_resources: entries.length, ...extra });

// one stay alone (exactly as in S1)
async function evaluateOne(u, entries, todo) {
  const res = await evaluate(entries, u.mp_year, todo);
  for (const m of todo) {
    const r = res[m.id];
    write(slicedLine(m, u, r.error ? r : { er: r.det.results[0], secs: r.secs }, entries));
  }
}
// several stays (same year, distinct patients) in one engine call per measure; any doubt -> alone
async function evaluateBatch(items) {
  const year = items[0].u.mp_year;
  for (const m of measures) {
    const its = items.filter(it => it.todo.includes(m));
    if (!its.length) continue;
    if (its.length === 1) { await evaluateOne(its[0].u, its[0].entries, [m]); continue; }
    const t = Date.now();
    let det = null;
    try { det = await Calculator.calculate(m.bundle, its.map(it => asBundle(it.entries)), periodOf(year)); } catch { det = null; }
    const secs = (Date.now() - t) / 1000;
    const byPid = new Map();
    if (det && Array.isArray(det.results) && det.results.length === its.length) {
      for (const er of det.results) byPid.set(er.patientId, byPid.has(er.patientId) ? null : er);
    }
    const matched = its.map(it => byPid.get(it.u.patient_id) || null);
    if (!det || matched.some(er => !er)) { batchFallbacks++; for (const it of its) await evaluateOne(it.u, it.entries, [m]); continue; }
    its.forEach((it, i) => write(slicedLine(m, it.u, { er: matched[i], secs: secs / its.length }, it.entries,
      { batch_n: its.length, batch_seconds: secs })));
  }
}
let batchFallbacks = 0;

(async () => {
  const t0 = Date.now();
  const log = (s) => console.log(`[${mode} ${shardI}/${shardN} ${((Date.now() - t0) / 1000).toFixed(0)}s] ${s}`);
  if (mode === 'sliced') {
    const shared = readNdjsonGz(path.join(prep, 'shared.ndjson.gz'));
    let n = 0, nb = 0;
    const pending = new Map();   // measurement year -> { items, pids } (batching only)
    const flush = async (year) => { const b = pending.get(year); pending.delete(year); if (b && b.items.length) { await evaluateBatch(b.items); nb++; } };
    for (const u of mine) {
      // exactly once: after an interruption, only the (measure, stay) pairs not yet written are evaluated
      const todo = measures.filter(m => !done.has(`${m.id}|${u.unit_id}`));
      if (!todo.length) continue;
      const f = path.join(prep, 'slices', `${u.unit_id}.ndjson.gz`);
      const entries = shared.concat(fs.existsSync(f) ? readNdjsonGz(f) : []).map(r => ({ resource: r }));
      if (batchN <= 1 || entries.length > batchMaxRes) {
        await evaluateOne(u, entries, todo);
      } else {
        if (pending.has(u.mp_year) && pending.get(u.mp_year).pids.has(u.patient_id)) await flush(u.mp_year);
        if (!pending.has(u.mp_year)) pending.set(u.mp_year, { items: [], pids: new Set() });
        const b = pending.get(u.mp_year);
        b.items.push({ u, entries, todo }); b.pids.add(u.patient_id);
        if (b.items.length >= batchN) await flush(u.mp_year);
      }
      if (++n % 25 === 0) log(`${n} stays`);
    }
    for (const year of [...pending.keys()]) await flush(year);
    log(`done: ${n} stays evaluated (${mine.length} in shard)` +
        (batchN > 1 ? `; batch ${batchN} (slices up to ${batchMaxRes} resources): ${nb} batched calls, ${batchFallbacks} fell back to one stay per call` : ''));
  } else {
    const sharedAll = readNdjsonGz(path.join(prep, 'shared_all.ndjson.gz'));
    const groups = new Map();
    for (const u of mine) { const k = `${u.patient_id}|${u.mp_year}`; if (!groups.has(k)) groups.set(k, []); groups.get(k).push(u); }
    let g = 0, cachePid = null, cacheRes = null;
    for (const [k, us] of groups) {
      if (us.every(u => measures.every(m => done.has(`${m.id}|${u.unit_id}`)))) { g++; continue; }
      const [pid, year] = k.split('|');
      if (pid !== cachePid) { cacheRes = readNdjsonGz(path.join(prep, 'patients', `${pid}.ndjson.gz`)); cachePid = pid; }
      const entries = sharedAll.concat(cacheRes).map(r => ({ resource: r }));
      const todo = measures.filter(m => us.some(u => !done.has(`${m.id}|${u.unit_id}`)));
      const res = await evaluate(entries, year, todo);
      for (const u of us) for (const m of todo) {
        if (done.has(`${m.id}|${u.unit_id}`)) continue;
        const r = res[m.id];
        write({ mode, measure: m.id, unit_id: u.unit_id, patient_id: u.patient_id, mp_year: year,
          ...(r.error ? { episode: null, pops: null, obs: null, error: r.error } : { ...episodeOf(r.det.results[0], u.unit_id), error: null }),
          seconds: r.secs, group_units: us.length, n_resources: entries.length });
      }
      g++;
      log(`group ${g}/${groups.size}: patient ${pid.slice(0, 8)} year ${year}, ${us.length} stay(s), ${entries.length} resources, ` +
          todo.map(m => `${m.id.slice(0, 6)} ${res[m.id].secs.toFixed(0)}s${res[m.id].error ? ' ERROR' : ''}`).join(', '));
    }
    log(`done: ${groups.size} patient-year groups`);
  }
})().catch(e => { console.error('FATAL', e); process.exitCode = 1; });
