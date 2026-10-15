#!/usr/bin/env node
/*
 * profile_summary.js — aggregate a V8 .cpuprofile into the tables the paper needs: share of CPU time
 * by package (fqm-execution, cql-execution, cql-exec-fhir, ucum-lhc, ...), by source file (cql-execution's
 * files are named after the CQL/ELM operator families: query, interval, datetime, quantity, ...), and
 * the top functions by self and by inclusive time. Function names and file paths only: no data.
 *
 *   node profile_summary.js <file.cpuprofile> [top=40]        prints JSON
 *   require('./profile_summary').summarize(profileObject, top)
 */
const fs = require('fs');
const path = require('path');

function where(url) {
  const file = String(url || '').replace(/^file:\/\//, '');
  if (!file) return { pkg: '(native or engine-internal)', file: '' };
  const i = file.lastIndexOf('node_modules/');
  if (i < 0) return { pkg: path.basename(file), file: path.basename(file) };
  const parts = file.slice(i + 'node_modules/'.length).split('/');
  const pkg = parts[0].startsWith('@') ? `${parts[0]}/${parts[1]}` : parts[0];
  const inner = parts.slice(parts[0].startsWith('@') ? 2 : 1).join('/');
  return { pkg, file: `${pkg}/${inner}` };
}

function summarize(profile, top = 40) {
  const nodes = new Map(profile.nodes.map((n) => [n.id, n]));
  const parent = new Map();
  for (const n of profile.nodes) for (const c of n.children || []) parent.set(c, n.id);
  const selfUs = new Map();
  const { samples = [], timeDeltas = [] } = profile;
  for (let i = 0; i < samples.length; i++) selfUs.set(samples[i], (selfUs.get(samples[i]) || 0) + Math.max(0, timeDeltas[i] || 0));
  let total = 0;
  for (const v of selfUs.values()) total += v;
  const key = (n) => {
    const cf = n.callFrame, w = where(cf.url);
    return { fn: `${cf.functionName || '(anonymous)'} ${w.file || w.pkg}:${(cf.lineNumber ?? -1) + 1}`, pkg: w.pkg, file: w.file || w.pkg };
  };
  const add = (m, k, v) => m.set(k, (m.get(k) || 0) + v);
  const fnSelf = new Map(), fnIncl = new Map(), pkgSelf = new Map(), fileSelf = new Map(), fileIncl = new Map();
  for (const [id, us] of selfUs) {
    const k = key(nodes.get(id));
    add(fnSelf, k.fn, us); add(pkgSelf, k.pkg, us); add(fileSelf, k.file, us);
    const seenFn = new Set(), seenFile = new Set();
    for (let cur = id; cur !== undefined; cur = parent.get(cur)) {      // each function/file once per stack
      const kk = key(nodes.get(cur));
      if (!seenFn.has(kk.fn)) { seenFn.add(kk.fn); add(fnIncl, kk.fn, us); }
      if (!seenFile.has(kk.file)) { seenFile.add(kk.file); add(fileIncl, kk.file, us); }
    }
  }
  const pct = (us) => Math.round((1000 * us) / (total || 1)) / 10;
  const rank = (m, n) => [...m.entries()].sort((a, b) => b[1] - a[1]).slice(0, n).map(([k, us]) => ({ name: k, seconds: Math.round(us / 1e4) / 100, pct: pct(us) }));
  return {
    total_seconds: Math.round(total / 1e4) / 100, samples: samples.length,
    by_package_self: rank(pkgSelf, 15), by_file_self: rank(fileSelf, 25), by_file_inclusive: rank(fileIncl, 25),
    top_functions_self: rank(fnSelf, top), top_functions_inclusive: rank(fnIncl, top),
  };
}

if (require.main === module) {
  const [f, top] = process.argv.slice(2);
  if (!f) { console.error('usage: node profile_summary.js <file.cpuprofile> [top]'); process.exit(1); }
  console.log(JSON.stringify(summarize(JSON.parse(fs.readFileSync(f, 'utf8')), Number(top || 40)), null, 2));
}
module.exports = { summarize };
