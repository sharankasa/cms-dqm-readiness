#!/usr/bin/env node
/*
 * run_fqm.js — conformance baseline with fqm-execution (MITRE), the engine CMS's MADiE uses to produce
 * the expected results shipped with CMS's test cases. Public data only (CMS measure bundles + synthetic
 * test patients); no server needed.
 *
 *   npm install                      # once, in this folder
 *   node run_fqm.js --repo <dqm-content clone> --measure CMS816FHIRHHHypo --out <results dir> \
 *        [--stub-missing-vs] [--patch-aggregate]
 *
 * --stub-missing-vs : add a placeholder ValueSet (one code no record carries) for each value set declared
 *                     in the bundle's CQL but not shipped in it (diagnostic; labelled in the output name)
 * --patch-aggregate : cqfm-aggregateMethod valueString "Sum" -> valueCode "sum" (labelled)
 *
 * Writes <out>/fqm[-patched][-stubvs]_<measure>.csv and prints "<label> <measure>: N/M test cases match".
 */
const fs = require('fs');
const path = require('path');
const { Calculator } = require('fqm-execution');

const args = process.argv.slice(2);
const opt = (k, d) => { const i = args.indexOf(k); return i >= 0 ? args[i + 1] : d; };
const flag = (k) => args.includes(k);
const repo = opt('--repo'), measure = opt('--measure'), out = opt('--out', 'results');
if (!repo || !measure) { console.error('usage: node run_fqm.js --repo <dir> --measure <id> [--out dir]'); process.exit(1); }

const bundle = JSON.parse(fs.readFileSync(path.join(repo, 'bundles/measure', measure, `${measure}-bundle.json`)));
let label = 'fqm';

// value sets declared in CQL but absent from the bundle
const declared = new Set(), present = new Set();
for (const { resource: r } of bundle.entry) {
  if (r.resourceType === 'ValueSet') present.add(r.url);
  if (r.resourceType === 'Library') for (const c of r.content || []) {
    if (c.contentType === 'text/cql' && c.data) {
      const cql = Buffer.from(c.data, 'base64').toString('utf8');
      for (const m of cql.matchAll(/'(http:\/\/cts\.nlm\.nih\.gov\/fhir\/ValueSet\/[0-9.]+)'/g)) declared.add(m[1]);
    }
  }
}
const missing = [...declared].filter(u => !present.has(u)).sort();
console.log(`value sets declared in CQL but absent from the bundle: ${missing.length}` +
  (missing.length ? ` -> ${missing.map(u => u.split('/').pop()).join(', ')}` : ''));

if (flag('--patch-aggregate')) {
  let n = 0;
  const fix = (node) => {
    if (Array.isArray(node)) return node.forEach(fix);
    if (node && typeof node === 'object') {
      if (String(node.url || '').endsWith('cqfm-aggregateMethod') && 'valueString' in node) {
        node.valueCode = node.valueString.trim().toLowerCase(); delete node.valueString; n++;
      }
      Object.values(node).forEach(fix);
    }
  };
  fix(bundle); label += '-patched';
  console.log(`PATCHED ${n} cqfm-aggregateMethod extension(s): valueString -> valueCode (lowercase)`);
}
// real value sets from the VSAC cache (fetch_vsac.py), before any placeholder
const vsCache = opt('--vs-cache');
let stillMissing = missing;
if (vsCache) {
  const found = [];
  for (const url of missing) {
    const p = path.join(vsCache, `${url.split('/').pop()}.json`);
    if (fs.existsSync(p)) { bundle.entry.push({ resource: JSON.parse(fs.readFileSync(p)) }); found.push(url); }
  }
  stillMissing = missing.filter(u => !found.includes(u));
  label += '-vsac';
  console.log(`VSAC: added ${found.length} value set(s) from ${vsCache}; still missing: ${stillMissing.length}`);
}
if (flag('--stub-missing-vs') && stillMissing.length) {
  const ph = { system: 'urn:dqm:stub', code: 'no-match', display: 'placeholder' };
  for (const url of stillMissing) {
    const oid = url.split('/').pop();
    bundle.entry.push({ resource: { resourceType: 'ValueSet', id: oid, url, status: 'active',
      compose: { include: [{ system: ph.system, concept: [{ code: ph.code }] }] },
      expansion: { timestamp: '2026-10-04T00:00:00Z', total: 1, contains: [ph] } } });
  }
  label += '-stubvs';
  console.log(`STUBBED ${stillMissing.length} missing value set(s) with placeholders that match nothing (diagnostic only)`);
}

// --detailed: compare from the engine's detailed results (needed for ratio measures with observations,
// e.g. CMS871: the engine's MeasureReport puts the observation sum into the denominator count)
const detailed = flag('--detailed');
if (detailed) label += '-detailed';
const measureRes = bundle.entry.find(e => e.resource.resourceType === 'Measure').resource;
const groupCodes = measureRes.group[0].population.map(p => p.code.coding[0].code);
const csvq = (s) => `"${String(s).replace(/"/g, '""')}"`;
(async () => {
  const tdir = path.join(repo, 'input/tests/measure', measure);
  const rows = [['engine', 'measure', 'patient_id', 'passed', 'expected', 'actual', 'diffs', 'description'].join(',')];
  let pass = 0, n = 0;
  for (const pid of fs.readdirSync(tdir).filter(d => fs.statSync(path.join(tdir, d)).isDirectory()).sort()) {
    let expected = null; const entries = [];
    for (const f of fs.readdirSync(path.join(tdir, pid)).filter(f => f.endsWith('.json')).sort()) {
      const r = JSON.parse(fs.readFileSync(path.join(tdir, pid, f)));
      if (r.resourceType === 'MeasureReport') expected = r; else entries.push({ resource: r });
    }
    if (!expected) continue;
    n++;
    const exp = {}, act = {}, expObs = [], actObs = [];
    (expected.group?.[0]?.population || []).forEach(p => {
      const code = p.code.coding[0].code;
      if (detailed && code === 'measure-observation') { if (p.count) expObs.push(p.count); } else exp[code] = p.count;
    });
    const desc = ((expected.extension || []).find(e => String(e.url).endsWith('cqfm-testCaseDescription')) || {}).valueMarkdown || '';
    let err = '';
    const pb = [{ resourceType: 'Bundle', type: 'collection', entry: entries }];
    const opts = { measurementPeriodStart: expected.period?.start || '2026-01-01',
                   measurementPeriodEnd: expected.period?.end || '2026-12-31' };
    try {
      if (!detailed) {
        const { results } = await Calculator.calculateMeasureReports(bundle, pb, { ...opts, reportType: 'individual' });
        const rep = Array.isArray(results) ? results[0] : results;
        (rep.group?.[0]?.population || []).forEach(p => { act[p.code.coding[0].code] = p.count; });
      } else {
        // count like CMS's MADiE: members per population (episodes for episode-based measures) and the
        // summed observation values per observation population, from the engine's detailed results
        const det = await Calculator.calculate(bundle, pb, opts);
        const pr = det.results[0].detailedResults[0];
        const units = pr.episodeResults?.length ? pr.episodeResults : [{ populationResults: pr.populationResults }];
        groupCodes.forEach((code, i) => {
          if (code === 'measure-observation') {
            const s = units.reduce((t, u) => { const r = u.populationResults[i];
              return t + (r?.result && r.observations ? r.observations.reduce((a, b) => a + (Number(b) || 0), 0) : 0); }, 0);
            if (s) actObs.push(s);
          } else {
            act[code] = (act[code] || 0) + units.filter(u => u.populationResults[i]?.result).length;
          }
        });
      }
    } catch (e) { err = String(e.message || e).slice(0, 300); }
    const sortN = a => [...a].sort((x, y) => x - y).join(',');
    const diffs = err ? [err] : Object.keys(exp).filter(k => exp[k] !== act[k]).map(k => `${k}: expected ${exp[k]}, got ${act[k]}`)
      .concat(detailed && sortN(expObs) !== sortN(actObs) ? [`measure-observation: expected [${expObs}], got [${actObs}]`] : []);
    if (detailed) { exp['measure-observation'] = expObs; act['measure-observation'] = actObs; }
    const ok = diffs.length === 0;
    if (ok) pass++;
    rows.push([label, measure, pid, ok, csvq(JSON.stringify(exp)), csvq(JSON.stringify(act)), csvq(diffs.join('; ')),
      csvq(desc.trim().replace(/\s+/g, ' '))].join(','));
  }
  fs.mkdirSync(out, { recursive: true });
  const file = path.join(out, `${label}_${measure}.csv`);
  fs.writeFileSync(file, rows.join('\n') + '\n');
  console.log(`${label} ${measure}: ${pass}/${n} test cases match expected -> ${file}`);
})();
