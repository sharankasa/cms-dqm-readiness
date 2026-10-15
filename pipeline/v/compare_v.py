#!/usr/bin/env python3
"""
compare_v.py — V: stay-by-stay agreement between fqm-execution (run_engine.js output) and the
independent implementation (reference_measures.py output).

  python3 compare_v.py --engine <engine*.jsonl ...> --reference <ref.jsonl> --out <dir> --label <name>

Compared per (measure, stay), after normalising the engine's reporting conventions:
  CMS816: initial population, numerator.
  CMS871: initial population, denominator exclusion, numerator, eligible days, event days.
  Not an episode = all false. The engine reports no observation for excluded stays and none for the
  numerator when there are no event days; both are read as 0 (CMS's logic returns 0 for excluded
  stays). Writes <out>/v_<label>_summary.json and v_<label>_disagreements.csv.
"""
import argparse
import collections
import csv
import glob
import json
import os


def load(paths):
    rows = {}
    for p in paths:
        for f in sorted(glob.glob(p)):
            for line in open(f):
                if line.strip():
                    try:
                        r = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    rows[(r["measure"], r["unit_id"])] = r
    return rows


def obs(r, key):
    v = (r.get("obs") or {}).get(key)
    return int(sum(v)) if v else 0


def view(r, measure):
    if r.get("error"):
        return {"error": r["error"]}
    ep = bool(r.get("episode"))
    p = r.get("pops") or {}
    v = {"initial_population": ep and bool(p.get("InitialPopulation_1")),
         "numerator": ep and bool(p.get("Numerator_1"))}
    if measure.startswith("CMS871"):
        excl = ep and bool(p.get("DenominatorExclusion_1"))
        v["exclusion"] = excl
        v["eligible_days"] = 0 if (not ep or excl) else obs(r, "MeasureObservation_1_1")
        v["event_days"] = 0 if (not ep or excl) else obs(r, "MeasureObservation_1_2")
    return v


ap = argparse.ArgumentParser()
ap.add_argument("--engine", nargs="+", required=True)
ap.add_argument("--reference", nargs="+", required=True)
ap.add_argument("--out", required=True)
ap.add_argument("--label", default="run")
ap.add_argument("--no-population-counts", action="store_true",
                help="pilot (registration §5): leave out how many stays are in each population (engine_* counts)")
a = ap.parse_args()
eng, ref = load(a.engine), load(a.reference)
summary = collections.defaultdict(collections.Counter)
rows = []
for k in sorted(set(eng) | set(ref)):
    m = k[0]
    s = summary[m]
    s["stays"] += 1
    if k not in eng or k not in ref:
        s["missing"] += 1
        rows.append([m, k[1], "missing", "engine" if k not in eng else "reference", ""])
        continue
    e, r = view(eng[k], m), view(ref[k], m)
    if "error" in e or "error" in r:
        s["errors"] += 1
        rows.append([m, k[1], "error", e.get("error", ""), r.get("error", "")])
        continue
    s["compared"] += 1
    same = e == r
    s["agree"] += same
    for f in e:
        if e[f] != r[f]:
            s[f"differ_{f}"] += 1
        if e[f] is True:
            s[f"engine_{f}"] += 1
    if not same:
        rows.append([m, k[1], "differ", json.dumps(e), json.dumps(r)])
os.makedirs(a.out, exist_ok=True)
out = {m: {k: v for k, v in s.items() if not (a.no_population_counts and k.startswith("engine_"))}
       for m, s in summary.items()}
json.dump(out, open(os.path.join(a.out, f"v_{a.label}_summary.json"), "w"), indent=2)
with open(os.path.join(a.out, f"v_{a.label}_disagreements.csv"), "w", newline="") as f:
    csv.writer(f).writerows([["measure", "unit_id", "kind", "engine", "reference"]] + rows)
print(json.dumps(out, indent=1))
