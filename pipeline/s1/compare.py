#!/usr/bin/env python3
"""
compare.py — the demo equivalence check / S1: stay-by-stay comparison of sliced vs reference results from run_engine.js.

  python3 compare.py --reference <ref*.jsonl ...> --sliced <sliced*.jsonl ...> --out <dir> [--label demo]

Identical means, per measure and stay: same episode presence, same true/false for every population,
and (CMS871) the same measure-observation values (eligible days, event days). A stay that is not an
episode counts as all-false. Any engine error on either side is reported, never treated as a match.
Writes <out>/s1_<label>_summary.json and s1_<label>_differences.csv; prints the summary.
"""
import argparse
import collections
import csv
import glob
import json
import os
import sys


BROKEN = collections.Counter()
DUPLICATES = collections.Counter()   # (side, measure) -> extra lines for an already-seen stay


def load(paths, side):
    rows = {}
    for p in paths:
        for f in sorted(glob.glob(p)):
            with open(f) as fh:
                for line in fh:
                    if not line.strip():
                        continue
                    try:
                        r = json.loads(line)
                    except json.JSONDecodeError:   # line cut by an interrupted run; the stay was redone
                        BROKEN[os.path.basename(f)] += 1
                        continue
                    if (r["measure"], r["unit_id"]) in rows:
                        DUPLICATES[(side, r["measure"])] += 1
                    rows[(r["measure"], r["unit_id"])] = r
    return rows


def norm(r):
    if r.get("error"):
        return ("ERROR", r["error"])
    if not r.get("episode"):
        return ("no-episode",)
    pops = tuple(sorted((k, v) for k, v in r["pops"].items() if not k.startswith("MeasureObservation")))
    obs = tuple(sorted((k, json.dumps(v)) for k, v in (r.get("obs") or {}).items()))
    return pops, obs


def members(r):
    if r.get("error") or not r.get("episode"):
        return set()
    return {k for k, v in r["pops"].items() if v and not k.startswith("MeasureObservation")}


ap = argparse.ArgumentParser()
ap.add_argument("--reference", nargs="+", required=True)
ap.add_argument("--sliced", nargs="+", required=True)
ap.add_argument("--out", required=True)
ap.add_argument("--label", default="run")
a = ap.parse_args()

ref, sl = load(a.reference, "reference"), load(a.sliced, "sliced")
keys = sorted(set(ref) | set(sl))
summary = collections.defaultdict(lambda: collections.Counter())
diffs = []
for k in keys:
    m = k[0]
    s = summary[m]
    s["stays"] += 1
    if k not in ref or k not in sl:
        s["missing_reference" if k not in ref else "missing_sliced"] += 1
        diffs.append([m, k[1], "missing", "reference" if k not in ref else "sliced", ""])
        continue
    r, x = ref[k], sl[k]
    if r.get("error") or x.get("error"):
        s["errors"] += 1
        diffs.append([m, k[1], "error", r.get("error") or "", x.get("error") or ""])
        continue
    s["compared"] += 1
    for p in members(r):
        s[f"ref_{p}"] += 1
    if norm(r) == norm(x):
        s["identical"] += 1
    else:
        s["different"] += 1
        diffs.append([m, k[1], "different", json.dumps({"pops": r.get("pops"), "obs": r.get("obs")}),
                      json.dumps({"pops": x.get("pops"), "obs": x.get("obs")})])
    s["ref_seconds_x1000"] += int(1000 * (r.get("seconds") or 0) / max(1, r.get("group_units") or 1))
    s["sliced_seconds_x1000"] += int(1000 * (x.get("seconds") or 0))

os.makedirs(a.out, exist_ok=True)
out = {}
for m, s in summary.items():
    out[m] = {k: v for k, v in s.items() if not k.endswith("_x1000")}
    out[m]["reference_engine_seconds"] = round(s["ref_seconds_x1000"] / 1000, 1)
    out[m]["sliced_engine_seconds"] = round(s["sliced_seconds_x1000"] / 1000, 1)
    out[m]["S1_holds"] = s["compared"] == s["stays"] and s["identical"] == s["compared"]
    out[m]["duplicate_lines_reference"] = DUPLICATES[("reference", m)]
    out[m]["duplicate_lines_sliced"] = DUPLICATES[("sliced", m)]
    out[m]["exactly_once"] = (DUPLICATES[("reference", m)] == 0 and DUPLICATES[("sliced", m)] == 0
                              and s["missing_reference"] == 0 and s["missing_sliced"] == 0)
if BROKEN:
    out["_broken_lines_skipped"] = dict(BROKEN)
json.dump(out, open(os.path.join(a.out, f"s1_{a.label}_summary.json"), "w"), indent=2)
with open(os.path.join(a.out, f"s1_{a.label}_differences.csv"), "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["measure", "unit_id", "kind", "reference", "sliced"])
    w.writerows(diffs)
print(json.dumps(out, indent=1))
sys.exit(0 if all(v["S1_holds"] for k, v in out.items() if not k.startswith("_")) else 2)
