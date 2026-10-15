#!/usr/bin/env python3
"""
testcases_to_ndjson.py — pack CMS's test cases for one measure into NDJSON.gz (one file per resource
type) so the same slicer and runner can be used on them as on MIMIC. Public data only.
MeasureReports (the expected results) are left out of the data.

  python3 testcases_to_ndjson.py --repo <dqm-content> --measure CMS816FHIRHHHypo --out <dir>
"""
import argparse
import collections
import gzip
import json
import os

ap = argparse.ArgumentParser()
ap.add_argument("--repo", required=True)
ap.add_argument("--measure", required=True)
ap.add_argument("--out", required=True)
a = ap.parse_args()

tdir = os.path.join(a.repo, "input/tests/measure", a.measure)
by_type, seen = collections.defaultdict(list), set()
for case in sorted(d for d in os.listdir(tdir) if os.path.isdir(os.path.join(tdir, d)) and not d.startswith(".")):
    for f in sorted(os.listdir(os.path.join(tdir, case))):
        if not f.endswith(".json"):
            continue
        r = json.load(open(os.path.join(tdir, case, f)))
        if r["resourceType"] == "MeasureReport":
            continue
        key = (r["resourceType"], r["id"])
        if key in seen:
            raise SystemExit(f"duplicate resource {key} across test cases")
        seen.add(key)
        by_type[r["resourceType"]].append(r)
os.makedirs(a.out, exist_ok=True)
for t, rs in by_type.items():
    with gzip.open(os.path.join(a.out, f"{t}.ndjson.gz"), "wt", encoding="utf-8") as fh:
        for r in rs:
            fh.write(json.dumps(r, separators=(",", ":")) + "\n")
print(a.measure, {t: len(rs) for t, rs in sorted(by_type.items())})
