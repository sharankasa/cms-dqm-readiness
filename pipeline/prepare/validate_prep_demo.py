#!/usr/bin/env python3
"""
validate_prep_demo.py — the prepare stage on the OPEN demo: is the Athena prepare stage (prepare_run.py --mode demo) the
same as the validated local pipeline? Open data only. Standard library only.

  python3 pipeline/prepare/validate_prep_demo.py --run <results/prep-demo-raw-<run>> \
      [--shim work/demo-mapped-v3] [--duckdb-manifest work/demo-prep-v3/manifest.csv] \
      [--make-prep work/demo-prep-athena]

Checks (written to <run>/validation.json; differences to <run>/validation_differences.csv):
  1. mapping: every resource Athena mapped vs the Python shim v3 (apply_shim.py), compared as parsed JSON
     (Athena re-serializes rewritten resources; reasonReference order normalised). Must all be equal.
  2. slices: per stay, Athena vs DuckDB (slice_stays.py --filter on the shim output): same stays, same
     windows, same number of resources, same slice_sha256 (same resources). content_sha256 differs by
     design where Athena re-serialized a resource; check 3 covers the bytes.
  3. export: recompute slice_sha256 and content_sha256 from the exported worker input; must equal the
     Athena manifest (the UNLOAD files carry the exact bytes that were hashed).
  --make-prep: writes the exported slices in the layout pipeline/s1/run_engine.js reads (slices/,
  shared.ndjson.gz, manifest.csv), to run CMS's engine on them and compare with the local results.
"""
import argparse
import collections
import csv
import glob
import gzip
import hashlib
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def gz_lines(pattern):
    for f in sorted(glob.glob(pattern, recursive=True)):
        if os.path.isdir(f):
            continue
        opener = gzip.open if f.endswith(".gz") or open(f, "rb").read(2) == b"\x1f\x8b" else open
        with opener(f, "rt", encoding="utf-8") as fh:
            for line in fh:
                line = line.rstrip("\n")
                if line:
                    yield f, line


def norm(r):
    if r.get("resourceType") == "Encounter" and isinstance(r.get("reasonReference"), list):
        r = dict(r)
        r["reasonReference"] = sorted(r["reasonReference"], key=lambda x: json.dumps(x, sort_keys=True))
    return r


def first_diff(a, b, path=""):
    if type(a) != type(b) and not (isinstance(a, (int, float)) and isinstance(b, (int, float))):
        return path or "/"
    if isinstance(a, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a or k not in b:
                return f"{path}/{k}"
            d = first_diff(a[k], b[k], f"{path}/{k}")
            if d:
                return d
        return None
    if isinstance(a, list):
        if len(a) != len(b):
            return f"{path}[len {len(a)} vs {len(b)}]"
        for i, (x, y) in enumerate(zip(a, b)):
            d = first_diff(x, y, f"{path}[{i}]")
            if d:
                return d
        return None
    return None if a == b else path


def sha(s):
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def ts(s):
    return (s or "").replace("T", " ")[:19]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--shim", default=os.path.join(ROOT, "work/demo-mapped-v3"))
    ap.add_argument("--duckdb-manifest", default=os.path.join(ROOT, "work/demo-prep-v3/manifest.csv"))
    ap.add_argument("--make-prep")
    a = ap.parse_args()
    exp = os.path.join(a.run, "export")
    res, diffs = {}, []

    # 1. mapping vs shim (Athena lines staged in a temporary SQLite file to keep memory small)
    import sqlite3
    import tempfile
    tmpdb = os.path.join(tempfile.gettempdir(), "prep_validate_mapped.sqlite")
    if os.path.exists(tmpdb):
        os.remove(tmpdb)
    con = sqlite3.connect(tmpdb)
    con.execute("CREATE TABLE m (sf TEXT, id TEXT, changed TEXT, keep TEXT, js TEXT, seen INTEGER DEFAULT 0, PRIMARY KEY (sf, id))")
    flags = collections.Counter()
    batch = []
    for _, line in gz_lines(os.path.join(exp, "mapped_open", "**", "*")):
        sf, changed, keep, js = line.split("\t", 3)
        rid = json.loads(js)["id"]
        batch.append((sf, rid, changed, keep, js))
        flags[(sf, changed, keep)] += 1
        if len(batch) >= 50000:
            con.executemany("INSERT INTO m (sf, id, changed, keep, js) VALUES (?,?,?,?,?)", batch); batch = []
    con.executemany("INSERT INTO m (sf, id, changed, keep, js) VALUES (?,?,?,?,?)", batch)
    con.commit()
    per_file = collections.defaultdict(collections.Counter)
    for f in sorted(glob.glob(os.path.join(a.shim, "*.ndjson.gz"))):
        sf = os.path.basename(f)[:-len(".ndjson.gz")]
        with gzip.open(f, "rt", encoding="utf-8") as fh:
            for line in fh:
                if not line.strip():
                    continue
                r = json.loads(line)
                row = con.execute("SELECT js FROM m WHERE sf = ? AND id = ?", (sf, r["id"])).fetchone()
                if row is None:
                    per_file[sf]["missing_in_athena"] += 1
                    diffs.append(["mapping", sf, r["id"], "missing in Athena"])
                    continue
                con.execute("UPDATE m SET seen = 1 WHERE sf = ? AND id = ?", (sf, r["id"]))
                x = json.loads(row[0])
                if norm(x) == norm(r):
                    per_file[sf]["identical"] += 1
                else:
                    per_file[sf]["different"] += 1
                    if per_file[sf]["different"] <= 5:
                        diffs.append(["mapping", sf, r["id"], first_diff(norm(x), norm(r))])
        con.commit()
    for sf, n in con.execute("SELECT sf, count(*) FROM m WHERE seen = 0 GROUP BY sf"):
        per_file[sf]["only_in_athena"] += n
    con.close()
    os.remove(tmpdb)
    res["mapping"] = {sf: dict(c) for sf, c in sorted(per_file.items())}
    res["mapping_rewritten_by_athena"] = {f"{sf}": n for (sf, ch, kp), n in sorted(flags.items()) if ch == "true"}
    tot = collections.Counter()
    for c in per_file.values():
        tot.update(c)
    res["mapping_total"] = dict(tot)
    res["mapping_identical"] = tot["different"] == 0 and tot["missing_in_athena"] == 0 and tot["only_in_athena"] == 0

    # 3 (first, it reads the export). recompute hashes from the worker input
    by_unit = collections.defaultdict(list)
    for _, line in gz_lines(os.path.join(exp, "slices", "**", "*")):
        unit, js = line.split("\t", 1)
        by_unit[unit].append(js)
    man = {}
    for _, line in gz_lines(os.path.join(exp, "manifest", "**", "*")):
        c = line.split(",")
        man[c[0]] = dict(unit_id=c[0], patient_id=c[1], mp_year=c[2], n_resources=c[3], slice_sha256=c[4],
                         content_sha256=c[5], h_lo=c[6], w_lo=c[7], w_hi=c[8], work_cms816=c[9], work_cms871=c[10])
    ok3 = 0
    for u, m in man.items():
        items = []
        for js in by_unit.get(u, []):
            r = json.loads(js)
            items.append((f"{r['resourceType']}/{r['id']}", js))
        items.sort(key=lambda t: t[0])
        s_sha = sha("\n".join(k for k, _ in items)) if items else ""
        c_sha = sha("\n".join(f"{k}:{sha(js)}" for k, js in items)) if items else ""
        good = (s_sha == m["slice_sha256"] and c_sha == m["content_sha256"] and str(len(items)) == m["n_resources"])
        ok3 += good
        if not good:
            diffs.append(["export", "", u, f"recomputed {len(items)} / manifest {m['n_resources']} resources"])
    res["export"] = {"stays_in_manifest": len(man), "stays_with_lines": len(by_unit), "hashes_match": ok3,
                     "identical": ok3 == len(man) and set(by_unit) <= set(man)}

    # 2. slices vs DuckDB
    duck = {r["unit_id"]: r for r in csv.DictReader(open(a.duckdb_manifest))}
    c = collections.Counter()
    for u in sorted(set(duck) | set(man)):
        if u not in man:
            c["only_duckdb"] += 1; diffs.append(["slices", "", u, "only in DuckDB"]); continue
        if u not in duck:
            c["only_athena"] += 1; diffs.append(["slices", "", u, "only in Athena"]); continue
        d, x = duck[u], man[u]
        win = (ts(d["hosp_start_lower_bound_utc"]), ts(d["window_start_utc"]), ts(d["window_end_utc"])) == (ts(x["h_lo"]), ts(x["w_lo"]), ts(x["w_hi"]))
        c["same_window"] += win
        c["same_n_resources"] += d["n_resources"] == x["n_resources"]
        c["same_slice_sha256"] += d["slice_sha256"] == x["slice_sha256"]
        c["same_mp_year"] += d["mp_year"] == x["mp_year"]
        c["same_content_sha256"] += d["content_sha256"] == x["content_sha256"]
        if not (win and d["slice_sha256"] == x["slice_sha256"] and d["mp_year"] == x["mp_year"]):
            diffs.append(["slices", "", u, json.dumps({"duckdb": d, "athena": x})])
    n = len(set(duck) | set(man))
    res["slices"] = dict(c, stays=n, identical_membership=(c["only_duckdb"] == 0 and c["only_athena"] == 0 and
                                                          c["same_window"] == c["same_slice_sha256"] == c["same_mp_year"] == n))

    if a.make_prep:
        os.makedirs(os.path.join(a.make_prep, "slices"), exist_ok=True)
        for u, lines in by_unit.items():
            with gzip.open(os.path.join(a.make_prep, "slices", f"{u}.ndjson.gz"), "wt", encoding="utf-8") as f:
                f.write("\n".join(lines) + "\n")
        with gzip.open(os.path.join(a.make_prep, "shared.ndjson.gz"), "wt", encoding="utf-8") as f:
            for _, line in gz_lines(os.path.join(exp, "shared", "**", "*")):
                f.write(line + "\n")
        with open(os.path.join(a.make_prep, "manifest.csv"), "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(next(iter(man.values())).keys()))
            w.writeheader()
            for u in sorted(man):
                w.writerow(man[u])
        json.dump({"source": os.path.abspath(a.run), "note": "Athena the prepare stage export (open demo), for run_engine.js --mode sliced"},
                  open(os.path.join(a.make_prep, "run.json"), "w"), indent=2)
        res["prep_written"] = a.make_prep

    json.dump(res, open(os.path.join(a.run, "validation.json"), "w"), indent=2)
    with open(os.path.join(a.run, "validation_differences.csv"), "w", newline="") as f:
        csv.writer(f).writerows([["check", "file", "id", "detail"]] + diffs)
    print(json.dumps({k: res[k] for k in ["mapping_total", "mapping_identical", "slices", "export"]}, indent=1))
    ok = res["mapping_identical"] and res["slices"]["identical_membership"] and res["export"]["identical"]
    print("the prepare stage demo validation:", "PASS" if ok else "FAIL (see validation_differences.csv)")
    sys.exit(0 if ok else 2)


if __name__ == "__main__":
    main()
