#!/usr/bin/env python3
"""
athena_run.py — the two-backend slicing check: run the same stay slicing in Athena (engine v3) and compare it with DuckDB.
Standard library + the AWS CLI only. Uses the AWS credentials already in the shell.

  python3 pipeline/prepare/athena_run.py --input work/demo-mapped --label demo \
      --bundles <CMS816 bundle> <CMS871 bundle> --vs-cache vendor/vsac-cache --filter \
      --compare-to work/demo-prep/manifest.csv

What it does (nothing is deleted or overwritten; every run writes under a new S3 prefix):
  1. checks the AWS identity and the Athena workgroup (engine v3; CTAS locations allowed);
  2. --input: syncs the NDJSON files to s3://<bucket>/dqm/open/<label>/ (OPEN data only; for
     credentialed data use --data-location with data already in S3);
  3. uploads the value-set codes (TSV) to the run prefix s3://<bucket>/dqm/prep/<label>-<run>/;
  4. (re)creates the tables of database <db> (DROP TABLE removes metadata only) from
     sql/athena/10_load_index.sql + sql/athena/generated/*.sql (SQLGlot translation of the core);
  5. downloads per-stay window bounds and hashes -> <out>/athena_manifest.csv;
  6. --compare-to: compares stay by stay with the DuckDB manifest -> <out>/backend_equivalence.json.
"""
import argparse
import csv
import datetime
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from common import sql_file, vs_codes_from  # noqa: E402

TABLES = ["raw_ndjson", "vs_codes", "res", "res_times", "res_index", "res_codes",
          "units", "unit_bound", "members", "shared", "slice", "slice_hash"]


def aws(*args, capture=True):
    r = subprocess.run(["aws", *args, "--output", "json"], capture_output=capture, text=True)
    if r.returncode != 0:
        sys.exit(f"aws {' '.join(args[:3])} failed:\n{r.stderr.strip()[:1500]}")
    return json.loads(r.stdout) if capture and r.stdout.strip() else None


class Athena:
    def __init__(self, workgroup, output_location):
        self.wg, self.out = workgroup, output_location
        self.scanned, self.engine_ms, self.n = 0, 0, 0

    def run(self, sql, db=None, label=""):
        args = ["athena", "start-query-execution", "--query-string", sql, "--work-group", self.wg]
        if db:
            args += ["--query-execution-context", f"Database={db}"]
        if self.out:
            args += ["--result-configuration", f"OutputLocation={self.out}"]
        qid = aws(*args)["QueryExecutionId"]
        while True:
            q = aws("athena", "get-query-execution", "--query-execution-id", qid)["QueryExecution"]
            state = q["Status"]["State"]
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            time.sleep(2)
        st = q.get("Statistics", {})
        self.scanned += st.get("DataScannedInBytes", 0)
        self.engine_ms += st.get("EngineExecutionTimeInMillis", 0)
        self.n += 1
        if state != "SUCCEEDED":
            sys.exit(f"\nAthena {state} on: {label or sql[:120]}\n{q['Status'].get('StateChangeReason', '')}\n(query id {qid})")
        print(f"  ok  {label or sql.split(chr(10))[0][:70]:<72} {st.get('EngineExecutionTimeInMillis', 0) / 1000:6.1f} s "
              f"{st.get('DataScannedInBytes', 0) / 1e6:8.1f} MB")
        return qid

    def rows(self, qid):
        res = aws("athena", "get-query-results", "--query-execution-id", qid)
        rows = [[c.get("VarCharValue") for c in r["Data"]] for r in res["ResultSet"]["Rows"]]
        return rows[0], rows[1:]


def norm_ts(s):
    if not s:
        return ""
    return s.replace("T", " ")[:19]


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--input", help="local dir of OPEN-data *.ndjson.gz to upload")
    g.add_argument("--data-location", help="s3:// prefix already holding the NDJSON(.gz)")
    ap.add_argument("--label", required=True)
    ap.add_argument("--bundles", nargs="+", required=True)
    ap.add_argument("--vs-cache")
    ap.add_argument("--filter", action="store_true")
    ap.add_argument("--lookback-hours", type=int, default=24)
    ap.add_argument("--lookahead-hours", type=int, default=1)
    ap.add_argument("--unit-identifier-system", default="http://mimic.mit.edu/fhir/mimic/identifier/encounter-hosp")
    ap.add_argument("--bucket", default="your-bucket")
    ap.add_argument("--workgroup", default="primary")
    ap.add_argument("--db", default="dqm_a4")
    ap.add_argument("--out", default=None)
    ap.add_argument("--compare-to", help="DuckDB manifest.csv from slice_stays.py (same parameters)")
    a = ap.parse_args()

    run_id = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = a.out or os.path.join("results", f"a4-athena-{a.label}-{run_id}")
    os.makedirs(out, exist_ok=True)
    loc = f"s3://{a.bucket}/dqm/prep/{a.label}-{run_id}"

    ident = aws("sts", "get-caller-identity")
    print(f"AWS identity: {ident['Arn']}  region: {os.environ.get('AWS_DEFAULT_REGION', '(CLI default)')}")
    wg = aws("athena", "get-work-group", "--work-group", a.workgroup)["WorkGroup"]
    cfg = wg.get("Configuration", {})
    engine = cfg.get("EngineVersion", {}).get("EffectiveEngineVersion", "?")
    if cfg.get("EnforceWorkGroupConfiguration"):
        sys.exit(f"workgroup {a.workgroup} enforces its settings: CTAS with external_location would fail. Use another workgroup.")
    if "3" not in engine:
        sys.exit(f"workgroup {a.workgroup} runs {engine}; engine v3 is required (any_match, from_iso8601_*).")
    out_loc = None if cfg.get("ResultConfiguration", {}).get("OutputLocation") else f"s3://{a.bucket}/dqm/athena-results/"
    print(f"workgroup {a.workgroup}: {engine}; query results -> {cfg.get('ResultConfiguration', {}).get('OutputLocation') or out_loc}")

    if a.input:
        data_loc = f"s3://{a.bucket}/dqm/open/{a.label}/"
        print(f"sync {a.input} -> {data_loc} (open data)")
        subprocess.run(["aws", "s3", "sync", a.input, data_loc, "--exclude", "*", "--include", "*.ndjson.gz",
                        "--only-show-errors"], check=True)
    else:
        data_loc = a.data_location.rstrip("/") + "/"

    codes, added = vs_codes_from(a.bundles, a.vs_cache) if a.filter else ([], [])
    tsv = os.path.join(out, "vs_codes.tsv")
    with open(tsv, "w") as f:
        for s, c in codes:
            f.write(f"{s}\t{c}\n")
    subprocess.run(["aws", "s3", "cp", tsv, f"{loc}/vs_codes/vs_codes.tsv", "--only-show-errors"], check=True)
    os.remove(tsv)   # VSAC-derived content: not kept in the results folder

    at = Athena(a.workgroup, out_loc)
    t0 = time.time()
    print(f"run prefix {loc}")
    at.run(f"CREATE DATABASE IF NOT EXISTS {a.db}", label=f"create database {a.db}")
    for t in TABLES:
        at.run(f"DROP TABLE IF EXISTS {a.db}.{t}", label=f"drop table (metadata only) {t}")
    params = dict(data_location=data_loc, loc=loc, unit_identifier_system=a.unit_identifier_system,
                  lookback_hours=a.lookback_hours, lookahead_hours=a.lookahead_hours,
                  filter_off="FALSE" if a.filter else "TRUE")
    for rel in ["athena/10_load_index.sql", "athena/generated/20_slice.sql", "athena/generated/30_filter_hash.sql"]:
        for s in sql_file(rel, **params):
            first = " ".join(s.split()[:4])
            at.run(s, db=a.db, label=f"{os.path.basename(rel)}: {first}")

    qid = at.run("""SELECT b.unit_id, b.patient_id, CAST(b.h_lo AS varchar), CAST(b.w_lo AS varchar), CAST(b.w_hi AS varchar),
                           coalesce(h.n_resources, 0), h.slice_sha256, h.content_sha256
                    FROM unit_bound b LEFT JOIN slice_hash h ON h.unit_id = b.unit_id ORDER BY b.unit_id""",
                 db=a.db, label="fetch per-stay hashes")
    _, rows = at.rows(qid)
    with open(os.path.join(out, "athena_manifest.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["unit_id", "patient_id", "hosp_start_lower_bound_utc", "window_start_utc", "window_end_utc",
                    "n_resources", "slice_sha256", "content_sha256"])
        w.writerows(rows)
    meta = {"label": a.label, "run_prefix": loc, "data_location": data_loc, "workgroup": a.workgroup, "engine": engine,
            "params": {k: v for k, v in params.items() if k not in ("loc", "data_location")},
            "filter_value_sets_from_vsac_cache": [u.split("/")[-1] for u in added],
            "queries": at.n, "data_scanned_mb": round(at.scanned / 1e6, 1),
            "engine_seconds": round(at.engine_ms / 1000, 1), "wall_seconds": round(time.time() - t0, 1),
            "stays": len(rows)}

    if a.compare_to:
        duck = {r["unit_id"]: r for r in csv.DictReader(open(a.compare_to))}
        ath = {r[0]: r for r in rows}
        keys = sorted(set(duck) | set(ath))
        c = {"stays_duckdb": len(duck), "stays_athena": len(ath), "only_duckdb": 0, "only_athena": 0,
             "same_window": 0, "same_n_resources": 0, "same_slice_sha256": 0, "same_content_sha256": 0}
        diffs = []
        for k in keys:
            if k not in ath:
                c["only_duckdb"] += 1; diffs.append([k, "only in DuckDB"]); continue
            if k not in duck:
                c["only_athena"] += 1; diffs.append([k, "only in Athena"]); continue
            d, x = duck[k], ath[k]
            win = all(norm_ts(d[f]) == norm_ts(v) for f, v in
                      zip(["hosp_start_lower_bound_utc", "window_start_utc", "window_end_utc"], x[2:5]))
            c["same_window"] += win
            c["same_n_resources"] += str(d["n_resources"]) == str(x[5])
            c["same_slice_sha256"] += d["slice_sha256"] == (x[6] or "")
            c["same_content_sha256"] += d.get("content_sha256", "") == (x[7] or "")
            if not (win and d["slice_sha256"] == (x[6] or "") and d.get("content_sha256", "") == (x[7] or "")):
                diffs.append([k, json.dumps({"duckdb": d, "athena": x})])
        c["identical"] = (c["only_duckdb"] == 0 and c["only_athena"] == 0 and
                          c["same_slice_sha256"] == c["same_content_sha256"] == c["same_window"] == len(keys))
        meta["backend_equivalence"] = c
        with open(os.path.join(out, "backend_differences.csv"), "w", newline="") as f:
            csv.writer(f).writerows([["unit_id", "detail"]] + diffs)

    json.dump(meta, open(os.path.join(out, "athena_run.json"), "w"), indent=2)
    print(json.dumps({k: meta[k] for k in ["stays", "queries", "data_scanned_mb", "engine_seconds", "wall_seconds"]
                      + (["backend_equivalence"] if a.compare_to else [])}, indent=1))
    print(f"results: {out}")
    if a.compare_to and not meta["backend_equivalence"]["identical"]:
        sys.exit(2)


if __name__ == "__main__":
    main()
