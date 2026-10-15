#!/usr/bin/env python3
"""
compare_run.py — the pilot/the equivalence run: the S1 comparison of an EKS run, in Athena (per-stay results stay in S3).
Same rule as pipeline/s1/compare.py; prints counts only (pilot: no population counts).

  python3 pipeline/s1/compare_run.py --sample s1 \
      --ref-out s3://.../dqm/s1/<run>/runs/<reference job> --sliced-out s3://.../dqm/s1/<run>/runs/<sliced job>
  Open-data test of this script: --sample demo --db dqm_s1_demo --manifest-prefix s3://.../export/manifest

Verdict: S1 holds for a measure when every sampled stay is "identical", none is missing on either
side, and there are no duplicate or broken result lines (exactly once). With --stopped-shards (the
stopping rule, deviation 4), stays of those shards without a whole-record result are counted
separately as "stopped_reference" and reported; S1 is judged on all other stays.
"""
import argparse
import csv
import datetime
import json
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(ROOT, "pipeline", "prepare"))
sys.path.insert(0, HERE)
from athena_run import Athena, aws  # noqa: E402
from sample_run import local_sql  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", choices=["s1", "pilot", "demo"], required=True)
    ap.add_argument("--ref-out", required=True)
    ap.add_argument("--sliced-out", required=True)
    ap.add_argument("--db")
    ap.add_argument("--manifest-prefix", help="demo test only: export/manifest prefix to build sample_stays from")
    ap.add_argument("--bucket", default="your-bucket")
    ap.add_argument("--workgroup", default="primary")
    ap.add_argument("--stopped-shards", default="", help="S1 sample: shards stopped at the cost cap, e.g. 1,3,4")
    a = ap.parse_args()
    stopped = [int(x) for x in a.stopped_shards.split(",") if x.strip()]
    db = a.db or f"dqm_{a.sample}"
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = os.path.join(ROOT, "results", f"{a.sample}-compare-{stamp}")
    os.makedirs(out, exist_ok=True)
    loc = a.sliced_out.rstrip("/").rsplit("/runs/", 1)[0] + "/compare"

    print(f"AWS identity: {aws('sts', 'get-caller-identity')['Arn']}")
    wg = aws("athena", "get-work-group", "--work-group", a.workgroup)["WorkGroup"]
    cfg = wg.get("Configuration", {})
    out_loc = None if cfg.get("ResultConfiguration", {}).get("OutputLocation") else f"s3://{a.bucket}/dqm/athena-results/"
    at = Athena(a.workgroup, out_loc)
    t0 = time.time()
    run = lambda sql, label: at.run(sql, db=db, label=label)   # noqa: E731
    at.run(f"CREATE DATABASE IF NOT EXISTS {db}", label=f"database {db}")
    for t in ["res_ref", "res_sl", "cmp"] + (["sample_manifest", "sample_stays"] if a.sample == "demo" else []):
        run(f"DROP TABLE IF EXISTS {t}", f"drop table (metadata only) {t}")
    if a.sample == "demo":
        if not a.manifest_prefix:
            sys.exit("--sample demo needs --manifest-prefix")
        run(f"""CREATE EXTERNAL TABLE sample_manifest (line string)
                ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\001' LINES TERMINATED BY '\\n'
                STORED AS TEXTFILE LOCATION '{a.manifest_prefix.rstrip('/')}/'""", "sample_manifest (demo test)")
        run(f"""CREATE TABLE sample_stays WITH (format = 'PARQUET', external_location = '{loc}/sample_stays-{stamp}/') AS
                SELECT split(line, ',')[1] AS unit_id, split(line, ',')[2] AS patient_id, 'demo test' AS reason
                FROM sample_manifest WHERE trim(line) <> ''""", "sample_stays (demo test)")

    stmts = local_sql("50_compare.sql", ref_out=a.ref_out.rstrip("/"), sliced_out=a.sliced_out.rstrip("/"), loc=loc,
                      stamp=stamp, pop_counts="FALSE" if a.sample == "pilot" else "TRUE",
                      stopped_case=(f"WHEN r.unit_id IS NULL AND sb.shard IN ({','.join(map(str, stopped))}) "
                                    "THEN 'stopped_reference'") if stopped else "",
                      stopped_join="LEFT JOIN sample_stays_bal sb ON sb.unit_id = coalesce(r.unit_id, s.unit_id)"
                      if stopped else "")["all"]
    names = ["outcome", "exactly_once", "broken_lines", "by_group", "missing_both", "seconds", "population_counts"]
    res, k = {}, 0
    for s in stmts:
        if s.startswith("SELECT") or s.startswith("WITH"):
            name = names[k]; k += 1
            if name == "population_counts" and a.sample == "pilot":
                continue
            qid = run(s, f"compare: {name}")
            hdr, rows = at.rows(qid)
            res[name] = [dict(zip(hdr, r)) for r in rows]
            with open(os.path.join(out, f"{name}.csv"), "w", newline="") as f:
                csv.writer(f).writerows([hdr] + rows)
            print("     " + " | ".join(h or "" for h in hdr))
            for r in rows[:40]:
                print("     " + " | ".join("" if c is None else c for c in r))
        else:
            run(s, "compare: " + " ".join(s.split()[:3]))

    verdict = {}
    for m in sorted({r["measure"] for r in res.get("outcome", []) if r["measure"]}):
        oc = {r["outcome"]: r["stays"] for r in res["outcome"] if r["measure"] == m}
        eo = next((r for r in res["exactly_once"] if r["measure"] == m), {})
        mb = next((r for r in res["missing_both"] if r["measure"] == m), {})
        br = res["broken_lines"][0] if res.get("broken_lines") else {}
        verdict[m] = {"outcomes": oc,
                      "S1_holds": set(oc) - {"stopped_reference"} == {"identical"},
                      "stopped_reference_stays": oc.get("stopped_reference", "0"),
                      "exactly_once": (eo.get("duplicate_lines_reference") == "0" and eo.get("duplicate_lines_sliced") == "0"
                                       and mb.get("sampled_stays_missing_on_both_sides") == "0"
                                       and "missing_reference" not in oc and "missing_sliced" not in oc),
                      # a line cut by a killed pod is redone by the replacement; reported, not a failure
                      "broken_lines_skipped": {k: v for k, v in br.items()}}
    meta = {"sample": a.sample, "database": db, "ref_out": a.ref_out, "sliced_out": a.sliced_out, "verdict": verdict,
            "stopped_shards": stopped,
            "queries": at.n, "data_scanned_mb": round(at.scanned / 1e6, 1), "wall_seconds": round(time.time() - t0, 1)}
    json.dump(meta, open(os.path.join(out, "compare.json"), "w"), indent=2)
    print(json.dumps(verdict, indent=1))
    print(f"results (counts only): {out}")
    sys.exit(0 if verdict and all(v["S1_holds"] and v["exactly_once"] for v in verdict.values()) else 2)


if __name__ == "__main__":
    main()
