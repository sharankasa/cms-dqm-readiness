#!/usr/bin/env python3
"""
run_check.py — is an engine (or V) run on a readiness input complete? Every stay of the input set must
have exactly one result per measure, with no engine error. Athena; counts only (1-9 shown as <10);
nothing about populations or rates is printed.

  cd ".../project2-dqm-infrastructure"; export AWS_PROFILE=your-aws-profile
  python3 pipeline/readiness/run_check.py --set-table dqm_readiness.set_cohort_a_<stamp> \
      --out s3://your-bucket/dqm/readiness/<stamp>/cohort-a/runs/<job> [--mode sliced|v]

Exit 0 = complete (every stay once per measure, no errors, no broken lines); 2 = not complete.
"""
import argparse
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(ROOT, "pipeline", "prepare"))
from athena_run import Athena, aws  # noqa: E402

BUCKET = "your-bucket"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--set-table", required=True)
    ap.add_argument("--out", required=True, help="the job's OUT (results under <OUT>/results/)")
    ap.add_argument("--mode", default="sliced", choices=["sliced", "v"])
    ap.add_argument("--workgroup", default="primary")
    a = ap.parse_args()
    print(f"AWS identity: {aws('sts', 'get-caller-identity')['Arn']}")
    wg = aws("athena", "get-work-group", "--work-group", a.workgroup)["WorkGroup"]
    cfg = wg.get("Configuration", {})
    out_loc = None if cfg.get("ResultConfiguration", {}).get("OutputLocation") else f"s3://{BUCKET}/dqm/athena-results/"
    at = Athena(a.workgroup, out_loc)
    t = "chk_" + "".join(c if c.isalnum() else "_" for c in a.out.rstrip("/").rsplit("/", 1)[1])
    at.run(f"DROP TABLE IF EXISTS {t}", db="dqm_readiness", label=f"drop (metadata only) {t}")
    at.run(f"""CREATE EXTERNAL TABLE {t} (line string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\001'
               LINES TERMINATED BY '\\n' STORED AS TEXTFILE LOCATION '{a.out.rstrip('/')}/results/'""",
           db="dqm_readiness", label="results table")
    hdr, rows = at.rows(at.run(f"""
        WITH raw AS (SELECT try(json_parse(line)) AS j FROM {t} WHERE trim(line) <> ''),
        r AS (SELECT json_extract_scalar(j, '$.measure') AS measure, json_extract_scalar(j, '$.unit_id') AS unit_id,
                     json_extract_scalar(j, '$.error') AS err, json_extract_scalar(j, '$.batch_n') AS batch_n FROM raw WHERE j IS NOT NULL),
        m AS (SELECT DISTINCT measure FROM r),
        exp AS (SELECT m.measure, s.unit_id FROM {a.set_table} s CROSS JOIN m),
        per AS (SELECT measure, unit_id, count(*) AS n, count(err) AS errors, count(batch_n) AS batched FROM r GROUP BY 1, 2),
        extra AS (SELECT p.measure, count(*) AS k FROM per p LEFT JOIN {a.set_table} s ON s.unit_id = p.unit_id
                  WHERE s.unit_id IS NULL GROUP BY 1),
        broken AS (SELECT count(*) AS k FROM raw WHERE j IS NULL),
        main AS (SELECT e.measure, count(*) AS stays_expected, count(p.unit_id) AS stays_with_result,
                        count(*) - count(p.unit_id) AS stays_missing, count_if(p.n > 1) AS stays_duplicated,
                        count_if(p.errors > 0) AS stays_with_error, count_if(p.batched > 0) AS stays_batched
                 FROM exp e LEFT JOIN per p ON p.measure = e.measure AND p.unit_id = e.unit_id
                 GROUP BY e.measure)
        SELECT main.*, coalesce(x.k, 0) AS results_not_in_set, b.k AS broken_lines
        FROM main LEFT JOIN extra x ON x.measure = main.measure CROSS JOIN broken b
        ORDER BY main.measure""", db="dqm_readiness", label="completeness"))
    ok = bool(rows)
    print("     " + " | ".join(hdr))
    for r in rows:
        d = dict(zip(hdr, r))
        print("     " + " | ".join(("<10" if v and v.isdigit() and 1 <= int(v) <= 9 else v or "") for v in r))
        ok = ok and d["stays_missing"] == "0" and d["stays_duplicated"] == "0" and d["stays_with_error"] == "0" \
            and d["broken_lines"] == "0" and d["results_not_in_set"] == "0"
    n_measures = len(rows)
    if n_measures != 2:
        ok = False
        print(f"expected results for 2 measures, found {n_measures}")
    print("COMPLETE: every stay once per measure, no errors" if ok else "NOT COMPLETE (see counts above)")
    sys.exit(0 if ok else 2)


if __name__ == "__main__":
    main()
