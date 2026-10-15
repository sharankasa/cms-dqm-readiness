#!/usr/bin/env python3
"""
s2_summary.py — S2 results (registration §4.3.1 and §6 "S2") from the runs recorded by s2_sequence.sh, plus
optional extra runs (the full run, the batching check). No patient data leaves AWS: per-pod status files
(times, memory, CPU seconds, node names) are read from S3; per-stay engine times are summarised in Athena
(percentiles, shares, regression coefficients); counts 1-9 are shown as <10.

  cd ".../project2-dqm-infrastructure"; export AWS_PROFILE=your-aws-profile
  python3 pipeline/s2/s2_summary.py --plan results/s2-timing \
      --set-table dqm_readiness.set_s1_sample_<stamp> \
      [--extra full=<job name>] [--extra batched-sample=<job name>]

What it reports:
  runs        per run: whole-job wall time (the Job's own start -> completion), pods, pod-hours, CPU seconds,
              CPU use per node (pod CPU seconds on the node / (wall x 2 vCPU)), peak pod memory, node boot time
              before cold runs (separate), instance-hours, cost = instance-hours x on-demand price, EKS fee as
              its own line
  conditions  cold and warm repeats: mean wall with 95% t-interval, p95 across repeats, CV (extension rule)
  scaling     1/2/4/8 nodes (cold): wall, speed-up vs 1 node, efficiency, cost
  per stay    engine seconds per stay and measure for each run: p50/p95/p99/max, share of total time in the
              top 1% of stays; on the timing sample also for the random 1,500 alone (population estimate,
              registration §4.3: the purposely chosen stays must not bias it)
  workload    ln(seconds) on ln(1 + work) per measure (work metric from the prepare stage), fitted on 70% of
              stays (hash split), R^2 on the fit and on the 30% held out, median absolute % error held out
  stages      prepare (the prepare stage run on the full data: Athena wall, engine time, data scanned), evaluate (the
              runs), aggregate (one Athena aggregation over a run's results: time and data scanned)
  storage     S3 bytes and objects of the input and of each run's output
Writes results/s2-summary-<stamp>/ (CSV + summary.json).
"""
import argparse
import csv
import datetime
import glob
import json
import math
import os
import statistics
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(ROOT, "pipeline", "prepare"))
from athena_run import Athena, aws  # noqa: E402

BUCKET = "your-bucket"
EKS_FEE = 0.10
VCPU = 2
T975 = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228}


def mask(n):
    return "<10" if n is not None and 1 <= int(n) <= 9 else str(n)


def s3_json_lines(prefix):
    """Latest status file per <mode>_<shard> under <OUT>/status/ (pods write one when they finish)."""
    bucket, key = prefix[5:].split("/", 1)
    keys, token = [], None
    while True:
        args = ["s3api", "list-objects-v2", "--bucket", bucket, "--prefix", key.rstrip("/") + "/status/", "--output", "json"]
        if token:
            args += ["--continuation-token", token]
        r = subprocess.run(["aws", *args], capture_output=True, text=True)
        if r.returncode != 0:
            sys.exit(f"aws s3api list-objects-v2 failed: {r.stderr[:300]}")
        page = json.loads(r.stdout or "{}")
        keys += [o["Key"] for o in page.get("Contents", [])]
        token = page.get("NextContinuationToken")
        if not token:
            break
    latest = {}
    for k in keys:
        name = k.rsplit("/", 1)[1]
        if name.endswith(".meta.json"):
            continue
        ms, stamp = name.rsplit(".", 2)[0], name.rsplit(".", 2)[1]
        if ms not in latest or int(stamp) > int(latest[ms][0]):
            latest[ms] = (stamp, k)
    out = []
    for _, k in latest.values():
        r = subprocess.run(["aws", "s3", "cp", f"s3://{bucket}/{k}", "-"], capture_output=True, text=True)
        if r.returncode == 0:
            out.append(json.loads(r.stdout))
    return out


def s3_size(prefix):
    r = subprocess.run(["aws", "s3", "ls", prefix.rstrip("/") + "/", "--recursive", "--summarize"], capture_output=True, text=True)
    objs = size = 0
    for line in r.stdout.splitlines():
        if "Total Objects:" in line:
            objs = int(line.split(":")[1])
        if "Total Size:" in line:
            size = int(line.split(":")[1])
    return objs, size


def iso(t):
    return datetime.datetime.fromisoformat(t.replace("Z", "+00:00")) if t else None


def load_extra(label, job):
    d = os.path.join(ROOT, "results", f"eks-{job}")
    run = json.load(open(os.path.join(d, "run.json")))
    st = json.load(open(os.path.join(d, "job_status.json"))).get("status", {})
    w = open(os.path.join(d, "wall.txt")).read().split(":")[1].strip() if os.path.exists(os.path.join(d, "wall.txt")) else None
    return {"step": label, "kind": "extra", "nodes": run.get("nodes"), "job": job, "out": run["out"], "input": run["input"],
            "image": run["image"], "instance_type": run.get("instance_type"), "parallel": run["parallel"], "batch": run.get("batch", 1),
            "node_hour_usd": run.get("node_hour_usd"), "start": st.get("startTime"), "completion": st.get("completionTime"),
            "wall_seconds": int(w) if w and w != "None" else None, "succeeded": st.get("succeeded", 0), "failed": st.get("failed", 0)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", help="s2_sequence.sh plan folder (runs.jsonl, nodes.jsonl)")
    ap.add_argument("--extra", action="append", default=[], help="label=<job name> of another run (full run, batching check)")
    ap.add_argument("--set-table", help="dqm_readiness.set_* table of the timing input (work metrics, sample reasons)")
    ap.add_argument("--prepare-run", default="results/prep-mimic-fhir-2.1-med-a-final-20261014T025603Z/run.json")
    ap.add_argument("--node-hour-usd", type=float, default=0.1008)
    ap.add_argument("--athena-usd-per-tb", type=float, default=5.0)
    ap.add_argument("--workgroup", default="primary")
    a = ap.parse_args()
    runs, nodes_log = [], []
    if a.plan:
        runs = [json.loads(l) for l in open(os.path.join(a.plan, "runs.jsonl")) if l.strip()]
        if os.path.exists(os.path.join(a.plan, "nodes.jsonl")):
            nodes_log = [json.loads(l) for l in open(os.path.join(a.plan, "nodes.jsonl")) if l.strip()]
    for e in a.extra:
        label, job = e.split("=", 1)
        runs.append(load_extra(label, job))
    if not runs:
        sys.exit("no runs: pass --plan and/or --extra")
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = os.path.join(ROOT, "results", f"s2-summary-{stamp}")
    os.makedirs(out, exist_ok=True)
    print(f"AWS identity: {aws('sts', 'get-caller-identity')['Arn']}")

    # ---- per run: pods, CPU, memory, cost
    rows = []
    for r in runs:
        st = s3_json_lines(r["out"])
        wall = r.get("wall_seconds") or 0
        price = r.get("node_hour_usd") or a.node_hour_usd
        per_node = {}
        for s in st:
            per_node.setdefault(s.get("node", "?"), 0.0)
            per_node[s.get("node", "?")] += s.get("cpu_seconds") or 0.0
        util = [c / (wall * VCPU) for c in per_node.values()] if wall else []
        boot = None
        if r["kind"] == "cold" and nodes_log:
            prior = [n for n in nodes_log if n["to"] > 0 and r.get("start") and n["utc"] <= r["start"].replace("+00:00", "Z")]
            boot = prior[-1]["seconds"] if prior else None
        inst_h = (r.get("nodes") or 0) * wall / 3600
        row = {"step": r["step"], "kind": r["kind"], "nodes": r.get("nodes"), "parallel": r.get("parallel"), "batch": r.get("batch", 1),
               "wall_seconds": wall, "node_boot_seconds_before": boot, "pods": len(st),
               "pods_failed_exit": sum(1 for s in st if s.get("exit_code") not in (0, None)),
               "pod_hours": round(sum(s.get("wall_seconds", 0) for s in st) / 3600, 3),
               "cpu_hours": round(sum(s.get("cpu_seconds") or 0 for s in st) / 3600, 3),
               "input_seconds_mean": round(statistics.mean([s.get("input_ready_seconds", 0) for s in st]), 1) if st else None,
               "cpu_use_per_node_mean": round(statistics.mean(util), 3) if util else None,
               "cpu_use_per_node_min": round(min(util), 3) if util else None,
               "cpu_use_per_node_max": round(max(util), 3) if util else None,
               "memory_peak_mib_max": max([s.get("memory_peak_mib") or 0 for s in st], default=None),
               "result_lines": sum(s.get("result_lines", 0) for s in st),
               "instance_hours": round(inst_h, 3), "cost_nodes_usd": round(inst_h * price, 3),
               "cost_eks_fee_usd": round(wall / 3600 * EKS_FEE, 3), "instance_type": r.get("instance_type"),
               "out": r["out"], "job": r["job"]}
        rows.append(row)
    with open(os.path.join(out, "runs.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print(f"\n{'run':<16} {'kind':<7} {'nodes':>5} {'wall s':>7} {'boot s':>6} {'pods':>4} {'pod-h':>6} {'cpu-h':>6} "
          f"{'cpu/node':>8} {'mem MiB':>7} {'$ nodes':>7} {'$ EKS':>6}")
    for x in rows:
        print(f"{x['step']:<16} {x['kind']:<7} {x['nodes']!s:>5} {x['wall_seconds']:>7} {x['node_boot_seconds_before'] or '':>6} "
              f"{x['pods']:>4} {x['pod_hours']:>6} {x['cpu_hours']:>6} {x['cpu_use_per_node_mean'] or '':>8} "
              f"{x['memory_peak_mib_max'] or '':>7} {x['cost_nodes_usd']:>7} {x['cost_eks_fee_usd']:>6}")

    # ---- repeats: mean with 95% t-interval, p95, CV
    cond = {}
    for kind in ("cold", "warm"):
        w = [x["wall_seconds"] for x in rows if x["kind"] == kind and x["step"].startswith(kind + "-") and x["wall_seconds"]]
        if len(w) >= 2:
            m, sd = statistics.mean(w), statistics.stdev(w)
            h = T975.get(len(w) - 1, 1.96) * sd / math.sqrt(len(w))
            p95 = sorted(w)[max(0, math.ceil(0.95 * len(w)) - 1)]
            cond[kind] = {"repeats": len(w), "mean_s": round(m, 1), "ci95_low_s": round(m - h, 1), "ci95_high_s": round(m + h, 1),
                          "p95_s": p95, "cv": round(sd / m, 4), "extension_rule_triggered": sd / m > 0.10}
    if cond:
        print("\nrepeats (whole-job wall seconds):")
        for k, v in cond.items():
            print(f"  {k}: n={v['repeats']}, mean {v['mean_s']} s (95% CI {v['ci95_low_s']}-{v['ci95_high_s']}), p95 {v['p95_s']} s, "
                  f"CV {v['cv']:.1%}{' -> extension rule' if v['extension_rule_triggered'] else ''}")

    # ---- scaling (cold at 1/2/4/8 nodes; 8 = cold-1)
    sc = {}
    for x in rows:
        if x["step"].startswith("scale-") or x["step"] == "cold-1":
            sc[x["nodes"]] = x
    scaling = []
    if sc:
        base = sc.get(1)
        for n in sorted(sc):
            x = sc[n]
            sp = round(base["wall_seconds"] / x["wall_seconds"], 2) if base and x["wall_seconds"] else None
            scaling.append({"nodes": n, "wall_seconds": x["wall_seconds"], "speedup_vs_1": sp,
                            "efficiency": round(sp / n, 2) if sp else None, "cost_nodes_usd": x["cost_nodes_usd"],
                            "cost_eks_fee_usd": x["cost_eks_fee_usd"], "run": x["step"]})
        print("\nscaling (cold):")
        for s in scaling:
            print(f"  {s['nodes']:>2} nodes: {s['wall_seconds']} s, speed-up {s['speedup_vs_1']}, efficiency {s['efficiency']}, "
                  f"${s['cost_nodes_usd']} nodes + ${s['cost_eks_fee_usd']} EKS")

    # ---- Athena: per-stay seconds, workload regression, aggregation stage
    wg = aws("athena", "get-work-group", "--work-group", a.workgroup)["WorkGroup"]
    cfg = wg.get("Configuration", {})
    out_loc = None if cfg.get("ResultConfiguration", {}).get("OutputLocation") else f"s3://{BUCKET}/dqm/athena-results/"
    at = Athena(a.workgroup, out_loc)
    db = "dqm_readiness"
    run = lambda sql, label: at.run(sql, db=db, label=label)   # noqa: E731

    def q(sql, label, fname=None):
        hdr, rr = at.rows(run(sql, label))
        if fname:
            with open(os.path.join(out, fname), "w", newline="") as f:
                csv.writer(f).writerows([hdr] + rr)
        return [dict(zip(hdr, r_)) for r_ in rr]

    tables = {}
    for x in rows:
        t = "s2res_" + "".join(ch if ch.isalnum() else "_" for ch in x["job"])
        run(f"DROP TABLE IF EXISTS {t}", f"drop (metadata only) {t}")
        run(f"""CREATE EXTERNAL TABLE {t} (line string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\001'
                LINES TERMINATED BY '\\n' STORED AS TEXTFILE LOCATION '{x['out'].rstrip('/')}/results/'""", f"results of {x['step']}")
        tables[x["step"]] = t
    per = lambda t: f"""SELECT json_extract_scalar(j, '$.measure') AS measure, json_extract_scalar(j, '$.unit_id') AS unit_id,
                               max(try(CAST(json_extract_scalar(j, '$.seconds') AS double))) AS s,
                               max(try(CAST(json_extract_scalar(j, '$.batch_n') AS integer))) AS batch_n
                        FROM (SELECT try(json_parse(line)) AS j FROM {t} WHERE trim(line) <> '') WHERE j IS NOT NULL
                        GROUP BY 1, 2"""  # noqa: E731
    stats_sql = lambda src, extra_join="", extra_where="": f"""
        WITH p AS ({src}), x AS (SELECT p.* FROM p {extra_join} WHERE p.s IS NOT NULL {extra_where}),
        r AS (SELECT measure, s, row_number() OVER (PARTITION BY measure ORDER BY s DESC) AS rk, count(*) OVER (PARTITION BY measure) AS n,
                     sum(s) OVER (PARTITION BY measure) AS tot FROM x)
        SELECT measure, max(n) AS stays, round(approx_percentile(s, 0.5), 3) AS p50_s, round(approx_percentile(s, 0.95), 2) AS p95_s,
               round(approx_percentile(s, 0.99), 1) AS p99_s, round(max(s), 1) AS max_s, round(max(tot), 0) AS total_s,
               round(sum(IF(rk <= greatest(1, CAST(ceil(n * 0.01) AS integer)), s, 0)) / max(tot), 3) AS top1pct_share
        FROM r GROUP BY measure ORDER BY measure"""  # noqa: E731
    per_stay = {}
    for step, t in tables.items():
        per_stay[step] = q(stats_sql(per(t)), f"per-stay seconds: {step}", f"per_stay_{step}.csv")
        if a.set_table and (step.startswith("cold") or step.startswith("warm") or step.startswith("scale")):
            per_stay[step + " (random 1,500)"] = q(stats_sql(per(t), f"JOIN {a.set_table} st ON st.unit_id = p.unit_id",
                                                             "AND st.reason = 'random'"), f"per-stay seconds, random: {step}",
                                                   f"per_stay_{step}_random.csv")
    print("\nper-stay engine seconds (p50 / p95 / p99 / max; share of time in the top 1% of stays):")
    for k, v in per_stay.items():
        for m in v:
            print(f"  {k:<26} {m['measure'][:6]} n={mask(m['stays'])}: {m['p50_s']} / {m['p95_s']} / {m['p99_s']} / {m['max_s']}; "
                  f"top 1% {m['top1pct_share']}")

    reg = []
    timing = [t for s_, t in tables.items() if s_.startswith(("cold-", "warm-"))]
    if a.set_table and timing:
        union = " UNION ALL ".join(per(t) for t in timing)
        reg = q(f"""
          WITH p AS (SELECT measure, unit_id, avg(s) AS s FROM ({union}) GROUP BY 1, 2),
          x AS (SELECT p.measure, p.s, ln(p.s) AS y,
                       ln(1 + CAST(IF(p.measure LIKE 'CMS816%', st.work_cms816, st.work_cms871) AS double)) AS lx,
                       mod(abs(from_big_endian_64(xxhash64(to_utf8(p.unit_id || '|s2-holdout')))), 10) < 7 AS fit
                FROM p JOIN {a.set_table} st ON st.unit_id = p.unit_id WHERE p.s > 0),
          f AS (SELECT measure, regr_slope(y, lx) AS b, regr_intercept(y, lx) AS a0, power(corr(y, lx), 2) AS r2_fit,
                       count(*) AS n_fit FROM x WHERE fit GROUP BY measure),
          h AS (SELECT x.measure, x.y, f.a0 + f.b * x.lx AS yhat, x.s, exp(f.a0 + f.b * x.lx) AS shat
                FROM x JOIN f ON f.measure = x.measure WHERE NOT x.fit),
          hm AS (SELECT measure, avg(y) AS ybar FROM h GROUP BY measure)
          SELECT f.measure, f.n_fit, count(*) AS n_holdout, round(f.b, 3) AS slope, round(f.a0, 3) AS intercept,
                 round(f.r2_fit, 3) AS r2_fit,
                 round(1 - sum(power(h.y - h.yhat, 2)) / sum(power(h.y - hm.ybar, 2)), 3) AS r2_holdout_log,
                 round(approx_percentile(abs(h.shat - h.s) / h.s, 0.5), 3) AS median_abs_pct_error_holdout
          FROM f JOIN h ON h.measure = f.measure JOIN hm ON hm.measure = f.measure
          GROUP BY f.measure, f.n_fit, f.b, f.a0, f.r2_fit ORDER BY f.measure""", "workload regression", "regression.csv")
        print("\nworkload regression, ln(seconds) on ln(1 + work), timing runs averaged per stay (70% fit / 30% held out):")
        for r_ in reg:
            print(f"  {r_['measure'][:6]}: slope {r_['slope']}, R2 fit {r_['r2_fit']}, R2 held out {r_['r2_holdout_log']}, "
                  f"median abs % error held out {r_['median_abs_pct_error_holdout']} (n fit {r_['n_fit']}, held out {r_['n_holdout']})")

    # aggregation stage: one Athena pass from per-stay results to population totals per measure (timed; not printed)
    agg = {}
    first = next(iter(tables.items()))
    before_ms, before_b = at.engine_ms, at.scanned
    q(f"""SELECT json_extract_scalar(j, '$.measure') AS measure, k AS population, count_if(v) AS stays
          FROM (SELECT try(json_parse(line)) AS j FROM {first[1]} WHERE trim(line) <> '')
          CROSS JOIN UNNEST(try(CAST(json_extract(j, '$.pops') AS MAP(VARCHAR, BOOLEAN)))) AS u(k, v)
          GROUP BY 1, 2""", f"aggregate stage on {first[0]} (timed only)")
    agg = {"run": first[0], "engine_seconds": round((at.engine_ms - before_ms) / 1000, 2),
           "data_scanned_mb": round((at.scanned - before_b) / 1e6, 2)}
    print(f"\naggregate stage ({first[0]}): {agg['engine_seconds']} s Athena engine time, {agg['data_scanned_mb']} MB scanned")

    prep = {}
    if a.prepare_run and os.path.exists(os.path.join(ROOT, a.prepare_run)):
        pr = json.load(open(os.path.join(ROOT, a.prepare_run)))
        prep = {"run": a.prepare_run, "wall_seconds": pr.get("wall_seconds"), "engine_seconds": pr.get("engine_seconds"),
                "data_scanned_mb": pr.get("data_scanned_mb"), "queries": pr.get("queries"),
                "athena_usd": round((pr.get("data_scanned_mb") or 0) / 1e6 * a.athena_usd_per_tb, 3)}
        print(f"prepare stage (full data, the prepare stage on Athena): {prep['wall_seconds']} s wall, {prep['engine_seconds']} s engine, "
              f"{prep['data_scanned_mb']} MB scanned (~${prep['athena_usd']})")

    storage = {}
    if rows:
        o, s = s3_size(runs[0]["input"])
        storage["timing_input"] = {"objects": o, "bytes": s}
        for x in rows:
            o, s = s3_size(x["out"])
            storage[x["step"]] = {"objects": o, "bytes": s}
        print(f"S3: timing input {storage['timing_input']['bytes'] / 1e6:.1f} MB in {storage['timing_input']['objects']} objects; "
              f"outputs {sum(v['bytes'] for k, v in storage.items() if k != 'timing_input') / 1e6:.1f} MB")

    meta = {"stamp": stamp, "plan": a.plan, "runs": rows, "conditions": cond, "scaling": scaling, "per_stay": per_stay,
            "regression": reg, "aggregate_stage": agg, "prepare_stage": prep, "storage": storage,
            "athena": {"queries": at.n, "data_scanned_mb": round(at.scanned / 1e6, 1)},
            "prices": {"node_hour_usd": a.node_hour_usd, "eks_fee_usd_per_hour": EKS_FEE, "athena_usd_per_tb": a.athena_usd_per_tb}}
    json.dump(meta, open(os.path.join(out, "summary.json"), "w"), indent=2)
    print(f"\nsaved (no patient data): {out}")


if __name__ == "__main__":
    main()
