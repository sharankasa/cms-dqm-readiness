#!/usr/bin/env python3
"""
rebalance_run.py — the equivalence run run plan: re-cut the S1 sample's worker input into shards by PREDICTED engine
time (heaviest patient-year groups first and alone), then print the run plan (wall time and cost by
cluster size). Scheduling only: the same 2,000 stays, the same full unfiltered records, the same
unmodified engine; the registered sample tables are not changed (new tables end in _bal).
Athena only (cents); row-level data stays in S3; only aggregates come back.

  python3 pipeline/s1/rebalance_run.py                         # latest S1 sample (results/s1-sample-*/run.json)
  python3 pipeline/s1/rebalance_run.py --plan-only results/s1-balance-<stamp> --measured 0=5.1,1=4.0,2=3.2

--plan-only recomputes the plan from a saved per-shard table (no AWS), optionally replacing predicted
shard hours with measured pod hours from the calibration run (pipeline/worker/eks/status_summary.py).
Model and rules: pipeline/s1/sql/70_balance.sql; export: 75_export_balanced.sql.
"""
import argparse
import csv
import datetime
import glob
import heapq
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(ROOT, "pipeline", "prepare"))
sys.path.insert(0, HERE)
from athena_run import Athena, aws  # noqa: E402
from sample_run import local_sql  # noqa: E402

TABLES = ["pilot_groups", "runtime_fit", "group_pred", "group_shard", "sample_stays_bal", "sample_patients_bal"]


def makespan(hours, pods):
    """Pods take shards in index order, the next pending index whenever one frees up (Indexed Job)."""
    free = [0.0] * pods
    heapq.heapify(free)
    end = 0.0
    for h in hours:
        t = heapq.heappop(free) + h
        end = max(end, t)
        heapq.heappush(free, t)
    return end


def plan(shard_hours, overhead_h, node_hour, pods_per_node, measured, nodes_list):
    hours = [measured.get(i, h + overhead_h) for i, h in enumerate(shard_hours)]
    rows = []
    for n in nodes_list:
        wall = makespan(hours, n * pods_per_node)
        rows.append({"nodes": n, "pods": n * pods_per_node, "wall_hours": round(wall, 2),
                     "node_hours": round(n * wall, 1), "cost_usd": round(wall * (n * node_hour + 0.10), 2)})
    return hours, rows


def print_plan(hours, rows, measured, node_hour):
    total = sum(hours)
    src = f"; measured: shards {', '.join(str(k) for k in sorted(measured))}" if measured else ""
    print(f"\nrun plan, reference side (both measures; {len(hours)} shards, {total:.1f} pod-hours in total; "
          f"longest shard {max(hours):.2f} h{src}):")
    print(f"  {'nodes':>5} {'pods':>5} {'wall h':>8} {'node-h':>8} {'est. USD':>9}")
    for r in rows:
        print(f"  {r['nodes']:>5} {r['pods']:>5} {r['wall_hours']:>8.2f} {r['node_hours']:>8.1f} {r['cost_usd']:>9.2f}")
    print(f"  (node ${node_hour}/h + EKS $0.10/h; wall time cannot go below the longest shard; sliced side and V add"
          f" well under an hour; cluster create/destroy ~25 min)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-prefix", help="S1 sample run prefix (default: latest results/s1-sample-*/run.json)")
    ap.add_argument("--db", default="dqm_s1")
    ap.add_argument("--pilot-db", default="dqm_pilot")
    ap.add_argument("--b4db")
    ap.add_argument("--shards", type=int, default=64)
    ap.add_argument("--alone", type=int, default=8, help="highest-work groups that always get a shard of their own")
    ap.add_argument("--fit-min-work", type=int, default=1000, help="fit the runtime model on pilot groups with work >= this")
    ap.add_argument("--fit-bins", type=int, default=10, help="equal-count work bins whose means the model is fitted to")
    ap.add_argument("--force", action="store_true", help="export even if the model check on the pilot fails")
    ap.add_argument("--node-hour-usd", type=float, default=0.1008, help="m7i.large on-demand, us-west-2")
    ap.add_argument("--pods-per-node", type=int, default=2)
    ap.add_argument("--pod-overhead-s", type=float, default=90, help="input download + engine start per pod")
    ap.add_argument("--nodes", default="3,4,6,8,12,16")
    ap.add_argument("--plan-only", help="saved balance folder: recompute the plan, no AWS")
    ap.add_argument("--measured", default="", help="shard=hours,... measured pod hours (calibration)")
    ap.add_argument("--bucket", default="your-bucket")
    ap.add_argument("--workgroup", default="primary")
    a = ap.parse_args()
    nodes_list = [int(x) for x in a.nodes.split(",")]
    measured = {int(k): float(v) for k, v in (p.split("=") for p in a.measured.split(",") if p)}
    overhead_h = a.pod_overhead_s / 3600

    if a.plan_only:
        rows = list(csv.DictReader(open(os.path.join(a.plan_only, "shards.csv"))))
        hours, plan_rows = plan([float(r["predicted_hours"]) for r in rows], overhead_h, a.node_hour_usd,
                                a.pods_per_node, measured, nodes_list)
        print_plan(hours, plan_rows, measured, a.node_hour_usd)
        return

    if not 2 <= a.shards <= 99:
        sys.exit("--shards must be 2..99")
    if a.run_prefix:
        loc, b4db = a.run_prefix.rstrip("/"), a.b4db or "dqm_prep"
    else:
        runs = sorted(glob.glob(os.path.join(ROOT, "results", "s1-sample-*", "run.json")))
        if not runs:
            sys.exit("no S1 sample run.json found: pass --run-prefix")
        meta = json.load(open(runs[-1]))
        loc, b4db = meta["run_prefix"].rstrip("/"), a.b4db or meta["prep_database"]
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    bal, xp = f"{loc}/balance-{stamp}", f"{loc}/export_bal-{stamp}"
    out = os.path.join(ROOT, "results", f"s1-balance-{stamp}")
    os.makedirs(out, exist_ok=True)

    print(f"AWS identity: {aws('sts', 'get-caller-identity')['Arn']}")
    wg = aws("athena", "get-work-group", "--work-group", a.workgroup)["WorkGroup"]
    cfg = wg.get("Configuration", {})
    if cfg.get("EnforceWorkGroupConfiguration"):
        sys.exit(f"workgroup {a.workgroup} enforces its settings")
    out_loc = None if cfg.get("ResultConfiguration", {}).get("OutputLocation") else f"s3://{a.bucket}/dqm/athena-results/"
    print(f"S1 sample {loc} (database {a.db}); pilot timings from {a.pilot_db}; {a.shards} shards; new input {xp}")
    at = Athena(a.workgroup, out_loc)
    t0 = time.time()
    run = lambda sql, label: at.run(sql, db=a.db, label=label)   # noqa: E731
    for t in TABLES:
        run(f"DROP TABLE IF EXISTS {t}", f"drop table (metadata only) {t}")
    parts = local_sql("70_balance.sql", bal=bal, pilot_db=a.pilot_db, shards=a.shards, alone=a.alone,
                      fit_min_work=a.fit_min_work, fit_bins=a.fit_bins)
    for part, label in [("part fit", "runtime model (pilot)"), ("part groups", "predicted time per group"),
                        ("part assign", "shards")]:
        for s in parts[part]:
            run(s, f"{label}: " + " ".join(s.split()[:3]))

    res = {}
    for i, s in enumerate(parts["part summary"]):
        hdr, rows = at.rows(run(s, f"summary {i + 1}"))
        res[i] = [dict(zip(hdr, r)) for r in rows]
    chk, fit, shards, pilot_chk, s1_bands = res[0][0], res[1][0], res[2], res[3], res[4]
    print("\nchecks: " + ", ".join(f"{k} {v}" for k, v in chk.items()))
    ok = (chk["stays_registered"] == chk["stays_balanced"] == chk["distinct_stays_balanced"]
          and chk["groups"] == chk["groups_assigned"] and chk["shards"] == chk["max_shard_plus_one"])
    if not ok:
        sys.exit("CHECK FAILED: every registered stay must be in exactly one shard, shards numbered without gaps. "
                 "Nothing exported.")
    print(f"runtime model (pilot, both measures): mean seconds = {fit['smear']} x exp({fit['a']}) x work^{fit['k']} "
          f"for work >= {a.fit_min_work} ({fit['n_fit']} groups in {fit['n_bins']} bins); {fit['base1_seconds']} s for 0 < work < {a.fit_min_work} "
          f"({fit['n_base1']} groups); {fit['base0_seconds']} s for work = 0 ({fit['n_base0']} groups)")
    show = lambda title, rows: (print(title), [print("   " + " | ".join("" if v is None else str(v) for v in r.values()))  # noqa: E731
                                                for r in [dict((k, k) for k in rows[0])] + rows])
    show("model check on the pilot (observed vs predicted):", pilot_chk)
    show("S1 sample, predicted time per work band:", s1_bands)
    bad = []
    for r in pilot_chk:
        if r["groups"] != "<10" and r["work_band"] >= "2" and r["observed_total_h"] and float(r["observed_total_h"]) > 0:
            ratio = float(r["predicted_total_h"] or 0) / float(r["observed_total_h"])
            if not 0.5 <= ratio <= 2.0:
                bad.append(f"{r['work_band']}: predicted/observed {ratio:.2f}")
    if bad:
        print("MODEL CHECK FAILED (pilot bands at or above 1k work, predicted total off by more than 2x): " + "; ".join(bad))
        if not a.force:
            sys.exit("Nothing exported. Adjust --fit-min-work, or --force to export anyway.")
    else:
        print("model check on the pilot: OK (bands at or above 1k work within 2x of observed totals)")
    with open(os.path.join(out, "shards.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["shard", "groups", "stays", "predicted_hours"])
        w.writeheader()
        w.writerows(shards)

    for i, s in enumerate(local_sql("75_export_balanced.sql", xp=xp, b4db=b4db)["all"]):
        run(s, f"UNLOAD {['manifest', 'slices', 'shared', 'reference', 'shared_all'][i]}")
    n = int(chk["shards"])
    for sub in ["manifest", "reference", "slices"]:
        r = subprocess.run(["aws", "s3", "ls", f"{xp}/{sub}/"], capture_output=True, text=True)
        got = sum(1 for line in r.stdout.splitlines() if "PRE shard=" in line)
        print(f"  {sub}: {got} shard folders")
        if sub != "slices" and got != n:
            sys.exit(f"CHECK FAILED: {sub} has {got} shard folders, expected {n}")

    hours, plan_rows = plan([float(r["predicted_hours"]) for r in shards], overhead_h, a.node_hour_usd,
                            a.pods_per_node, {}, nodes_list)
    nh = int(chk["own_shard_groups"])
    print(f"\n{nh} patient-year groups have a shard of their own (the {a.alone} highest-work groups plus any predicted "
          f"to need an even share; shards 0..{nh - 1}, heaviest first); the rest fill shards {nh}..{n - 1} evenly.")
    print("calibration candidates (shards 0, 1, 2): " +
          ", ".join(f"shard {r['shard']}: {r['groups']} group(s), predicted {float(r['predicted_hours']):.2f} h" for r in shards[:3]))
    print_plan(hours, plan_rows, {}, a.node_hour_usd)
    meta = {"stamp": stamp, "run_prefix": loc, "worker_input": xp, "balance_tables": bal, "database": a.db,
            "pilot_db": a.pilot_db, "shards": n, "own_shard_groups": chk["own_shard_groups"], "fit": fit, "alone": a.alone, "fit_min_work": a.fit_min_work, "fit_bins": a.fit_bins, "pilot_check": pilot_chk, "s1_bands": s1_bands,
            "plan": plan_rows, "pod_overhead_s": a.pod_overhead_s, "node_hour_usd": a.node_hour_usd,
            "queries": at.n, "data_scanned_mb": round(at.scanned / 1e6, 1), "wall_seconds": round(time.time() - t0, 1)}
    json.dump(meta, open(os.path.join(out, "balance.json"), "w"), indent=2)
    print(f"\nworker input: {xp}\nAthena: {at.n} queries, {at.scanned / 1e9:.1f} GB scanned (~${at.scanned / 1e12 * 5:.2f})")
    print(f"saved: {out}")


if __name__ == "__main__":
    main()
