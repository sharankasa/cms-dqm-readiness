#!/usr/bin/env python3
"""
finish_sets.py — for engine runs started by launch_sets.sh (or with OUT=<set root>/runs/engine-sliced): when a
set's job is complete, run finish_job.sh (status, logs to S3, job deleted) and run_check.py (every stay once
per measure, no errors). Jobs still running are left alone; rerun later. S2 timing jobs are never touched.

  cd ".../project2-dqm-infrastructure"; export AWS_PROFILE=your-aws-profile
  python3 pipeline/readiness/finish_sets.py SCENARIO_STATUS_LOWER SCENARIO_STATUS_UPPER
  python3 pipeline/readiness/finish_sets.py --all-scenarios
"""
import glob
import json
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def env_sets():
    out = subprocess.run([sys.executable, os.path.join(ROOT, "pipeline", "readiness", "inputs_env.py")],
                         capture_output=True, text=True, check=True).stdout
    v = {}
    for line in out.splitlines():
        k, _, val = line[len("export "):].partition("=")
        v[k] = val
    return v


def main():
    args = sys.argv[1:]
    if not args:
        sys.exit(__doc__)
    v = env_sets()
    names = sorted({k[:-6] for k in v if k.startswith("SCENARIO_") and k.endswith("_INPUT")}) if args == ["--all-scenarios"] else args
    runs = {}
    for f in glob.glob(os.path.join(ROOT, "results", "eks-*", "run.json")):
        r = json.load(open(f))
        runs.setdefault(r.get("out", "").rstrip("/"), []).append((r.get("started_epoch", 0), r["job"]))
    pending = 0
    for k in names:
        root, table = v.get(f"{k}_ROOT"), v.get(f"{k}_TABLE")
        if not root:
            print(f"{k}: unknown set"); continue
        out = f"{root}/runs/engine-sliced"
        if out not in runs:
            print(f"{k}: no job started for {out}"); continue
        job = sorted(runs[out])[-1][1]
        local = os.path.join(ROOT, "results", f"eks-{job}")
        if os.path.exists(os.path.join(local, "job_status.json")) and os.path.exists(os.path.join(local, "wall.txt")):
            print(f"{k}: {job} finished earlier")
        else:
            st = subprocess.run(["kubectl", "get", "job", job, "-o", "jsonpath={.status.conditions[*].type}"],
                                capture_output=True, text=True)
            if st.returncode != 0:
                print(f"{k}: job {job} not on the cluster and not finished here: check by hand"); continue
            if "Complete" not in st.stdout and "Failed" not in st.stdout:
                print(f"{k}: {job} still running"); pending += 1; continue
            subprocess.run(["bash", os.path.join(ROOT, "pipeline", "worker", "eks", "finish_job.sh"), job], check=False)
        r = subprocess.run([sys.executable, os.path.join(ROOT, "pipeline", "readiness", "run_check.py"),
                            "--set-table", table, "--out", out], capture_output=True, text=True)
        verdict = [l for l in r.stdout.splitlines() if l.startswith(("COMPLETE", "NOT COMPLETE"))]
        print(f"{k}: {verdict[0] if verdict else 'check failed: ' + r.stderr[-300:]}")
        if r.returncode != 0:
            print("\n".join("   " + l for l in r.stdout.splitlines()[-6:]))
    if pending:
        print(f"{pending} still running: rerun this later")


if __name__ == "__main__":
    main()
