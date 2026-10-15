#!/usr/bin/env python3
"""
raise_watchdog_limits.py — change the running cluster watchdog's cost cap and maximum age without a Terraform
apply (an apply would also reset the node groups' sizes, which the S2 sequence changes during a run).
Reads the watchdog Lambda's environment, prints the current estimate from its state parameter, and replaces
only COST_CAP_USD and MAX_HOURS. Daily role (AWS_PROFILE=your-aws-profile). Terraform's own values stay as
they were; the next apply or the destroy uses them again.

  python3 infra/eks/raise_watchdog_limits.py --cap 45 --hours 32
  python3 infra/eks/raise_watchdog_limits.py --show
"""
import argparse
import json
import subprocess
import sys

FN, REGION, PARAM = "dqm-eks-watchdog", "us-west-2", "/dqm/watchdog/dqm-eks"


def aws(*a):
    r = subprocess.run(["aws", *a, "--region", REGION, "--output", "json"], capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(f"aws {' '.join(a[:2])} failed: {r.stderr.strip()[:400]}")
    return json.loads(r.stdout) if r.stdout.strip() else {}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cap", type=float)
    ap.add_argument("--hours", type=float)
    ap.add_argument("--show", action="store_true")
    a = ap.parse_args()
    env = aws("lambda", "get-function-configuration", "--function-name", FN)["Environment"]["Variables"]
    state = aws("ssm", "get-parameter", "--name", PARAM).get("Parameter", {}).get("Value", "?")
    print(f"watchdog now: cap ${env['COST_CAP_USD']}, max {env['MAX_HOURS']} h; state: {state}")
    if a.show or (a.cap is None and a.hours is None):
        return
    if a.cap is not None:
        env["COST_CAP_USD"] = str(a.cap)
    if a.hours is not None:
        env["MAX_HOURS"] = str(a.hours)
    aws("lambda", "update-function-configuration", "--function-name", FN,
        "--environment", json.dumps({"Variables": env}))
    env2 = aws("lambda", "get-function-configuration", "--function-name", FN)["Environment"]["Variables"]
    print(f"watchdog updated: cap ${env2['COST_CAP_USD']}, max {env2['MAX_HOURS']} h (other settings unchanged)")


if __name__ == "__main__":
    main()
