#!/usr/bin/env python3
"""
status_summary.py — pod timings of an EKS job from its status files in S3 (no data: the status files
hold counts, times and pod/node names only). Use after the the equivalence run calibration run to get measured hours
for the run plan (pipeline/s1/rebalance_run.py --plan-only ... --measured ...).

  python3 pipeline/worker/eks/status_summary.py s3://your-bucket/dqm/s1/<run>/runs/b6-reference
"""
import json
import subprocess
import sys


def aws(*args):
    r = subprocess.run(["aws", *args], capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(f"aws {' '.join(args[:3])} failed: {r.stderr.strip()[:400]}")
    return r.stdout


def masked(n):
    return "<10" if n is not None and n < 10 else str(n)


def main():
    if len(sys.argv) != 2 or not sys.argv[1].startswith("s3://"):
        sys.exit(__doc__)
    out = sys.argv[1].rstrip("/")
    bucket, prefix = out[5:].split("/", 1)
    keys, token = [], None
    while True:
        args = ["s3api", "list-objects-v2", "--bucket", bucket, "--prefix", f"{prefix}/status/", "--output", "json"]
        if token:
            args += ["--continuation-token", token]
        page = json.loads(aws(*args) or "{}")
        keys += [o["Key"] for o in page.get("Contents", [])]
        token = page.get("NextContinuationToken")
        if not token:
            break
    latest = {}
    for k in keys:
        name = k.rsplit("/", 1)[1]
        if name.endswith(".meta.json"):
            continue
        mode_shard, stamp = name.rsplit(".", 2)[0], name.rsplit(".", 2)[1]   # <mode>_<shard>.<ms>.json
        if mode_shard not in latest or int(stamp) > int(latest[mode_shard][0]):
            latest[mode_shard] = (stamp, k)
    if not latest:
        sys.exit(f"no status files under {out}/status/ (pods write them when they finish)")
    rows = []
    for ms, (_, k) in latest.items():
        st = json.loads(aws("s3", "cp", f"s3://{bucket}/{k}", "-"))
        rows.append(st)
    rows.sort(key=lambda s: (s["mode"], s["shard"]))
    print(f"{'mode':<10} {'shard':>5} {'exit':>4} {'pod hours':>9} {'input s':>8} {'stays':>6} {'resumed':>8} {'mem MiB':>13}  node")
    for s in rows:
        print(f"{s['mode']:<10} {s['shard']:>5} {s.get('exit_code', '?')!s:>4} {s.get('wall_seconds', 0) / 3600:>9.2f} "
              f"{s.get('input_ready_seconds', 0):>8.0f} {masked(s.get('stays_in_shard')):>6} "
              f"{masked(s.get('resumed_lines', 0)) if s.get('resumed_lines') else '-':>8} "
              f"{str(s.get('memory_peak_mib', '?')) + '/' + str(s.get('memory_limit_mib', '?')):>13}  {s.get('node', '')}")
    done = [s for s in rows if s.get("exit_code") == 0]
    mem = [s["memory_peak_mib"] for s in rows if s.get("memory_peak_mib")]
    if mem:
        print(f"memory peak: max {max(mem)} MiB over {len(mem)} pods (peak / limit per pod above)")
    print(f"\n{len(done)} of {len(rows)} shards with a status file finished OK (exit 0).")
    if done:
        print("for the run plan: --measured " + ",".join(f"{s['shard']}={s['wall_seconds'] / 3600:.2f}" for s in done))


if __name__ == "__main__":
    main()
