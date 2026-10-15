#!/usr/bin/env python3
"""
s2_eta.py: where the S2 timing sequence (s2_sequence.sh) is and roughly when it will finish. Reads only the
plan folder's local files (state.txt, jobs.txt, runs.jsonl, nodes.jsonl): no AWS or kubectl calls, so it never
asks for MFA and is safe to run in any terminal while the sequence runs in another.

  python3 pipeline/s2/s2_eta.py [--plan results/s2-timing]

Estimates: an 8-node run takes the mean wall time of the finished 8-node runs; a cold step adds the observed
gap between one run's end and the next launch (node group to 0 and back, finish, launch); an n-node scaling
run is assumed to take 8/n times an 8-node run (a rough, linear assumption: the real scaling is what S2
measures). The ETA assumes every MFA prompt in the sequence's terminal is answered when it appears.
"""
import argparse
import datetime as dt
import json
import os
import re
import statistics

try:
    from zoneinfo import ZoneInfo
    LOCAL = ZoneInfo("America/Los_Angeles")
except Exception:
    LOCAL = None

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
UTC = dt.timezone.utc


def launch_time(job):
    m = re.search(r"(\d{8}t\d{6}z)$", job)
    return dt.datetime.strptime(m.group(1).upper(), "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC) if m else None


def fmt(t):
    s = t.strftime("%H:%M UTC")
    if LOCAL:
        s += t.astimezone(LOCAL).strftime(" (%a %-I:%M %p Seattle)")
    return s


def mins(s):
    return f"{s / 60:.0f} min" if s < 5400 else f"{s / 3600:.1f} h"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", default=os.path.join(ROOT, "results", "s2-timing"))
    a = ap.parse_args()
    d = a.plan if os.path.isabs(a.plan) else os.path.join(ROOT, a.plan)
    done = [l.strip() for l in open(os.path.join(d, "state.txt")) if l.strip()]
    jobs = [l.split() for l in open(os.path.join(d, "jobs.txt")) if l.strip()]
    runs = [json.loads(l) for l in open(os.path.join(d, "runs.jsonl")) if l.strip()]
    now = dt.datetime.now(UTC)

    w8 = [r["wall_seconds"] for r in runs if r["nodes"] == 8 and r.get("wall_seconds")]
    mean8 = statistics.mean(w8)
    wall = {r["step"]: r["wall_seconds"] for r in runs if r.get("wall_seconds")}
    gaps = []
    for (s1, j1), (s2, j2) in zip(jobs, jobs[1:]):
        t1, t2 = launch_time(j1), launch_time(j2)
        if s2.startswith(("cold", "scale")) and s1 in wall and t1 and t2:
            g = (t2 - t1).total_seconds() - wall[s1]
            if 0 < g < 1800:
                gaps.append(g)
    cold_gap = statistics.median(gaps) if gaps else 200.0

    colds = [wall[s] for s in sorted(wall) if s.startswith("cold-")][:5]
    cv = statistics.stdev(colds) / statistics.mean(colds) if len(colds) >= 2 else None
    plan = [f"cold-{i}" for i in range(2, 6)] + ["scale-4", "scale-2", "scale-1"]
    remaining = [s for s in plan if s not in done]
    launched = {s: j for s, j in jobs}
    current = next((s for s in remaining if s in launched), None)

    def est(step):
        return mean8 * 8 / int(step.split("-")[1]) if step.startswith("scale-") else mean8

    print(f"done: {', '.join(done)}")
    print(f"8-node run: mean {mins(mean8)} over {len(w8)} runs; gap per cold step {cold_gap:.0f} s")
    if cv is not None:
        print(f"cold-run CV so far ({len(colds)} runs): {cv:.1%} (the registered rule adds 5 more cold runs if > 10% after 5)")
    t = now
    if current:
        start = launch_time(launched[current])
        left = max(est(current) - (now - start).total_seconds(), 60)
        t = now + dt.timedelta(seconds=left)
        print(f"running: {current}, launched {fmt(start)}, about {mins(left)} left -> ends ~{fmt(t)}")
    for s in remaining:
        if s == current:
            continue
        t = t + dt.timedelta(seconds=cold_gap + est(s))
        print(f"next:    {s:<8} about {mins(est(s))} -> ends ~{fmt(t)}")
    print(f"\nestimated finish: ~{fmt(t)} ({mins((t - now).total_seconds())} from now), if every MFA prompt is answered")
    print("scaling estimates assume time grows as 8/n; the 1-node run is the least certain")


if __name__ == "__main__":
    main()
