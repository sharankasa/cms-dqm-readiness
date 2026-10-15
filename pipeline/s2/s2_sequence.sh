#!/bin/bash
# s2_sequence.sh — S2 runtime protocol (registration §4.3.1) on the timing sample, one run after another on ONE
# node group, so no other job shares its nodes. Each run is the sliced side of the 2,000-stay S1 sample
# (one stay per engine call, as in S1) into its own output folder, so no run resumes another's results.
#
#   cd ".../project2-dqm-infrastructure"; export AWS_PROFILE=your-aws-profile
#   INPUT=s3://your-bucket/dqm/s1/20261010T054127Z/export_slbal-<stamp> \
#   IMAGE_TAG=<tag> bash pipeline/s2/s2_sequence.sh
#
# Steps (registered; the 8-node scaling point is cold-1):
#   cold-1 (8 nodes) | warmup (discarded) | warm-1..5 | [warm-6..10 if CV > 10%] | cold-2..5 |
#   [cold-6..10 if CV > 10%] | scale-4 | scale-2 | scale-1
#   cold  = the node group is set to 0 and back to N first (new instances, image not cached; nodes.sh);
#           the time to get nodes Ready is recorded separately (nodes.jsonl), not in the job's wall time
#   warm  = the same nodes again right after the previous run (image cached, OS caches warm)
# Every run: run_eks_job.sh with DETACH=1 on NODEGROUP, then this script waits for the job (kubectl,
# every 30 s), runs finish_job.sh (status, wall time from the Job's own start/completion times, logs to
# S3) and appends one line to runs.jsonl. Resumable: rerun the same command; finished steps are skipped,
# a step whose job is still on the cluster is waited for, and a step whose job vanished unfinished is
# rerun into a new folder (suffix -retry<n>), so a timing never includes resumed work.
# Plan folder: results/s2-<PLAN>/ (PLAN default: timing). Optional: GROUP (default
# dqm-eks-workers), NODES (default 8), STEPS (space-separated subset, e.g. "scale-2 scale-1").
# Nothing here deletes data. Ctrl-C stops the waiting only; a running job keeps running (rerun to resume).
set -u
cd "$(dirname "$0")/../.." || exit 1
INPUT=${INPUT:?set INPUT=<S1 run>/export_slbal-<stamp> (pipeline/readiness/build_inputs.py --sets s1-sample)}
INPUT=${INPUT%/}
IMAGE_TAG=${IMAGE_TAG:?set IMAGE_TAG (the worker image tag pushed for this run)}
GROUP=${GROUP:-dqm-eks-workers}; NODES=${NODES:-8}; PLAN=${PLAN:-timing}
RUNROOT=${INPUT%/export*}
DIR="results/s2-$PLAN"; mkdir -p "$DIR"
STATE="$DIR/state.txt"; JOBS="$DIR/jobs.txt"; RUNS="$DIR/runs.jsonl"; touch "$STATE" "$JOBS"
NODE_HOUR=${NODE_HOUR_USD:-$(terraform -chdir=infra/eks output -raw node_hour_usd 2>/dev/null)}
[ -n "$NODE_HOUR" ] || { echo "node price unknown: set NODE_HOUR_USD. Stopping."; exit 1; }
CTX=$(kubectl config current-context 2>/dev/null)
case "$CTX" in *dqm-eks*) ;; *) echo "kubectl context is '$CTX', expected the dqm-eks cluster. Stopping."; exit 1;; esac
trap 'echo; echo "stopped waiting; any running job keeps running. Rerun the same command to resume."; exit 130' INT TERM
echo "{\"plan\":\"$PLAN\",\"input\":\"$INPUT\",\"group\":\"$GROUP\",\"nodes\":$NODES,\"image_tag\":\"$IMAGE_TAG\",\"node_hour_usd\":$NODE_HOUR,\"written_utc\":\"$(date -u +%FT%TZ)\"}" > "$DIR/plan.json"

done_step() { grep -qx "$1" "$STATE"; }
job_of() { awk -v s="$1" '$1 == s {j = $2} END {print j}' "$JOBS"; }
tries_of() { awk -v s="$1" '$1 == s {n++} END {print n + 0}' "$JOBS"; }
ready_nodes() { kubectl get nodes -l "eks.amazonaws.com/nodegroup=$GROUP" --no-headers 2>/dev/null | grep -c ' Ready'; }

wait_job() {   # $1 = job name; returns when Complete/Failed (or the job is gone)
  local j=$1 st
  while true; do
    st=$(kubectl get job "$j" -o jsonpath='{.status.succeeded}/{.status.failed}/{.status.conditions[*].type}' 2>/dev/null) || return 1
    case "$st" in *Complete*|*Failed*) echo "  $(date -u +%T) $j: $st"; return 0;; esac
    sleep 30
  done
}

record() {   # $1 step, $2 kind, $3 nodes, $4 job
  local L="results/eks-$4"
  python3 - "$1" "$2" "$3" "$4" "$L" "$RUNS" <<'EOF'
import json, os, sys
step, kind, nodes, job, d, runs = sys.argv[1:]
run = json.load(open(f"{d}/run.json"))
st = json.load(open(f"{d}/job_status.json")).get("status", {})
wall = open(f"{d}/wall.txt").read().split(":")[1].strip() if os.path.exists(f"{d}/wall.txt") else None
cond = [c["type"] for c in st.get("conditions", []) if c.get("status") == "True"]
line = {"step": step, "kind": kind, "nodes": int(nodes), "job": job, "out": run["out"], "input": run["input"],
        "image": run["image"], "instance_type": run.get("instance_type"), "parallel": run["parallel"], "batch": run.get("batch", 1),
        "node_hour_usd": run.get("node_hour_usd"), "start": st.get("startTime"), "completion": st.get("completionTime"),
        "wall_seconds": int(wall) if wall and wall != "None" else None, "succeeded": st.get("succeeded", 0),
        "failed": st.get("failed", 0), "conditions": cond}
open(runs, "a").write(json.dumps(line) + "\n")
print(f"  recorded {step}: {kind}, {nodes} nodes, wall {line['wall_seconds']} s, {','.join(cond)}")
EOF
}

run_step() {   # $1 step id, $2 kind (cold|warm|warmup), $3 nodes
  local step=$1 kind=$2 n=$3 j tries out
  done_step "$step" && { echo "== $step: done earlier, skipped"; return 0; }
  j=$(job_of "$step")
  if [ -n "$j" ] && kubectl get job "$j" >/dev/null 2>&1; then
    echo "== $step: job $j is on the cluster: waiting for it"
  else
    if [ -n "$j" ]; then echo "== $step: job $j vanished unfinished: rerunning into a new folder"; fi
    tries=$(tries_of "$step")
    out="$RUNROOT/runs/s2-$PLAN-$step"; [ "$tries" -gt 0 ] && out="$out-retry$tries"
    if [ "$(kubectl get pods -l app=dqm-worker --field-selector=status.phase=Running -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' 2>/dev/null \
            | grep -v '^$' | while read -r nd; do kubectl get node "$nd" -o jsonpath='{.metadata.labels.eks\.amazonaws\.com/nodegroup}'; echo; done | grep -c "^$GROUP$")" != 0 ]; then
      echo "== $step: other worker pods are running on $GROUP: a timing run must have the nodes to itself. Stopping."; exit 1
    fi
    if [ "$kind" = cold ]; then
      echo "== $step: cold: $GROUP to 0 nodes, then $n new nodes"
      RECORD="$DIR/nodes.jsonl" bash pipeline/s2/nodes.sh "$GROUP" 0 || exit 1
      RECORD="$DIR/nodes.jsonl" bash pipeline/s2/nodes.sh "$GROUP" "$n" || exit 1
    else
      [ "$(ready_nodes)" = "$n" ] || { echo "== $step: warm run needs $n Ready nodes in $GROUP (found $(ready_nodes)). Stopping."; exit 1; }
    fi
    echo "== $step: launching ($kind, $n nodes) -> $out"
    j=$(MODE=sliced INPUT="$INPUT" OUT="$out" NODEGROUP="$GROUP" BATCH=1 JOB_TAG="s2-$step" DETACH=1 NO_BUILD=1 \
          IMAGE_TAG="$IMAGE_TAG" NODE_HOUR_USD="$NODE_HOUR" bash pipeline/worker/eks/run_eks_job.sh | tee /dev/stderr \
        | sed -n 's/^=== \([^:]*\):.*/\1/p' | head -1)
    [ -n "$j" ] || { echo "== $step: launch failed. Stopping."; exit 1; }
    echo "$step $j" >> "$JOBS"
  fi
  wait_job "$j" || { echo "== $step: cannot read job $j (credentials?). Rerun to resume."; exit 1; }
  bash pipeline/worker/eks/finish_job.sh "$j" || exit 1
  record "$step" "$kind" "$n" "$j"
  echo "$step" >> "$STATE"
}

cv_says_extend() {   # $1 = kind (cold|warm): "yes" if the coefficient of variation of the first 5 walls > 10%
  python3 - "$RUNS" "$1" <<'EOF'
import json, statistics, sys
runs, kind = sys.argv[1], sys.argv[2]
w = [r["wall_seconds"] for r in map(json.loads, open(runs)) if r["kind"] == kind and r["nodes"] and r["step"].startswith(kind + "-")
     and r["wall_seconds"]][:5]
if len(w) < 5:
    print("no"); sys.exit()
cv = statistics.stdev(w) / statistics.mean(w)
print("yes" if cv > 0.10 else "no")
print(f"  {kind}: CV of whole-job wall time over 5 repeats = {cv:.1%} (extension rule: > 10% -> 5 more)", file=sys.stderr)
EOF
}

STEPS=${STEPS:-}
if [ -n "$STEPS" ]; then
  for s in $STEPS; do
    case "$s" in cold-*) run_step "$s" cold "$NODES";; warm-*) run_step "$s" warm "$NODES";; warmup) run_step warmup warmup "$NODES";;
      scale-*) run_step "$s" cold "${s#scale-}";; *) echo "unknown step $s"; exit 1;; esac
  done
  exit 0
fi
run_step cold-1 cold "$NODES"
run_step warmup warmup "$NODES"
for i in 1 2 3 4 5; do run_step "warm-$i" warm "$NODES"; done
if [ "$(cv_says_extend warm)" = yes ]; then for i in 6 7 8 9 10; do run_step "warm-$i" warm "$NODES"; done; fi
for i in 2 3 4 5; do run_step "cold-$i" cold "$NODES"; done
if [ "$(cv_says_extend cold)" = yes ]; then for i in 6 7 8 9 10; do run_step "cold-$i" cold "$NODES"; done; fi
for n in 4 2 1; do run_step "scale-$n" cold "$n"; done
echo "S2 timing sequence complete: $RUNS. Summary: python3 pipeline/s2/s2_summary.py --plan $DIR"
echo "The node group $GROUP still has nodes: set it to 0 when nothing else needs it: bash pipeline/s2/nodes.sh $GROUP 0"
