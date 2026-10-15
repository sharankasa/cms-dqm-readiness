#!/bin/bash
# launch_sets.sh — start the engine (sliced side) on several engine-input sets at once, detached, on one node
# group: the readiness scenario sets made by r1_run.py scenario-inputs (or any set of build_inputs.py).
# Names are the inputs_env.py names without the suffix, e.g. SCENARIO_STATUS_LOWER; each run writes to
# <set root>/runs/engine-sliced. Nothing is timed here (several jobs share the nodes).
#
#   cd ".../project2-dqm-infrastructure"; export AWS_PROFILE=your-aws-profile; export IMAGE_TAG=<tag>
#   bash pipeline/readiness/launch_sets.sh SCENARIO_STATUS_LOWER SCENARIO_STATUS_UPPER
#   bash pipeline/readiness/launch_sets.sh --all-scenarios
# Optional: NODEGROUP (default dqm-eks-burst), BATCH (default 20; 1 if the batching check did not pass).
# Then: python3 pipeline/readiness/finish_sets.py <same names> (finishes completed jobs, checks completeness).
set -u
cd "$(dirname "$0")/../.." || exit 1
IMAGE_TAG=${IMAGE_TAG:?set IMAGE_TAG (the worker image pushed for this run)}
GROUP=${NODEGROUP:-dqm-eks-burst}; BATCH=${BATCH:-20}
eval "$(python3 pipeline/readiness/inputs_env.py)" || exit 1
NAMES=("$@")
if [ "${1:-}" = "--all-scenarios" ]; then
  NAMES=($(python3 pipeline/readiness/inputs_env.py | sed -n 's/^export \(SCENARIO_[A-Z0-9_]*\)_INPUT=.*/\1/p'))
fi
[ "${#NAMES[@]}" -ge 1 ] || { echo "usage: launch_sets.sh <SET_NAME> [...] | --all-scenarios"; exit 1; }
for K in "${NAMES[@]}"; do
  IV="${K}_INPUT"; RV="${K}_ROOT"
  IN="${!IV:-}"; RT="${!RV:-}"
  if [ -z "$IN" ]; then echo "$K: no input recorded (inputs_env.py --show). Skipped."; continue; fi
  if aws s3 ls "$RT/runs/engine-sliced/results/" >/dev/null 2>&1; then
    echo "$K: $RT/runs/engine-sliced already has results (a rerun would resume it). Skipped."; continue
  fi
  TAG=$(echo "$K" | tr 'A-Z_' 'a-z-' | sed 's/^scenario-/scen-/' | cut -c1-28 | sed 's/-$//')
  echo "== $K"
  MODE=sliced INPUT="$IN" OUT="$RT/runs/engine-sliced" NODEGROUP="$GROUP" BATCH="$BATCH" JOB_TAG="$TAG" DETACH=1 NO_BUILD=1 \
    bash pipeline/worker/eks/run_eks_job.sh | grep -E '^===|^detached|Stopping'
done
kubectl get jobs
