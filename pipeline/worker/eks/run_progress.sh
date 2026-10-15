#!/bin/bash
# run_progress.sh — one progress snapshot of a run on the cluster, no patient data: the watchdog's spend
# estimate, pod status and restarts, memory of running pods, finished shards (hours, peak memory) and
# the result files in S3 (they grow as patient-years finish).
#
#   cd ".../project2-dqm-infrastructure"; export AWS_PROFILE=your-aws-profile
#   bash pipeline/worker/eks/run_progress.sh
#
# OUT defaults to the the equivalence run reference results; CLUSTER to dqm-eks.
set -u
cd "$(dirname "$0")/../../.." || exit 1
OUT=${OUT:-s3://your-bucket/dqm/s1/20261010T054127Z/runs/b6-reference}
CLUSTER=${CLUSTER:-dqm-eks}
echo "== $(date -u +%FT%TZ) UTC"
echo "== watchdog: estimated cluster spend so far"
aws ssm get-parameter --name "/dqm/watchdog/$CLUSTER" --query Parameter.Value --output text
echo "== nodes: $(kubectl get nodes --no-headers 2>/dev/null | grep -c ' Ready') Ready"
echo "== pods (name, status, restarts, node)"
kubectl get pods -l app=dqm-worker -o wide --no-headers | awk '{print $1, $3, $4, $7}'
echo "== memory of running pods (bytes; C/D limit ~7.5e9, A/B limit ~14e9)"
kubectl get pods -l app=dqm-worker --field-selector=status.phase=Running -o name | while read -r p; do
  echo "$p $(kubectl exec "${p#pod/}" -- cat /sys/fs/cgroup/memory.current 2>/dev/null)"
done
echo "== finished shards"
python3 pipeline/worker/eks/status_summary.py "$OUT"
echo "== result files"
aws s3 ls "$OUT/results/" --human-readable
