#!/bin/bash
# run_local_job.sh — the local fault test: the sliced S1 evaluation as a Kubernetes Indexed Job on a workstation
# (Docker Desktop Kubernetes, kubeadm). OPEN demo data only.
#
#   cd ".../project2-dqm-infrastructure"
#   bash pipeline/worker/run_local_job.sh             # undisturbed run
#   KILL=2 bash pipeline/worker/run_local_job.sh      # fault test: kill 2 running pods mid-run
#
# Builds the worker image (engine + runner, no data), runs SHARDS pods (PARALLEL at a time) over the
# demo slices in work/demo-prep, waits for the Job, saves pod/job status and logs, then removes
# the Job object (results stay in results/a5-k8s-<run>/). compare the results
# with the the demo equivalence check run: every stay identical and written exactly once.
set -u
cd "$(dirname "$0")/../.." || exit 1          # -> project root
PROJECT=$(pwd)
SHARDS=${SHARDS:-8}
PARALLEL=${PARALLEL:-4}
KILL=${KILL:-0}
RUN=$(date -u +%Y%m%dT%H%M%SZ)
NAME="s1-sliced-$(echo "$RUN" | tr '[:upper:]' '[:lower:]')"
OUT="results/a5-k8s-$RUN"

docker info >/dev/null 2>&1 || { echo "Docker Desktop is not running. Start it, then rerun."; exit 1; }
CTX=$(kubectl config current-context 2>/dev/null)
[ "$CTX" = "docker-desktop" ] || { echo "kubectl context is '$CTX', expected docker-desktop (Settings > Kubernetes > Enable). Stopping."; exit 1; }
kubectl get nodes 2>/dev/null | grep -q " Ready" || { echo "Kubernetes node not Ready yet. Wait a minute and rerun."; exit 1; }
[ -f work/demo-prep/manifest.csv ] || { echo "Missing work/demo-prep. Stopping."; exit 1; }

echo "=== the local fault test local Job $NAME: $SHARDS shards, $PARALLEL in parallel, kill $KILL pod(s); output $OUT"
docker build -q -t dqm-worker:dev -f pipeline/worker/Dockerfile . > /dev/null || { echo "image build failed"; exit 1; }
mkdir -p "$OUT"
docker image inspect dqm-worker:dev --format '{{.Id}}' > "$OUT/image_id.txt"
sed -e "s#__NAME__#$NAME#g" -e "s#__SHARDS__#$SHARDS#g" -e "s#__PARALLEL__#$PARALLEL#g" \
    -e "s#__PROJECT__#$PROJECT#g" -e "s#__OUT__#$OUT#g" pipeline/worker/job-sliced-local.yaml > "$OUT/job.yaml"
kubectl apply -f "$OUT/job.yaml" || exit 1
START=$(date +%s)

killed=0
while [ "$killed" -lt "$KILL" ]; do
  lines=$(cat "$OUT"/sliced_*.jsonl 2>/dev/null | wc -l | tr -d ' ')
  if [ "$lines" -ge $((40 + killed * 60)) ]; then
    POD=$(kubectl get pods -l job="$NAME" --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    if [ -n "$POD" ]; then
      echo "  killing pod $POD after $lines result lines"
      echo "$(date -u +%FT%TZ) killed $POD at $lines lines" >> "$OUT/kills.txt"
      kubectl delete pod "$POD" --grace-period=1 --wait=false >/dev/null
      killed=$((killed + 1))
    fi
  fi
  kubectl get job "$NAME" -o jsonpath='{.status.succeeded}' 2>/dev/null | grep -q "^$SHARDS$" && break
  sleep 2
done

kubectl wait --for=condition=complete "job/$NAME" --timeout=1800s || echo "Job did not complete; status saved"
echo "wall seconds: $(( $(date +%s) - START ))" | tee "$OUT/wall.txt"
kubectl get job "$NAME" -o json > "$OUT/job_status.json"
kubectl get pods -l job="$NAME" -o wide > "$OUT/pods.txt" 2>&1
kubectl logs -l job="$NAME" --tail=-1 --prefix > "$OUT/logs.txt" 2>&1
cat "$OUT/pods.txt"
kubectl delete job "$NAME" --wait=false >/dev/null
echo "=== done: $OUT"
