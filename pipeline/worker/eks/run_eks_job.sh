#!/bin/bash
# run_eks_job.sh — the pilot-the readiness inputs: one side (sliced or reference) of a sample as a Kubernetes Indexed Job on EKS.
#
#   cd ".../project2-dqm-infrastructure"
#   MODE=sliced    INPUT=s3://your-bucket/dqm/pilot/<run>/export bash pipeline/worker/eks/run_eks_job.sh
#   MODE=reference INPUT=s3://your-bucket/dqm/pilot/<run>/export bash pipeline/worker/eks/run_eks_job.sh
#   Fault test (S1, registered): KILL_PODS=2 KILL_NODE=1 MODE=sliced INPUT=... bash pipeline/worker/eks/run_eks_job.sh
#   Image only (no cluster needed): PUSH_ONLY=1 bash pipeline/worker/eks/run_eks_job.sh
#   Start and return (no watching; the watchdog guards cost; finish with finish_job.sh): DETACH=1 ...
#   Shards N.. only (a second job next to one running shards 0..N-1): SHARD_OFFSET=N [SHARDS=<count>]
#   Added 2026-10-14 (S2 / R2):
#     NODEGROUP=dqm-eks-workers|dqm-eks-burst  keep the pods on one node group (two jobs side by side on
#                separate nodes: S2 timing runs must not share nodes); PARALLEL and the cost estimate
#                then count that group's nodes only
#     BATCH=<n> [BATCH_MAX_RES=400]  sliced side: stays per engine call (default 1, as in S1)
#     ENGINE_PREFIX=sliced_  compare-v against a sliced job (V on the readiness cohort)
#     JOB_TAG=<word>  added to the job name (e.g. s2-cold-3), so runs are recognisable
#     NO_BUILD=1 with IMAGE_TAG=<tag>: use that image without the ECR/Docker build step
#   the equivalence run calibration (the 3 heaviest shards of a balanced input, one pod per node, more memory; results
#   go where the full reference run will resume from):
#     MODE=reference INPUT=<run>/export_bal-<stamp> SHARDS=3 PARALLEL=3 CPU=1500m HEAP=5632 MEMREQ=6Gi \
#     MEMLIM=6656Mi OUT=<run>/runs/b6-reference bash pipeline/worker/eks/run_eks_job.sh
#
# Needs: the EKS cluster up (infra/eks, with the worker role), kubectl pointed at it, Docker Desktop
# (to build the image the first time), AWS credentials of the daily role in the shell.
# What it does: builds and pushes the worker image to ECR only if this code version is not there yet
# (repository dqm-worker, created if missing); runs SHARDS pods, PARALLEL at a time; optionally deletes
# KILL_PODS running pods and terminates the EC2 instance of one node (KILL_NODE=1) mid-run; waits;
# saves job and pod status (no data) to results/eks-<job>/ and the pod logs to S3
# (<OUT>/logs/, they name patients); deletes the Job object. Results stay in S3 under <OUT>/results/.
set -u
cd "$(dirname "$0")/../../.." || exit 1
REGION=us-west-2
build_push() {   # the worker image, tagged by a hash of everything that goes into it; built only if new
  ACCOUNT=$(aws sts get-caller-identity --query Account --output text) || { echo "no AWS credentials"; exit 1; }
  REG="$ACCOUNT.dkr.ecr.$REGION.amazonaws.com"; REPO=dqm-worker
  TAG=$(cat pipeline/worker/eks/Dockerfile pipeline/worker/eks/worker_s3.js pipeline/worker/eks/package-lock.json \
            pipeline/s1/run_engine.js testcases/package-lock.json pipeline/v/reference_measures.py pipeline/v/compare_v.py \
            pipeline/worker/eks/profile_run.js pipeline/worker/eks/profile_summary.js \
            | shasum -a 256 | cut -c1-12)
  if [ -n "${IMAGE_TAG:-}" ]; then   # pin an earlier image (e.g. to finish the equivalence run on the image it started with)
    aws ecr describe-images --repository-name "$REPO" --image-ids imageTag="$IMAGE_TAG" --region "$REGION" >/dev/null 2>&1 || \
      { echo "IMAGE_TAG=$IMAGE_TAG is not in ECR. Stopping."; exit 1; }
    TAG=$IMAGE_TAG
  fi
  IMAGE="$REG/$REPO:$TAG"
  aws ecr describe-repositories --repository-names "$REPO" --region "$REGION" >/dev/null 2>&1 || \
    aws ecr create-repository --repository-name "$REPO" --region "$REGION" --image-scanning-configuration scanOnPush=true >/dev/null || exit 1
  if aws ecr describe-images --repository-name "$REPO" --image-ids imageTag="$TAG" --region "$REGION" >/dev/null 2>&1; then
    echo "image $IMAGE already in ECR"
  else
    docker info >/dev/null 2>&1 || { echo "Docker Desktop is not running (needed once to build the image). Stopping."; exit 1; }
    aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$REG" >/dev/null || exit 1
    docker buildx build --platform linux/amd64 -t "$IMAGE" -f pipeline/worker/eks/Dockerfile --push . || { echo "image build/push failed"; exit 1; }
  fi
}
if [ "${PUSH_ONLY:-0}" = 1 ]; then build_push; echo "image ready: $IMAGE (PUSH_ONLY: no job started)"; exit 0; fi
MODE=${MODE:?set MODE=sliced, reference, v or compare-v}
ENGINE_OUT=${ENGINE_OUT:-}; V_OUT=${V_OUT:-}; NO_POP_COUNTS=${NO_POP_COUNTS:-}; LABEL=${LABEL:-run}
NODEGROUP=${NODEGROUP:-}; BATCH=${BATCH:-1}; BATCH_MAX_RES=${BATCH_MAX_RES:-400}; ENGINE_PREFIX=${ENGINE_PREFIX:-reference_}
JOB_TAG=${JOB_TAG:-}
NODESEL=""; NODE_LABEL=""
if [ -n "$NODEGROUP" ]; then NODESEL="eks.amazonaws.com/nodegroup: $NODEGROUP"; NODE_LABEL="-l eks.amazonaws.com/nodegroup=$NODEGROUP"; fi
RESULTS_FROM=${RESULTS_FROM:-}; PROFILE_TARGET_S=${PROFILE_TARGET_S:-2400}; PROFILE_SECONDS=${PROFILE_SECONDS:-3600}
if [ "$MODE" = compare-v ]; then
  [ -n "$ENGINE_OUT" ] && [ -n "$V_OUT" ] || { echo "compare-v needs ENGINE_OUT (reference job) and V_OUT (v job). Stopping."; exit 1; }
  SHARDS=1; PARALLEL=1
fi
INPUT=${INPUT:?set INPUT=s3://.../export}
INPUT=${INPUT%/}
MAXSHARD=$(aws s3 ls "$INPUT/manifest/" | sed -n 's/.*PRE shard=\([0-9]*\)\/.*/\1/p' | sort -n | tail -1)
SHARD_OFFSET=${SHARD_OFFSET:-0}
SHARDS=${SHARDS:-$(( ${MAXSHARD:--1} + 1 - SHARD_OFFSET ))}
PARALLEL=${PARALLEL:-$(( $(kubectl get nodes $NODE_LABEL --no-headers 2>/dev/null | grep -c ' Ready') * 2 ))}
KILL_PODS=${KILL_PODS:-0}; KILL_NODE=${KILL_NODE:-0}; KILL_AFTER=${KILL_AFTER:-90}
TIMEOUT=${TIMEOUT:-43200}
if [ "$MODE" = reference ] || [ "$MODE" = v ] || [ "$MODE" = profile ]; then HEAP=${HEAP:-3072}; MEMREQ=${MEMREQ:-3Gi}; MEMLIM=${MEMLIM:-3584Mi};
else HEAP=${HEAP:-2560}; MEMREQ=${MEMREQ:-2Gi}; MEMLIM=${MEMLIM:-3Gi}; fi
CPU=${CPU:-850m}
RUNROOT=${INPUT%/export*}; ENGINE=${ENGINE:-$RUNROOT/engine}   # export/ or export_bal-<stamp>/ (the equivalence run shards by predicted time)
RUN=$(date -u +%Y%m%dT%H%M%SZ); NAME="dqm-$MODE-${JOB_TAG:+$JOB_TAG-}$(echo "$RUN" | tr '[:upper:]' '[:lower:]')"
OUT=${OUT:-$RUNROOT/runs/$NAME}
LOCAL="results/eks-$NAME"

CTX=$(kubectl config current-context 2>/dev/null)
case "$CTX" in *dqm-eks*) ;; *) echo "kubectl context is '$CTX', expected the dqm-eks cluster. Stopping."; exit 1;; esac
[ "$PARALLEL" -ge 1 ] || { echo "no Ready nodes. Stopping."; exit 1; }
[ "$SHARDS" -ge 1 ] || { echo "no shards under $INPUT/manifest/. Stopping."; exit 1; }
aws s3 ls "$ENGINE/bundles/" >/dev/null || { echo "missing engine inputs under $ENGINE. Stopping."; exit 1; }
if [ "${NO_BUILD:-0}" = 1 ] && [ -n "${IMAGE_TAG:-}" ]; then
  IMAGE="$(aws sts get-caller-identity --query Account --output text).dkr.ecr.$REGION.amazonaws.com/dqm-worker:$IMAGE_TAG"; TAG=$IMAGE_TAG
else
  build_push
fi

# Cost guard for this job (the cluster-wide guard is the watchdog Lambda, infra/eks/watchdog.tf, which
# works even if this laptop sleeps). Burn = nodes x node price + EKS fee, counted from job start.
NODE_HOUR=${NODE_HOUR_USD:-$(terraform -chdir=infra/eks output -raw node_hour_usd 2>/dev/null)}
[ -n "$NODE_HOUR" ] || { echo "node price unknown (no infra/eks terraform output). Set NODE_HOUR_USD. Stopping."; exit 1; }
NNODES=$(kubectl get nodes $NODE_LABEL --no-headers 2>/dev/null | wc -l | tr -d ' ')
BURN=$(awk -v n="$NNODES" -v p="$NODE_HOUR" 'BEGIN{printf "%.4f", n*p + 0.10}')
JOB_COST_CAP=${JOB_COST_CAP:-5}
if [ "${DETACH:-0}" = 1 ]; then
  echo "cost: $NNODES nodes x \$$NODE_HOUR/h + EKS \$0.10/h = \$$BURN/h; detached, so no per-job cap: the watchdog's cluster cap applies"
else
  echo "cost guard: $NNODES nodes x \$$NODE_HOUR/h + EKS \$0.10/h = \$$BURN/h; this job stops at an estimated \$$JOB_COST_CAP ($(awk -v c="$JOB_COST_CAP" -v b="$BURN" 'BEGIN{printf "%.1f", c/b}') h); warning at 80%"
fi

mkdir -p "$LOCAL"
echo "=== $NAME: mode $MODE, shards $SHARD_OFFSET..$(( SHARD_OFFSET + SHARDS - 1 )), $PARALLEL in parallel${NODEGROUP:+ on node group $NODEGROUP}, batch $BATCH, image $TAG; results $OUT"
sed -e "s#__NAME__#$NAME#g" -e "s#__MODE__#$MODE#g" -e "s#__SHARDS__#$SHARDS#g" -e "s#__PARALLEL__#$PARALLEL#g" \
    -e "s#__IMAGE__#$IMAGE#g" -e "s#__INPUT__#$INPUT#g" -e "s#__ENGINE__#$ENGINE#g" -e "s#__OUT__#$OUT#g" \
    -e "s#__HEAP__#$HEAP#g" -e "s#__CPU__#$CPU#g" -e "s#__MEMREQ__#$MEMREQ#g" -e "s#__MEMLIM__#$MEMLIM#g" \
    -e "s#__ENGINEOUT__#$ENGINE_OUT#g" -e "s#__VOUT__#$V_OUT#g" -e "s#__NOPOP__#$NO_POP_COUNTS#g" -e "s#__LABEL__#$LABEL#g" \
    -e "s#__SHARDOFFSET__#$SHARD_OFFSET#g" -e "s#__RESULTSFROM__#$RESULTS_FROM#g" \
    -e "s#__PROFTARGET__#$PROFILE_TARGET_S#g" -e "s#__PROFSECONDS__#$PROFILE_SECONDS#g" \
    -e "s#__BATCH__#$BATCH#g" -e "s#__BATCHMAXRES__#$BATCH_MAX_RES#g" -e "s#__ENGINEPREFIX__#$ENGINE_PREFIX#g" \
    -e "s#__NODESEL__#$NODESEL#g" \
    pipeline/worker/eks/job-eks.yaml > "$LOCAL/job.yaml"
kubectl apply -f "$LOCAL/job.yaml" || exit 1
START=$(date +%s)
echo "{\"job\":\"$NAME\",\"mode\":\"$MODE\",\"shards\":$SHARDS,\"shard_offset\":$SHARD_OFFSET,\"parallel\":$PARALLEL,\"image\":\"$IMAGE\",\"input\":\"$INPUT\",\"out\":\"$OUT\",\"nodes\":$NNODES,\"nodegroup\":\"$NODEGROUP\",\"batch\":$BATCH,\"batch_max_res\":$BATCH_MAX_RES,\"node_hour_usd\":$NODE_HOUR,\"job_tag\":\"$JOB_TAG\",\"created_utc\":\"$(date -u +%FT%TZ)\",\"instance_type\":\"$(kubectl get nodes -o jsonpath='{.items[0].metadata.labels.node\.kubernetes\.io/instance-type}')\",\"started_epoch\":$START}" > "$LOCAL/run.json"

if [ "${DETACH:-0}" = 1 ]; then
  echo "detached: $NAME runs on the cluster by itself; the watchdog (infra/eks/watchdog.tf) is the cost guard."
  echo "  progress: bash pipeline/worker/eks/run_progress.sh   |   when complete: bash pipeline/worker/eks/finish_job.sh $NAME"
  exit 0
fi
# Ctrl-C only stops the watching, never the job (a Ctrl-C at an MFA prompt used to let the loop run on into
# its limits and delete the job, 2026-10-11)
trap 'echo; echo "stopped watching; $NAME keeps running. Finish later: bash pipeline/worker/eks/finish_job.sh $NAME"; exit 130' INT TERM
killed=0; node_killed=0; warned=0; over=0
while true; do
  st=$(kubectl get job "$NAME" -o jsonpath='{.status.succeeded}/{.status.failed}/{.status.conditions[*].type}' 2>/dev/null)
  case "$st" in *Complete*|*Failed*) break;; esac
  el=$(( $(date +%s) - START ))
  if [ "$killed" -lt "$KILL_PODS" ] && [ "$el" -ge $(( KILL_AFTER + killed * 60 )) ]; then
    POD=$(kubectl get pods -l job="$NAME" --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    if [ -n "$POD" ]; then
      echo "  deleting pod $POD at ${el}s"; echo "$(date -u +%FT%TZ) deleted pod $POD at ${el}s" >> "$LOCAL/faults.txt"
      kubectl delete pod "$POD" --grace-period=5 --wait=false >/dev/null; killed=$((killed + 1))
    fi
  fi
  if [ "$KILL_NODE" = 1 ] && [ "$node_killed" = 0 ] && [ "$killed" -ge "$KILL_PODS" ] && [ "$el" -ge $(( KILL_AFTER + KILL_PODS * 60 + 60 )) ]; then
    NODE=$(kubectl get pods -l job="$NAME" --field-selector=status.phase=Running -o jsonpath='{.items[0].spec.nodeName}' 2>/dev/null)
    if [ -n "$NODE" ]; then
      IID=$(kubectl get node "$NODE" -o jsonpath='{.spec.providerID}' | sed 's#.*/##')
      echo "  terminating node $NODE ($IID) at ${el}s"; echo "$(date -u +%FT%TZ) terminated node $NODE $IID at ${el}s" >> "$LOCAL/faults.txt"
      aws ec2 terminate-instances --instance-ids "$IID" --region "$REGION" >/dev/null && node_killed=1
    fi
  fi
  if [ "$el" -ge "$TIMEOUT" ]; then   # stop watching only; the job keeps running (no deletion on timeout)
    echo "timeout after ${el}s: stopped watching; $NAME keeps running. Finish later: bash pipeline/worker/eks/finish_job.sh $NAME"
    exit 4
  fi
  SPENT=$(awk -v e="$el" -v b="$BURN" 'BEGIN{printf "%.2f", e/3600*b}')
  if [ "$warned" = 0 ] && awk -v s="$SPENT" -v c="$JOB_COST_CAP" 'BEGIN{exit !(s >= 0.8*c)}'; then
    echo "  WARNING: this job's estimated cost \$$SPENT has reached 80% of its cap \$$JOB_COST_CAP"; warned=1
  fi
  if awk -v s="$SPENT" -v c="$JOB_COST_CAP" 'BEGIN{exit !(s >= c)}'; then
    echo "ERROR: estimated job cost \$$SPENT reached the cap \$$JOB_COST_CAP: stopping the job (finished results are kept; a rerun resumes)."
    echo "$(date -u +%FT%TZ) cost cap: estimated \$$SPENT >= \$$JOB_COST_CAP; job stopped" >> "$LOCAL/faults.txt"
    over=1; break
  fi
  sleep 10
done
END=$(date +%s)
echo "wall seconds: $((END - START))" | tee "$LOCAL/wall.txt"
awk -v e="$((END - START))" -v b="$BURN" -v n="$NNODES" -v p="$NODE_HOUR" 'BEGIN{printf "{\"nodes\":%d,\"node_hour_usd\":%s,\"burn_usd_per_hour\":%s,\"wall_seconds\":%d,\"estimated_cost_usd\":%.2f}\n", n, p, b, e, e/3600*b}' > "$LOCAL/cost_estimate.json"
cat "$LOCAL/cost_estimate.json"
kubectl get job "$NAME" -o json > "$LOCAL/job_status.json"
kubectl get pods -l job="$NAME" -o wide > "$LOCAL/pods.txt" 2>&1
kubectl get nodes -o wide > "$LOCAL/nodes.txt" 2>&1
kubectl logs -l job="$NAME" --tail=-1 --prefix 2>&1 | aws s3 cp - "$OUT/logs/$NAME.txt" --only-show-errors
cat "$LOCAL/pods.txt" | awk '{print $1, $3, $4, $7}'
kubectl delete job "$NAME" --wait=false >/dev/null
[ "$over" = 1 ] && { echo "Job stopped by the cost cap. Raise JOB_COST_CAP deliberately to continue (results resume), or destroy the cluster."; exit 3; }
case "$MODE" in
  reference) echo "results in $OUT/results/ (S3). S1 comparison: pipeline/s1/compare_run.py --ref-out $OUT ...; V comparison: ENGINE_OUT=$OUT";;
  sliced) echo "results in $OUT/results/ (S3). S1 comparison: pipeline/s1/compare_run.py --sliced-out $OUT ...";;
  v) echo "results in $OUT/results/ (S3). V comparison: MODE=compare-v V_OUT=$OUT ENGINE_OUT=<reference job OUT>";;
  compare-v) echo "summary (counts): aws s3 cp $OUT/compare-v/v_${LABEL}_summary.json -";;
esac
