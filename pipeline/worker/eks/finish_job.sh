#!/bin/bash
# finish_job.sh — the end-of-job steps of run_eks_job.sh, for jobs whose monitoring terminal was stopped
# while the job kept running on the cluster (the equivalence run, 2026-10-11: the terminals' own time and cost limits
# would otherwise have deleted still-running jobs). Per job: saves job, pod and node status (no data)
# to results/eks-<job>/, copies the pod logs to S3 as <OUT>/logs/<job>.txt (they name
# patients: never paste them), writes wall.txt, deletes the Job object. Results are already in S3.
#
#   bash pipeline/worker/eks/finish_job.sh <job-name> [<job-name> ...]
#
# A job that has not finished is left alone (FORCE=1 to finish it anyway, which stops its pods).
set -u
cd "$(dirname "$0")/../../.." || exit 1
CTX=$(kubectl config current-context 2>/dev/null)
case "$CTX" in *dqm-eks*) ;; *) echo "kubectl context is '$CTX', expected the dqm-eks cluster. Stopping."; exit 1;; esac
[ $# -ge 1 ] || { echo "usage: bash pipeline/worker/eks/finish_job.sh <job-name> [<job-name> ...]"; exit 1; }
for NAME in "$@"; do
  LOCAL="results/eks-$NAME"
  [ -f "$LOCAL/run.json" ] || { echo "$NAME: no $LOCAL/run.json (not started by run_eks_job.sh here). Skipped."; continue; }
  OUT=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['out'])" "$LOCAL/run.json")
  if ! kubectl get job "$NAME" >/dev/null 2>&1; then echo "$NAME: no such job on the cluster (already finished?). Skipped."; continue; fi
  st=$(kubectl get job "$NAME" -o jsonpath='{.status.conditions[*].type}')
  case "$st" in
    *Complete*|*Failed*) ;;
    *) if [ "${FORCE:-0}" != 1 ]; then
         echo "$NAME: still running ($(kubectl get job "$NAME" -o jsonpath='{.status.succeeded}') succeeded, $(kubectl get job "$NAME" -o jsonpath='{.status.active}') active). Left alone."
         continue
       fi;;
  esac
  kubectl get job "$NAME" -o json > "$LOCAL/job_status.json"
  kubectl get pods -l job="$NAME" -o wide > "$LOCAL/pods.txt" 2>&1
  kubectl get nodes -o wide > "$LOCAL/nodes.txt" 2>&1
  kubectl logs -l job="$NAME" --tail=-1 --prefix 2>&1 | aws s3 cp - "$OUT/logs/$NAME.txt" --only-show-errors
  python3 - "$LOCAL" <<'EOF'
import datetime, json, sys
d = sys.argv[1]
s = json.load(open(f"{d}/job_status.json"))["status"]
t = lambda v: datetime.datetime.fromisoformat(v.replace("Z", "+00:00")) if v else None
start = t(s.get("startTime"))
end = t(s.get("completionTime")) or max([t(c.get("lastTransitionTime")) for c in s.get("conditions", [])] or [None])
wall = int((end - start).total_seconds()) if start and end else None
open(f"{d}/wall.txt", "w").write(f"wall seconds: {wall}\n")
cond = ",".join(c["type"] for c in s.get("conditions", []) if c.get("status") == "True")
print(f"  {d.split('eks-', 1)[1]}: {cond or 'not finished'}; {s.get('succeeded', 0)} pods succeeded, "
      f"{s.get('failed', 0)} failed; wall {wall / 3600 if wall else 0:.2f} h")
EOF
  kubectl delete job "$NAME" --wait=false >/dev/null && echo "  $NAME: status saved to $LOCAL, logs to $OUT/logs/$NAME.txt, job deleted"
done
