#!/bin/bash
# nodes.sh — S2: set one EKS node group to N nodes and wait until exactly N of its nodes are Ready (N = 0:
# until none is left). Used for COLD runs (registration §4.3.1: fresh instances and containers): the group goes
# to 0 and back to N, so every node is a new instance without the worker image cached.
#
#   cd ".../project2-dqm-infrastructure"; export AWS_PROFILE=your-aws-profile
#   bash pipeline/s2/nodes.sh dqm-eks-workers 0
#   bash pipeline/s2/nodes.sh dqm-eks-workers 8
#   RECORD=results/s2-<plan>/nodes.jsonl bash pipeline/s2/nodes.sh dqm-eks-workers 8
#
# Prints and (RECORD=...) appends one JSON line: group, target, seconds until Ready, instance ids. Before
# scaling a group to 0 it checks that another node is Ready (cluster DNS, CoreDNS, must keep a home; if
# none is, it says so and CoreDNS waits for the new nodes). After scaling up it waits for CoreDNS to be
# Ready, so the first pods can resolve S3. The watchdog (infra/eks/watchdog.tf) keeps guarding cost:
# its estimate follows the node groups' desired sizes.
set -u
cd "$(dirname "$0")/../.." || exit 1
GROUP=${1:?usage: nodes.sh <node group> <count>}; N=${2:?usage: nodes.sh <node group> <count>}
CLUSTER=${CLUSTER:-dqm-eks}; REGION=${REGION:-us-west-2}; TIMEOUT=${TIMEOUT:-1200}
CTX=$(kubectl config current-context 2>/dev/null)
case "$CTX" in *dqm-eks*) ;; *) echo "kubectl context is '$CTX', expected the dqm-eks cluster. Stopping."; exit 1;; esac
ready() { kubectl get nodes -l "eks.amazonaws.com/nodegroup=$GROUP" --no-headers 2>/dev/null | grep -c ' Ready'; }
present() { kubectl get nodes -l "eks.amazonaws.com/nodegroup=$GROUP" --no-headers 2>/dev/null | wc -l | tr -d ' '; }
SC=$(aws eks describe-nodegroup --cluster-name "$CLUSTER" --nodegroup-name "$GROUP" --region "$REGION" \
       --query 'nodegroup.scalingConfig.[desiredSize,maxSize]' --output text) || { echo "no node group $GROUP"; exit 1; }
CUR=$(echo "$SC" | awk '{print $1}'); MAX=$(echo "$SC" | awk '{print $2}')
[ "$N" -gt "$MAX" ] && MAX=$N
[ "$MAX" -lt 1 ] && MAX=1
if [ "$N" = 0 ]; then
  OTHER=$(kubectl get nodes --no-headers -l "eks.amazonaws.com/nodegroup!=$GROUP" 2>/dev/null | grep -c ' Ready')
  [ "$OTHER" -ge 1 ] || echo "note: no Ready node outside $GROUP: CoreDNS will wait for the next nodes"
  BUSY=$(kubectl get pods -l app=dqm-worker --field-selector=status.phase=Running \
           -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' 2>/dev/null | grep -v '^$' \
         | while read -r n; do kubectl get node "$n" -o jsonpath='{.metadata.labels.eks\.amazonaws\.com/nodegroup}' 2>/dev/null; echo; done \
         | grep -c "^$GROUP$")
  [ "$BUSY" = 0 ] || { echo "$BUSY worker pod(s) still running on $GROUP: not scaling it to 0. Stopping."; exit 1; }
fi
T0=$(date +%s)
echo "$(date -u +%FT%TZ) $GROUP: desired $CUR -> $N (max $MAX)"
aws eks update-nodegroup-config --cluster-name "$CLUSTER" --nodegroup-name "$GROUP" --region "$REGION" \
  --scaling-config "minSize=0,maxSize=$MAX,desiredSize=$N" >/dev/null || exit 1
while true; do
  r=$(ready); p=$(present); el=$(( $(date +%s) - T0 ))
  if [ "$N" = 0 ] && [ "$p" = 0 ]; then break; fi
  if [ "$N" -gt 0 ] && [ "$r" = "$N" ] && [ "$p" = "$N" ]; then break; fi
  [ "$el" -ge "$TIMEOUT" ] && { echo "timeout after ${el}s: $r Ready, $p present (target $N). Stopping."; exit 2; }
  sleep 15
done
if [ "$N" -gt 0 ]; then
  kubectl -n kube-system wait --for=condition=Ready pod -l k8s-app=kube-dns --timeout=300s >/dev/null 2>&1 || echo "note: CoreDNS not Ready yet"
  kubectl -n kube-system wait --for=condition=Ready pod -l app.kubernetes.io/name=eks-pod-identity-agent --timeout=300s >/dev/null 2>&1 || true
fi
EL=$(( $(date +%s) - T0 ))
IDS=$(kubectl get nodes -l "eks.amazonaws.com/nodegroup=$GROUP" -o jsonpath='{range .items[*]}{.spec.providerID}{" "}{end}' 2>/dev/null | sed 's#[^ ]*/##g' | xargs)
LINE="{\"utc\":\"$(date -u +%FT%TZ)\",\"group\":\"$GROUP\",\"from\":$CUR,\"to\":$N,\"seconds\":$EL,\"instances\":\"$IDS\"}"
echo "$LINE"
[ -n "${RECORD:-}" ] && { mkdir -p "$(dirname "$RECORD")"; echo "$LINE" >> "$RECORD"; }
exit 0
