#!/bin/bash
# test_local.sh — the EKS worker IMAGE on a workstation (Docker Desktop), on OPEN demo data, with S3 replaced
# by a local folder (S3_LOCAL_ROOT). Catches image problems before any EKS cost. No AWS calls.
#
#   cd ".../project2-dqm-infrastructure"
#   bash pipeline/worker/eks/test_local.sh
#
# Input: the the prepare stage open-demo export (results/prep-demo-raw-*/export) for the sliced side, and
# the demo's whole-patient records (work/demo-prep-v3/patients) for the reference side, both for
# shard 12 (16 stays). Runs both sides in the container, then a resume check (rerun = nothing to do),
# then compare.py. Expected: S1 holds (16/16 per measure), exactly once.
set -u
cd "$(dirname "$0")/../../.." || exit 1
the prepare stage=$(ls -d results/prep-demo-raw-*/export 2>/dev/null | tail -1)
[ -n "$the prepare stage" ] || { echo "no the prepare stage demo export found (run prepare_run.py --mode demo first). Stopping."; exit 1; }
docker info >/dev/null 2>&1 || { echo "Docker Desktop is not running. Stopping."; exit 1; }
T=work/eks-local-test
rm -rf "$T"; mkdir -p "$T/s3/bkt/run/export" "$T/s3/bkt/run/engine/bundles" "$T/s3/bkt/run/engine/vsac-cache"
cp -R "$the prepare stage/manifest" "$the prepare stage/slices" "$the prepare stage/shared" "$T/s3/bkt/run/export/"
cp vendor/dqm-content/bundles/measure/CMS816FHIRHHHypo/CMS816FHIRHHHypo-bundle.json \
   vendor/dqm-content/bundles/measure/CMS871FHIRHHHyper/CMS871FHIRHHHyper-bundle.json "$T/s3/bkt/run/engine/bundles/"
cp vendor/vsac-cache/*.json "$T/s3/bkt/run/engine/vsac-cache/"
python3 - "$T" <<'EOF'
import glob, gzip, os, shutil, sys
t = sys.argv[1]
pids = set()
for f in glob.glob(f"{t}/s3/bkt/run/export/manifest/shard=12/*"):
    for l in gzip.open(f, "rt"):
        if l.strip():
            pids.add(l.split(",")[1])
os.makedirs(f"{t}/s3/bkt/run/export/reference/shard=12", exist_ok=True)
os.makedirs(f"{t}/s3/bkt/run/export/shared_all", exist_ok=True)
with gzip.open(f"{t}/s3/bkt/run/export/reference/shard=12/part.gz", "wt") as out:
    for p in sorted(pids):
        for l in gzip.open(f"work/demo-prep-v3/patients/{p}.ndjson.gz", "rt"):
            if l.strip():
                out.write(p + "\t" + l.rstrip("\n") + "\n")
shutil.copy("work/demo-prep-v3/shared_all.ndjson.gz", f"{t}/s3/bkt/run/export/shared_all/part.gz")
print(f"reference input: {len(pids)} patients")
EOF
docker build -q -t dqm-worker-eks:test -f pipeline/worker/eks/Dockerfile . >/dev/null || { echo "image build failed"; exit 1; }
for MODE in sliced reference sliced; do
  docker run --rm -v "$PWD/$T/s3:/s3" -e S3_LOCAL_ROOT=/s3 -e MODE=$MODE -e SHARD=12 -e INPUT=s3://bkt/run/export \
    -e ENGINE=s3://bkt/run/engine -e OUT=s3://bkt/run/runs/test dqm-worker-eks:test 2>&1 | grep -v 'Failed to locate' | grep 'worker'
done
python3 pipeline/s1/compare.py --reference "$T/s3/bkt/run/runs/test/results/reference_12.jsonl" \
  --sliced "$T/s3/bkt/run/runs/test/results/sliced_12.jsonl" --out "$T" --label docker | tail -30
# V (independent implementation) on the reference input, then the in-pod comparison (no population counts)
docker run --rm -v "$PWD/$T/s3:/s3" -e S3_LOCAL_ROOT=/s3 -e MODE=v -e SHARD=12 -e INPUT=s3://bkt/run/export \
  -e ENGINE=s3://bkt/run/engine -e OUT=s3://bkt/run/runs/test dqm-worker-eks:test 2>&1 | grep 'worker'
docker run --rm -v "$PWD/$T/s3:/s3" -e S3_LOCAL_ROOT=/s3 -e MODE=compare-v -e ENGINE_OUT=s3://bkt/run/runs/test \
  -e V_OUT=s3://bkt/run/runs/test -e OUT=s3://bkt/run/runs/cmpv -e NO_POP_COUNTS=1 -e LABEL=demo dqm-worker-eks:test 2>&1 | tail -14
