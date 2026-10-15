#!/bin/bash
# run_s1_demo.sh — the demo equivalence check / S1 on the OPEN MIMIC demo, on a workstation with Docker Desktop. Open data only.
#
#   cd ".../project2-dqm-infrastructure"
#   bash pipeline/s1/run_s1_demo.sh                # SHARDS=4 by default
#   SHARDS=6 bash pipeline/s1/run_s1_demo.sh       # more parallel workers if Docker has the memory
#
# Runs CMS's reference engine (fqm-execution, unmodified) in a throwaway node:22-slim container:
#   1. sliced    : one bundle per stay (fast, ~1-2 min)
#   2. reference : every patient's full, unfiltered record per discharge year (SLOW: hours; leave it)
# Each worker needs up to ~4 GB: Docker Desktop > Settings > Resources > Memory should be at least
# SHARDS x 4 GB. Safe to stop (Ctrl+C) and rerun: finished stays are skipped.
# Input: work/demo-prep (written by pipeline/prepare/slice_stays.py --whole-patient --filter).
# Output: results/s1-demo/ (compare the two sides with pipeline/s1/compare.py).
set -u
cd "$(dirname "$0")/../.." || exit 1     # -> project root
ROOT=$(pwd)
SHARDS=${SHARDS:-4}
PREP=${PREP:-work/demo-prep}            # e.g. PREP=work/demo-prep-v3 (CMS871 mapping)
OUT=${OUT:-results/s1-demo}       # e.g. OUT=results/s1-demo-v3
B=vendor/dqm-content/bundles/measure
BUN="$B/CMS816FHIRHHHypo/CMS816FHIRHHHypo-bundle.json $B/CMS871FHIRHHHyper/CMS871FHIRHHHyper-bundle.json"

docker info >/dev/null 2>&1 || { echo "Docker Desktop is not running. Start it (open -a Docker), wait ~30 s, rerun."; exit 1; }
[ -f "$PREP/manifest.csv" ] && [ -d "$PREP/patients" ] || { echo "Missing $PREP (regenerate it with pipeline/prepare). Stopping."; exit 1; }
for f in 2.16.840.1.113762.1.4.1099.24.json 2.16.840.1.113762.1.4.1196.394.json; do
  [ -f "vendor/vsac-cache/$f" ] || { echo "Missing VSAC cache file $f. Stopping."; exit 1; }
done
mkdir -p "$OUT"
cp "$PREP/run.json" "$OUT/prep_run.json"
echo "=== S1 demo: $SHARDS workers; output $OUT; started $(date)"

docker run --rm -v "$ROOT":/w -w /w node:22-slim sh -c "
  cd testcases && npm install --silent --no-audit --no-fund >/dev/null 2>&1; cd /w
  echo '--- sliced'
  i=0; while [ \$i -lt $SHARDS ]; do
    node --max-old-space-size=4096 pipeline/s1/run_engine.js --mode sliced --prep $PREP --out $OUT/sliced_\$i.jsonl \
      --shard \$i/$SHARDS --bundles $BUN --vs-cache vendor/vsac-cache 2>&1 | grep -v 'Failed to locate' > $OUT/sliced_\$i.log &
    i=\$((i+1)); done; wait; tail -qn1 $OUT/sliced_*.log
  echo '--- reference (slow; progress: tail -f $OUT/reference_*.log in another Terminal)'
  i=0; while [ \$i -lt $SHARDS ]; do
    node --max-old-space-size=4096 pipeline/s1/run_engine.js --mode reference --prep $PREP --out $OUT/reference_\$i.jsonl \
      --shard \$i/$SHARDS --bundles $BUN --vs-cache vendor/vsac-cache 2>&1 | grep -v 'Failed to locate' > $OUT/reference_\$i.log &
    i=\$((i+1)); done; wait; tail -qn1 $OUT/reference_*.log
"
echo "=== finished $(date). Next: compare the two sides (pipeline/s1/compare.py)."
