#!/bin/bash
# fetch_rxnorm.sh — the medication mapping: get the current RxNorm Full Monthly Release with NLM's documented APIs and put
# the three tables the medication mapping needs into S3 (your account only). No manual download.
#
#   cd ".../project2-dqm-infrastructure"
#   eval "$(aws configure export-credentials --profile your-aws-profile --format env)"
#   export AWS_DEFAULT_REGION=us-west-2
#   bash pipeline/terminology/fetch_rxnorm.sh
#
# 1. Release API (public): which file is current            https://uts-ws.nlm.nih.gov/releases
# 2. Download API (UMLS API key, typed at a hidden prompt, never stored or echoed)
#                                                           https://uts-ws.nlm.nih.gov/download
# 3. Keeps RXNCONSO.RRF (names, term types), RXNSAT.RRF (NDC attributes), RXNREL.RRF (relationships,
#    e.g. tradename_of for brand -> generic) and uploads them to
#    s3://<bucket>/dqm/ref/rxnorm/<version>/<table>/  (one prefix per table, for Athena)
# 4. Writes a manifest (version, file names, sizes, SHA-256) next to them and locally in
#    results/rxnorm-<version>.json. Deletes its temporary download folder.
# Licensed content (UMLS licence): never committed, never redistributed, stays in your account.
# Why not the RxNav web API: it would send MIMIC-derived NDC lists to a public service, and its
# answers follow whatever RxNorm version is live; a release file is pinned and stays inside AWS.
set -euo pipefail
B=${B:-s3://your-bucket}
aws sts get-caller-identity >/dev/null 2>&1 || { echo "No AWS credentials in this shell (eval the your-aws-profile export first)."; exit 1; }

REL=$(curl -sf "https://uts-ws.nlm.nih.gov/releases?releaseType=rxnorm-full-monthly-release&current=true")
read -r FILE VER URL < <(python3 -c 'import json,sys; r=json.loads(sys.argv[1])[0]; print(r["fileName"], r["releaseVersion"], r["downloadUrl"])' "$REL")
echo "current RxNorm Full Monthly Release: $FILE (version $VER)"
DEST="$B/dqm/ref/rxnorm/$VER"
if aws s3 ls "$DEST/manifest.json" >/dev/null 2>&1; then
  echo "already in S3: $DEST (manifest present). Nothing to do."; exit 0
fi

read -r -s -p "UMLS API key (input hidden): " KEY; echo
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
echo "downloading (a few hundred MB)..."
curl -sfL "https://uts-ws.nlm.nih.gov/download?url=${URL}&apiKey=${KEY}" -o "$TMP/$FILE" || { echo "download failed (check the key)"; exit 1; }
unset KEY
unzip -tq "$TMP/$FILE" >/dev/null || { echo "not a valid zip (the API may have returned an error page)"; exit 1; }
unzip -q -j "$TMP/$FILE" "rrf/RXNCONSO.RRF" "rrf/RXNSAT.RRF" "rrf/RXNREL.RRF" -d "$TMP/rrf"

MAN="$TMP/manifest.json"
python3 - "$TMP/rrf" "$FILE" "$VER" "$URL" > "$MAN" <<'EOF'
import hashlib, json, os, sys, datetime
d, f, v, u = sys.argv[1:5]
files = {}
for n in sorted(os.listdir(d)):
    h = hashlib.sha256(open(os.path.join(d, n), "rb").read()).hexdigest()
    files[n] = {"bytes": os.path.getsize(os.path.join(d, n)), "sha256": h}
print(json.dumps({"release_file": f, "release_version": v, "download_url": u, "files": files,
                  "fetched_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")}, indent=2))
EOF
for t in RXNCONSO RXNSAT RXNREL; do
  aws s3 cp "$TMP/rrf/$t.RRF" "$DEST/$(echo $t | tr '[:upper:]' '[:lower:]')/$t.RRF" --only-show-errors
done
aws s3 cp "$MAN" "$DEST/manifest.json" --only-show-errors
mkdir -p results
cp "$MAN" "results/rxnorm-$VER.json"
echo "done: $DEST"
cat "$MAN"
