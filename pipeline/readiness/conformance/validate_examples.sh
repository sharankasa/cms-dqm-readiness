#!/bin/bash
# validate_examples.sh: would a FHIR profile validator flag the records whose problems changed the measure
# results? Validates example records from the mapped OPEN demo (make_examples.py) against QI-Core 6.0.0 with
# the HL7 FHIR validator. Runs on a workstation (needs Java 11+ and internet for the validator and its packages).
#
#   cd ".../project2-dqm-infrastructure"; bash pipeline/readiness/conformance/validate_examples.sh
#
# Terminology server off (-tx n/a): no record content leaves the machine; only the validator jar and the FHIR
# packages are downloaded. Code-binding checks are therefore not run; structure, cardinality and invariants
# (FHIRPath constraints such as US Core's UCUM rule) are. Writes results/conformance-<stamp>/.
set -euo pipefail
cd "$(dirname "$0")/../../.." || exit 1
JAR_DIR="$HOME/.cache/fhir-validator"; JAR="$JAR_DIR/validator_cli.jar"
JV=$(java -version 2>&1 | awk -F'"' '/version/ {print $2}' | cut -d. -f1) || true
if [ -z "${JV:-}" ] || [ "$JV" -lt 11 ] 2>/dev/null; then
  echo "Java 11 or newer is needed (found: ${JV:-none}). Install: brew install openjdk@17, then follow brew's PATH hint. Stopping."
  exit 1
fi
mkdir -p "$JAR_DIR"
if [ ! -s "$JAR" ]; then
  echo "downloading the HL7 FHIR validator to $JAR"
  curl -fsSL -o "$JAR" https://github.com/hapifhir/org.hl7.fhir.core/releases/latest/download/validator_cli.jar
fi
STAMP=$(date -u +%Y%m%dT%H%M%SZ); OUT="results/conformance-$STAMP"
python3 pipeline/readiness/conformance/make_examples.py --out "$OUT"
echo "validating (first run downloads the QI-Core 6.0.0 package and its dependencies; a few minutes)"
set +e
java -jar "$JAR" "$OUT/examples" -version 4.0.1 -ig hl7.fhir.us.qicore#6.0.0 -tx n/a \
  -output "$OUT/validation.json" > "$OUT/validator.log" 2>&1
RC=$?
set -e
shasum -a 256 "$JAR" | awk '{print $1}' > "$OUT/validator_sha256.txt"
grep -m1 -i "FHIR Validation tool Version" "$OUT/validator.log" > "$OUT/validator_version.txt" || true
echo "validator exit code $RC (non-zero means at least one error); log $OUT/validator.log"
python3 - "$OUT" <<'EOF'
import json, os, sys
out = sys.argv[1]
v = json.load(open(os.path.join(out, "validation.json")))
oos = [e["resource"] for e in v.get("entry", [])] if v.get("resourceType") == "Bundle" else [v]
FILE = "http://hl7.org/fhir/StructureDefinition/operationoutcome-file"
lines = ["# Profile validation of example records (QI-Core 6.0.0, HL7 FHIR validator, terminology off)", "",
         open(os.path.join(out, "validator_version.txt")).read().strip(), "",
         "| Example | Errors | Warnings | Error messages |", "|---|---:|---:|---|"]
for oo in oos:
    f = next((x.get("valueString") for x in oo.get("extension", []) if x.get("url") == FILE), "?")
    iss = oo.get("issue", [])
    err = [i for i in iss if i.get("severity") in ("error", "fatal")]
    warn = [i for i in iss if i.get("severity") == "warning"]
    msgs = "; ".join(sorted({(i.get("details", {}).get("text") or i.get("diagnostics") or "")[:220] for i in err}))
    lines.append(f"| {os.path.basename(f).replace('.json', '')} | {len(err)} | {len(warn)} | {msgs} |")
open(os.path.join(out, "summary.md"), "w").write("\n".join(lines) + "\n")
print("\n".join(lines))
EOF
echo "summary: $OUT/summary.md"
