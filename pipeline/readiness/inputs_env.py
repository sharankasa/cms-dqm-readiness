#!/usr/bin/env python3
"""
inputs_env.py — shell exports for the newest engine-input sets built by build_inputs.py, so the run
commands can use names instead of long S3 paths. Reads results/readiness-inputs-*/inputs.json
(local files, no AWS). For each set (newest build wins), e.g. cohort-a:
  COHORT_A_INPUT   worker input (export/)          COHORT_A_ROOT   run root (results go to <root>/runs/)
  COHORT_A_TABLE   Athena set table                COHORT_A_V_INPUT  V's one-stay-one-record input (if built)

  cd ".../project2-dqm-infrastructure"
  eval "$(python3 pipeline/readiness/inputs_env.py)"
  python3 pipeline/readiness/inputs_env.py --show      # list what would be exported
"""
import glob
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def main():
    found = {}
    for f in sorted(glob.glob(os.path.join(ROOT, "results", "readiness-inputs-*", "inputs.json"))):
        for s in json.load(open(f)).get("sets", []):
            if s.get("stays"):
                found[s["set"]] = s
    if not found:
        sys.exit("no readiness-inputs-*/inputs.json with sets: run pipeline/readiness/build_inputs.py first")
    lines = []
    for name, s in sorted(found.items()):
        k = name.upper().replace("-VS-A", "").replace("-", "_")
        root = s["input"].rsplit("/", 1)[0]
        lines += [f"export {k}_INPUT={s['input']}", f"export {k}_ROOT={root}", f"export {k}_TABLE={s['athena_table']}"]
        if s.get("v_input"):
            lines.append(f"export {k}_V_INPUT={s['v_input']}")
    if "--show" in sys.argv:
        for name, s in sorted(found.items()):
            print(f"{name}: {s['stays']} stays, {s['shards']} shards, the prepare stage {s['prep_database']}, input {s['input']}", file=sys.stderr)
    print("\n".join(lines))


if __name__ == "__main__":
    main()
