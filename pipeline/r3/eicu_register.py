#!/usr/bin/env python3
"""
eicu_register.py: R3 step 1. Makes the eICU-CRD 2.0 tables that R3 needs queryable in Athena (database dqm_eicu).

The raw files sit together under s3://<bucket>/raw/eicu-crd/2.0/; an Athena table needs a folder of its own, so
each needed file is copied (server-side, same bucket, data stays in S3) to s3://<bucket>/dqm/eicu/tables/<name>/.
Column names come from each file's own header (read from the first compressed bytes), so no schema is guessed;
every column is a string. Prints table names and column counts only: no data rows.

  export AWS_PROFILE=your-aws-profile
  python3 pipeline/r3/eicu_register.py
  python3 pipeline/r3/eicu_register.py --dry-run      (list what would be copied and created)

Then: python3 pipeline/athena/run_sql.py --sql pipeline/r3/sql/10_eicu_completeness.sql --db dqm_eicu \
        --out results/r3-eicu-<stamp> --param loc=s3://<bucket>/dqm/eicu/r3-<stamp>
"""
import argparse
import json
import os
import subprocess
import sys
import tempfile
import zlib

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "prepare"))
from athena_run import Athena, aws  # noqa: E402

NEEDED = {   # table name -> columns R3's SQL uses (checked against the header before anything is created)
    "patient": ["patientunitstayid", "hospitalid", "age"],
    "lab": ["patientunitstayid", "labname", "labresult", "labmeasurenamesystem"],
    "medication": ["patientunitstayid", "drugname", "drugordercancelled"],
    "infusiondrug": ["patientunitstayid", "drugname"],
    "diagnosis": ["patientunitstayid", "diagnosisstring", "icd9code"],
    "pasthistory": ["patientunitstayid", "pasthistorypath"],
    "careplangeneral": ["patientunitstayid", "cplgroup", "cplitemvalue"],
    "careplaneol": ["patientunitstayid"],
}


def header_of(bucket, key):
    with tempfile.NamedTemporaryFile() as tf:
        subprocess.run(["aws", "s3api", "get-object", "--bucket", bucket, "--key", key, "--range", "bytes=0-262143",
                        tf.name], check=True, capture_output=True)
        raw = open(tf.name, "rb").read()
    text = zlib.decompressobj(16 + zlib.MAX_WBITS).decompress(raw).decode("utf-8", "replace")
    return [c.strip().strip('"').lower() for c in text.splitlines()[0].split(",")]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", default="your-bucket")
    ap.add_argument("--raw-prefix", default="raw/eicu-crd/2.0/")
    ap.add_argument("--dest-prefix", default="dqm/eicu/tables/")
    ap.add_argument("--db", default="dqm_eicu")
    ap.add_argument("--workgroup", default="primary")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    ident = aws("sts", "get-caller-identity")
    print(f"account {ident['Account']}; bucket {a.bucket}")
    objs = aws("s3api", "list-objects-v2", "--bucket", a.bucket, "--prefix", a.raw_prefix).get("Contents", [])
    by_name = {}
    for o in objs:
        base = os.path.basename(o["Key"])
        if base.lower().endswith(".csv.gz"):
            by_name[base[:-7].lower()] = (o["Key"], o["Size"])
    missing = [t for t in NEEDED if t not in by_name]
    if missing:
        sys.exit(f"not found under s3://{a.bucket}/{a.raw_prefix}: {missing}; present: {sorted(by_name)}")

    wg = aws("athena", "get-work-group", "--work-group", a.workgroup)["WorkGroup"]
    has_loc = bool(wg.get("Configuration", {}).get("ResultConfiguration", {}).get("OutputLocation"))
    at = Athena(a.workgroup, None if has_loc else f"s3://{a.bucket}/dqm/athena-results/")
    if not a.dry_run:
        at.run(f"CREATE DATABASE IF NOT EXISTS {a.db}", label=f"database {a.db}")
    summary = {}
    for t, cols_needed in NEEDED.items():
        key, size = by_name[t]
        dest_dir = f"{a.dest_prefix}{t}/"
        dest_key = dest_dir + os.path.basename(key)
        cols = header_of(a.bucket, key)
        lacking = [c for c in cols_needed if c not in cols]
        if lacking:
            sys.exit(f"{t}: header lacks {lacking}; header columns: {cols}")
        have = aws("s3api", "list-objects-v2", "--bucket", a.bucket, "--prefix", dest_dir).get("Contents", [])
        print(f"{t:<16} {size / 1e6:9.1f} MB  {len(cols):3d} columns  {'copied earlier' if have else 'copy needed'}")
        summary[t] = {"source": key, "columns": cols, "bytes": size}
        if a.dry_run:
            continue
        if not have:
            subprocess.run(["aws", "s3", "cp", f"s3://{a.bucket}/{key}", f"s3://{a.bucket}/{dest_key}", "--only-show-errors"],
                           check=True)
        coldefs = ",\n  ".join(f"`{c}` string" for c in cols)
        at.run(f"DROP TABLE IF EXISTS {a.db}.{t}", label=f"drop {t} (metadata only)")
        at.run(f"""CREATE EXTERNAL TABLE {a.db}.{t} (
  {coldefs})
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\\\')
LOCATION 's3://{a.bucket}/{dest_dir}'
TBLPROPERTIES ('skip.header.line.count' = '1')""", label=f"table {a.db}.{t}")
    out = os.path.join(HERE, "..", "..", "results", "r3-eicu-tables.json")
    if not a.dry_run:
        json.dump({"db": a.db, "tables": summary}, open(out, "w"), indent=1)
        print(f"done: {len(summary)} tables in {a.db}; {at.n} Athena statements; columns saved to {os.path.relpath(out)}")


if __name__ == "__main__":
    main()
