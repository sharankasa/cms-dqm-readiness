#!/usr/bin/env python3
"""
sample_run.py — the pilot pilot / the equivalence run S1 sample: choose the stays, build both sides' inputs in Athena, write
them to S3 per worker shard. Credentialed data: row-level output stays in S3; only masked aggregates
come back. Standard library + the AWS CLI (credentials from the shell, AWS_PROFILE=your-aws-profile).

  python3 pipeline/s1/sample_run.py --sample s1    --b4db dqm_prep --bundles <CMS816> <CMS871> --vs-cache vendor/vsac-cache
  python3 pipeline/s1/sample_run.py --sample pilot --b4db dqm_prep --bundles <CMS816> <CMS871> --vs-cache vendor/vsac-cache

Sides of the S1 comparison:
  sliced    = the stay's slice from the prepare stage (mapping + data-requirements filter + slicing), unchanged
  reference = the patient's FULL, UNFILTERED record with the same mapping (the prepare stage mapping SQL run again,
              restricted to the sampled patients, nothing filtered), evaluated per patient and
              discharge year by the unmodified engine (registration §4.3; the demo equivalence check design)
Also uploads the measure bundles and the VSAC cache (UMLS licence: private bucket, own use) to the run
prefix, so EKS workers need nothing else.

Selection rules: pipeline/s1/sql/10_select_s1.sql (2,000 stays) and 10_select_pilot.sql (1% of
patients). Seeds are fixed below and recorded in run.json.
"""
import argparse
import csv
import datetime
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(ROOT, "pipeline", "prepare"))
from athena_run import Athena, aws  # noqa: E402
from common import sql_file, vs_codes_from, vs_members_from  # noqa: E402
from prepare_run import first_code, glucose_rows, q, upload_tsv  # noqa: E402

SEEDS = {"s1": "dqm-s1-sample-20261010", "pilot": "dqm-pilot-20261010"}


def local_sql(rel, **params):
    """Read pipeline/s1/sql/<rel> with the same splitting rules as common.sql_file."""
    with open(os.path.join(HERE, "sql", rel)) as f:
        text = f.read()
    for k, v in params.items():
        text = text.replace("{" + k + "}", str(v))
    stmts, buf, parts, part = [], [], {}, "all"
    for line in text.splitlines():
        if line.strip().startswith("-- part "):
            part = line.strip()[3:]
            continue
        if line.strip().startswith("--"):
            continue
        buf.append(line)
        if line.rstrip().endswith(";"):
            parts.setdefault(part, []).append("\n".join(buf).strip().rstrip(";"))
            buf = []
    return parts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", choices=["s1", "pilot"], required=True)
    ap.add_argument("--b4db", default="dqm_prep", help="database of the the prepare stage run whose slices are used")
    ap.add_argument("--version", default="20261005")
    ap.add_argument("--bundles", nargs=2, required=True, metavar=("CMS816_BUNDLE", "CMS871_BUNDLE"))
    ap.add_argument("--vs-cache", required=True)
    ap.add_argument("--shards", type=int, default=64)
    ap.add_argument("--source-table", default="mimic_fhir.ndjson_parts")
    ap.add_argument("--bucket", default="your-bucket")
    ap.add_argument("--workgroup", default="primary")
    a = ap.parse_args()
    if not 1 <= a.shards <= 99:
        sys.exit("--shards must be 1..99")
    db = f"dqm_{a.sample}"
    seed = SEEDS[a.sample]
    run_id = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = os.path.join(ROOT, "results", f"{a.sample}-sample-{run_id}")
    os.makedirs(out, exist_ok=True)
    loc = f"s3://{a.bucket}/dqm/{a.sample}/{run_id}"

    ident = aws("sts", "get-caller-identity")
    print(f"AWS identity: {ident['Arn']}")
    wg = aws("athena", "get-work-group", "--work-group", a.workgroup)["WorkGroup"]
    cfg = wg.get("Configuration", {})
    if cfg.get("EnforceWorkGroupConfiguration"):
        sys.exit(f"workgroup {a.workgroup} enforces its settings")
    out_loc = None if cfg.get("ResultConfiguration", {}).get("OutputLocation") else f"s3://{a.bucket}/dqm/athena-results/"
    print(f"sample {a.sample}; database {db}; the prepare stage tables from {a.b4db}; run prefix {loc}; seed {seed}")

    cms816, cms871 = a.bundles
    obs_sys, obs_code, obs_disp = first_code(cms816, "ObservationServices")
    hos_sys, hos_code, hos_disp = first_code(cms871, "DischargedToHomeForHospiceCare")
    codes, _ = vs_codes_from(a.bundles, a.vs_cache)
    upload_tsv(glucose_rows(), f"{loc}/glucose_xw/glucose_xw.tsv")
    upload_tsv(codes, f"{loc}/vs_codes/vs_codes.tsv")
    upload_tsv(vs_members_from(a.bundles, a.vs_cache), f"{loc}/vs_members/vs_members.tsv")
    # engine inputs for the workers (measure bundles + VSAC expansions the bundles lack)
    for b in a.bundles:
        subprocess.run(["aws", "s3", "cp", b, f"{loc}/engine/bundles/{os.path.basename(b)}", "--only-show-errors"], check=True)
    subprocess.run(["aws", "s3", "cp", "--recursive", a.vs_cache, f"{loc}/engine/vsac-cache/", "--exclude", "*",
                    "--include", "*.json", "--only-show-errors"], check=True)

    at = Athena(a.workgroup, out_loc)
    t0 = time.time()
    run = lambda sql, label: at.run(sql, db=db, label=label)   # noqa: E731
    at.run(f"CREATE DATABASE IF NOT EXISTS {db}", label=f"create database {db}")
    run("DROP VIEW IF EXISTS src", "drop view src")
    for t in ["stay_flags", "sample_stays", "sample_patients", "glucose_xw", "vs_codes", "vs_members", "med_map",
              "enc_start", "enc_dx", "mapped", "ref_lines"]:
        run(f"DROP TABLE IF EXISTS {t}", f"drop table (metadata only) {t}")
    run(f"CREATE VIEW src AS SELECT source_file, line FROM {a.source_table}", f"src -> {a.source_table}")

    p = dict(loc=loc, b4db=a.b4db, seed=seed, shards=a.shards, version=a.version)
    for s in local_sql(f"10_select_{a.sample}.sql", **p)["all"]:
        run(s, f"select ({a.sample}): " + " ".join(s.split()[:3]))
    ref = local_sql("20_reference.sql", **p)
    for s in ref["part 1"]:
        run(s, "sample_patients")

    for s in sql_file("athena/prep_00_inputs.sql", **p):
        run(s, "prep_00: " + " ".join(s.split()[:4]))
    for s in sql_file("athena/prep_05_med_map_ndc.sql", **p):
        run(s, "prep_05: med_map")
    pat = ("regexp_extract(coalesce(json_extract_scalar(line, '$.subject.reference'), "
           "json_extract_scalar(line, '$.patient.reference'), json_extract_scalar(line, '$.beneficiary.reference')), "
           "'^Patient/(.+)$', 1)")
    source_filter = (f"((CASE WHEN source_file = 'MimicPatient' THEN json_extract_scalar(line, '$.id') ELSE {pat} END)"
                     f" IN (SELECT patient_id FROM sample_patients)"
                     f" OR (source_file <> 'MimicPatient' AND {pat} IS NULL))")
    mp = dict(p, source_filter=source_filter, keep_filter="TRUE", obs_code=q(obs_code), obs_display=q(obs_disp),
              hospice_system=q(hos_sys), hospice_code=q(hos_code), hospice_display=q(hos_disp))
    for s in sql_file("athena/prep_10_map.sql", **mp):
        run(s, "prep_10 (sampled patients, unfiltered): " + " ".join(s.split()[:3]))
    for s in ref["part 2"]:
        run(s, "ref_lines")

    for i, s in enumerate(local_sql("30_export.sql", **p)["all"], 1):
        run(s, f"UNLOAD {['manifest', 'sliced', 'shared', 'reference', 'shared_all'][i - 1]}")

    flags = "stay_flags" if a.sample == "s1" else "sample_stays"
    summary = {}
    for i, s in enumerate(local_sql("40_summary.sql", **dict(p, flags_relation=flags))["all"], 1):
        qid = run(s, f"summary {i}")
        hdr, rows = at.rows(qid)
        with open(os.path.join(out, f"summary_{i:02d}.csv"), "w", newline="") as f:
            csv.writer(f).writerows([hdr] + rows)
        summary[i] = rows
        print("     " + " | ".join(h or "" for h in hdr))
        for r in rows[:30]:
            print("     " + " | ".join("" if c is None else c for c in r))

    meta = {"sample": a.sample, "seed": seed, "database": db, "prep_database": a.b4db, "run_prefix": loc,
            "shards": a.shards, "worker_input": f"{loc}/export/", "engine_inputs": f"{loc}/engine/",
            "queries": at.n, "data_scanned_mb": round(at.scanned / 1e6, 1),
            "engine_seconds": round(at.engine_ms / 1000, 1), "wall_seconds": round(time.time() - t0, 1)}
    json.dump(meta, open(os.path.join(out, "run.json"), "w"), indent=2)
    print(json.dumps({k: meta[k] for k in ["run_prefix", "queries", "data_scanned_mb", "wall_seconds"]}, indent=1))
    print(f"results: {out}")


if __name__ == "__main__":
    main()
