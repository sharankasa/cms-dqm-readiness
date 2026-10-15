#!/usr/bin/env python3
"""
prepare_run.py — the prepare stage: the full prepare stage in Athena: baseline mapping -> stay slicing with the
data-requirements filter -> per-stay manifest with the workload metric -> worker input per shard.
Standard library + the AWS CLI only (AWS credentials from the shell, e.g. AWS_PROFILE=your-aws-profile).

  OPEN DEMO (validation; downloads its outputs):
    python3 pipeline/prepare/prepare_run.py --mode demo --label demo-raw \
        --input vendor/mimic-iv-fhir-demo/mimic-iv-clinical-database-demo-on-fhir-2.1.0/fhir \
        --bundles <CMS816 bundle> <CMS871 bundle> --vs-cache vendor/vsac-cache
  CREDENTIALED (everything row-level stays in S3; only masked aggregates come back):
    python3 pipeline/prepare/prepare_run.py --mode full --label mimic-fhir-2.1 --version 20261005 \
        --bundles <CMS816 bundle> <CMS871 bundle> --vs-cache vendor/vsac-cache

SQL (in order): sql/athena/prep_00_inputs.sql, prep_05_med_map_{crosswalk|b2}.sql, prep_10_map.sql, then the
the two-backend slicing check slicing SQL unchanged (sql/athena/10_load_index.sql without its two input tables, and the SQLGlot
translation of sql/core/20_slice.sql + 30_filter_hash.sql), then prep_40_manifest.sql,
prep_45_summary.sql (aggregates), prep_50_export.sql (+ prep_55_export_mapped_open.sql on the demo).
Nothing is overwritten or deleted: every run writes under a new prefix s3://<bucket>/dqm/prep/prep-<label>-<run>/
(DROP TABLE removes catalog metadata only).
"""
import argparse
import csv
import datetime
import json
import os
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, HERE)
from athena_run import Athena, aws  # noqa: E402
from common import sql_file, vs_codes_from, vs_members_from  # noqa: E402

XW = os.path.join(ROOT, "crosswalk")
MIMIC_CS = "http://mimic.mit.edu/fhir/mimic/CodeSystem/"
NOT_RETRIEVED = ["MimicMedicationRequest", "MimicMedicationDispense", "MimicMedicationDispenseED",
                 "MimicMedicationStatementED", "MimicSpecimen", "MimicSpecimenLab"]
TABLES = ["src_raw", "glucose_xw", "vs_codes", "vs_members", "med_xw", "med_map", "enc_start", "enc_dx", "mapped",
          "res", "res_times", "res_index", "res_codes", "units", "unit_bound", "members", "shared", "slice",
          "slice_hash", "stay_manifest"]
VIEWS = ["raw_ndjson", "cohort"]


def q(s):
    """SQL string-literal content (single quotes doubled)."""
    return str(s).replace("'", "''")


def first_code(bundle_path, vs_name):
    for e in json.load(open(bundle_path))["entry"]:
        r = e["resource"]
        if r["resourceType"] == "ValueSet" and r.get("name") == vs_name:
            c = r["expansion"]["contains"][0]
            return c["system"], c["code"], c.get("display", "")
    sys.exit(f"{vs_name} value set not found in {bundle_path}")


def glucose_rows():
    out = []
    for row in csv.DictReader(open(os.path.join(XW, "glucose_crosswalk.csv"))):
        if "EXCLUDE" in row["note"]:
            continue
        out.append((row["source_table"], row["source_itemid"], row["target_code"], row["target_display"]))
    return out


def med_xw_rows():
    return [(MIMIC_CS + r["source_system"], r["source_code"], r["target_code"], r["target_display"])
            for r in csv.DictReader(open(os.path.join(XW, "medication_crosswalk_demo.csv")))]


def upload_tsv(rows, dest):
    with tempfile.NamedTemporaryFile("w", suffix=".tsv", delete=False) as f:
        for r in rows:
            f.write("\t".join(str(x).replace("\t", " ").replace("\n", " ") for x in r) + "\n")
        path = f.name
    subprocess.run(["aws", "s3", "cp", path, dest, "--only-show-errors"], check=True)
    os.remove(path)   # terminology content (VSAC-derived): not kept on disk


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["demo", "full"], required=True)
    ap.add_argument("--label", required=True)
    ap.add_argument("--input", help="demo: local dir of the OPEN MIMIC-FHIR demo *.ndjson.gz (uploaded)")
    ap.add_argument("--source-table", default="mimic_fhir.ndjson_parts", help="full: (source_file, line) relation")
    ap.add_argument("--version", default="20261005", help="full: RxNorm release of the the medication mapping tables")
    ap.add_argument("--bundles", nargs=2, required=True, metavar=("CMS816_BUNDLE", "CMS871_BUNDLE"))
    ap.add_argument("--vs-cache", required=True)
    ap.add_argument("--shards", type=int, default=64)
    ap.add_argument("--db")
    ap.add_argument("--bucket", default="your-bucket")
    ap.add_argument("--workgroup", default="primary")
    ap.add_argument("--out")
    ap.add_argument("--med-map", choices=["b2", "audited"], default="b2",
                    help="full: medication mapping: b2 = NDC path before the audit; audited = after the medication audit (41_audit_apply.sql)")
    ap.add_argument("--variant", choices=["a", "b", "c", "d"], default="a",
                    help="--med-map audited: registered mapping variant (a baseline, b conservative, c liberal, "
                         "d model-proposed)")
    ap.add_argument("--audit-round", default="r1", help="--med-map audited: audit round of dqm_map.med_additions_<round>")
    a = ap.parse_args()
    if a.mode == "demo" and not a.input:
        sys.exit("--mode demo needs --input (the open demo folder)")
    if not 1 <= a.shards <= 99:
        sys.exit("--shards must be 1..99 (Athena UNLOAD partition limit)")
    db = a.db or ("dqm_prep_demo" if a.mode == "demo" else "dqm_prep")

    run_id = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = a.out or os.path.join(ROOT, "results", f"prep-{a.label}-{run_id}")
    os.makedirs(out, exist_ok=True)
    loc = f"s3://{a.bucket}/dqm/prep/prep-{a.label}-{run_id}"

    ident = aws("sts", "get-caller-identity")
    print(f"AWS identity: {ident['Arn']}")
    wg = aws("athena", "get-work-group", "--work-group", a.workgroup)["WorkGroup"]
    cfg = wg.get("Configuration", {})
    engine = cfg.get("EngineVersion", {}).get("EffectiveEngineVersion", "?")
    if cfg.get("EnforceWorkGroupConfiguration"):
        sys.exit(f"workgroup {a.workgroup} enforces its settings: CTAS with external_location would fail")
    if "3" not in engine:
        sys.exit(f"workgroup {a.workgroup} runs {engine}; engine v3 is required")
    out_loc = None if cfg.get("ResultConfiguration", {}).get("OutputLocation") else f"s3://{a.bucket}/dqm/athena-results/"
    print(f"mode {a.mode}; database {db}; run prefix {loc}")

    # reference inputs (terminology only)
    cms816, cms871 = a.bundles
    obs_sys, obs_code, obs_disp = first_code(cms816, "ObservationServices")
    hos_sys, hos_code, hos_disp = first_code(cms871, "DischargedToHomeForHospiceCare")
    codes, _ = vs_codes_from(a.bundles, a.vs_cache)
    members = vs_members_from(a.bundles, a.vs_cache)
    upload_tsv(glucose_rows(), f"{loc}/glucose_xw/glucose_xw.tsv")
    upload_tsv(codes, f"{loc}/vs_codes/vs_codes.tsv")
    upload_tsv(members, f"{loc}/vs_members/vs_members.tsv")
    if a.mode == "demo":
        upload_tsv(med_xw_rows(), f"{loc}/med_xw/med_xw.tsv")

    at = Athena(a.workgroup, out_loc)
    t0 = time.time()
    run = lambda sql, label=None: at.run(sql, db=db, label=label)   # noqa: E731
    at.run(f"CREATE DATABASE IF NOT EXISTS {db}", label=f"create database {db}")
    for v in VIEWS + (["src"] if a.mode == "full" else []):
        run(f"DROP VIEW IF EXISTS {v}", f"drop view {v}")
    for t in TABLES + (["src"] if a.mode == "demo" else []):
        run(f"DROP TABLE IF EXISTS {t}", f"drop table (metadata only) {t}")

    # source relation src(source_file, line)
    if a.mode == "demo":
        data_loc = f"s3://{a.bucket}/dqm/open/{a.label}/"
        print(f"sync {a.input} -> {data_loc} (open data)")
        subprocess.run(["aws", "s3", "sync", a.input, data_loc, "--exclude", "*", "--include", "*.ndjson.gz",
                        "--only-show-errors"], check=True)
        run(f"""CREATE EXTERNAL TABLE src_raw (line string)
                ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\001' LINES TERMINATED BY '\\n'
                STORED AS TEXTFILE LOCATION '{data_loc}'""", "src_raw (open demo files)")
        run(f"""CREATE TABLE src WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/src/') AS
                SELECT regexp_extract("$path", '([^/]+)\\.ndjson\\.gz$', 1) AS source_file, line
                FROM src_raw WHERE trim(line) <> ''""", "src (open demo, one row per resource)")
    else:
        run(f"CREATE VIEW src AS SELECT source_file, line FROM {a.source_table}", f"src -> {a.source_table}")

    p = dict(loc=loc, version=a.version, shards=a.shards)
    for s in sql_file("athena/prep_00_inputs.sql", **p):
        run(s, "prep_00: " + " ".join(s.split()[:4]))
    if a.mode == "demo":
        med = "athena/prep_05_med_map_crosswalk.sql"
    elif a.med_map == "audited":
        med = "athena/prep_05_med_map_audited.sql"
    else:
        med = "athena/prep_05_med_map_ndc.sql"
    print(f"medication mapping: {os.path.basename(med)}"
          + (f" (variant {a.variant}, audit round {a.audit_round})" if med.endswith("audited.sql") else ""))
    model_tables = a.variant == "d"
    med_p = dict(p, variant=a.variant,
                 additions_table=f"dqm_map.med_additions{'_model' if model_tables else ''}_{a.audit_round}",
                 drops_table=f"dqm_map.med_drops{'_model' if model_tables else ''}_{a.audit_round}")
    for s in sql_file(med, **med_p):
        run(s, os.path.basename(med) + ": " + " ".join(s.split()[:4]))

    if a.mode == "demo":
        source_filter, keep_filter = "TRUE", "TRUE"   # map every resource: compared one by one with the shim
    else:
        g = glucose_rows()
        lab = " OR ".join(f"strpos(line, '\"{r[1]}\"') > 0" for r in g if r[0] == "labevents")
        chart = " OR ".join(f"strpos(line, '\"{r[1]}\"') > 0" for r in g if r[0] == "chartevents")
        source_filter = (f"source_file NOT IN ({', '.join(repr(x) for x in NOT_RETRIEVED)})"
                         f" AND (source_file <> 'MimicObservationLabevents' OR {lab})"
                         f" AND (source_file <> 'MimicObservationChartevents' OR {chart})")
        keep_filter = "keep"
    mp = dict(p, source_filter=source_filter, keep_filter=keep_filter, obs_code=q(obs_code), obs_display=q(obs_disp),
              hospice_system=q(hos_sys), hospice_code=q(hos_code), hospice_display=q(hos_disp))
    for s in sql_file("athena/prep_10_map.sql", **mp):
        run(s, "prep_10: " + " ".join(s.split()[:3]))

    # the two-backend slicing check slicing, unchanged, reading the mapped + prefiltered resources
    run("CREATE VIEW raw_ndjson AS SELECT line FROM mapped WHERE keep", "raw_ndjson -> mapped (kept)")
    sp = dict(data_location="(unused)", loc=loc, unit_identifier_system="http://mimic.mit.edu/fhir/mimic/identifier/encounter-hosp",
              lookback_hours=24, lookahead_hours=1, filter_off="FALSE")
    for rel in ["athena/10_load_index.sql", "athena/generated/20_slice.sql", "athena/generated/30_filter_hash.sql"]:
        for s in sql_file(rel, **sp):
            head = " ".join(s.split()[:4])
            if head.startswith("CREATE EXTERNAL TABLE raw_ndjson") or head.startswith("CREATE EXTERNAL TABLE vs_codes"):
                continue
            run(s, f"{os.path.basename(rel)}: {head}")

    for s in sql_file("athena/prep_40_manifest.sql", **p):
        run(s, "prep_40: stay_manifest")
    if a.mode == "full":
        run("CREATE VIEW cohort AS SELECT encounter_id, cohort_group FROM dqm_cohort.stay_years", "cohort -> dqm_cohort.stay_years")
    else:
        run("CREATE VIEW cohort AS SELECT unit_id AS encounter_id, 'open demo' AS cohort_group FROM units", "cohort (demo: one group)")

    summary = {}
    for i, s in enumerate(sql_file("athena/prep_45_summary.sql", **p), 1):
        qid = run(s, f"prep_45 summary {i}")
        hdr, rows = at.rows(qid)
        name = f"summary_{i:02d}"
        with open(os.path.join(out, name + ".csv"), "w", newline="") as f:
            csv.writer(f).writerows([hdr] + rows)
        summary[name] = {"columns": hdr, "rows": rows}
        print("     " + " | ".join(h or "" for h in hdr))
        for r in rows[:40]:
            print("     " + " | ".join("" if c is None else c for c in r))

    for i, s in enumerate(sql_file("athena/prep_50_export.sql", **p), 1):
        run(s, f"prep_50: UNLOAD {['slices', 'manifest', 'shared'][i - 1] if i <= 3 else i}")
    if a.mode == "demo":
        for s in sql_file("athena/prep_55_export_mapped_open.sql", **p):
            run(s, "prep_55: UNLOAD mapped (open demo)")
        subprocess.run(["aws", "s3", "cp", "--recursive", f"{loc}/export/", os.path.join(out, "export"),
                        "--only-show-errors"], check=True)
        print(f"open-demo outputs downloaded to {os.path.join(out, 'export')}")

    meta = {"label": a.label, "mode": a.mode, "database": db, "run_prefix": loc, "engine": engine,
            "shards": a.shards, "rxnorm_version": a.version if a.mode == "full" else None,
            "medication_mapping": None if a.mode == "demo" else
            ({"map": "audited", "variant": a.variant, "audit_round": a.audit_round} if a.med_map == "audited"
             else {"map": "b2 (NDC path, before the audit)"}),
            "observation_services": [obs_sys, obs_code], "hospice": [hos_sys, hos_code],
            "queries": at.n, "data_scanned_mb": round(at.scanned / 1e6, 1),
            "engine_seconds": round(at.engine_ms / 1000, 1), "wall_seconds": round(time.time() - t0, 1),
            "worker_input": {"slices": f"{loc}/export/slices/", "manifest": f"{loc}/export/manifest/",
                             "shared": f"{loc}/export/shared/"}}
    json.dump(meta, open(os.path.join(out, "run.json"), "w"), indent=2)
    print(json.dumps({k: meta[k] for k in ["queries", "data_scanned_mb", "engine_seconds", "wall_seconds"]}, indent=1))
    print(f"results: {out}")


if __name__ == "__main__":
    main()
