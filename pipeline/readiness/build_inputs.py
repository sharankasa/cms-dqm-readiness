#!/usr/bin/env python3
"""
build_inputs.py — engine inputs for the readiness runs (R2, V) and the runtime runs (S2): selects a set
of stays from a the prepare stage run, cuts it into worker shards of about equal predicted SLICED time, and writes
the worker input in the the prepare stage export format. Athena only (cents); row-level data stays in S3; only
counts come back (1-9 shown as <10).

  cd ".../project2-dqm-infrastructure"; export AWS_PROFILE=your-aws-profile
  python3 pipeline/readiness/build_inputs.py --sets cohort:a changed:b changed:c changed:d --with-v
  python3 pipeline/readiness/build_inputs.py --sets all:a --shards 96
  python3 pipeline/readiness/build_inputs.py --sets s1-sample --light-factor 1

Sets (variant letter -> the prepare stage database: a = dqm_prep_vaf, b = dqm_prep_vb, c = dqm_prep_vcf, d = dqm_prep_vd; the
medication mapping variants of registration §4.2, a = baseline after the recall audit):
  cohort:<v>   the 2017-2019 readiness cohort on mapping <v>          -> dqm/readiness/<stamp>/cohort-<v>/
  changed:<v>  cohort stays whose slice under <v> differs from the slice under the base mapping (a):
               resource set or any resource's content (the prepare stage slice hashes). Only these are re-evaluated;
               every other stay's result is the base result. Stops if the shared resources differ.
                                                                       -> dqm/readiness/<stamp>/changed-<v>-vs-a/
  all:<v>      every stay (S2 full run)                                -> <the prepare stage run>/export_slbal-<stamp>/
  s1-sample    the 2,000 S1 sample stays, sliced from the the prepare stage run the S1 sample used (dqm_prep), so the
               slices are byte-for-byte the S1 sliced input (S2 timing sample, batching check)
                                                                       -> <S1 run>/export_slbal-<stamp>/
--with-v adds, for cohort sets, V's input with one stay = one record (V reads exactly the slice the
engine reads): <root>/export_v/. Each new root gets the S1 engine inputs (measure bundles + VSAC
expansions) copied to <root>/engine/ unless already there, so every run uses the same engine inputs.
Shards: by predicted time on the sliced side (pipeline/readiness/sql/10 and 20); with batching (BATCH>1
in run_eks_job.sh) slices of at most --batch-max-res resources are predicted at --light-factor of their
one-stay-per-call time. Stays without a 4-digit discharge year are left out and counted.
"""
import argparse
import csv
import datetime
import glob
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(ROOT, "pipeline", "prepare"))
from athena_run import Athena, aws  # noqa: E402

BUCKET = "your-bucket"
S1_RUN = f"s3://{BUCKET}/dqm/s1/20261010T054127Z"
S1_SLICED_OUT = f"{S1_RUN}/runs/sliced-per-stay"
VARIANT_DB = {"a": "dqm_prep_vaf", "b": "dqm_prep_vb", "c": "dqm_prep_vcf", "d": "dqm_prep_vd"}
COHORT = "1 primary 2017-2019"
COLS = ("m.unit_id, m.patient_id, m.mp_year, m.n_resources, m.slice_sha256, m.content_sha256, "
        "m.h_lo, m.w_lo, m.w_hi, m.work_cms816, m.work_cms871")
YEAR_OK = "regexp_like(coalesce(m.mp_year, ''), '^[0-9]{4}$')"


def local_sql(rel, **params):
    """pipeline/readiness/sql/<rel> split into statements by '-- part <name>' sections."""
    text = open(os.path.join(HERE, "sql", rel)).read()
    for k, v in params.items():
        text = text.replace("{" + k + "}", str(v))
    parts, buf, part = {}, [], "all"
    for line in text.splitlines():
        st = line.strip()
        if st.startswith("-- part "):
            part = st[3:]
            continue
        if st.startswith("--"):
            continue
        buf.append(line)
        if line.rstrip().endswith(";"):
            parts.setdefault(part, []).append("\n".join(buf).strip().rstrip(";"))
            buf = []
    return parts


def prep_prefix(db):
    for f in glob.glob(os.path.join(ROOT, "results", "prep-*", "run.json")):
        m = json.load(open(f))
        if m.get("database") == db:
            return m["run_prefix"].rstrip("/"), m.get("medication_mapping")
    sys.exit(f"no results/prep-*/run.json for database {db}")


def mask(v):
    try:
        n = int(v)
    except (TypeError, ValueError):
        return v
    return "<10" if 1 <= n <= 9 else str(n)


def selection(kind, db, base_db):
    if kind == "cohort":
        return (f"SELECT {COLS}, 'cohort 2017-2019' AS reason FROM {db}.stay_manifest m "
                f"JOIN dqm_cohort.stay_years c ON c.encounter_id = m.unit_id "
                f"WHERE c.cohort_group = '{COHORT}' AND {YEAR_OK}")
    if kind == "changed":
        return (f"SELECT {COLS}, CASE WHEN a.unit_id IS NULL THEN 'not in base' "
                f"WHEN coalesce(m.slice_sha256, '') <> coalesce(a.slice_sha256, '') THEN 'resources differ' "
                f"WHEN coalesce(m.content_sha256, '') <> coalesce(a.content_sha256, '') THEN 'content differs' "
                f"ELSE 'year differs' END AS reason "
                f"FROM {db}.stay_manifest m JOIN dqm_cohort.stay_years c ON c.encounter_id = m.unit_id "
                f"LEFT JOIN {base_db}.stay_manifest a ON a.unit_id = m.unit_id "
                f"WHERE c.cohort_group = '{COHORT}' AND {YEAR_OK} AND (a.unit_id IS NULL "
                f"OR coalesce(m.slice_sha256, '') <> coalesce(a.slice_sha256, '') "
                f"OR coalesce(m.content_sha256, '') <> coalesce(a.content_sha256, '') "
                f"OR coalesce(m.mp_year, '') <> coalesce(a.mp_year, ''))")
    if kind == "all":
        return f"SELECT {COLS}, 'all stays' AS reason FROM {db}.stay_manifest m WHERE {YEAR_OK}"
    if kind == "s1-sample":
        return (f"SELECT {COLS}, s.reason FROM {db}.stay_manifest m "
                f"JOIN dqm_s1.sample_stays s ON s.unit_id = m.unit_id WHERE {YEAR_OK}")
    raise ValueError(kind)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sets", nargs="+", required=True, help="cohort:<v> changed:<v> all:<v> s1-sample")
    ap.add_argument("--base", default="a", help="base mapping for changed sets")
    ap.add_argument("--with-v", action="store_true", help="cohort sets: also V's one-stay-one-record input")
    ap.add_argument("--shards", type=int, default=64, help="maximum shards per set (Athena writes at most 100)")
    ap.add_argument("--min-stays-per-shard", type=int, default=150, help="small sets get fewer shards")
    ap.add_argument("--batch-max-res", type=int, default=400)
    ap.add_argument("--light-factor", type=float, default=0.35,
                    help="predicted share of one-stay-per-call time for batchable slices (1 = no batching)")
    ap.add_argument("--s1-b4db", default="dqm_prep", help="the prepare stage database the S1 sample was sliced from")
    ap.add_argument("--workgroup", default="primary")
    a = ap.parse_args()
    if not 1 <= a.shards <= 100:
        sys.exit("--shards must be 1..100 (Athena UNLOAD writes at most 100 partitions)")
    sets = []
    for s in a.sets:
        kind, _, v = s.partition(":")
        if kind not in ("cohort", "changed", "all", "s1-sample") or (kind != "s1-sample" and v not in VARIANT_DB):
            sys.exit(f"bad set {s}: use cohort:<a-d>, changed:<b-d>, all:<a-d>, s1-sample")
        if kind == "changed" and v == a.base:
            sys.exit("changed:<v> needs a variant other than the base")
        sets.append((kind, v))

    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = os.path.join(ROOT, "results", f"readiness-inputs-{stamp}")
    os.makedirs(out, exist_ok=True)
    print(f"AWS identity: {aws('sts', 'get-caller-identity')['Arn']}")
    wg = aws("athena", "get-work-group", "--work-group", a.workgroup)["WorkGroup"]
    cfg = wg.get("Configuration", {})
    if cfg.get("EnforceWorkGroupConfiguration"):
        sys.exit(f"workgroup {a.workgroup} enforces its settings")
    out_loc = None if cfg.get("ResultConfiguration", {}).get("OutputLocation") else f"s3://{BUCKET}/dqm/athena-results/"
    at = Athena(a.workgroup, out_loc)
    t0 = time.time()
    db = "dqm_readiness"
    run = lambda sql, label: at.run(sql, db=db, label=label)   # noqa: E731
    at.run(f"CREATE DATABASE IF NOT EXISTS {db}", label=f"database {db}")

    def select_print(sql, label, fname):
        """Prints and saves with counts 1-9 as <10; returns the raw values (for the checks)."""
        hdr, rows = at.rows(run(sql, label))
        keep = {i for i, h in enumerate(hdr) if h in ("shard", "band", "shards", "distinct_shards")}
        shown = [[mask(c) if c is not None and c.isdigit() and i not in keep else c for i, c in enumerate(r)] for r in rows]
        with open(os.path.join(out, fname), "w", newline="") as f:
            csv.writer(f).writerows([hdr] + shown)
        print("     " + " | ".join(h or "" for h in hdr))
        for r in shown[:40]:
            print("     " + " | ".join("" if c is None else c for c in r))
        if len(rows) > 40:
            print(f"     ... {len(rows) - 40} more rows in {fname}")
        return [dict(zip(hdr, r)) for r in rows]

    # runtime model (sliced side), from the S1 sample's sliced run
    for t in ["s1_sliced_results", "sliced_runtime_bands"]:
        run(f"DROP TABLE IF EXISTS {t}", f"drop table (metadata only) {t}")
    model = local_sql("10_sliced_runtime_model.sql", s1_sliced_out=S1_SLICED_OUT, s1_b4db=a.s1_b4db,
                      model_loc=f"s3://{BUCKET}/dqm/readiness/{stamp}/model")
    for s in model["part model"]:
        run(s, "runtime model: " + " ".join(s.split()[:3]))
    print("sliced runtime by work band (S1 sample, one stay per engine call):")
    select_print(model["part summary"][0], "runtime bands", "runtime_bands.csv")

    base_db = VARIANT_DB[a.base]
    engine_src = f"{S1_RUN}/engine"
    made = []
    for kind, v in sets:
        if kind == "s1-sample":
            label, b4db = "s1-sample", a.s1_b4db
            root, xp = S1_RUN, f"{S1_RUN}/export_slbal-{stamp}"
            mapping = prep_prefix(b4db)[1]
        elif kind == "all":
            label, b4db = f"all-{v}", VARIANT_DB[v]
            root, mapping = prep_prefix(b4db)
            xp = f"{root}/export_slbal-{stamp}"
        else:
            label = f"cohort-{v}" if kind == "cohort" else f"changed-{v}-vs-{a.base}"
            b4db = VARIANT_DB[v]
            mapping = prep_prefix(b4db)[1]
            root = f"s3://{BUCKET}/dqm/readiness/{stamp}/{label}"
            xp = f"{root}/export"
        xv = f"{root}/export_v" if (a.with_v and kind == "cohort") else None
        table = f"set_{label.replace('-', '_')}_{stamp.lower()}"
        print(f"\n=== {label}: the prepare stage database {b4db} (medication mapping {json.dumps(mapping)}); input {xp}")

        if kind == "changed":
            chk = select_print(f"""
                WITH x AS (SELECT s.rtype, s.id, to_hex(sha256(to_utf8(r.json))) AS h FROM {b4db}.shared s
                           JOIN {b4db}.res r ON r.rtype = s.rtype AND r.id = s.id),
                     y AS (SELECT s.rtype, s.id, to_hex(sha256(to_utf8(r.json))) AS h FROM {base_db}.shared s
                           JOIN {base_db}.res r ON r.rtype = s.rtype AND r.id = s.id)
                SELECT count_if(x.id IS NULL OR y.id IS NULL) AS shared_resources_differing,
                       (SELECT count(*) FROM {b4db}.stay_manifest) AS stays_variant,
                       (SELECT count(*) FROM {base_db}.stay_manifest) AS stays_base
                FROM x FULL OUTER JOIN y ON x.rtype = y.rtype AND x.id = y.id AND x.h = y.h""",
                f"{label}: shared resources vs base", f"{label}_shared_check.csv")[0]
            if chk["shared_resources_differing"] != "0":
                sys.exit(f"{label}: the shared Location/Organization resources differ from the base: every stay "
                         "would need re-evaluation. Nothing exported for this set.")
            if chk["stays_variant"] != chk["stays_base"]:
                print(f"  NOTE: {label}: stay counts differ from the base ({chk['stays_variant']} vs {chk['stays_base']})")

        sel = selection(kind, b4db, base_db)
        n = int(at.rows(run(f"SELECT count(*) FROM ({sel})", f"{label}: count"))[1][0][0])
        if n == 0:
            print(f"  {label}: no stays: nothing to export")
            made.append({"set": label, "stays": 0})
            continue
        shards = max(1, min(a.shards, -(-n // a.min_stays_per_shard)))
        run(f"DROP TABLE IF EXISTS {table}", f"drop table (metadata only) {table}")
        p = local_sql("20_select_and_shard.sql", table=table, root=f"s3://{BUCKET}/dqm/readiness/{stamp}/sets/{label}",
                      selection=sel, shards=shards, batch_max_res=a.batch_max_res, light_factor=a.light_factor)
        for s in p["part select"]:
            run(s, f"{label}: select + shards")
        summ = p["part summary"]
        tot = select_print(summ[0], f"{label}: totals", f"{label}_totals.csv")[0]
        select_print(summ[1], f"{label}: by reason", f"{label}_reasons.csv")
        sh = select_print(summ[2], f"{label}: by shard", f"{label}_shards.csv")
        if not (tot["stays"] == tot["distinct_stays"] and tot["shards"] == tot["distinct_shards"]
                and tot["stays_without_year"] == "0"):
            sys.exit(f"CHECK FAILED ({label}): every stay once, shards without gaps, every stay with a year. Nothing exported.")
        if kind == "changed":
            # R2 holds everything but the mapping fixed: which resource types differ between the variant's and
            # the base's slices of the changed stays (expected: medication resources only)
            diff = select_print(f"""
                WITH ch AS (SELECT unit_id FROM {table}),
                x AS (SELECT DISTINCT s.rtype, s.id, to_hex(sha256(to_utf8(r.json))) AS h FROM {b4db}.slice s
                      JOIN ch ON ch.unit_id = s.unit_id JOIN {b4db}.res r ON r.rtype = s.rtype AND r.id = s.id),
                y AS (SELECT DISTINCT s.rtype, s.id, to_hex(sha256(to_utf8(r.json))) AS h FROM {base_db}.slice s
                      JOIN ch ON ch.unit_id = s.unit_id JOIN {base_db}.res r ON r.rtype = s.rtype AND r.id = s.id)
                SELECT coalesce(x.rtype, y.rtype) AS resource_type, count_if(x.id IS NULL) AS only_in_base_slices,
                       count_if(y.id IS NULL) AS only_in_variant_slices, count_if(x.h <> y.h) AS content_differs
                FROM x FULL OUTER JOIN y ON x.rtype = y.rtype AND x.id = y.id
                GROUP BY 1 HAVING count_if(x.id IS NULL OR y.id IS NULL OR x.h <> y.h) > 0 ORDER BY 1""",
                f"{label}: resource types that differ from the base", f"{label}_differing_types.csv")
            other = [d["resource_type"] for d in diff if d["resource_type"] not in ("MedicationAdministration", "Medication")]
            if other:
                print(f"  WARNING ({label}): resources other than medications differ from the base ({', '.join(other)}): "
                      "the variant's the prepare stage run differs in more than the medication mapping; check before using it for R2")
        excluded = int(at.rows(run(
            f"SELECT count(*) FROM ({selection(kind, b4db, base_db).replace(YEAR_OK, 'NOT ' + YEAR_OK)})",
            f"{label}: stays without a discharge year"))[1][0][0])

        x = local_sql("30_export.sql", table=table, b4db=b4db, xp=xp, xv=xv or "unused")
        for i, s in enumerate(x["part export"]):
            run(s, f"{label}: UNLOAD {['manifest', 'slices', 'shared'][i]}")
        if xv:
            for i, s in enumerate(x["part v"]):
                run(s, f"{label}: UNLOAD V input {['manifest', 'reference', 'shared_all'][i]}")
        nsh = int(tot["shards"])
        for pre, subs in [(xp, ["manifest", "slices"])] + ([(xv, ["manifest", "reference"])] if xv else []):
            for sub in subs:
                r = subprocess.run(["aws", "s3", "ls", f"{pre}/{sub}/"], capture_output=True, text=True)
                got = sum(1 for line in r.stdout.splitlines() if "PRE shard=" in line)
                print(f"  {pre.rsplit('/', 1)[1]}/{sub}: {got} shard folders (expected {nsh})")
                if got != nsh:
                    sys.exit(f"CHECK FAILED ({label}): {sub} has {got} shard folders, expected {nsh}")
        r = subprocess.run(["aws", "s3", "ls", f"{root}/engine/bundles/"], capture_output=True, text=True)
        if r.returncode != 0 or not r.stdout.strip():
            subprocess.run(["aws", "s3", "cp", "--recursive", f"{engine_src}/", f"{root}/engine/", "--only-show-errors"],
                           check=True)
            print(f"  engine inputs copied from {engine_src} to {root}/engine/")
        else:
            print(f"  engine inputs already under {root}/engine/")
        made.append({"set": label, "kind": kind, "variant": v or None, "prep_database": b4db, "medication_mapping": mapping,
                     "input": xp, "v_input": xv, "root": root, "athena_table": f"{db}.{table}", "stays": int(tot["stays"]),
                     "patients": tot["patients"], "shards": nsh, "own_shards": tot["own_shards"],
                     "predicted_hours_one_stay_per_call": tot["predicted_hours"], "stays_batchable": tot["stays_batchable"],
                     "stays_left_out_no_year": excluded, "heaviest_shard_predicted_hours":
                     max(float(r_["predicted_hours"]) for r_ in sh)})

    meta = {"stamp": stamp, "base": a.base, "variants": VARIANT_DB, "cohort_group": COHORT, "runtime_model_from": S1_SLICED_OUT,
            "light_factor": a.light_factor, "batch_max_res": a.batch_max_res, "max_shards": a.shards,
            "engine_inputs_from": engine_src, "sets": made,
            "queries": at.n, "data_scanned_mb": round(at.scanned / 1e6, 1), "wall_seconds": round(time.time() - t0, 1)}
    json.dump(meta, open(os.path.join(out, "inputs.json"), "w"), indent=2)
    print("\n=== inputs ready")
    for m in made:
        if m.get("stays"):
            print(f"  {m['set']:<22} {mask(m['stays']):>8} stays, {m['shards']:>3} shards; INPUT={m['input']}"
                  + (f"\n  {'':<22} V input: {m['v_input']}" if m.get("v_input") else ""))
    print(f"Athena: {at.n} queries, {at.scanned / 1e9:.1f} GB scanned (~${at.scanned / 1e12 * 5:.2f}); "
          f"{time.time() - t0:.0f} s\nsaved (counts only): {out}")


if __name__ == "__main__":
    main()
