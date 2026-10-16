#!/usr/bin/env python3
"""
slice_stays.py — the slicing check: cut FHIR NDJSON into one slice per hospital stay (DuckDB backend).

  python3 slice_stays.py --input <dir with *.ndjson.gz> --out <dir> \
      [--filter --bundles <CMS816 bundle.json> <CMS871 bundle.json> [--vs-cache <dir>]] \
      [--lookback-hours 24] [--lookahead-hours 1] [--no-write]

SQL: sql/duckdb/10_load_index.sql (backend adapter) + sql/core/*.sql (portable core; the rule
and its derivation from the measures' CQL are documented in sql/core/20_slice.sql).

Outputs in <out>:
  slices/<unit_id>.ndjson.gz   resources of each stay's slice (raw JSON lines, unchanged)
  shared.ndjson.gz             Location + Organization, to add to every slice
  manifest.csv                 unit_id, patient_id, window bounds, n_resources, slice_sha256
  run.json                     parameters, input files, counts, versions
Open data only on a laptop; credentialed data runs the same SQL in AWS (Athena adapter, the two-backend slicing check).
"""
import argparse
import csv
import datetime
import glob
import gzip
import json
import os
import sys
import time

import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))


from common import sql_file, vs_codes_from


def run(con, rel, **params):
    for s in sql_file(rel, **params):
        con.execute(s)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--filter", action="store_true")
    ap.add_argument("--bundles", nargs="*", default=[])
    ap.add_argument("--vs-cache")
    ap.add_argument("--lookback-hours", type=int, default=24)
    ap.add_argument("--lookahead-hours", type=int, default=1)
    ap.add_argument("--unit-identifier-system", default="http://mimic.mit.edu/fhir/mimic/identifier/encounter-hosp",
                    help="Encounter.identifier system that marks a hospital stay (one slice each)")
    ap.add_argument("--db", default=":memory:")
    ap.add_argument("--no-write", action="store_true", help="manifest only, no slice files")
    ap.add_argument("--whole-patient", action="store_true",
                    help="also write the UNSLICED, UNFILTERED reference input: patients/<id>.ndjson.gz (every "
                         "resource of the patient) + shared_all.ndjson.gz (every resource without a patient)")
    a = ap.parse_args()
    if a.filter and not a.bundles:
        sys.exit("--filter needs --bundles")

    t0 = time.time()
    os.makedirs(a.out, exist_ok=True)
    con = duckdb.connect(a.db)
    import tempfile   # spill files go to the system temp folder, never into the project
    con.execute(f"SET temp_directory = '{os.path.join(tempfile.gettempdir(), 'dqm_duckdb_tmp')}'")
    inputs = sorted(glob.glob(os.path.join(a.input, "*.ndjson.gz")))
    run(con, "duckdb/10_load_index.sql", input_glob=os.path.join(a.input, "*.ndjson.gz"),
        unit_identifier_system=a.unit_identifier_system)
    t_load = time.time() - t0
    dup = con.execute("SELECT count(*) - count(DISTINCT rtype || '/' || id) FROM res").fetchone()[0]
    if dup:
        sys.exit(f"{dup} duplicate resources (same type and id) in the input: resolve before slicing")

    added = []
    con.execute("CREATE TABLE vs_codes (system VARCHAR, code VARCHAR)")
    if a.filter:
        codes, added = vs_codes_from(a.bundles, a.vs_cache)
        con.executemany("INSERT INTO vs_codes VALUES (?, ?)", sorted(codes))
    run(con, "core/20_slice.sql", lookback_hours=a.lookback_hours, lookahead_hours=a.lookahead_hours)
    run(con, "core/30_filter_hash.sql", filter_off="FALSE" if a.filter else "TRUE")
    t_slice = time.time() - t0 - t_load

    # manifest. mp_year = calendar year of the stay's discharge as recorded (local time in the data):
    # the measurement period each stay is evaluated in (study plan: the one-year period containing discharge).
    rows = con.execute("""
        SELECT b.unit_id, b.patient_id, b.enc_start, b.enc_end, b.h_lo, b.w_lo, b.w_hi,
               coalesce(h.n_resources, 0), h.slice_sha256, h.content_sha256,
               substr(json_extract_string(r.json, '$.period.end'), 1, 4) AS mp_year
        FROM unit_bound b
        LEFT JOIN slice_hash h ON h.unit_id = b.unit_id
        JOIN res r ON r.rtype = 'Encounter' AND r.id = b.unit_id
        ORDER BY b.unit_id""").fetchall()
    with open(os.path.join(a.out, "manifest.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["unit_id", "patient_id", "enc_start_utc", "enc_end_utc", "hosp_start_lower_bound_utc",
                    "window_start_utc", "window_end_utc", "n_resources", "slice_sha256", "content_sha256", "mp_year"])
        for r in rows:
            w.writerow([r[0], r[1]] + [x.isoformat() if hasattr(x, "isoformat") else x for x in r[2:7]] + [r[7], r[8], r[9], r[10]])

    # unsliced, unfiltered reference input (packaging only: every resource goes to its patient's file,
    # or to shared_all if it has no patient; the counts are checked)
    whole = None
    if a.whole_patient:
        pdir = os.path.join(a.out, "patients")
        os.makedirs(pdir, exist_ok=True)
        cur = con.execute("""SELECT i.patient_id, r.json FROM res_index i JOIN res r ON r.rtype = i.rtype AND r.id = i.id
                             WHERE i.patient_id IS NOT NULL ORDER BY i.patient_id, i.rtype, i.id""")
        fh, current, n_pat, n_res = None, None, 0, 0
        while True:
            batch = cur.fetchmany(50000)
            if not batch:
                break
            for pid, js in batch:
                if pid != current:
                    if fh:
                        fh.close()
                    fh = gzip.open(os.path.join(pdir, f"{pid}.ndjson.gz"), "wt", encoding="utf-8")
                    current, n_pat = pid, n_pat + 1
                fh.write(js + "\n")
                n_res += 1
        if fh:
            fh.close()
        n_shared = 0
        with gzip.open(os.path.join(a.out, "shared_all.ndjson.gz"), "wt", encoding="utf-8") as f:
            for (js,) in con.execute("""SELECT r.json FROM res_index i JOIN res r ON r.rtype = i.rtype AND r.id = i.id
                                        WHERE i.patient_id IS NULL ORDER BY i.rtype, i.id""").fetchall():
                f.write(js + "\n")
                n_shared += 1
        total = con.execute("SELECT count(*) FROM res").fetchone()[0]
        if n_res + n_shared != total:
            sys.exit(f"whole-patient packaging lost resources: {n_res} + {n_shared} != {total}")
        whole = {"patients": n_pat, "patient_resources": n_res, "shared_resources": n_shared, "total_in": total}

    # slice files
    n_written = 0
    if not a.no_write:
        sdir = os.path.join(a.out, "slices")
        os.makedirs(sdir, exist_ok=True)
        cur = con.execute("""SELECT s.unit_id, r.json FROM slice s JOIN res r ON r.rtype = s.rtype AND r.id = s.id
                             ORDER BY s.unit_id, s.rtype, s.id""")
        fh, current = None, None
        while True:
            batch = cur.fetchmany(50000)
            if not batch:
                break
            for unit_id, js in batch:
                if unit_id != current:
                    if fh:
                        fh.close()
                    fh = gzip.open(os.path.join(sdir, f"{unit_id}.ndjson.gz"), "wt", encoding="utf-8")
                    current = unit_id
                    n_written += 1
                fh.write(js + "\n")
        if fh:
            fh.close()
        with gzip.open(os.path.join(a.out, "shared.ndjson.gz"), "wt", encoding="utf-8") as f:
            for (js,) in con.execute("SELECT r.json FROM shared s JOIN res r ON r.rtype = s.rtype AND r.id = s.id ORDER BY s.rtype, s.id").fetchall():
                f.write(js + "\n")

    counts = dict(con.execute("SELECT rtype, count(*) FROM res GROUP BY 1 ORDER BY 1").fetchall())
    slice_counts = dict(con.execute("SELECT rtype, count(*) FROM slice GROUP BY 1 ORDER BY 1").fetchall())
    stats = con.execute("""SELECT count(*), min(n_resources), median(n_resources), max(n_resources), sum(n_resources)
                           FROM (SELECT b.unit_id, coalesce(h.n_resources, 0) n_resources
                                 FROM unit_bound b LEFT JOIN slice_hash h USING (unit_id))""").fetchone()
    no_window = con.execute("SELECT count(*) FROM unit_bound WHERE w_lo IS NULL OR w_hi IS NULL").fetchone()[0]
    widened = con.execute("SELECT count(*) FROM unit_bound WHERE h_lo < enc_start").fetchone()[0]
    meta = {
        "generated_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "backend": f"duckdb {duckdb.__version__}",
        "params": {"lookback_hours": a.lookback_hours, "lookahead_hours": a.lookahead_hours, "filter": a.filter,
                   "unit_identifier_system": a.unit_identifier_system},
        "inputs": [{"file": os.path.basename(p), "bytes": os.path.getsize(p)} for p in inputs],
        "filter_value_sets_from_vsac_cache": [u.split("/")[-1] for u in added],
        "resources_in": counts, "resources_in_slices": slice_counts,
        "units": stats[0], "units_without_window": no_window, "units_start_extended_by_prior_encounter": widened,
        "slice_size": {"min": stats[1], "median": stats[2], "max": stats[3], "sum": stats[4]},
        "slice_files_written": n_written,
        "whole_patient_reference": whole,
        "seconds": {"load_index": round(t_load, 1), "slice_hash": round(t_slice, 1), "total": round(time.time() - t0, 1)},
    }
    json.dump(meta, open(os.path.join(a.out, "run.json"), "w"), indent=2)
    print(json.dumps({k: meta[k] for k in ["units", "units_without_window", "units_start_extended_by_prior_encounter",
                                           "slice_size", "resources_in_slices", "slice_files_written",
                                           "whole_patient_reference", "seconds"]}, indent=1))


if __name__ == "__main__":
    main()
