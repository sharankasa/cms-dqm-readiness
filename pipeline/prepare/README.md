# prepare: stay slicing (+ optional data-requirements filter)

Cuts FHIR NDJSON into one slice per hospital stay, so stays can be evaluated in parallel by
CMS's reference engine without changing any stay's result (claim S1, proven in the demo equivalence check).

```
python3 slice_stays.py --input <dir of *.ndjson.gz> --out <dir> \
  [--filter --bundles <CMS816 bundle> <CMS871 bundle> --vs-cache <dir>]
```

| File | Role |
|---|---|
| `sql/duckdb/10_load_index.sql` | Backend adapter: load NDJSON, index patient/encounter links and UTC time ranges, extract codings |
| `sql/core/20_slice.sql` | Portable core: stay windows and slice membership. **The slicing rule and its derivation from the measures' CQL are documented at the top** |
| `sql/core/30_filter_hash.sql` | Portable core: optional data-requirements filter; SHA-256 per stay |
| `slice_stays.py` | Driver (DuckDB); writes `slices/`, `shared.ndjson.gz`, `manifest.csv`, `run.json` |

**Rule in one line.** Window = [lower bound of the start of CMS's hospitalization interval (ED and
observation chaining, computed type-agnostically so it is never later than the true start) − 24 h,
end of stay + 1 h]; plus everything linked to the stay, everything without a time, and all
Patient/Condition/Coverage resources. The slice is a superset of what the measures can read.

**Status (2026-10-09, open demo, DuckDB 1.5.6):** 275 stays, 275 slices, deterministic (identical
hashes across runs). Unfiltered: median 1,596 resources per stay (max 42,477). Filtered: median 13
(max 237), 6,780 in total from ~1.0M. Not yet shown to give identical engine results: that is the demo equivalence check.

The Athena adapter (the two-backend slicing check) must produce the same three tables (`res`, `res_index`, `res_codes`); the
core SQL is translated with SQLGlot, and the per-stay hashes must match.

## the prepare stage: the full prepare stage in Athena (`prepare_run.py`)

```
baseline mapping (prep_10_map.sql) -> the two-backend slicing check slicing SQL, unchanged -> stay manifest + workload (prep_40)
  -> masked aggregates to the laptop (prep_45) -> worker input per shard in S3 (prep_50)
```

| File | Role |
|---|---|
| `prepare_run.py` | Driver. `--mode demo` (open demo; downloads outputs) or `--mode full` (credentialed; row-level output stays in S3, only masked aggregates come back) |
| `sql/athena/prep_00_inputs.sql` | Reference tables uploaded as TSV: glucose crosswalk, value-set codes, value-set members |
| `sql/athena/prep_05_med_map_ndc.sql` | Credentialed: RxNorm codings per eMAR administration from the medication mapping (`dqm_map.ma_emar_path`) |
| `sql/athena/prep_05_med_map_crosswalk.sql` | Open demo only: the demo shim's medication rule (for the comparison) |
| `sql/athena/prep_10_map.sql` | The baseline mapping (the open-demo mapping rules, version 3, deviation 3) as SQL, plus the per-resource data-requirements filter (`keep`) |
| `sql/athena/prep_40_manifest.sql` | One row per stay: window, hashes, `mp_year`, shard, workload metric (registration §4.3) |
| `sql/athena/prep_45_summary.sql` | Aggregates only: mapping counts, slice sizes and workload by cohort group, ED overlap (the ED-overlap issue), administration time shapes (MC-06) |
| `sql/athena/prep_50_export.sql` | Worker input: `export/slices/shard=<n>/` (`unit_id<TAB>json`), `export/manifest/shard=<n>/`, `export/shared/` |
| `validate_prep_demo.py` | Open demo: Athena mapping = Python shim (all resources), Athena slices = DuckDB slices (membership), exported bytes = hashed bytes; `--make-prep` writes engine input |

**Filtering before slicing.** The data-requirements filter decides per resource (type and codes), so
applying it before slicing gives the same filtered slices as applying it after (the demo equivalence check/the two-backend slicing check): a resource
that fails it can never be in a filtered slice. This cuts the input from hundreds of millions of resources
(chartevents alone: 313.6 M; labs 118.2 M) to the few million the measures can read (Patient, Encounter, Medication, glucose results, diabetes diagnoses,
measure-drug administrations). The unfiltered whole-patient reference for S1 (the equivalence run) is built separately
for the 2,000-stay sample.

**Not portable yet.** The mapping SQL is Athena-only (MIMIC-specific data preparation, not the
cloud-neutral core). The slicing core is unchanged and still runs on both backends.
