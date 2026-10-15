-- =============================================================================
-- prep_00_inputs.sql — the prepare stage prepare: small reference tables uploaded by prepare_run.py as TSV under the
-- run prefix {loc} (terminology and crosswalks only; no patient data).
--   glucose_xw (source_table, itemid, loinc, display)    crosswalk/glucose_crosswalk.csv,
--                                                       rows marked EXCLUDE left out (as the shim)
--   vs_codes   (system, code)                           every code of every value set the two
--                                                       measures declare (bundles + VSAC cache)
--   vs_members (oid, system, code)                      the same, with the value set (workload metric)
-- The source relation `src (source_file, line)` is created by the runner (open demo: NDJSON files;
-- credentialed: mimic_fhir.ndjson_parts). The medication map `med_map` comes from prep_05_*.sql.
-- =============================================================================

CREATE EXTERNAL TABLE glucose_xw (source_table string, itemid string, loinc string, display string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '{loc}/glucose_xw/';

CREATE EXTERNAL TABLE vs_codes (system string, code string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '{loc}/vs_codes/';

CREATE EXTERNAL TABLE vs_members (oid string, system string, code string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '{loc}/vs_members/';
