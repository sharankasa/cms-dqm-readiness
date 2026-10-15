-- =============================================================================
-- 41_audit_apply.sql — the medication audit: read the reviewer's decisions on the medication audit worklist
-- (40_audit_worklist.sql; the medication audit guide, not distributed), check them, report the registered gate
-- (registration §4.2), and build the reviewed medication additions for the three registered mapping
-- variants, which the the prepare stage rerun applies (pipeline/prepare/sql/athena/prep_05_med_map_audited.sql):
--   (a) baseline     NDC path (all, minus mixes the reviewer dropped) + "map" decisions (incl. ICU)
--   (b) conservative NDC path, single products only; no name-based additions (exact matches only)
--   (c) liberal      (a) + "possible" decisions
-- Decisions are joined on item_id; every other column is taken from {worklist}, not from
-- the edited file. Where a decision applies (match_level, revised 2026-10-13 after the first full
-- check showed one generic local insulin code shared by many differently named products):
--   parts C, D, E: per local code AND medication name (the eMAR Medication's names), so each dose
--                  gets the product decided for its own name; E "no" drops only that mix's NDC codings;
--   parts A, B, F: per local code (an audit code, or an ICU item).
-- Conflicts (the same application key decided differently) are reported and must be resolved.
-- Printed output: counts only (masked below 10), no drug names.
-- Run:
--   python3 pipeline/athena/run_sql.py --sql pipeline/terminology/41_audit_apply.sql \
--     --out results/medication-audit-apply-r1 --db dqm_map --param version=20261005 \
--     --param worklist=dqm_map.audit_worklist --param round=r1 --param decisions=s3://your-bucket/dqm/map/audit-decisions/round1 \
--     --param loc=s3://your-bucket/dqm/map/audit-apply-r1-<UTC stamp>
-- =============================================================================

DROP TABLE IF EXISTS dqm_map.audit_decisions_raw_{round};

CREATE EXTERNAL TABLE dqm_map.audit_decisions_raw_{round} (
  sort_order string, item_id string, part string, flagged string, flag_words string, med_system string,
  med_code string, names string, administrations string, administrations_scope string, ndcs string,
  ndc_path_rxcuis string, ndc_path_drugs string, suggestion_1_rxcui string, suggestion_1_name string,
  suggestion_2_rxcui string, suggestion_2_name string, suggestion_3_rxcui string, suggestion_3_name string,
  decision string, rxcui string, note string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION '{decisions}/'
TBLPROPERTIES ('skip.header.line.count' = '1');

DROP TABLE IF EXISTS dqm_map.audit_decisions_{round};

-- 1. Decisions joined to the worklist, normalised and classified
CREATE TABLE dqm_map.audit_decisions_{round}
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/audit_decisions/') AS
WITH d AS (
  SELECT trim(item_id) AS item_id, lower(trim(decision)) AS decision,
         regexp_replace(trim(rxcui), '\.0+$', '') AS rxcui, note
  FROM dqm_map.audit_decisions_raw_{round}
  WHERE trim(coalesce(item_id, '')) LIKE 'i%'
),
vs AS (
  SELECT code AS rxcui,
         max(IF(oid = '2.16.840.1.113762.1.4.1196.393', 1, 0)) = 1 AS in_cms816,
         max(IF(oid = '2.16.840.1.113762.1.4.1196.394', 1, 0)) = 1 AS in_cms871
  FROM dqm_ref.value_set_codes_{version}
  WHERE oid IN ('2.16.840.1.113762.1.4.1196.393', '2.16.840.1.113762.1.4.1196.394')
    AND system = 'http://www.nlm.nih.gov/research/umls/rxnorm'
  GROUP BY code
),
rx AS (
  SELECT rxcui, max(tty) AS any_tty, max(IF(tty IN ('SCD', 'GPCK', 'SBD', 'BPCK'), tty)) AS product_tty,
         max(IF(tty IN ('SCD', 'GPCK', 'SBD', 'BPCK'), suppress)) AS product_suppress
  FROM dqm_ref.rxnconso_{version}
  WHERE sab = 'RXNORM' AND suppress <> 'O'
  GROUP BY rxcui
),
brand AS (   -- a brand product given as rxcui is counted as its generic (as the NDC path does)
  SELECT DISTINCT rxcui2 AS brand_rxcui, rxcui1 AS generic_rxcui FROM dqm_ref.rxnrel_{version}
  WHERE sab = 'RXNORM' AND rela = 'tradename_of'
)
SELECT w.item_id, w.part, w.med_system, w.med_code, w.names,
       IF(w.part LIKE 'C%' OR w.part LIKE 'D%' OR w.part LIKE 'E%', 'names', 'code') AS match_level,
       w.administrations, w.administrations_scope,
       w.ndc_path_rxcuis, d.decision, d.rxcui AS rxcui_given, d.note,
       CASE WHEN d.item_id IS NULL THEN 'missing'
            WHEN d.decision IN ('map', 'possible', 'no') THEN d.decision
            WHEN d.decision = '' OR d.decision IS NULL THEN 'blank'
            ELSE 'invalid' END AS status,
       r.product_tty, r.any_tty, r.product_suppress,
       coalesce(IF(r.product_tty IN ('SBD', 'BPCK'), b.generic_rxcui), d.rxcui) AS rxcui,
       coalesce(v.in_cms816, false) AS in_cms816, coalesce(v.in_cms871, false) AS in_cms871
FROM {worklist} w
LEFT JOIN d ON d.item_id = w.item_id
LEFT JOIN rx r ON r.rxcui = d.rxcui
LEFT JOIN brand b ON b.brand_rxcui = d.rxcui AND r.product_tty IN ('SBD', 'BPCK')
LEFT JOIN vs v ON v.rxcui = coalesce(IF(r.product_tty IN ('SBD', 'BPCK'), b.generic_rxcui), d.rxcui);

-- A. Coverage: every worklist row has exactly one decision row; no unknown item ids in the file
SELECT (SELECT count(*) FROM {worklist}) AS worklist_rows,
       (SELECT count(*) FROM dqm_map.audit_decisions_raw_{round} WHERE trim(coalesce(item_id, '')) LIKE 'i%') AS decision_rows,
       (SELECT count(*) - count(DISTINCT trim(item_id)) FROM dqm_map.audit_decisions_raw_{round}
         WHERE trim(coalesce(item_id, '')) LIKE 'i%') AS duplicate_item_ids,
       (SELECT count(*) FROM dqm_map.audit_decisions_raw_{round} r
         WHERE trim(coalesce(r.item_id, '')) LIKE 'i%'
           AND trim(r.item_id) NOT IN (SELECT item_id FROM {worklist})) AS unknown_item_ids;

-- B. Decisions by part, and problems to fix before going on (counts only)
SELECT part, status,
       CASE WHEN count(*) BETWEEN 1 AND 9 THEN '<10' ELSE CAST(count(*) AS varchar) END AS entries,
       count_if(status IN ('map', 'possible') AND coalesce(rxcui_given, '') = '' AND part NOT LIKE 'E%') AS needs_rxcui,
       count_if(status IN ('map', 'possible') AND coalesce(rxcui_given, '') <> '' AND any_tty IS NULL) AS rxcui_not_in_rxnorm,
       count_if(status IN ('map', 'possible') AND any_tty IS NOT NULL AND product_tty IS NULL) AS rxcui_not_a_product,
       count_if(status IN ('map', 'possible') AND (in_cms816 OR in_cms871)) AS in_a_value_set,
       count_if(status IN ('map', 'possible') AND product_suppress = 'E' AND NOT (in_cms816 OR in_cms871)) AS non_prescribable_code
FROM dqm_map.audit_decisions_{round}
GROUP BY 1, 2 ORDER BY 1, 2;

-- C. Conflicts: one application key (local code, plus the medication name for parts C-E) with different
--    decisions or codes (resolve before the the prepare stage rerun)
WITH eff AS (
  SELECT med_system, med_code, IF(match_level = 'names', coalesce(names, ''), '') AS name_key,
         count(DISTINCT IF(status IN ('map', 'possible'), status || ':' || coalesce(rxcui, ''), status)) AS variants_
  FROM dqm_map.audit_decisions_{round}
  WHERE status IN ('map', 'possible', 'no')
  GROUP BY 1, 2, 3
)
SELECT count_if(variants_ > 1) AS local_codes_with_conflicting_decisions FROM eff;

-- D. THE GATE (registered, parts A and B): glucose-lowering codes found among the excluded codes, and
--    those whose RxNorm product is in a measure value set (= relevant misses). Administrations are
--    2017-2019 counts as in the worklist (already masked below 10 there).
SELECT part,
       count(*) AS codes_reviewed,
       count_if(status IN ('map', 'possible')) AS glucose_lowering_found,
       count_if(status IN ('map', 'possible') AND (in_cms816 OR in_cms871)) AS relevant_misses,
       count_if(status IN ('map', 'possible') AND in_cms816) AS relevant_cms816,
       count_if(status IN ('map', 'possible') AND in_cms871) AS relevant_cms871,
       sum(IF(status IN ('map', 'possible') AND (in_cms816 OR in_cms871) AND administrations <> '<10',
              try_cast(administrations AS bigint), 0)) AS relevant_miss_administrations_10plus,
       count_if(status IN ('map', 'possible') AND (in_cms816 OR in_cms871) AND administrations = '<10') AS relevant_misses_under_10_administrations,
       count_if(status IN ('blank', 'missing', 'invalid')) AS undecided
FROM dqm_map.audit_decisions_{round}
WHERE part LIKE 'A%' OR part LIKE 'B%'
GROUP BY 1 ORDER BY 1;

-- E. The tail the 300 codes were drawn from (to scale the miss estimate; same rules as
--    30_review_package.sql): excluded codes outside the ~95% high-volume set, 2017-2019
WITH cohort AS (
  SELECT encounter_id FROM dqm_cohort.stay_years WHERE cohort_group = '1 primary 2017-2019'
),
codes AS (
  SELECT m.med_system, m.med_code, count(*) AS administrations, count_if(m.in_value_set) AS administrations_in_value_set
  FROM dqm_map.ma_codes m JOIN cohort c ON c.encounter_id = m.stay_id
  GROUP BY 1, 2
),
ranked AS (
  SELECT *, sum(administrations) OVER (ORDER BY administrations DESC, med_system, med_code
                                       ROWS UNBOUNDED PRECEDING) AS cum,
         sum(administrations) OVER () AS total
  FROM codes
),
review AS (
  SELECT DISTINCT regexp_extract(med_system, '[^/]+$') AS med_system, med_code FROM dqm_map.review_candidates
),
classified AS (
  SELECT r.*, (r.cum - r.administrations) < 0.95 * r.total AS high_volume,
         (r.administrations_in_value_set = 0 AND rv.med_code IS NULL) AS excluded
  FROM ranked r
  LEFT JOIN review rv ON rv.med_system = r.med_system AND rv.med_code = r.med_code
)
SELECT count_if(excluded AND NOT high_volume) AS tail_excluded_codes,
       sum(IF(excluded AND NOT high_volume, administrations, 0)) AS tail_excluded_administrations,
       sum(administrations) AS all_administrations_2017_2019,
       sum(administrations_in_value_set) AS value_set_administrations_2017_2019
FROM classified;

-- F. Review list (parts C-F): what the decisions add (counts only; administrations all years)
SELECT part, status,
       count_if(in_cms816 OR in_cms871) AS entries_landing_in_a_value_set,
       sum(IF((in_cms816 OR in_cms871) AND administrations <> '<10', try_cast(administrations AS bigint), 0)) AS administrations_10plus
FROM dqm_map.audit_decisions_{round}
WHERE part NOT LIKE 'A%' AND part NOT LIKE 'B%' AND status IN ('map', 'possible', 'no')
GROUP BY 1, 2 ORDER BY 1, 2;

DROP TABLE IF EXISTS dqm_map.med_additions_{round};

-- 2. Reviewed additions per variant, by local code (only codes landing in a value set matter for the
--    measures, but every decided product is kept for the record). Part E (mixes) is already mapped
--    by NDC: "no" there means drop, handled by med_drops below.
CREATE TABLE dqm_map.med_additions_{round}
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/med_additions/') AS
SELECT DISTINCT v.variant, d.match_level, d.med_system, d.med_code,
       IF(d.match_level = 'names', coalesce(d.names, ''), '') AS names, d.rxcui, d.part
FROM dqm_map.audit_decisions_{round} d
CROSS JOIN (VALUES 'a', 'c') AS v(variant)
WHERE d.part NOT LIKE 'E%'
  AND d.product_tty IS NOT NULL
  AND (d.status = 'map' OR (d.status = 'possible' AND v.variant = 'c'));

DROP TABLE IF EXISTS dqm_map.med_drops_{round};

CREATE TABLE dqm_map.med_drops_{round}
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/med_drops/') AS
SELECT DISTINCT med_system, med_code, coalesce(names, '') AS names
FROM dqm_map.audit_decisions_{round}
WHERE part LIKE 'E%' AND status = 'no';

DROP TABLE IF EXISTS dqm_map.med_names;

-- Medication id -> its names, built exactly as review_candidates' med_names (20_medication_mapping.sql),
-- so name-level decisions can be applied to every eMAR administration (prep_05_med_map_audited.sql)
CREATE TABLE dqm_map.med_names
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/med_names/') AS
SELECT json_extract_scalar(line, '$.id') AS med_id,
       array_join(transform(CAST(json_extract(line, '$.identifier') AS ARRAY(JSON)),
                            x -> json_extract_scalar(x, '$.value')), ' | ') AS names
FROM mimic_fhir.ndjson_parts WHERE source_file IN ('MimicMedication', 'MimicMedicationMix');

-- G. What each variant would map (counts only): local codes added, mixes dropped
SELECT variant, match_level, count(*) AS keys_added, count(DISTINCT med_system || '|' || med_code) AS local_codes,
       count(DISTINCT rxcui) AS products
FROM dqm_map.med_additions_{round} GROUP BY 1, 2 ORDER BY 1, 2;

SELECT count(*) AS mixes_dropped FROM dqm_map.med_drops_{round};
