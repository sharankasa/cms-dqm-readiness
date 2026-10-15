-- =============================================================================
-- 31_status_checks.sql — the medication mapping follow-up: did the FHIR conversion keep the eMAR events?
-- Non-outcome: no measure, no rate. Counts only (masked below 10). No drug names.
--
-- Why: the review package (30_review_package.sql, 2026-10-09) found far fewer not-given events in
-- MIMIC-IV-on-FHIR than in the raw eMAR table: "Not Given" 140,099 vs 2,978,274; "Not Given per
-- Sliding Scale" 1,236 vs 943,457; while 2,539,964 FHIR administrations have no event at all (raw
-- blank: 446,102). The raw table is MIMIC-IV 3.1 (adds 2020-2022), the FHIR release is built from 2.2,
-- so the totals cannot be compared directly. This script compares the SAME hospital stays.
--   1. dqm_map.ma_emar_method  status, dosage present, and event (dosage.method) per FHIR eMAR
--                             administration
--   A. FHIR: status x dosage present x event present
--   B. Same stays, raw 3.1 eMAR vs FHIR, per event (whitespace trimmed on both sides)
--   C. Same stays, eMAR rows per stay: raw vs FHIR (same, FHIR fewer, FHIR more, missing)
--   D. Administrations that map into a measure value set, by event: all years and 2017-2019 cohort
--      (how much the status problem touches the two measures)
-- Run with pipeline/athena/run_sql.py --db dqm_map --param loc=s3://.../dqm/map/<run>
-- Needs: dqm_map.ma_codes (30_review_package.sql), dqm_cohort.stay_years (the year linkage), mimiciv_hosp.emar (P2),
-- mimic_fhir.ndjson_parts. Scans about 4 to 5 GB (a few cents).
-- =============================================================================

DROP TABLE IF EXISTS dqm_map.ma_emar_method;

CREATE TABLE dqm_map.ma_emar_method
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/ma_emar_method/') AS
SELECT json_extract_scalar(line, '$.id') AS ma_id,
       json_extract_scalar(line, '$.status') AS status,
       json_extract(line, '$.dosage') IS NOT NULL AS has_dosage,
       json_extract(line, '$.dosage.dose') IS NOT NULL AS has_dose,
       json_extract_scalar(line, '$.dosage.method.coding[0].code') AS method
FROM mimic_fhir.ndjson_parts
WHERE source_file = 'MimicMedicationAdministration';

-- A. FHIR eMAR administrations: status x dosage present x dose present x event present
SELECT status, has_dosage, has_dose, method IS NOT NULL AS has_event,
       CASE WHEN count(*) < 10 THEN '<10' ELSE cast(count(*) AS varchar) END AS administrations
FROM dqm_map.ma_emar_method
GROUP BY 1, 2, 3, 4 ORDER BY count(*) DESC;

-- B. Same stays (FHIR hospital stays matched to MIMIC-IV 3.1), per event: raw eMAR rows vs FHIR
--    administrations. Ratio shown only when both counts are 10 or more.
WITH stays AS (
  SELECT encounter_id, try_cast(hadm_id AS bigint) AS hadm_id
  FROM dqm_cohort.stay_years WHERE anchor_year_group IS NOT NULL
),
raw AS (
  SELECT trim(coalesce(e.event_txt, '')) AS event, count(*) AS n
  FROM mimiciv_hosp.emar e JOIN stays s ON s.hadm_id = e.hadm_id
  GROUP BY 1
),
fhir AS (
  SELECT trim(coalesce(m.method, '')) AS event, count(*) AS n
  FROM dqm_map.ma_emar_method m
  JOIN dqm_map.ma_codes c ON c.ma_id = m.ma_id
  JOIN stays s ON s.encounter_id = c.stay_id
  GROUP BY 1
)
SELECT '[' || coalesce(r.event, f.event) || ']' AS event,
       CASE WHEN r.n IS NULL THEN '0' WHEN r.n < 10 THEN '<10' ELSE cast(r.n AS varchar) END AS raw_emar_rows,
       CASE WHEN f.n IS NULL THEN '0' WHEN f.n < 10 THEN '<10' ELSE cast(f.n AS varchar) END AS fhir_administrations,
       CASE WHEN r.n >= 10 AND f.n >= 10 THEN round(1.0 * f.n / r.n, 3) END AS fhir_over_raw
FROM raw r FULL OUTER JOIN fhir f ON f.event = r.event
ORDER BY coalesce(r.n, 0) DESC, coalesce(f.n, 0) DESC;

-- C. Same stays, eMAR rows per stay: raw vs FHIR
WITH stays AS (
  SELECT encounter_id, try_cast(hadm_id AS bigint) AS hadm_id
  FROM dqm_cohort.stay_years WHERE anchor_year_group IS NOT NULL
),
raw AS (
  SELECT e.hadm_id, count(*) AS n
  FROM mimiciv_hosp.emar e JOIN stays s ON s.hadm_id = e.hadm_id
  GROUP BY 1
),
fhir AS (
  SELECT s.hadm_id, count(*) AS n
  FROM dqm_map.ma_codes c JOIN stays s ON s.encounter_id = c.stay_id
  WHERE c.source_file = 'MimicMedicationAdministration'
  GROUP BY 1
),
cmp AS (
  SELECT CASE WHEN r.n IS NULL THEN 'FHIR rows, no raw eMAR rows'
              WHEN f.n IS NULL THEN 'raw eMAR rows, no FHIR rows'
              WHEN f.n = r.n THEN 'same count'
              WHEN f.n < r.n THEN 'FHIR fewer'
              ELSE 'FHIR more' END AS comparison,
         coalesce(r.n, 0) AS raw_n, coalesce(f.n, 0) AS fhir_n
  FROM raw r FULL OUTER JOIN fhir f ON f.hadm_id = r.hadm_id
)
SELECT comparison,
       CASE WHEN count(*) < 10 THEN '<10' ELSE cast(count(*) AS varchar) END AS stays,
       CASE WHEN sum(raw_n) < 10 THEN '<10' ELSE cast(sum(raw_n) AS varchar) END AS raw_emar_rows,
       CASE WHEN sum(fhir_n) < 10 THEN '<10' ELSE cast(sum(fhir_n) AS varchar) END AS fhir_administrations
FROM cmp GROUP BY 1 ORDER BY count(*) DESC;

-- D. Administrations that map into a measure value set (CMS816/CMS871 drugs), by event:
--    all years, and the 2017-2019 readiness cohort
WITH cohort AS (
  SELECT encounter_id FROM dqm_cohort.stay_years WHERE cohort_group = '1 primary 2017-2019'
),
vs AS (
  SELECT c.ma_id, c.stay_id, '[' || coalesce(trim(m.method), '<absent>') || ']' AS event
  FROM dqm_map.ma_codes c JOIN dqm_map.ma_emar_method m ON m.ma_id = c.ma_id
  WHERE c.in_value_set
)
SELECT v.event,
       CASE WHEN count(*) < 10 THEN '<10' ELSE cast(count(*) AS varchar) END AS all_years,
       CASE WHEN count(k.encounter_id) < 10 THEN '<10' ELSE cast(count(k.encounter_id) AS varchar) END AS cohort_2017_2019
FROM vs v LEFT JOIN cohort k ON k.encounter_id = v.stay_id
GROUP BY 1 ORDER BY count(*) DESC;
