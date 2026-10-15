-- =============================================================================
-- 10_eicu_completeness.sql: R3 (registered secondary, registration §4.7). How completely do ICUs in the eICU
-- Collaborative Research Database (2014-2015, US hospitals) document the data elements CMS816 and CMS871 use?
-- SQL only: no measure is run. Per hospital, share of adult ICU stays with: any glucose result, a bedside
-- glucose, a laboratory glucose, an insulin order or infusion, a diabetes diagnosis, comfort-care or
-- end-of-life documentation; and the share of glucose results recorded in mg/dL (the unit both measures compare).
-- Hospitals with < 10 eligible stays are pooled (registered). Output: aggregates only.
-- Run: python3 pipeline/athena/run_sql.py --sql pipeline/r3/sql/10_eicu_completeness.sql --db dqm_eicu
--        --out results/r3-eicu-<stamp> --param loc=s3://your-bucket/dqm/eicu/r3-<stamp>
-- Reads dqm_eicu.{patient, lab, medication, infusiondrug, diagnosis, pasthistory, careplangeneral, careplaneol}
-- (pipeline/r3/eicu_register.py). Element definitions are deliberately simple and reported as such.
-- =============================================================================

DROP TABLE IF EXISTS dqm_eicu.r3_stay_flags;

CREATE TABLE dqm_eicu.r3_stay_flags
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/r3_stay_flags/') AS
WITH stays AS (
  SELECT patientunitstayid AS stay, hospitalid AS hospital
  FROM dqm_eicu.patient
  WHERE age = '> 89' OR try_cast(age AS integer) >= 18
),
glu AS (
  SELECT patientunitstayid AS stay,
         max(IF(lower(labname) = 'bedside glucose', 1, 0)) AS bedside,
         max(IF(lower(labname) = 'glucose', 1, 0)) AS lab_glucose,
         count(*) AS results,
         count_if(lower(trim(coalesce(labmeasurenamesystem, ''))) = 'mg/dl') AS results_mgdl
  FROM dqm_eicu.lab
  WHERE lower(labname) IN ('glucose', 'bedside glucose') AND try_cast(labresult AS double) IS NOT NULL
  GROUP BY 1
),
ins AS (
  SELECT DISTINCT patientunitstayid AS stay FROM dqm_eicu.medication
  WHERE lower(drugname) LIKE '%insulin%' AND lower(coalesce(drugordercancelled, '')) <> 'yes'
  UNION
  SELECT DISTINCT patientunitstayid FROM dqm_eicu.infusiondrug WHERE lower(drugname) LIKE '%insulin%'
),
dm AS (
  SELECT DISTINCT patientunitstayid AS stay FROM dqm_eicu.diagnosis
  WHERE lower(diagnosisstring) LIKE '%diabetes mellitus%'
     OR regexp_like(coalesce(icd9code, ''), '(^|[ ,])(250|E08|E09|E10|E11|E13)')
),
dm_hist AS (
  SELECT DISTINCT patientunitstayid AS stay FROM dqm_eicu.pasthistory WHERE lower(pasthistorypath) LIKE '%diabetes%'
),
eol AS (
  SELECT DISTINCT patientunitstayid AS stay FROM dqm_eicu.careplangeneral
  WHERE lower(cplgroup) = 'care limitation' AND lower(cplitemvalue) LIKE '%comfort%'
  UNION
  SELECT DISTINCT patientunitstayid FROM dqm_eicu.careplaneol
)
SELECT s.stay, s.hospital,
       IF(g.stay IS NULL, 0, 1)             AS has_glucose,
       coalesce(g.bedside, 0)               AS has_bedside_glucose,
       coalesce(g.lab_glucose, 0)           AS has_lab_glucose,
       coalesce(g.results, 0)               AS glucose_results,
       coalesce(g.results_mgdl, 0)          AS glucose_results_mgdl,
       IF(i.stay IS NULL, 0, 1)             AS has_insulin,
       IF(d.stay IS NULL, 0, 1)             AS has_diabetes_diagnosis,
       IF(d.stay IS NULL AND h.stay IS NULL, 0, 1) AS has_diabetes_any,
       IF(e.stay IS NULL, 0, 1)             AS has_comfort_or_eol
FROM stays s
LEFT JOIN glu g ON g.stay = s.stay
LEFT JOIN ins i ON i.stay = s.stay
LEFT JOIN dm d ON d.stay = s.stay
LEFT JOIN dm_hist h ON h.stay = s.stay
LEFT JOIN eol e ON e.stay = s.stay;

-- 1. Overall (all adult ICU stays): the funnel plots' reference proportions; counts masked below 10
SELECT CASE WHEN count(*) < 10 THEN '<10' ELSE cast(count(*) AS varchar) END AS eligible_stays,
       count(DISTINCT hospital) AS hospitals,
       round(100.0 * avg(has_glucose), 2) AS pct_glucose,
       round(100.0 * avg(has_bedside_glucose), 2) AS pct_bedside_glucose,
       round(100.0 * avg(has_lab_glucose), 2) AS pct_lab_glucose,
       round(100.0 * avg(has_insulin), 2) AS pct_insulin,
       round(100.0 * avg(has_diabetes_diagnosis), 2) AS pct_diabetes_diagnosis,
       round(100.0 * avg(has_diabetes_any), 2) AS pct_diabetes_any,
       round(100.0 * avg(has_comfort_or_eol), 2) AS pct_comfort_or_eol,
       round(100.0 * sum(glucose_results_mgdl) / nullif(sum(glucose_results), 0), 2) AS pct_glucose_results_mgdl
FROM dqm_eicu.r3_stay_flags;

-- 2. Per hospital (hospitals with < 10 eligible stays pooled into one row): funnel plot input
WITH h AS (
  SELECT hospital, count(*) AS n FROM dqm_eicu.r3_stay_flags GROUP BY 1
),
f AS (
  SELECT IF(h.n < 10, 'pooled', s.hospital) AS hospital,
         s.has_glucose, s.has_bedside_glucose, s.has_lab_glucose, s.glucose_results, s.glucose_results_mgdl,
         s.has_insulin, s.has_diabetes_diagnosis, s.has_diabetes_any, s.has_comfort_or_eol
  FROM dqm_eicu.r3_stay_flags s JOIN h ON h.hospital = s.hospital
)
SELECT hospital,
       count(*) AS eligible_stays,
       round(100.0 * avg(has_glucose), 2) AS pct_glucose,
       round(100.0 * avg(has_bedside_glucose), 2) AS pct_bedside_glucose,
       round(100.0 * avg(has_lab_glucose), 2) AS pct_lab_glucose,
       round(100.0 * avg(has_insulin), 2) AS pct_insulin,
       round(100.0 * avg(has_diabetes_diagnosis), 2) AS pct_diabetes_diagnosis,
       round(100.0 * avg(has_diabetes_any), 2) AS pct_diabetes_any,
       round(100.0 * avg(has_comfort_or_eol), 2) AS pct_comfort_or_eol,
       sum(glucose_results) AS glucose_results,
       round(100.0 * sum(glucose_results_mgdl) / nullif(sum(glucose_results), 0), 2) AS pct_glucose_results_mgdl
FROM f
GROUP BY 1
HAVING count(*) >= 10
ORDER BY 2 DESC;

-- 3. Spread across hospitals with >= 100 eligible stays (registered summary): median, quartiles, range
WITH p AS (
  SELECT hospital, count(*) AS n,
         100.0 * avg(has_glucose) AS glucose, 100.0 * avg(has_bedside_glucose) AS bedside,
         100.0 * avg(has_insulin) AS insulin, 100.0 * avg(has_diabetes_any) AS diabetes,
         100.0 * avg(has_comfort_or_eol) AS comfort_eol,
         100.0 * sum(glucose_results_mgdl) / nullif(sum(glucose_results), 0) AS mgdl
  FROM dqm_eicu.r3_stay_flags GROUP BY 1 HAVING count(*) >= 100
)
SELECT count(*) AS hospitals_100_plus,
       round(approx_percentile(glucose, 0.5), 1) AS glucose_median,
       round(approx_percentile(glucose, 0.25), 1) AS glucose_q1, round(approx_percentile(glucose, 0.75), 1) AS glucose_q3,
       round(min(glucose), 1) AS glucose_min, round(max(glucose), 1) AS glucose_max,
       round(approx_percentile(bedside, 0.5), 1) AS bedside_median,
       round(approx_percentile(bedside, 0.25), 1) AS bedside_q1, round(approx_percentile(bedside, 0.75), 1) AS bedside_q3,
       round(min(bedside), 1) AS bedside_min, round(max(bedside), 1) AS bedside_max,
       round(approx_percentile(insulin, 0.5), 1) AS insulin_median,
       round(approx_percentile(insulin, 0.25), 1) AS insulin_q1, round(approx_percentile(insulin, 0.75), 1) AS insulin_q3,
       round(min(insulin), 1) AS insulin_min, round(max(insulin), 1) AS insulin_max,
       round(approx_percentile(diabetes, 0.5), 1) AS diabetes_median,
       round(approx_percentile(diabetes, 0.25), 1) AS diabetes_q1, round(approx_percentile(diabetes, 0.75), 1) AS diabetes_q3,
       round(min(diabetes), 1) AS diabetes_min, round(max(diabetes), 1) AS diabetes_max,
       round(approx_percentile(comfort_eol, 0.5), 1) AS comfort_eol_median,
       round(approx_percentile(comfort_eol, 0.25), 1) AS comfort_eol_q1, round(approx_percentile(comfort_eol, 0.75), 1) AS comfort_eol_q3,
       round(min(comfort_eol), 1) AS comfort_eol_min, round(max(comfort_eol), 1) AS comfort_eol_max,
       round(approx_percentile(mgdl, 0.5), 1) AS mgdl_median,
       round(min(mgdl), 1) AS mgdl_min, round(max(mgdl), 1) AS mgdl_max
FROM p;
