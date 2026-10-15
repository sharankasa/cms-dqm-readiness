-- =============================================================================
-- 20_persistence_2020_2022.sql: registered sensitivity check (registration §7): do the source-data patterns behind
-- the readiness problems persist in MIMIC-IV 3.1 admissions from 2020-2022, which MIMIC-IV-on-FHIR 2.1 does
-- not contain? Compared with 2017-2019 (the study cohort's years), excluding admissions coded U07.1
-- (COVID-19). SQL only, no measure; aggregates only, counts below 10 masked.
-- Patterns (source side of the FHIR-level problems):
--   observation-type admissions (become observation encounters, outside both measures);
--   an emergency-department stay ending after the admission time (pre-admission results fall outside windows);
--   bedside glucose with a value but no unit (ICU items 225664, 226537; the study's largest effect);
--   laboratory glucose (items 50931, 50809) in mg/dL.
-- Run: python3 pipeline/athena/run_sql.py --sql pipeline/cohort/20_persistence_2020_2022.sql --db analytics
--        --out results/persistence-2020-2022-<stamp> --param loc=s3://your-bucket/dqm/cohort/persistence-<stamp>
-- Statement 3 reads the 3.3 GB chartevents file in one pass (about 20-30 minutes, a few cents).
-- Reads analytics.dqm_adm, mimiciv_raw.admissions (the typed view mimiciv_hosp.admissions omits the ED times),
-- mimiciv_hosp.{diagnoses_icd, labevents}, mimiciv_raw.chartevents.
-- =============================================================================

DROP TABLE IF EXISTS analytics.dqm_persistence_groups;

CREATE TABLE analytics.dqm_persistence_groups
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/groups/') AS
WITH r AS (
  SELECT hadm_id, is_observation_type, admittime,
         cast(substr(anchor_year_group, 1, 4) AS integer) + (year(admittime) - anchor_year) AS rs,
         cast(substr(anchor_year_group, 8, 4) AS integer) + (year(admittime) - anchor_year) AS re
  FROM analytics.dqm_adm
),
covid AS (
  SELECT DISTINCT try_cast(hadm_id AS bigint) AS hadm_id
  FROM mimiciv_hosp.diagnoses_icd
  WHERE cast(icd_version AS varchar) = '10' AND icd_code = 'U071'
)
SELECT r.hadm_id, r.is_observation_type, r.admittime,
       CASE WHEN r.rs >= 2017 AND r.re <= 2019 THEN '2017-2019' WHEN r.rs >= 2020 THEN '2020-2022' END AS year_group
FROM r LEFT JOIN covid c ON c.hadm_id = r.hadm_id
WHERE c.hadm_id IS NULL AND ((r.rs >= 2017 AND r.re <= 2019) OR r.rs >= 2020);

-- 1. Admissions: observation type, and an emergency-department stay ending after the admission time
SELECT g.year_group,
       count(*) AS admissions,
       round(100.0 * count_if(g.is_observation_type) / count(*), 1) AS pct_observation_type,
       count_if(try_cast(nullif(a.edregtime, '') AS timestamp) IS NOT NULL) AS admissions_with_ed_stay,
       round(100.0 * count_if(try_cast(nullif(a.edouttime, '') AS timestamp) > try_cast(nullif(a.admittime, '') AS timestamp))
             / nullif(count_if(try_cast(nullif(a.edregtime, '') AS timestamp) IS NOT NULL), 0), 1) AS pct_ed_ends_after_admission
FROM analytics.dqm_persistence_groups g
JOIN mimiciv_raw.admissions a ON try_cast(a.hadm_id AS bigint) = g.hadm_id
GROUP BY 1 ORDER BY 1;

-- 2. Laboratory glucose results: share recorded in mg/dL
SELECT g.year_group,
       count(*) AS lab_glucose_results,
       round(100.0 * count_if(lower(coalesce(l.valueuom, '')) = 'mg/dl') / count(*), 2) AS pct_mg_dl,
       round(100.0 * count_if(coalesce(l.valueuom, '') = '') / count(*), 2) AS pct_no_unit
FROM mimiciv_hosp.labevents l
JOIN analytics.dqm_persistence_groups g ON g.hadm_id = try_cast(l.hadm_id AS bigint)
WHERE try_cast(l.itemid AS integer) IN (50931, 50809) AND try_cast(l.valuenum AS double) IS NOT NULL
GROUP BY 1 ORDER BY 1;

-- 3. Bedside (ICU) glucose results: share with a numeric value but no unit (one pass over chartevents)
SELECT g.year_group,
       count(*) AS bedside_glucose_results,
       round(100.0 * count_if(coalesce(c.valueuom, '') = '') / count(*), 2) AS pct_no_unit,
       round(100.0 * count_if(lower(coalesce(c.valueuom, '')) = 'mg/dl') / count(*), 2) AS pct_mg_dl
FROM mimiciv_raw.chartevents c
JOIN analytics.dqm_persistence_groups g ON g.hadm_id = try_cast(nullif(c.hadm_id, '') AS bigint)
WHERE c.itemid IN ('225664', '226537') AND try_cast(nullif(c.valuenum, '') AS double) IS NOT NULL
GROUP BY 1 ORDER BY 1;
