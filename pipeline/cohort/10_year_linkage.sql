-- =============================================================================
-- 10_year_linkage.sql — the year linkage: which real years each MIMIC-IV-on-FHIR hospital stay can fall in, and its
-- cohort group (registration §3 Cohort, decided 2026-10-06, refined 2026-10-07). Non-outcome: no measure,
-- no rate; counts only (masked below 10).
--
-- Rule (as registered):
--   1. In MIMIC-IV 3.1, per hadm_id: possible real years = anchor_year_group + (year(admittime) -
--      anchor_year): a 3-year range, clipped at 2019 (end of MIMIC-IV 2.2, which the FHIR release uses).
--   2. Join to FHIR by stay: Encounter.identifier with system .../identifier/encounter-hosp = hadm_id.
--   3. Primary (readiness) cohort: the whole possible range within 2017-2019.
--   4. Sensitivity group: ranges within 2014-2016, and ranges straddling 2016/2017.
--      Operationalised here as: range_start >= 2014 and range_end <= 2016; or range_start <= 2016 and
--      range_end >= 2017. Ranges reaching before 2014 are "older" (scale pillar only).
--   5. Unmatched FHIR stays are counted and excluded from readiness rates.
-- The scale pillar (S1, S2) uses ALL stays regardless of group.
-- Run with pipeline/athena/run_sql.py --db dqm_cohort --param loc=s3://.../dqm/cohort/<run>
-- Reads: mimic_fhir.ndjson_parts (MimicEncounter), analytics.dqm_adm (MIMIC-IV 3.1 admissions, P2).
-- =============================================================================

CREATE DATABASE IF NOT EXISTS dqm_cohort;

DROP TABLE IF EXISTS dqm_cohort.stay_years;

CREATE TABLE dqm_cohort.stay_years
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/stay_years/') AS
WITH fhir AS (
  SELECT json_extract_scalar(line, '$.id') AS encounter_id,
         regexp_extract(json_extract_scalar(line, '$.subject.reference'), '[^/]+$') AS patient_id,
         substr(json_extract_scalar(line, '$.period.end'), 1, 4) AS mp_year_shifted,
         json_extract_scalar(element_at(filter(CAST(json_extract(line, '$.identifier') AS ARRAY(JSON)),
                                               x -> json_extract_scalar(x, '$.system') LIKE '%/identifier/encounter-hosp'), 1),
                             '$.value') AS hadm_id
  FROM mimic_fhir.ndjson_parts
  WHERE source_file = 'MimicEncounter'
),
mimic AS (
  SELECT hadm_id, anchor_year_group,
         cast(substr(anchor_year_group, 1, 4) AS integer) + (year(admittime) - anchor_year) AS range_start,
         cast(substr(anchor_year_group, 8, 4) AS integer) + (year(admittime) - anchor_year) AS range_end_unclipped
  FROM analytics.dqm_adm
)
SELECT f.encounter_id, f.patient_id, f.hadm_id, f.mp_year_shifted, m.anchor_year_group,
       m.range_start, least(m.range_end_unclipped, 2019) AS range_end, m.range_end_unclipped,
       CASE WHEN m.hadm_id IS NULL THEN 'unmatched'
            WHEN m.range_start > 2019 THEN 'outside MIMIC-IV 2.2 years'
            WHEN m.range_start >= 2017 THEN '1 primary 2017-2019'
            WHEN m.range_start >= 2014 AND least(m.range_end_unclipped, 2019) <= 2016 THEN '2a sensitivity 2014-2016'
            WHEN m.range_start <= 2016 AND least(m.range_end_unclipped, 2019) >= 2017 THEN '2b sensitivity straddles 2016/2017'
            ELSE '3 older (possible years before 2014)' END AS cohort_group
FROM fhir f
LEFT JOIN mimic m ON m.hadm_id = try_cast(f.hadm_id AS bigint);

-- 1. Stays and patients per cohort group
SELECT cohort_group,
       CASE WHEN count(*) < 10 THEN '<10' ELSE cast(count(*) AS varchar) END AS stays,
       CASE WHEN count(DISTINCT patient_id) < 10 THEN '<10' ELSE cast(count(DISTINCT patient_id) AS varchar) END AS patients
FROM dqm_cohort.stay_years GROUP BY 1 ORDER BY 1;

-- 2. Linkage quality: FHIR stays, with a hadm_id identifier, matched to MIMIC-IV 3.1
SELECT count(*) AS fhir_hospital_stays,
       count(hadm_id) AS with_hadm_identifier,
       count_if(anchor_year_group IS NOT NULL) AS matched_in_mimic_iv_3_1,
       round(100.0 * count_if(anchor_year_group IS NOT NULL) / count(*), 3) AS pct_matched,
       count(DISTINCT hadm_id) AS distinct_hadm_ids
FROM dqm_cohort.stay_years;

-- 3. Possible-year ranges that the clip at 2019 shortened (stays in FHIR whose range would reach past 2019)
SELECT CASE WHEN range_end_unclipped > 2019 THEN 'range reaches past 2019 (clipped)' ELSE 'within 2008-2019' END AS clip,
       cohort_group,
       CASE WHEN count(*) < 10 THEN '<10' ELSE cast(count(*) AS varchar) END AS stays
FROM dqm_cohort.stay_years WHERE anchor_year_group IS NOT NULL GROUP BY 1, 2 ORDER BY 1, 2;

-- 4. Size of the SQL-only persistence-check set (registration §7): MIMIC-IV 3.1 admissions whose whole
--    possible range lies in 2020-2022 (not in the FHIR release)
SELECT count(*) AS admissions_2020_2022_only,
       count(DISTINCT subject_id) AS patients
FROM analytics.dqm_adm
WHERE cast(substr(anchor_year_group, 1, 4) AS integer) + (year(admittime) - anchor_year) >= 2020;
