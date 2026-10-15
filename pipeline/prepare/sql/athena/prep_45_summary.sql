-- =============================================================================
-- prep_45_summary.sql — the prepare stage prepare: what comes back to the laptop. AGGREGATES ONLY, counts below 10
-- masked as '<10' (credentialed data rule; applied on the open demo too, for one code path).
-- `cohort (encounter_id, cohort_group)` is a view made by the runner (credentialed: the year linkage's
-- dqm_cohort.stay_years; open demo: one group).
--   A. mapping: resources read, rewritten and kept, per source file
--   B. hospital-stay classes the mapping leaves untyped (not inpatient for the measures)
--   C. slices: stays, resources, size distribution, by cohort group
--   D. workload metric distribution per measure, by cohort group
--   E. ED overlap: linked ED visit vs admission start (does ED time join the hospitalization?)
--   F. MC-06: time shape of medication administrations (in slices, and all)
-- =============================================================================

-- A. Mapping per source file. rows_in_mapped_table = every row read on the open demo; on credentialed
--    data only the kept rows are stored (so it equals kept_for_slicing there).
SELECT source_file,
       CASE WHEN count(*) < 10 THEN '<10' ELSE cast(count(*) AS varchar) END AS rows_in_mapped_table,
       CASE WHEN count_if(changed) BETWEEN 1 AND 9 THEN '<10' ELSE cast(count_if(changed) AS varchar) END AS rewritten,
       CASE WHEN count_if(keep) BETWEEN 1 AND 9 THEN '<10' ELSE cast(count_if(keep) AS varchar) END AS kept_for_slicing
FROM mapped GROUP BY 1 ORDER BY 1;

-- B. Hospital stays by encounter class: typed by the mapping or not
SELECT coalesce(json_extract_scalar(line, '$.class.code'), '<none>') AS encounter_class,
       changed AS typed_by_mapping,
       CASE WHEN count(*) < 10 THEN '<10' ELSE cast(count(*) AS varchar) END AS stays
FROM mapped WHERE source_file = 'MimicEncounter'
GROUP BY 1, 2 ORDER BY 1, 2;

-- C. Slices by cohort group
SELECT coalesce(k.cohort_group, '<no cohort row>') AS cohort_group,
       CASE WHEN count(*) < 10 THEN '<10' ELSE cast(count(*) AS varchar) END AS stays,
       CASE WHEN count_if(m.n_resources > 0) BETWEEN 1 AND 9 THEN '<10' ELSE cast(count_if(m.n_resources > 0) AS varchar) END AS stays_with_slice,
       IF(count(*) >= 10, sum(m.n_resources)) AS resources_in_slices,
       IF(count(*) >= 10, approx_percentile(m.n_resources, 0.5)) AS p50_resources,
       IF(count(*) >= 10, approx_percentile(m.n_resources, 0.99)) AS p99_resources,
       IF(count(*) >= 10, max(m.n_resources)) AS max_resources
FROM stay_manifest m LEFT JOIN cohort k ON k.encounter_id = m.unit_id
GROUP BY 1 ORDER BY 1;

-- D. Workload metric by cohort group
SELECT coalesce(k.cohort_group, '<no cohort row>') AS cohort_group,
       CASE WHEN count_if(m.work_cms816 > 0) BETWEEN 1 AND 9 THEN '<10' ELSE cast(count_if(m.work_cms816 > 0) AS varchar) END AS stays_work816_gt0,
       IF(count(*) >= 10, approx_percentile(m.work_cms816, 0.5))   AS w816_p50,
       IF(count(*) >= 10, approx_percentile(m.work_cms816, 0.9))   AS w816_p90,
       IF(count(*) >= 10, approx_percentile(m.work_cms816, 0.99))  AS w816_p99,
       IF(count(*) >= 10, approx_percentile(m.work_cms816, 0.999)) AS w816_p999,
       IF(count(*) >= 10, max(m.work_cms816)) AS w816_max,
       IF(count(*) >= 10, sum(m.work_cms816)) AS w816_sum,
       IF(count(*) >= 10, approx_percentile(m.work_cms871, 0.5))   AS w871_p50,
       IF(count(*) >= 10, approx_percentile(m.work_cms871, 0.9))   AS w871_p90,
       IF(count(*) >= 10, approx_percentile(m.work_cms871, 0.99))  AS w871_p99,
       IF(count(*) >= 10, approx_percentile(m.work_cms871, 0.999)) AS w871_p999,
       IF(count(*) >= 10, max(m.work_cms871)) AS w871_max,
       IF(count(*) >= 10, sum(m.work_cms871)) AS w871_sum
FROM stay_manifest m LEFT JOIN cohort k ON k.encounter_id = m.unit_id
GROUP BY 1 ORDER BY 1;

-- E. ED overlap: the latest-ending linked ED visit of each stay vs the admission start. CMS's
--    hospitalizationWithObservation adds an ED visit only if it ends 1 hour or less on or before the
--    inpatient start; an ED record that ends after the start never extends the hospitalization.
WITH ed AS (
  SELECT i.part_of_id AS unit_id, max(i.t_end) AS ed_end
  FROM res_index i JOIN res r ON r.rtype = i.rtype AND r.id = i.id
  WHERE i.rtype = 'Encounter' AND i.part_of_id IS NOT NULL AND json_extract_scalar(r.json, '$.class.code') = 'EMER'
  GROUP BY 1
),
x AS (
  SELECT u.unit_id, u.enc_start, ed.ed_end,
         CASE WHEN ed.ed_end IS NULL THEN '0 no linked ED visit'
              WHEN ed.ed_end > u.enc_start THEN '1 ED ends after the admission starts (ED time not added)'
              WHEN ed.ed_end >= u.enc_start - INTERVAL '1' HOUR THEN '2 ED ends within 1 h before admission (added)'
              ELSE '3 ED ends more than 1 h before admission (not added)' END AS ed_vs_admission
  FROM units u LEFT JOIN ed ON ed.unit_id = u.unit_id
)
SELECT coalesce(k.cohort_group, '<no cohort row>') AS cohort_group, x.ed_vs_admission,
       CASE WHEN count(*) < 10 THEN '<10' ELSE cast(count(*) AS varchar) END AS stays,
       IF(count(*) >= 10, approx_percentile(date_diff('minute', x.enc_start, x.ed_end), 0.5)) AS median_minutes_ed_end_after_start
FROM x LEFT JOIN cohort k ON k.encounter_id = x.unit_id
GROUP BY 1, 2 ORDER BY 1, 2;

-- F1. MC-06: administrations kept for slicing (measure drugs), by source and time shape. CMS871 counts an
--     administration only if its whole period lies within the hospitalization: a start without an end
--     never counts there (CMS816 needs only the start).
SELECT CASE WHEN json_extract_scalar(json, '$.meta.profile[0]') LIKE '%-icu' THEN 'ICU' ELSE 'eMAR' END AS source,
       CASE WHEN json_extract_scalar(json, '$.effectiveDateTime') IS NOT NULL THEN 'single time'
            WHEN json_extract_scalar(json, '$.effectivePeriod.end') IS NOT NULL THEN 'period with end'
            WHEN json_extract_scalar(json, '$.effectivePeriod.start') IS NOT NULL THEN 'period without end'
            ELSE 'no time' END AS time_shape,
       CASE WHEN count(*) < 10 THEN '<10' ELSE cast(count(*) AS varchar) END AS administrations
FROM res WHERE rtype = 'MedicationAdministration'
GROUP BY 1, 2 ORDER BY 1, 2;

-- F2. The same for ALL administrations in the source data (any drug)
SELECT source_file,
       CASE WHEN json_extract_scalar(line, '$.effectiveDateTime') IS NOT NULL THEN 'single time'
            WHEN json_extract_scalar(line, '$.effectivePeriod.end') IS NOT NULL THEN 'period with end'
            WHEN json_extract_scalar(line, '$.effectivePeriod.start') IS NOT NULL THEN 'period without end'
            ELSE 'no time' END AS time_shape,
       CASE WHEN count(*) < 10 THEN '<10' ELSE cast(count(*) AS varchar) END AS administrations
FROM src WHERE source_file IN ('MimicMedicationAdministration', 'MimicMedicationAdministrationICU')
GROUP BY 1, 2 ORDER BY 1, 2;
