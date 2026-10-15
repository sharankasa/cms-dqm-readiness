-- =============================================================================
-- 60_runtime_model.sql — plan the equivalence run's reference side from the the pilot pilot (counts and timings only).
-- A. Pilot: the unmodified engine's time per patient-year group (whole record), by record size.
--    One group = one patient and discharge year; its record is the patient's FULL record, so size =
--    the record's resource count (n_resources in each result line). Seconds = one evaluation of that
--    group for one measure.
-- B. S1 sample: how many patient-year groups fall in each record-size bucket (what the equivalence run will run).
-- Run with pipeline/athena/run_sql.py --db dqm_pilot (reads dqm_pilot.res_ref from compare_run.py and
-- the dqm_s1 tables from sample_run.py).
-- =============================================================================

-- A.
WITH r AS (
  SELECT try(json_parse(line)) AS j FROM dqm_pilot.res_ref WHERE trim(line) <> ''
),
g AS (
  SELECT json_extract_scalar(j, '$.measure') AS measure,
         json_extract_scalar(j, '$.patient_id') AS pid, json_extract_scalar(j, '$.mp_year') AS yr,
         max(try(CAST(json_extract_scalar(j, '$.n_resources') AS bigint))) AS n,
         max(try(CAST(json_extract_scalar(j, '$.seconds') AS double))) AS s
  FROM r WHERE j IS NOT NULL GROUP BY 1, 2, 3
)
SELECT measure,
       CASE WHEN n < 1000 THEN '1 under 1k' WHEN n < 5000 THEN '2 1k-5k' WHEN n < 20000 THEN '3 5k-20k'
            WHEN n < 50000 THEN '4 20k-50k' WHEN n < 100000 THEN '5 50k-100k' ELSE '6 100k+' END AS record_size,
       CASE WHEN count(*) < 10 THEN '<10' ELSE cast(count(*) AS varchar) END AS groups,
       round(approx_percentile(n, 0.5)) AS median_resources,
       round(approx_percentile(s, 0.5), 1) AS median_seconds,
       round(approx_percentile(s, 0.9), 1) AS p90_seconds,
       round(max(s), 1) AS max_seconds,
       round(sum(s)) AS total_seconds
FROM g GROUP BY 1, 2 ORDER BY 1, 2;

-- B.
WITH sizes AS (
  SELECT patient_id, count(*) AS n FROM dqm_s1.ref_lines WHERE patient_id IS NOT NULL GROUP BY 1
),
groups AS (
  SELECT DISTINCT patient_id, mp_year FROM dqm_s1.sample_stays
)
SELECT CASE WHEN s.n < 1000 THEN '1 under 1k' WHEN s.n < 5000 THEN '2 1k-5k' WHEN s.n < 20000 THEN '3 5k-20k'
            WHEN s.n < 50000 THEN '4 20k-50k' WHEN s.n < 100000 THEN '5 50k-100k'
            WHEN s.n < 200000 THEN '6 100k-200k' ELSE '7 200k+' END AS record_size,
       CASE WHEN count(*) < 10 THEN '<10' ELSE cast(count(*) AS varchar) END AS patient_year_groups,
       round(approx_percentile(s.n, 0.5)) AS median_resources
FROM groups g JOIN sizes s ON s.patient_id = g.patient_id
GROUP BY 1 ORDER BY 1;
