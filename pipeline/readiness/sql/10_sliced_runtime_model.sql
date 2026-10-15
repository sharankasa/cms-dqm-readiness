-- =============================================================================
-- 10_sliced_runtime_model.sql — predicted engine time per stay on the SLICED side, for cutting worker
-- inputs into shards of about equal time (scheduling only: which pod evaluates a stay, never how).
-- Fitted on the S1 sample's sliced run (2,000 stays, one stay per engine call, m7i.large; registered
-- run, outcome already reported): per measure, stays are grouped into work bands by the pre-registered
-- work metric (band = floor(log10(work)); band -1 = work 0) and each band's MEAN seconds is the
-- prediction for any stay in that band. Above the highest band seen, the top band's seconds per unit of
-- work is extrapolated linearly. Work metrics of the sample stays come from the the prepare stage run the sample was
-- sliced from ({s1_b4db}).
-- Database: dqm_readiness. Printed: per measure and band, stays / mean / max seconds (no identifiers).
-- =============================================================================

-- part model
CREATE EXTERNAL TABLE s1_sliced_results (line string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION '{s1_sliced_out}/results/';

CREATE TABLE sliced_runtime_bands
WITH (format = 'PARQUET', external_location = '{model_loc}/sliced_runtime_bands/') AS
WITH r AS (
  SELECT json_extract_scalar(j, '$.measure') AS measure, json_extract_scalar(j, '$.unit_id') AS unit_id,
         max(try(CAST(json_extract_scalar(j, '$.seconds') AS double))) AS s
  FROM (SELECT try(json_parse(line)) AS j FROM s1_sliced_results WHERE trim(line) <> '')
  WHERE j IS NOT NULL AND json_extract_scalar(j, '$.error') IS NULL
  GROUP BY 1, 2
),
x AS (
  SELECT IF(r.measure LIKE 'CMS816%', 'CMS816', 'CMS871') AS measure,
         CAST(IF(r.measure LIKE 'CMS816%', m.work_cms816, m.work_cms871) AS double) AS work, r.s
  FROM r JOIN {s1_b4db}.stay_manifest m ON m.unit_id = r.unit_id
  WHERE r.s IS NOT NULL
)
SELECT measure, IF(work <= 0, -1, CAST(floor(log10(work)) AS integer)) AS band,
       count(*) AS stays, avg(s) AS mean_s, max(s) AS max_s, avg(work) AS mean_work
FROM x GROUP BY 1, 2;

-- part summary
SELECT measure, band,
       CASE WHEN stays BETWEEN 1 AND 9 THEN '<10' ELSE CAST(stays AS varchar) END AS stays,
       round(mean_s, 2) AS mean_seconds, round(max_s, 1) AS max_seconds
FROM sliced_runtime_bands ORDER BY measure, band;
