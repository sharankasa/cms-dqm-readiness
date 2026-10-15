-- =============================================================================
-- 50_compare.sql — the pilot/the equivalence run: S1 comparison in Athena (the per-stay results stay in S3). Same rule as
-- pipeline/s1/compare.py: per (measure, stay), identical = same episode presence (no episode = all
-- false), same true/false for every population, and the same measure-observation values (CMS871
-- eligible days and event days); an engine error on either side is never a match.
-- Outputs COUNTS ONLY. Population counts (how many stays are in each population) are printed for
-- the S1 sample only; the pilot never shows them ("rates are not looked at", registration §5): {pop_counts}.
-- Tables: res_ref, res_sl = one row per result line (external, over <job>/results/).
-- Stopping rule (deviation 4): stays of shards stopped at the cost cap that have no whole-record
-- (reference) result are counted as 'stopped_reference', not 'missing_reference' ({stopped_case}
-- and {stopped_join} are filled by compare_run.py --stopped-shards; empty otherwise).
-- =============================================================================

CREATE EXTERNAL TABLE res_ref (line string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION '{ref_out}/results/';

CREATE EXTERNAL TABLE res_sl (line string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION '{sliced_out}/results/';

CREATE TABLE cmp
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/cmp-{stamp}/') AS
WITH raw AS (
  SELECT 'reference' AS side, try(json_parse(line)) AS j FROM res_ref WHERE trim(line) <> ''
  UNION ALL
  SELECT 'sliced' AS side, try(json_parse(line)) AS j FROM res_sl WHERE trim(line) <> ''
),
rows_ AS (
  SELECT side, j,
         json_extract_scalar(j, '$.measure') AS measure, json_extract_scalar(j, '$.unit_id') AS unit_id,
         json_extract_scalar(j, '$.error') AS err,
         coalesce(try(CAST(json_extract(j, '$.episode') AS boolean)), false) AS episode,
         try(CAST(json_extract(j, '$.pops') AS MAP(VARCHAR, BOOLEAN))) AS pops,
         try(CAST(json_extract(j, '$.obs') AS MAP(VARCHAR, JSON))) AS obs,
         try(CAST(json_extract_scalar(j, '$.seconds') AS double)) AS seconds,
         coalesce(try(CAST(json_extract_scalar(j, '$.group_units') AS integer)), 1) AS group_units
  FROM raw
),
normed AS (
  SELECT side, measure, unit_id, err, seconds / greatest(group_units, 1) AS seconds,
         CASE WHEN j IS NULL THEN 'BROKEN'
              WHEN err IS NOT NULL THEN 'ERROR'
              WHEN NOT episode THEN 'no-episode'
              ELSE array_join(array_sort(transform(filter(map_keys(pops), k -> NOT starts_with(k, 'MeasureObservation')),
                                                   k -> k || '=' || CAST(element_at(pops, k) AS varchar))), ';')
                   || '|' || IF(obs IS NULL, '', array_join(array_sort(transform(map_keys(obs),
                                                   k -> k || '=' || coalesce(json_format(element_at(obs, k)), 'null'))), ';'))
         END AS norm,
         IF(episode AND err IS NULL, filter(map_keys(pops), k -> NOT starts_with(k, 'MeasureObservation') AND element_at(pops, k)),
            CAST(ARRAY[] AS ARRAY(VARCHAR))) AS member_of
  FROM rows_
),
per_side AS (
  SELECT side, measure, unit_id, count(*) AS n_lines, min(norm) AS norm, max(norm) AS norm_max,
         max(seconds) AS seconds, max(member_of) AS member_of
  FROM normed WHERE norm <> 'BROKEN'
  GROUP BY side, measure, unit_id
)
SELECT coalesce(r.measure, s.measure) AS measure, coalesce(r.unit_id, s.unit_id) AS unit_id,
       r.n_lines AS ref_lines, s.n_lines AS sliced_lines,
       r.norm AS ref_norm, s.norm AS sliced_norm,
       (r.norm <> r.norm_max OR s.norm <> s.norm_max) AS inconsistent_duplicates,
       r.seconds AS ref_seconds, s.seconds AS sliced_seconds, r.member_of AS ref_member_of,
       CASE {stopped_case}
            WHEN r.unit_id IS NULL THEN 'missing_reference'
            WHEN s.unit_id IS NULL THEN 'missing_sliced'
            WHEN r.norm = 'ERROR' OR s.norm = 'ERROR' THEN 'error'
            WHEN r.norm = s.norm THEN 'identical'
            ELSE 'different' END AS outcome
FROM (SELECT * FROM per_side WHERE side = 'reference') r
FULL OUTER JOIN (SELECT * FROM per_side WHERE side = 'sliced') s
  ON s.measure = r.measure AND s.unit_id = r.unit_id
{stopped_join};

-- A. Outcome per measure (counts)
SELECT measure, outcome,
       CASE WHEN count(*) BETWEEN 1 AND 9 THEN '<10' ELSE cast(count(*) AS varchar) END AS stays
FROM cmp GROUP BY 1, 2 ORDER BY 1, 2;

-- B. Exactly once: extra lines for an already-seen stay, lines that are not valid JSON
SELECT measure,
       sum(greatest(coalesce(ref_lines, 1) - 1, 0)) AS duplicate_lines_reference,
       sum(greatest(coalesce(sliced_lines, 1) - 1, 0)) AS duplicate_lines_sliced,
       count_if(inconsistent_duplicates) AS stays_with_conflicting_duplicates
FROM cmp GROUP BY 1 ORDER BY 1;

SELECT (SELECT count(*) FROM res_ref WHERE trim(line) <> '' AND try(json_parse(line)) IS NULL) AS broken_lines_reference,
       (SELECT count(*) FROM res_sl  WHERE trim(line) <> '' AND try(json_parse(line)) IS NULL) AS broken_lines_sliced;

-- C. Outcome per sample group (S1 sample: reason; pilot: one group)
SELECT c.measure, coalesce(ss.reason, '<not in sample>') AS reason, c.outcome,
       CASE WHEN count(*) BETWEEN 1 AND 9 THEN '<10' ELSE cast(count(*) AS varchar) END AS stays
FROM cmp c LEFT JOIN sample_stays ss ON ss.unit_id = c.unit_id
GROUP BY 1, 2, 3 ORDER BY 1, 2, 3;

-- C2. Sampled stays with no result line on either side (exactly once also means: none missing)
SELECT m.measure, count_if(c.unit_id IS NULL) AS sampled_stays_missing_on_both_sides
FROM sample_stays ss
CROSS JOIN (VALUES 'CMS816FHIRHHHypo', 'CMS871FHIRHHHyper') AS m(measure)
LEFT JOIN cmp c ON c.unit_id = ss.unit_id AND c.measure = m.measure
GROUP BY 1 ORDER BY 1;

-- D. Engine seconds per stay and side (S2 input; aggregates)
SELECT measure,
       round(sum(ref_seconds), 1) AS ref_seconds_total, round(sum(sliced_seconds), 1) AS sliced_seconds_total,
       round(approx_percentile(ref_seconds, 0.5), 3) AS ref_p50, round(approx_percentile(ref_seconds, 0.99), 2) AS ref_p99,
       round(max(ref_seconds), 1) AS ref_max,
       round(approx_percentile(sliced_seconds, 0.5), 3) AS sliced_p50, round(approx_percentile(sliced_seconds, 0.99), 2) AS sliced_p99,
       round(max(sliced_seconds), 1) AS sliced_max
FROM cmp GROUP BY 1 ORDER BY 1;

-- E. Population counts on the reference side (S1 sample only; never for the pilot)
SELECT c.measure, p AS population,
       CASE WHEN count(*) BETWEEN 1 AND 9 THEN '<10' ELSE cast(count(*) AS varchar) END AS stays
FROM cmp c CROSS JOIN UNNEST(c.ref_member_of) AS u(p)
WHERE {pop_counts}
GROUP BY 1, 2 ORDER BY 1, 2;
