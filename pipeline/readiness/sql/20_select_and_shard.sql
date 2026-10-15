-- =============================================================================
-- 20_select_and_shard.sql — one engine-input set: which stays, and which worker shard each goes to.
--   {table}      dqm_readiness.<set>: one row per stay (stay_manifest columns + reason, predicted
--                seconds, shard); Parquet under {root}/set/
--   {selection}  the stays (filled by build_inputs.py; columns of {b4db}.stay_manifest + reason):
--                  cohort   the 2017-2019 readiness cohort (dqm_cohort.stay_years, '1 primary 2017-2019')
--                  changed  cohort stays whose slice differs from the base mapping's slice (resource set
--                           or any resource's content, by the the prepare stage slice hashes), so only they are
--                           re-evaluated; every other stay's result equals the base result (the engine is
--                           deterministic, S1)
--                  all      every stay (S2 full run)
--                  s1-sample  the 2,000 S1 sample stays (S2 timing sample)
-- Predicted seconds (scheduling only) = CMS816 + CMS871 band means from 10_sliced_runtime_model.sql,
-- times {light_factor} for slices of at most {batch_max_res} resources when the run batches stays
-- (1.0 otherwise). Shards: stays predicted to need at least an even share of the total each get a
-- shard of their own (heaviest = shard 0, at most half the shards); the rest, in decreasing order,
-- fill the remaining shards to equal predicted time (each stay where the midpoint of its time falls on
-- the cumulative time line); shards renumbered 0..n-1 without gaps.
-- =============================================================================

-- part select
CREATE TABLE {table}
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{root}/set/') AS
WITH s AS (
  {selection}
),
b AS (SELECT * FROM dqm_readiness.sliced_runtime_bands),
top AS (
  SELECT measure, max_by(band, band) AS band, max_by(mean_s, band) AS mean_s, max_by(mean_work, band) AS mean_work
  FROM b WHERE band >= 0 GROUP BY measure
),
zero AS (SELECT measure, mean_s FROM b WHERE band = -1),
p AS (
  SELECT s.*,
         coalesce(b6.mean_s,
                  IF(s.work_cms816 > 0 AND floor(log10(s.work_cms816)) > t6.band,
                     t6.mean_s * s.work_cms816 / t6.mean_work, z6.mean_s)) AS pred_cms816,
         coalesce(b7.mean_s,
                  IF(s.work_cms871 > 0 AND floor(log10(s.work_cms871)) > t7.band,
                     t7.mean_s * s.work_cms871 / t7.mean_work, z7.mean_s)) AS pred_cms871
  FROM s
  LEFT JOIN b b6 ON b6.measure = 'CMS816' AND b6.band = IF(s.work_cms816 <= 0, -1, CAST(floor(log10(s.work_cms816)) AS integer))
  LEFT JOIN b b7 ON b7.measure = 'CMS871' AND b7.band = IF(s.work_cms871 <= 0, -1, CAST(floor(log10(s.work_cms871)) AS integer))
  LEFT JOIN top t6 ON t6.measure = 'CMS816'
  LEFT JOIN top t7 ON t7.measure = 'CMS871'
  LEFT JOIN zero z6 ON z6.measure = 'CMS816'
  LEFT JOIN zero z7 ON z7.measure = 'CMS871'
),
q AS (
  SELECT p.*, (coalesce(pred_cms816, 0.5) + coalesce(pred_cms871, 0.5))
              * IF(n_resources <= {batch_max_res}, {light_factor}, 1.0) AS pred_s
  FROM p
),
ranked AS (
  SELECT q.*, row_number() OVER (ORDER BY pred_s DESC, unit_id) AS rk,
         sum(pred_s) OVER () AS total_s, count(*) OVER () AS n
  FROM q
),
k AS (
  SELECT least(count_if(pred_s >= total_s / {shards}), {shards} / 2) AS k FROM ranked
),
rest AS (
  SELECT r.unit_id,
         sum(r.pred_s) OVER (ORDER BY r.rk ROWS UNBOUNDED PRECEDING) - r.pred_s / 2 AS mid,
         sum(r.pred_s) OVER () AS rest_total
  FROM ranked r CROSS JOIN k WHERE r.rk > k.k
),
raw_shard AS (
  SELECT r.*, k.k AS own_shards,
         CASE WHEN r.rk <= k.k THEN r.rk - 1
              ELSE k.k + least({shards} - k.k - 1,
                               CAST(floor(x.mid / greatest(x.rest_total, 1e-9) * ({shards} - k.k)) AS integer)) END AS shard0
  FROM ranked r CROSS JOIN k LEFT JOIN rest x ON x.unit_id = r.unit_id
)
SELECT unit_id, patient_id, mp_year, n_resources, slice_sha256, content_sha256,
       h_lo, w_lo, w_hi, work_cms816, work_cms871, reason,
       pred_cms816, pred_cms871, pred_s, own_shards, rk,
       CAST(dense_rank() OVER (ORDER BY shard0) - 1 AS integer) AS shard
FROM raw_shard;

-- part summary
SELECT count(*) AS stays, count(DISTINCT patient_id) AS patients, count(DISTINCT unit_id) AS distinct_stays,
       max(shard) + 1 AS shards, count(DISTINCT shard) AS distinct_shards, max(own_shards) AS own_shards,
       round(sum(pred_s) / 3600, 2) AS predicted_hours, round(max(pred_s) / 3600, 3) AS longest_stay_predicted_hours,
       count_if(mp_year IS NULL OR mp_year = '') AS stays_without_year,
       count_if(n_resources <= {batch_max_res}) AS stays_batchable
FROM {table};

SELECT reason, CASE WHEN count(*) BETWEEN 1 AND 9 THEN '<10' ELSE CAST(count(*) AS varchar) END AS stays
FROM {table} GROUP BY 1 ORDER BY 1;

SELECT shard, CASE WHEN count(*) BETWEEN 1 AND 9 THEN '<10' ELSE CAST(count(*) AS varchar) END AS stays,
       round(sum(pred_s) / 3600, 3) AS predicted_hours
FROM {table} GROUP BY shard ORDER BY shard;
