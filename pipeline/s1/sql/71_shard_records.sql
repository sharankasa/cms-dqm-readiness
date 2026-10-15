-- =============================================================================
-- 71_shard_records.sql — the equivalence run: how big are the full records each balanced shard's pods load? Pod memory
-- follows record size (the engine holds the whole record), not the workload metric. The pilot's
-- largest loaded record (109,861 resources incl. the shared ones) peaked at 2.0 GiB of a 3.5 GiB limit.
-- Loaded = the patient's record + the resources without a patient (shared_all), as run_engine.js builds it.
-- Counts only; record sizes rounded to thousands. Run on the S1 sample database after rebalance_run.py:
--   python3 pipeline/athena/run_sql.py --db dqm_s1 --sql pipeline/s1/sql/71_shard_records.sql --out <dir>
-- =============================================================================

-- A. shards by the largest record their pods load
WITH sh_all AS (SELECT count(*) AS n FROM ref_lines WHERE patient_id IS NULL),
rec AS (SELECT patient_id, count(*) AS n FROM ref_lines WHERE patient_id IS NOT NULL GROUP BY 1),
sh AS (
  SELECT p.shard, max(r.n) + max(a.n) AS max_loaded
  FROM sample_patients_bal p JOIN rec r ON r.patient_id = p.patient_id CROSS JOIN sh_all a
  GROUP BY 1
),
own AS (SELECT shard, bool_or(heavy) AS alone FROM group_shard GROUP BY 1)
SELECT CASE WHEN max_loaded < 110000 THEN 'a up to 110k (pilot-proven)' WHEN max_loaded < 150000 THEN 'b 110k-150k'
            WHEN max_loaded < 250000 THEN 'c 150k-250k' WHEN max_loaded < 350000 THEN 'd 250k-350k' ELSE 'e 350k+' END AS largest_loaded,
       count(*) AS shards, count_if(own.alone) AS of_which_alone, min(sh.shard) AS lowest_shard, max(sh.shard) AS highest_shard
FROM sh JOIN own ON own.shard = sh.shard
GROUP BY 1 ORDER BY 1;

-- B. every shard above the pilot-proven size, and the calibration shards 0-2
WITH sh_all AS (SELECT count(*) AS n FROM ref_lines WHERE patient_id IS NULL),
rec AS (SELECT patient_id, count(*) AS n FROM ref_lines WHERE patient_id IS NOT NULL GROUP BY 1),
sh AS (
  SELECT p.shard, max(r.n) + max(a.n) AS max_loaded
  FROM sample_patients_bal p JOIN rec r ON r.patient_id = p.patient_id CROSS JOIN sh_all a
  GROUP BY 1
),
own AS (SELECT shard, bool_or(heavy) AS alone, round(sum(pred_s) / 3600, 2) AS predicted_h FROM group_shard GROUP BY 1)
SELECT sh.shard, own.alone, own.predicted_h, CAST(round(sh.max_loaded / 1000.0) AS integer) AS largest_loaded_k
FROM sh JOIN own ON own.shard = sh.shard
WHERE sh.max_loaded >= 110000 OR sh.shard <= 2
ORDER BY sh.max_loaded DESC;
