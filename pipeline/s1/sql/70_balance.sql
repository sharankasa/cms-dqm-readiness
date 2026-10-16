-- =============================================================================
-- 70_balance.sql — the equivalence run run plan: assign the S1 sample's patient-year groups to worker shards by
-- PREDICTED ENGINE TIME, heaviest first and alone (longest-processing-time-first scheduling).
-- Scheduling only: every group is still evaluated by the unmodified engine on the patient's full,
-- unfiltered record, exactly as before; only which pod runs it, and when, changes. The registered
-- sample tables (sample_stays, sample_patients) are not touched; new tables get the suffix _bal.
-- Database: {db} (the S1 sample). Pilot timings from {pilot_db} (the pilot, registered, excluded).
--
-- (fit)     pilot_groups: the pilot's patient-year groups (no identifiers): whole-record workload metric
--              work = G x M (from the pilot, 11 Oct 2026) and engine seconds (CMS816 + CMS871).
--              runtime_fit: the MEAN time as a power of work, mean(seconds) = scale x exp(a) x work^k,
--              fitted where the work term dominates (work >= {fit_min_work}; below it a fixed start-up
--              cost flattens the slope): the fit groups are cut into {fit_bins} equal-count bins by work,
--              and ln(mean seconds) is regressed on ln(mean work) across the bins; scale makes predicted
--              and observed totals over the fit range equal. Means, not medians: time spreads out more as
--              work grows, so a fit on log times (the second try) tracked the typical group and
--              predicted the 10k-100k band at half its observed total. Below the threshold: the pilot's
--              mean seconds (base0 for work = 0, base1 for 0 < work < {fit_min_work}). The pilot's
--              unfinished shard 47 group is not in the fit (censored): the calibration run measures the
--              heaviest groups.
-- (groups)  group_pred: every S1 patient-year group with its work and predicted seconds.
-- (assign)  group_shard: the {alone} groups with the highest work, and any group predicted to take at
--              least an even share of the time, get a shard of their own (at most {shards} - 1),
--              heaviest = shard 0, so they start first whatever the model's error in size; the rest, in
--              decreasing order, fill the remaining shards to about equal predicted time (each group
--              where its midpoint falls on the cumulative time line). Shards renumbered without gaps. A
--              patient whose years land in different shards has their full record in each.
--              sample_stays_bal / sample_patients_bal: the same 2,000 stays with the new shards.
-- (summary) aggregates only (no identifiers; counts under 10 shown as <10), incl. the model's check on
--              the pilot (observed vs predicted time per work band).
-- =============================================================================

-- part fit
CREATE TABLE pilot_groups
WITH (format = 'PARQUET', external_location = '{bal}/pilot_groups/') AS
WITH vs AS (
  SELECT oid, system, code FROM {pilot_db}.vs_members
  WHERE oid IN ('2.16.840.1.113762.1.4.1248.34', '2.16.840.1.113762.1.4.1196.393')
),
codes AS (
  SELECT DISTINCT l.patient_id, l.id, v.oid
  FROM {pilot_db}.ref_lines l
  CROSS JOIN UNNEST(CAST(coalesce(json_extract(l.line, '$.code.coding'),
                                  json_extract(l.line, '$.medicationCodeableConcept.coding')) AS ARRAY(JSON))) AS u(c)
  JOIN vs v ON v.system = json_extract_scalar(u.c, '$.system') AND v.code = json_extract_scalar(u.c, '$.code')
  WHERE l.rtype IN ('Observation', 'MedicationAdministration') AND l.patient_id IS NOT NULL
),
pw AS (
  SELECT patient_id,
         count_if(oid = '2.16.840.1.113762.1.4.1248.34') AS g,
         count_if(oid = '2.16.840.1.113762.1.4.1196.393') AS m
  FROM codes GROUP BY 1
),
r AS (
  SELECT try(json_parse(line)) AS j FROM {pilot_db}.res_ref WHERE trim(line) <> ''
),
per_measure AS (
  SELECT json_extract_scalar(j, '$.patient_id') AS pid, json_extract_scalar(j, '$.mp_year') AS yr,
         json_extract_scalar(j, '$.measure') AS measure,
         max(try(CAST(json_extract_scalar(j, '$.seconds') AS double))) AS s
  FROM r GROUP BY 1, 2, 3
),
grp AS (
  SELECT pid, yr, sum(s) AS s, count(*) AS n_measures FROM per_measure GROUP BY 1, 2
)
SELECT CAST(coalesce(pw.g, 0) * coalesce(pw.m, 0) AS double) AS work, grp.s
FROM grp LEFT JOIN pw ON pw.patient_id = grp.pid
WHERE grp.n_measures = 2 AND grp.s > 0;

CREATE TABLE runtime_fit
WITH (format = 'PARQUET', external_location = '{bal}/runtime_fit/') AS
WITH d AS (
  SELECT work, s, ntile({fit_bins}) OVER (ORDER BY work, s) AS bin FROM pilot_groups WHERE work >= {fit_min_work}
),
bm AS (
  SELECT bin, avg(work) AS mw, avg(s) AS ms FROM d GROUP BY bin
),
f AS (
  SELECT regr_slope(ln(ms), ln(mw)) AS k, regr_intercept(ln(ms), ln(mw)) AS a, count(*) AS n_bins FROM bm
),
sm AS (
  SELECT f.k, f.a, f.n_bins, count(*) AS n_fit, sum(d.s) / sum(exp(f.a) * power(d.work, f.k)) AS smear
  FROM d CROSS JOIN f
  GROUP BY f.k, f.a, f.n_bins
),
b AS (
  SELECT avg(IF(work = 0, s)) AS base0, count_if(work = 0) AS n_base0,
         avg(IF(work > 0 AND work < {fit_min_work}, s)) AS base1, count_if(work > 0 AND work < {fit_min_work}) AS n_base1
  FROM pilot_groups
)
SELECT sm.k, sm.a, sm.smear, sm.n_fit, sm.n_bins, b.base0, b.n_base0, b.base1, b.n_base1, CAST({fit_min_work} AS double) AS fit_min_work
FROM sm CROSS JOIN b;

-- part groups
CREATE TABLE group_pred
WITH (format = 'PARQUET', external_location = '{bal}/group_pred/') AS
WITH vs AS (
  SELECT oid, system, code FROM vs_members
  WHERE oid IN ('2.16.840.1.113762.1.4.1248.34', '2.16.840.1.113762.1.4.1196.393')
),
codes AS (
  SELECT DISTINCT l.patient_id, l.id, v.oid
  FROM ref_lines l
  CROSS JOIN UNNEST(CAST(coalesce(json_extract(l.line, '$.code.coding'),
                                  json_extract(l.line, '$.medicationCodeableConcept.coding')) AS ARRAY(JSON))) AS u(c)
  JOIN vs v ON v.system = json_extract_scalar(u.c, '$.system') AND v.code = json_extract_scalar(u.c, '$.code')
  WHERE l.rtype IN ('Observation', 'MedicationAdministration') AND l.patient_id IS NOT NULL
),
pw AS (
  SELECT patient_id,
         count_if(oid = '2.16.840.1.113762.1.4.1248.34') AS g,
         count_if(oid = '2.16.840.1.113762.1.4.1196.393') AS m
  FROM codes GROUP BY 1
),
grp AS (
  SELECT patient_id, coalesce(mp_year, '') AS mp_year, count(*) AS n_stays FROM sample_stays GROUP BY 1, 2
),
w AS (
  SELECT grp.patient_id, grp.mp_year, grp.n_stays, CAST(coalesce(pw.g, 0) * coalesce(pw.m, 0) AS double) AS work
  FROM grp LEFT JOIN pw ON pw.patient_id = grp.patient_id
)
SELECT w.patient_id, w.mp_year, w.n_stays, w.work,
       CASE WHEN w.work = 0 THEN f.base0
            WHEN w.work < f.fit_min_work THEN f.base1
            ELSE greatest(f.base1, f.smear * exp(f.a) * power(w.work, f.k)) END AS pred_s
FROM w CROSS JOIN runtime_fit f;

-- part assign
CREATE TABLE group_shard
WITH (format = 'PARQUET', external_location = '{bal}/group_shard/') AS
WITH o AS (
  SELECT patient_id, mp_year, n_stays, pred_s,
         row_number() OVER (ORDER BY pred_s DESC, work DESC, patient_id, mp_year) AS rnk
  FROM group_pred
),
-- the {alone} highest-work groups always alone, plus every group above an "even share", refined three
-- times: after the heaviest groups take their own shards, the share of the remaining shards is smaller,
-- so more groups may qualify (at most {shards} - 1 groups alone)
s0 AS (SELECT sum(pred_s) AS total FROM o),
i1 AS (SELECT count_if((o.rnk <= {alone} OR o.pred_s >= s0.total / {shards}) AND o.rnk < {shards}) AS nh,
              sum(IF((o.rnk <= {alone} OR o.pred_s >= s0.total / {shards}) AND o.rnk < {shards}, o.pred_s, CAST(0 AS double))) AS hs
       FROM o CROSS JOIN s0),
e2 AS (SELECT (s0.total - i1.hs) / ({shards} - i1.nh) AS e FROM s0 CROSS JOIN i1),
i2 AS (SELECT count_if((o.rnk <= {alone} OR o.pred_s >= e2.e) AND o.rnk < {shards}) AS nh,
              sum(IF((o.rnk <= {alone} OR o.pred_s >= e2.e) AND o.rnk < {shards}, o.pred_s, CAST(0 AS double))) AS hs
       FROM o CROSS JOIN e2),
e3 AS (SELECT (s0.total - i2.hs) / ({shards} - i2.nh) AS e FROM s0 CROSS JOIN i2),
i3 AS (SELECT count_if((o.rnk <= {alone} OR o.pred_s >= e3.e) AND o.rnk < {shards}) AS nh,
              sum(IF((o.rnk <= {alone} OR o.pred_s >= e3.e) AND o.rnk < {shards}, o.pred_s, CAST(0 AS double))) AS hs
       FROM o CROSS JOIN e3),
e4 AS (SELECT (s0.total - i3.hs) / ({shards} - i3.nh) AS e FROM s0 CROSS JOIN i3),
h AS (
  SELECT o.*, ((o.rnk <= {alone} OR o.pred_s >= e4.e) AND o.rnk < {shards}) AS heavy FROM o CROSS JOIN e4
),
hc AS (
  SELECT count_if(heavy) AS nh, greatest(sum(IF(heavy, CAST(0 AS double), pred_s)), CAST(0.000000001 AS double)) AS rest FROM h
),
l AS (
  SELECT h.*, hc.nh, hc.rest,
         coalesce(sum(IF(h.heavy, CAST(0 AS double), h.pred_s)) OVER (ORDER BY h.rnk ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), CAST(0 AS double))
           AS cum_before
  FROM h CROSS JOIN hc
),
raw AS (
  SELECT patient_id, mp_year, n_stays, pred_s, heavy,
         CASE WHEN heavy THEN CAST(rnk - 1 AS integer)
              -- each remaining group goes to the shard where its midpoint falls on the cumulative time line
              ELSE CAST(nh + least({shards} - nh - 1,
                                   CAST(floor((cum_before + pred_s / 2) / (rest / ({shards} - nh))) AS integer)) AS integer)
         END AS raw_shard
  FROM l
)
SELECT patient_id, mp_year, n_stays, pred_s, heavy,
       CAST(dense_rank() OVER (ORDER BY raw_shard) - 1 AS integer) AS shard
FROM raw;

CREATE TABLE sample_stays_bal
WITH (format = 'PARQUET', external_location = '{bal}/sample_stays_bal/') AS
SELECT s.unit_id, s.patient_id, s.mp_year, s.reason, s.g2_start_extended, s.g2_linked_ed, s.g3_readmit_24h,
       s.g4_spans_year, g.shard
FROM sample_stays s
JOIN group_shard g ON g.patient_id = s.patient_id AND g.mp_year = coalesce(s.mp_year, '');

CREATE TABLE sample_patients_bal
WITH (format = 'PARQUET', external_location = '{bal}/sample_patients_bal/') AS
SELECT DISTINCT patient_id, shard FROM sample_stays_bal;

-- part summary
SELECT (SELECT count(*) FROM sample_stays) AS stays_registered,
       (SELECT count(*) FROM sample_stays_bal) AS stays_balanced,
       (SELECT count(DISTINCT unit_id) FROM sample_stays_bal) AS distinct_stays_balanced,
       (SELECT count(*) FROM group_pred) AS groups,
       (SELECT count(*) FROM group_shard) AS groups_assigned,
       (SELECT count(DISTINCT shard) FROM group_shard) AS shards,
       (SELECT max(shard) + 1 FROM group_shard) AS max_shard_plus_one,
       (SELECT count_if(heavy) FROM group_shard) AS own_shard_groups,
       (SELECT count(*) FROM sample_patients_bal) AS patient_shard_pairs,
       (SELECT count(DISTINCT patient_id) FROM sample_patients_bal) AS patients;

SELECT round(k, 3) AS k, round(a, 3) AS a, round(smear, 2) AS smear, n_fit, n_bins, round(base0, 2) AS base0_seconds, n_base0,
       round(base1, 2) AS base1_seconds, n_base1, fit_min_work
FROM runtime_fit;

SELECT shard,
       CASE WHEN count(*) < 10 THEN '<10' ELSE CAST(count(*) AS varchar) END AS groups,
       CASE WHEN sum(n_stays) < 10 THEN '<10' ELSE CAST(sum(n_stays) AS varchar) END AS stays,
       round(sum(pred_s) / 3600, 2) AS predicted_hours
FROM group_shard GROUP BY shard ORDER BY shard;

-- model check on the pilot: observed vs predicted seconds per work band (same bands as the pilot check)
WITH p AS (
  SELECT x.work, x.s,
         CASE WHEN x.work = 0 THEN f.base0 WHEN x.work < f.fit_min_work THEN f.base1
              ELSE greatest(f.base1, f.smear * exp(f.a) * power(x.work, f.k)) END AS pred
  FROM pilot_groups x CROSS JOIN runtime_fit f
)
SELECT CASE WHEN work = 0 THEN '0 none' WHEN work < 1000 THEN '1 under 1k' WHEN work < 10000 THEN '2 1k-10k'
            WHEN work < 100000 THEN '3 10k-100k' WHEN work < 1000000 THEN '4 100k-1M' WHEN work < 10000000 THEN '5 1M-10M'
            ELSE '6 10M+' END AS work_band,
       CASE WHEN count(*) < 10 THEN '<10' ELSE CAST(count(*) AS varchar) END AS groups,
       IF(count(*) >= 10, round(avg(s), 1)) AS observed_mean_s,
       IF(count(*) >= 10, round(avg(pred), 1)) AS predicted_mean_s,
       IF(count(*) >= 10, round(sum(s) / 3600, 2)) AS observed_total_h,
       IF(count(*) >= 10, round(sum(pred) / 3600, 2)) AS predicted_total_h
FROM p GROUP BY 1 ORDER BY 1;

-- S1 sample: predicted time per work band
SELECT CASE WHEN work = 0 THEN '0 none' WHEN work < 1000 THEN '1 under 1k' WHEN work < 10000 THEN '2 1k-10k'
            WHEN work < 100000 THEN '3 10k-100k' WHEN work < 1000000 THEN '4 100k-1M' WHEN work < 10000000 THEN '5 1M-10M'
            ELSE '6 10M+' END AS work_band,
       CASE WHEN count(*) < 10 THEN '<10' ELSE CAST(count(*) AS varchar) END AS groups,
       IF(count(*) >= 10, round(avg(pred_s) / 3600, 2)) AS predicted_mean_h,
       IF(count(*) >= 10, round(sum(pred_s) / 3600, 1)) AS predicted_total_h
FROM group_pred GROUP BY 1 ORDER BY 1;
