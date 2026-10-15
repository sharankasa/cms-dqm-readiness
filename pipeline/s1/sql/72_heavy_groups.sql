-- =============================================================================
-- 72_heavy_groups.sql — why are some patient-years so slow on the unmodified engine, and how long
-- should the unfinished ones take? Counts and fitted numbers only: no identifiers; counts under 10
-- shown as <10; glucose readings and doses rounded to the nearest 100. Database: the S1 sample
-- (after rebalance_run.py: group_shard). the prepare stage tables in {b4db}; whole-record results in {results}.
--   python3 pipeline/athena/run_sql.py --db dqm_s1 --sql pipeline/s1/sql/72_heavy_groups.sql \
--     --param b4db=dqm_prep --param results=s3://.../runs/b6-reference/results --out <dir>
--
-- Per patient-year (the unit the unmodified engine evaluates): E = the patient's stays discharged in
-- that year (episodes the engine evaluates), S = all of the patient's stays, G = glucose results and
-- M = glucose-lowering doses in the patient's WHOLE record. The registered workload for the unmodified
-- engine is "the whole record, summed over the patient's stays" (registration §4.3.1), i.e. E x G x M; the
-- run plan used G x M only. A. compares how well each explains the finished patient-years' times.
-- B. predicts the unfinished shards from the E x G x M fit. Exploratory (run planning, explanation).
-- =============================================================================

DROP TABLE IF EXISTS b6_ref_results;

CREATE EXTERNAL TABLE b6_ref_results (line string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION '{results}/';

DROP TABLE IF EXISTS heavy_feat;

CREATE TABLE heavy_feat WITH (format = 'PARQUET') AS
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
  SELECT patient_id, count_if(oid = '2.16.840.1.113762.1.4.1248.34') AS g, count_if(oid = '2.16.840.1.113762.1.4.1196.393') AS m
  FROM codes GROUP BY 1
),
pats AS (SELECT DISTINCT patient_id FROM group_shard),
st AS (
  SELECT sm.patient_id, coalesce(sm.mp_year, '') AS mp_year, count(*) AS e
  FROM {b4db}.stay_manifest sm JOIN pats p ON p.patient_id = sm.patient_id GROUP BY 1, 2
),
stot AS (SELECT patient_id, sum(e) AS s_total FROM st GROUP BY 1),
r AS (SELECT try(json_parse(line)) AS j FROM b6_ref_results WHERE trim(line) <> ''),
pm AS (
  SELECT json_extract_scalar(j, '$.patient_id') AS pid, json_extract_scalar(j, '$.mp_year') AS yr,
         json_extract_scalar(j, '$.measure') AS measure, max(try(CAST(json_extract_scalar(j, '$.seconds') AS double))) AS s
  FROM r GROUP BY 1, 2, 3
),
done AS (SELECT pid, yr, sum(s) AS s FROM pm GROUP BY 1, 2 HAVING count(*) = 2)
SELECT gs.shard, gs.heavy, gs.mp_year, coalesce(st.e, 0) AS e, coalesce(stot.s_total, 0) AS s_total,
       coalesce(pw.g, 0) AS g, coalesce(pw.m, 0) AS m, d.s AS seconds
FROM group_shard gs
LEFT JOIN pw ON pw.patient_id = gs.patient_id
LEFT JOIN st ON st.patient_id = gs.patient_id AND st.mp_year = gs.mp_year
LEFT JOIN stot ON stot.patient_id = gs.patient_id
LEFT JOIN done d ON d.pid = gs.patient_id AND d.yr = gs.mp_year;

-- A. finished patient-years with glucose readings and doses: how well does each workload explain time?
WITH x AS (
  SELECT ln(seconds) AS y, ln(CAST(g AS double) * m) AS gm, ln(CAST(e AS double) * g * m) AS egm,
         ln(CAST(s_total AS double) * g * m) AS sgm, ln(CAST(e AS double)) AS le
  FROM heavy_feat WHERE seconds > 0 AND g > 0 AND m > 0 AND e > 0
)
SELECT 'G x M (run plan)' AS workload, count(*) AS patient_years, round(regr_slope(y, gm), 3) AS exponent, round(power(corr(y, gm), 2), 3) AS r2 FROM x
UNION ALL
SELECT 'E x G x M (registered: summed over stays in the year)', count(*), round(regr_slope(y, egm), 3), round(power(corr(y, egm), 2), 3) FROM x
UNION ALL
SELECT 'S x G x M (all of the patient''s stays)', count(*), round(regr_slope(y, sgm), 3), round(power(corr(y, sgm), 2), 3) FROM x
UNION ALL
SELECT 'E alone (stays in the year)', count(*), round(regr_slope(y, le), 3), round(power(corr(y, le), 2), 3) FROM x;

-- B. unfinished shards: what their patient-years look like and what the E x G x M fit predicts
WITH x AS (
  SELECT ln(seconds) AS y, ln(CAST(e AS double) * g * m) AS w
  FROM heavy_feat WHERE seconds > 0 AND g > 0 AND m > 0 AND e > 0
),
f AS (SELECT regr_slope(y, w) AS k, regr_intercept(y, w) AS a FROM x),
sm AS (SELECT f.k, f.a, avg(exp(x.y - (f.a + f.k * x.w))) AS smear, approx_percentile(x.y - (f.a + f.k * x.w), 0.9) AS q90 FROM x CROSS JOIN f GROUP BY f.k, f.a),
unfinished AS (
  SELECT h.*, CASE WHEN h.g > 0 AND h.m > 0 AND h.e > 0 THEN ln(CAST(h.e AS double) * h.g * h.m) END AS w
  FROM heavy_feat h WHERE h.seconds IS NULL
)
SELECT o.shard,
       CASE WHEN count(*) < 10 THEN '<10' ELSE CAST(count(*) AS varchar) END AS open_patient_years,
       CASE WHEN max(o.e) < 10 THEN '<10' ELSE CAST(max(o.e) AS varchar) END AS max_stays_in_year,
       CASE WHEN max(o.s_total) < 10 THEN '<10' ELSE CAST(max(o.s_total) AS varchar) END AS max_stays_total,
       CAST(round(max(o.g), -2) AS bigint) AS max_glucose_results_rounded,
       CAST(round(max(o.m), -2) AS bigint) AS max_doses_rounded,
       round(sum(coalesce(exp(sm.a + sm.k * o.w), 0)) / 3600, 1) AS predicted_hours_typical,
       round(sum(coalesce(exp(sm.a + sm.k * o.w) * sm.smear, 0)) / 3600, 1) AS predicted_hours_mean,
       round(sum(coalesce(exp(sm.a + sm.k * o.w + sm.q90), 0)) / 3600, 1) AS predicted_hours_p90
FROM unfinished o CROSS JOIN sm
GROUP BY o.shard
ORDER BY predicted_hours_mean DESC;
