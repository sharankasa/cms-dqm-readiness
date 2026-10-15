-- =============================================================================
-- 61_runtime_model_work.sql — the equivalence run planning, second model: the unmodified engine's CMS816 time per
-- patient-year group against the registered workload metric computed on the WHOLE record
-- (registration §4.3.1: "for the unmodified engine it is the patient's whole record"):
--   work = G x M, G = glucose results ("Glucose Lab Test Mass Per Volume") in the patient's record,
--          M = administrations with a CMS816 hypoglycemic code in the patient's record.
-- 60_runtime_model.sql showed record size alone does not predict time (a slow group in the smallest
-- size band); CMS816's logic pairs each low reading with doses, so G x M is the candidate driver.
-- A. Pilot: CMS816 seconds per group, by whole-record work band.
-- B. S1 sample: patient-year groups per whole-record work band (what the equivalence run will run).
-- Counts and timings only. Run with pipeline/athena/run_sql.py --db dqm_pilot.
-- =============================================================================

-- A.
WITH vs AS (
  SELECT oid, system, code FROM dqm_pilot.vs_members
  WHERE oid IN ('2.16.840.1.113762.1.4.1248.34', '2.16.840.1.113762.1.4.1196.393')
),
codes AS (
  SELECT DISTINCT l.patient_id, l.id, v.oid
  FROM dqm_pilot.ref_lines l
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
  SELECT try(json_parse(line)) AS j FROM dqm_pilot.res_ref WHERE trim(line) <> ''
),
grp AS (
  SELECT json_extract_scalar(j, '$.patient_id') AS pid, json_extract_scalar(j, '$.mp_year') AS yr,
         max(try(CAST(json_extract_scalar(j, '$.seconds') AS double))) AS s
  FROM r WHERE json_extract_scalar(j, '$.measure') = 'CMS816FHIRHHHypo'
  GROUP BY 1, 2
),
x AS (
  SELECT grp.s, coalesce(pw.g, 0) * coalesce(pw.m, 0) AS work
  FROM grp LEFT JOIN pw ON pw.patient_id = grp.pid
)
SELECT CASE WHEN work = 0 THEN '0 none' WHEN work < 1000 THEN '1 under 1k' WHEN work < 10000 THEN '2 1k-10k'
            WHEN work < 100000 THEN '3 10k-100k' WHEN work < 1000000 THEN '4 100k-1M' WHEN work < 10000000 THEN '5 1M-10M'
            ELSE '6 10M+' END AS whole_record_work,
       CASE WHEN count(*) < 10 THEN '<10' ELSE cast(count(*) AS varchar) END AS groups,
       IF(count(*) >= 10, round(approx_percentile(work, 0.5))) AS median_work,
       IF(count(*) >= 10, round(approx_percentile(s, 0.5), 1)) AS median_seconds,
       IF(count(*) >= 10, round(approx_percentile(s, 0.9), 1)) AS p90_seconds,
       IF(count(*) >= 10, round(max(s), 1)) AS max_seconds,
       IF(count(*) >= 10, round(sum(s))) AS total_seconds
FROM x GROUP BY 1 ORDER BY 1;

-- B.
WITH vs AS (
  SELECT oid, system, code FROM dqm_s1.vs_members
  WHERE oid IN ('2.16.840.1.113762.1.4.1248.34', '2.16.840.1.113762.1.4.1196.393')
),
codes AS (
  SELECT DISTINCT l.patient_id, l.id, v.oid
  FROM dqm_s1.ref_lines l
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
  SELECT DISTINCT patient_id, mp_year FROM dqm_s1.sample_stays
),
x AS (
  SELECT coalesce(pw.g, 0) * coalesce(pw.m, 0) AS work
  FROM grp LEFT JOIN pw ON pw.patient_id = grp.patient_id
)
SELECT CASE WHEN work = 0 THEN '0 none' WHEN work < 1000 THEN '1 under 1k' WHEN work < 10000 THEN '2 1k-10k'
            WHEN work < 100000 THEN '3 10k-100k' WHEN work < 1000000 THEN '4 100k-1M' WHEN work < 10000000 THEN '5 1M-10M'
            ELSE '6 10M+' END AS whole_record_work,
       CASE WHEN count(*) < 10 THEN '<10' ELSE cast(count(*) AS varchar) END AS patient_year_groups,
       IF(count(*) >= 10, round(approx_percentile(work, 0.5))) AS median_work,
       IF(count(*) >= 10, max(work)) AS max_work
FROM x GROUP BY 1 ORDER BY 1;
