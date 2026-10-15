-- =============================================================================
-- 40_summary.sql — the pilot/the equivalence run sample: what comes back to the laptop. AGGREGATES ONLY, counts below 10
-- masked. No measure is computed here.
--   A. sample by reason: stays, patients
--   B. how many stays in the whole data satisfy each risk-group flag (S1 sample only; pilot: none)
--   C. reference input: resources by type; resources per patient
--   D. sliced input for the sample: resources
-- =============================================================================

-- A.
SELECT reason,
       CASE WHEN count(*) < 10 THEN '<10' ELSE cast(count(*) AS varchar) END AS stays,
       CASE WHEN count(DISTINCT patient_id) < 10 THEN '<10' ELSE cast(count(DISTINCT patient_id) AS varchar) END AS patients,
       IF(count(*) >= 10, approx_percentile(work_cms816, 0.5)) AS work816_p50,
       IF(count(*) >= 10, approx_percentile(work_cms816, 0.99)) AS work816_p99
FROM sample_stays GROUP BY 1 ORDER BY 1;

-- B.
SELECT CASE WHEN count_if(g2_start_extended) BETWEEN 1 AND 9 THEN '<10' ELSE cast(count_if(g2_start_extended) AS varchar) END AS all_stays_start_extended,
       CASE WHEN count_if(g2_linked_ed) BETWEEN 1 AND 9 THEN '<10' ELSE cast(count_if(g2_linked_ed) AS varchar) END AS all_stays_linked_ed,
       CASE WHEN count_if(g3_readmit_24h) BETWEEN 1 AND 9 THEN '<10' ELSE cast(count_if(g3_readmit_24h) AS varchar) END AS all_stays_readmit_24h,
       CASE WHEN count_if(g4_spans_year) BETWEEN 1 AND 9 THEN '<10' ELSE cast(count_if(g4_spans_year) AS varchar) END AS all_stays_span_year
FROM {flags_relation};

-- C1.
SELECT coalesce(rtype, '<none>') AS rtype, patient_id IS NULL AS without_patient,
       CASE WHEN count(*) < 10 THEN '<10' ELSE cast(count(*) AS varchar) END AS resources
FROM ref_lines GROUP BY 1, 2 ORDER BY 1, 2;

-- C2.
SELECT count(*) AS patients,
       IF(count(*) >= 10, approx_percentile(n, 0.5)) AS resources_p50,
       IF(count(*) >= 10, approx_percentile(n, 0.9)) AS resources_p90,
       IF(count(*) >= 10, approx_percentile(n, 0.99)) AS resources_p99,
       IF(count(*) >= 10, max(n)) AS resources_max,
       IF(count(*) >= 10, sum(n)) AS resources_total
FROM (SELECT patient_id, count(*) AS n FROM ref_lines WHERE patient_id IS NOT NULL GROUP BY 1);

-- D.
SELECT count(DISTINCT ss.unit_id) AS stays, count(*) AS resources_in_slices
FROM sample_stays ss JOIN {b4db}.slice s ON s.unit_id = ss.unit_id;
