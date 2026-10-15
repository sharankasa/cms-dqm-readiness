-- =============================================================================
-- 10_select_pilot.sql — the pilot: the registered 1% feasibility pilot (registration §5): a 1% random sample of
-- PATIENTS (fixed seed '{seed}', recorded), all of their stays. Only timings, cost and stay-level
-- agreement counts are examined; no rates (enforced by the comparison SQL, which outputs counts only).
-- Same columns as the S1 sample, so the same export and worker code serve both.
-- =============================================================================

CREATE TABLE sample_stays
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/sample_stays/') AS
SELECT sm.unit_id, sm.patient_id, sm.mp_year, 'pilot 1% of patients' AS reason, sm.work_cms816, sm.work_cms871,
       false AS g2_start_extended, false AS g2_linked_ed, false AS g3_readmit_24h, false AS g4_spans_year,
       mod(mod(from_big_endian_64(xxhash64(to_utf8(sm.patient_id))), {shards}) + {shards}, {shards}) AS shard
FROM {b4db}.stay_manifest sm
WHERE mod(mod(from_big_endian_64(xxhash64(to_utf8(sm.patient_id || '|{seed}'))), 100) + 100, 100) = 0;
