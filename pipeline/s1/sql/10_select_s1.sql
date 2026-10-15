-- =============================================================================
-- 10_select_s1.sql — the equivalence run: the registered S1 equivalence sample (registration §4.3, decided 2026-10-06):
-- n = 2,000 stays from all 2008-2019 stays (the prepare stage prepare tables in database {b4db}).
--   1,500 at random (seeded order: xxhash64 of unit_id + '{seed}')
--   + about 125 from each situation where slicing can go wrong, drawn in this order, each excluding
--     stays already chosen:
--     g1 highest workload: glucose readings x glucose-lowering doses (CMS816 work metric), top 125
--     g2 ED or observation time just before admission: stays whose slice start was moved earlier by
--        an encounter ending 1 h or less before admission (h_lo < admission start); topped up, if
--        fewer than 125, with stays that have a linked ED visit (the prepare stage: these ED visits end after the
--        admission starts, the ED-overlap issue), seeded order
--     g3 readmission within 24 h of a previous stay of the same patient, seeded order
--     g4 discharge at a measurement-year boundary: stay starts in one calendar year and is discharged
--        in the next (recorded local times), seeded order
-- Every flag is kept for every sampled stay, so results can be reported per group.
-- shard = patient-level hash bucket ({shards}); all stays of a patient go to the same worker.
-- =============================================================================

CREATE TABLE stay_flags
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/stay_flags/') AS
WITH m AS (
  SELECT sm.unit_id, sm.patient_id, sm.mp_year, sm.enc_start, sm.enc_end, sm.h_lo, sm.work_cms816, sm.work_cms871,
         substr(json_extract_scalar(r.json, '$.period.start'), 1, 4) AS start_year_local,
         xxhash64(to_utf8(sm.unit_id || '|{seed}')) AS rk
  FROM {b4db}.stay_manifest sm
  JOIN {b4db}.res r ON r.rtype = 'Encounter' AND r.id = sm.unit_id
),
prev AS (
  SELECT a.unit_id, max(b.enc_end) AS prev_end
  FROM m a JOIN m b ON b.patient_id = a.patient_id AND b.unit_id <> a.unit_id AND b.enc_end <= a.enc_start
  GROUP BY a.unit_id
),
ed AS (
  SELECT DISTINCT i.part_of_id AS unit_id
  FROM {b4db}.res_index i JOIN {b4db}.res r ON r.rtype = i.rtype AND r.id = i.id
  WHERE i.rtype = 'Encounter' AND i.part_of_id IS NOT NULL AND json_extract_scalar(r.json, '$.class.code') = 'EMER'
)
SELECT m.unit_id, m.patient_id, m.mp_year, m.work_cms816, m.work_cms871, m.rk,
       m.h_lo < m.enc_start AS g2_start_extended,
       ed.unit_id IS NOT NULL AS g2_linked_ed,
       coalesce(m.enc_start - pv.prev_end <= INTERVAL '24' HOUR, false) AS g3_readmit_24h,
       coalesce(m.start_year_local <> m.mp_year, false) AS g4_spans_year,
       mod(mod(from_big_endian_64(xxhash64(to_utf8(m.patient_id))), {shards}) + {shards}, {shards}) AS shard
FROM m
LEFT JOIN prev pv ON pv.unit_id = m.unit_id
LEFT JOIN ed ON ed.unit_id = m.unit_id;

CREATE TABLE sample_stays
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/sample_stays/') AS
WITH
r0 AS (SELECT unit_id, 'random' AS reason FROM stay_flags ORDER BY rk, unit_id LIMIT 1500),
g1 AS (SELECT unit_id, 'g1 highest workload' AS reason FROM stay_flags
       WHERE unit_id NOT IN (SELECT unit_id FROM r0)
       ORDER BY work_cms816 DESC, rk, unit_id LIMIT 125),
g2 AS (SELECT unit_id, 'g2 ED or observation just before admission' AS reason FROM stay_flags
       WHERE (g2_start_extended OR g2_linked_ed)
         AND unit_id NOT IN (SELECT unit_id FROM r0 UNION ALL SELECT unit_id FROM g1)
       ORDER BY g2_start_extended DESC, rk, unit_id LIMIT 125),
g3 AS (SELECT unit_id, 'g3 readmission within 24 h' AS reason FROM stay_flags
       WHERE g3_readmit_24h
         AND unit_id NOT IN (SELECT unit_id FROM r0 UNION ALL SELECT unit_id FROM g1 UNION ALL SELECT unit_id FROM g2)
       ORDER BY rk, unit_id LIMIT 125),
g4 AS (SELECT unit_id, 'g4 spans a year boundary' AS reason FROM stay_flags
       WHERE g4_spans_year
         AND unit_id NOT IN (SELECT unit_id FROM r0 UNION ALL SELECT unit_id FROM g1 UNION ALL SELECT unit_id FROM g2
                             UNION ALL SELECT unit_id FROM g3)
       ORDER BY rk, unit_id LIMIT 125),
picked AS (
  SELECT * FROM r0 UNION ALL SELECT * FROM g1 UNION ALL SELECT * FROM g2 UNION ALL SELECT * FROM g3 UNION ALL SELECT * FROM g4
)
SELECT f.unit_id, f.patient_id, f.mp_year, p.reason, f.work_cms816, f.work_cms871,
       f.g2_start_extended, f.g2_linked_ed, f.g3_readmit_24h, f.g4_spans_year, f.shard
FROM picked p JOIN stay_flags f ON f.unit_id = p.unit_id;
