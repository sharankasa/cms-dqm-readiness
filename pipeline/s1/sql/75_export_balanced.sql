-- =============================================================================
-- 75_export_balanced.sql — the equivalence run: the same worker input as 30_export.sql (same lines, same format), cut
-- into the shards of 70_balance.sql (sample_stays_bal / sample_patients_bal) and written to a new
-- folder {xp}/. Shard 0 holds the heaviest predicted patient-year group. A patient whose groups are in
-- several shards has their full record in each of them (the reference side reads whole records).
-- Credentialed data: stays in S3; EKS workers read it there.
-- =============================================================================

UNLOAD (
  SELECT array_join(ARRAY[unit_id, patient_id, coalesce(mp_year, ''), reason,
                          CAST(g2_start_extended AS varchar), CAST(g2_linked_ed AS varchar),
                          CAST(g3_readmit_24h AS varchar), CAST(g4_spans_year AS varchar)], ',') AS line,
         shard
  FROM sample_stays_bal
) TO '{xp}/manifest/'
WITH (format = 'TEXTFILE', compression = 'GZIP', partitioned_by = ARRAY['shard']);

UNLOAD (
  SELECT s.unit_id || chr(9) || r.json AS line, ss.shard
  FROM {b4db}.slice s
  JOIN {b4db}.res r ON r.rtype = s.rtype AND r.id = s.id
  JOIN sample_stays_bal ss ON ss.unit_id = s.unit_id
) TO '{xp}/slices/'
WITH (format = 'TEXTFILE', compression = 'GZIP', partitioned_by = ARRAY['shard']);

UNLOAD (
  SELECT r.json AS line
  FROM {b4db}.shared s JOIN {b4db}.res r ON r.rtype = s.rtype AND r.id = s.id
) TO '{xp}/shared/'
WITH (format = 'TEXTFILE', compression = 'GZIP');

UNLOAD (
  SELECT l.patient_id || chr(9) || l.line AS line, p.shard
  FROM ref_lines l JOIN sample_patients_bal p ON p.patient_id = l.patient_id
) TO '{xp}/reference/'
WITH (format = 'TEXTFILE', compression = 'GZIP', partitioned_by = ARRAY['shard']);

UNLOAD (
  SELECT line FROM ref_lines WHERE patient_id IS NULL
) TO '{xp}/shared_all/'
WITH (format = 'TEXTFILE', compression = 'GZIP');
