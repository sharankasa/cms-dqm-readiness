-- =============================================================================
-- 30_export.sql — the pilot/the equivalence run: worker input for the sample, one folder per shard (shard=<n>/), gzip text,
-- one text column per row. Credentialed data: stays in S3; EKS workers read it there.
--   export/manifest/shard=<n>/   "unit_id,patient_id,mp_year,reason,g2_start_extended,g2_linked_ed,
--                                 g3_readmit_24h,g4_spans_year"
--   export/slices/shard=<n>/     "<unit_id><TAB><json>": the stay's the prepare stage slice (filtered), exact bytes
--   export/shared/               Location + Organization (added to every slice)
--   export/reference/shard=<n>/  "<patient_id><TAB><json>": the patient's full unfiltered mapped record
--   export/shared_all/           every resource without a patient (reference side)
-- =============================================================================

UNLOAD (
  SELECT array_join(ARRAY[unit_id, patient_id, coalesce(mp_year, ''), reason,
                          CAST(g2_start_extended AS varchar), CAST(g2_linked_ed AS varchar),
                          CAST(g3_readmit_24h AS varchar), CAST(g4_spans_year AS varchar)], ',') AS line,
         shard
  FROM sample_stays
) TO '{loc}/export/manifest/'
WITH (format = 'TEXTFILE', compression = 'GZIP', partitioned_by = ARRAY['shard']);

UNLOAD (
  SELECT s.unit_id || chr(9) || r.json AS line, ss.shard
  FROM {b4db}.slice s
  JOIN {b4db}.res r ON r.rtype = s.rtype AND r.id = s.id
  JOIN sample_stays ss ON ss.unit_id = s.unit_id
) TO '{loc}/export/slices/'
WITH (format = 'TEXTFILE', compression = 'GZIP', partitioned_by = ARRAY['shard']);

UNLOAD (
  SELECT r.json AS line
  FROM {b4db}.shared s JOIN {b4db}.res r ON r.rtype = s.rtype AND r.id = s.id
) TO '{loc}/export/shared/'
WITH (format = 'TEXTFILE', compression = 'GZIP');

UNLOAD (
  SELECT l.patient_id || chr(9) || l.line AS line, p.shard
  FROM ref_lines l JOIN sample_patients p ON p.patient_id = l.patient_id
) TO '{loc}/export/reference/'
WITH (format = 'TEXTFILE', compression = 'GZIP', partitioned_by = ARRAY['shard']);

UNLOAD (
  SELECT line FROM ref_lines WHERE patient_id IS NULL
) TO '{loc}/export/shared_all/'
WITH (format = 'TEXTFILE', compression = 'GZIP');
