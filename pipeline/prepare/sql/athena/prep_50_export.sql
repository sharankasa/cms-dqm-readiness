-- =============================================================================
-- prep_50_export.sql — the prepare stage prepare: worker input in S3, one folder per shard (shard=<n>/), gzip text.
--   {loc}/export/slices/shard=<n>/...   lines "<unit_id><TAB><resource JSON>" (the slice resources,
--                                       exact bytes from `res`; a worker groups lines by unit_id)
--   {loc}/export/manifest/shard=<n>/... lines "unit_id,patient_id,mp_year,n_resources,slice_sha256,
--                                       content_sha256,h_lo,w_lo,w_hi,work_cms816,work_cms871"
--   {loc}/export/shared/...             Location and Organization resources (added to every slice)
-- One text column per row (plus the partition column), so no field delimiter or escaping is involved.
-- Credentialed data: these files stay in S3; workers on EKS read them there (the pilot).
-- =============================================================================

UNLOAD (
  SELECT s.unit_id || chr(9) || r.json AS line, m.shard
  FROM slice s
  JOIN res r ON r.rtype = s.rtype AND r.id = s.id
  JOIN stay_manifest m ON m.unit_id = s.unit_id
) TO '{loc}/export/slices/'
WITH (format = 'TEXTFILE', compression = 'GZIP', partitioned_by = ARRAY['shard']);

UNLOAD (
  SELECT array_join(ARRAY[unit_id, patient_id, coalesce(mp_year, ''), CAST(n_resources AS varchar),
                          coalesce(slice_sha256, ''), coalesce(content_sha256, ''),
                          coalesce(CAST(h_lo AS varchar), ''), coalesce(CAST(w_lo AS varchar), ''),
                          coalesce(CAST(w_hi AS varchar), ''),
                          CAST(work_cms816 AS varchar), CAST(work_cms871 AS varchar)], ',') AS line,
         shard
  FROM stay_manifest
) TO '{loc}/export/manifest/'
WITH (format = 'TEXTFILE', compression = 'GZIP', partitioned_by = ARRAY['shard']);

UNLOAD (
  SELECT r.json AS line
  FROM shared s JOIN res r ON r.rtype = s.rtype AND r.id = s.id
) TO '{loc}/export/shared/'
WITH (format = 'TEXTFILE', compression = 'GZIP');
