-- =============================================================================
-- 30_export.sql — worker input for one engine-input set, in the the prepare stage export format (same lines as
-- pipeline/prepare/sql/athena/prep_50_export.sql), cut into the set's shards:
--   {xp}/manifest/shard=<n>/  "unit_id,patient_id,mp_year,n_resources,slice_sha256,content_sha256,
--                              h_lo,w_lo,w_hi,work_cms816,work_cms871"
--   {xp}/slices/shard=<n>/    "<unit_id><TAB><resource JSON>" (exact bytes from {b4db}.res)
--   {xp}/shared/              Location and Organization resources (added to every slice)
-- part v (cohort sets only): the independent implementation's input, ONE STAY = ONE RECORD: V reads
-- each stay's own slice as the patient's record (manifest "unit_id,unit_id,mp_year"; reference lines
-- "<unit_id><TAB><json>"), i.e. exactly the data the engine's sliced side sees:
--   {xv}/manifest/shard=<n>/, {xv}/reference/shard=<n>/, {xv}/shared_all/
-- Credentialed data: stays in S3; EKS workers read it there.
-- =============================================================================

-- part export
UNLOAD (
  SELECT array_join(ARRAY[unit_id, patient_id, coalesce(mp_year, ''), CAST(n_resources AS varchar),
                          coalesce(slice_sha256, ''), coalesce(content_sha256, ''),
                          coalesce(CAST(h_lo AS varchar), ''), coalesce(CAST(w_lo AS varchar), ''),
                          coalesce(CAST(w_hi AS varchar), ''),
                          CAST(work_cms816 AS varchar), CAST(work_cms871 AS varchar)], ',') AS line,
         shard
  FROM {table}
) TO '{xp}/manifest/'
WITH (format = 'TEXTFILE', compression = 'GZIP', partitioned_by = ARRAY['shard']);

UNLOAD (
  SELECT s.unit_id || chr(9) || r.json AS line, t.shard
  FROM {b4db}.slice s
  JOIN {table} t ON t.unit_id = s.unit_id
  JOIN {b4db}.res r ON r.rtype = s.rtype AND r.id = s.id
) TO '{xp}/slices/'
WITH (format = 'TEXTFILE', compression = 'GZIP', partitioned_by = ARRAY['shard']);

UNLOAD (
  SELECT r.json AS line
  FROM {b4db}.shared s JOIN {b4db}.res r ON r.rtype = s.rtype AND r.id = s.id
) TO '{xp}/shared/'
WITH (format = 'TEXTFILE', compression = 'GZIP');

-- part v
UNLOAD (
  SELECT array_join(ARRAY[unit_id, unit_id, coalesce(mp_year, '')], ',') AS line, shard
  FROM {table}
) TO '{xv}/manifest/'
WITH (format = 'TEXTFILE', compression = 'GZIP', partitioned_by = ARRAY['shard']);

UNLOAD (
  SELECT s.unit_id || chr(9) || r.json AS line, t.shard
  FROM {b4db}.slice s
  JOIN {table} t ON t.unit_id = s.unit_id
  JOIN {b4db}.res r ON r.rtype = s.rtype AND r.id = s.id
) TO '{xv}/reference/'
WITH (format = 'TEXTFILE', compression = 'GZIP', partitioned_by = ARRAY['shard']);

UNLOAD (
  SELECT r.json AS line
  FROM {b4db}.shared s JOIN {b4db}.res r ON r.rtype = s.rtype AND r.id = s.id
) TO '{xv}/shared_all/'
WITH (format = 'TEXTFILE', compression = 'GZIP');
