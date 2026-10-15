-- =============================================================================
-- 40_scenario_export.sql — worker input for one readiness scenario (r1_run.py scenario-inputs): the
-- baseline slices ({b4db}, mapping a) of the scenario's changed stays, with the scenario's changed records
-- ({overrides}: rtype, id, j) in place of their baseline JSON. Same format as 30_export.sql. The manifest's
-- slice hashes are the baseline's (informational; the worker does not read them).
-- Slice membership is the baseline's: every scenario only degrades records the measures use (a record the
-- data-requirements filter would now drop is one the engine can no longer match either), so the engine's
-- result on these slices equals its result on the whole changed record set.
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
  SELECT s.unit_id || chr(9) || coalesce(o.j, r.json) AS line, t.shard
  FROM {b4db}.slice s
  JOIN {table} t ON t.unit_id = s.unit_id
  JOIN {b4db}.res r ON r.rtype = s.rtype AND r.id = s.id
  LEFT JOIN {overrides} o ON o.rtype = s.rtype AND o.id = s.id
) TO '{xp}/slices/'
WITH (format = 'TEXTFILE', compression = 'GZIP', partitioned_by = ARRAY['shard']);

UNLOAD (
  SELECT r.json AS line
  FROM {b4db}.shared s JOIN {b4db}.res r ON r.rtype = s.rtype AND r.id = s.id
) TO '{xp}/shared/'
WITH (format = 'TEXTFILE', compression = 'GZIP');
