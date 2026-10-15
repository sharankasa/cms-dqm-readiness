-- =============================================================================
-- prep_55_export_mapped_open.sql — OPEN DEMO ONLY (validation). Every mapped resource, so the runner can
-- download it and compare it with the Python shim's output (work/demo-mapped-v3).
-- Lines "<source_file><TAB><changed><TAB><keep><TAB><resource JSON>". Never run on credentialed data.
-- =============================================================================

UNLOAD (
  SELECT source_file || chr(9) || CAST(changed AS varchar) || chr(9) || CAST(keep AS varchar) || chr(9) || line AS line
  FROM mapped
) TO '{loc}/export/mapped_open/'
WITH (format = 'TEXTFILE', compression = 'GZIP');
