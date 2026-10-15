-- =============================================================================
-- prep_05_med_map_audited.sql — CREDENTIALED: the medication mapping after the recall audit (the medication audit,
-- pipeline/terminology/41_audit_apply.sql), for one registered mapping variant (registration §4.2):
--   (a) baseline     NDC path (minus mixes the reviewer dropped) + reviewed "map" additions (eMAR and ICU)
--   (b) conservative NDC path for single products only (exact identifier matches; no mixes, no
--                    name-based additions, so no ICU items)
--   (c) liberal      (a) + reviewed "possible" additions
--   (d) model        NDC path (minus mixes the model dropped) + the model's "map" answers, no human
--                    (43_model_compare.sql: dqm_map.med_additions_model_* / med_drops_model_*)
-- Output as prep_05_med_map_ndc.sql: med_map(ma_id, codings_json), every generic RxNorm concept an
-- administration reaches, sorted by RXCUI, named by RxNorm.
-- Placeholders: {loc}, {version} (RxNorm release), {variant} (a | b | c | d), {additions_table},
-- {drops_table} (prepare_run.py: reviewed tables for a-c, model tables for d).
-- Reads: dqm_map.ma_emar_path, dqm_map.ma_codes, dqm_map.med_names, the additions and drops tables,
--        dqm_ref.rxnconso_{version}.
-- Matching (41_audit_apply.sql): additions with match_level 'code' (audit codes, ICU items) apply to every
-- administration with that local code; 'names' (review list) only to eMAR administrations whose
-- Medication has those names. Drops (mixes) apply by code + names to mix administrations only.
-- =============================================================================

CREATE TABLE med_map
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/med_map/') AS
WITH emar AS (
  SELECT p.ma_id, regexp_extract(p.med_system, '[^/]+$') AS med_system, p.med_code, p.kind, p.generic_rxcui,
         coalesce(n.names, '') AS names
  FROM dqm_map.ma_emar_path p
  LEFT JOIN dqm_map.med_names n ON n.med_id = p.med_id
),
drops AS (
  SELECT med_system, med_code, coalesce(names, '') AS names FROM {drops_table} WHERE '{variant}' <> 'b'
),
ndc AS (
  SELECT DISTINCT e.ma_id, e.generic_rxcui AS rxcui
  FROM emar e
  LEFT JOIN drops d ON e.kind = 'mix' AND d.med_system = e.med_system AND d.med_code = e.med_code AND d.names = e.names
  WHERE e.generic_rxcui IS NOT NULL
    AND d.med_code IS NULL
    AND ('{variant}' <> 'b' OR e.kind = 'single')
),
added AS (
  SELECT DISTINCT m.ma_id, a.rxcui
  FROM dqm_map.ma_codes m
  JOIN {additions_table} a ON a.med_system = m.med_system AND a.med_code = m.med_code
  WHERE a.variant = '{variant}' AND a.match_level = 'code'
  UNION
  SELECT DISTINCT e.ma_id, a.rxcui
  FROM emar e
  JOIN {additions_table} a ON a.med_system = e.med_system AND a.med_code = e.med_code AND a.names = e.names
  WHERE a.variant = '{variant}' AND a.match_level = 'names'
),
g AS (
  SELECT ma_id, rxcui FROM ndc
  UNION
  SELECT ma_id, rxcui FROM added
),
nm AS (
  SELECT rxcui, min(str) AS display
  FROM dqm_ref.rxnconso_{version}
  WHERE sab = 'RXNORM' AND tty IN ('SCD', 'GPCK') AND suppress <> 'O'
  GROUP BY rxcui
)
SELECT g.ma_id,
       json_format(CAST(array_agg(CAST(MAP(ARRAY['system', 'code', 'display'],
                                           ARRAY[CAST('http://www.nlm.nih.gov/research/umls/rxnorm' AS JSON),
                                                 CAST(g.rxcui AS JSON), CAST(coalesce(nm.display, '') AS JSON)]) AS JSON)
                                  ORDER BY g.rxcui) AS JSON)) AS codings_json
FROM g LEFT JOIN nm ON nm.rxcui = g.rxcui
GROUP BY g.ma_id;
