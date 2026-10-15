-- =============================================================================
-- prep_05_med_map_ndc.sql — CREDENTIALED (baseline variant a): the the medication mapping medication mapping
-- (pipeline/terminology/20_medication_mapping.sql -> dqm_map.ma_emar_path, current run) turned into the
-- RxNorm codings each eMAR administration gets: every generic RxNorm concept (SCD/GPCK) its NDC path
-- reaches (single product: one; mix: one per mapped ingredient), sorted by RXCUI, named by RxNorm.
-- Not here yet (wait for the pharmacist, the medication audit): ICU item codes, entries without an NDC path, review
-- corrections. Re-run the prepare stage after the medication audit with the reviewed map (cheap).
-- Placeholders: {loc}, {version} (RxNorm release, e.g. 20261005).
-- =============================================================================

CREATE TABLE med_map
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/med_map/') AS
WITH g AS (
  SELECT DISTINCT ma_id, generic_rxcui AS rxcui
  FROM dqm_map.ma_emar_path
  WHERE generic_rxcui IS NOT NULL
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
