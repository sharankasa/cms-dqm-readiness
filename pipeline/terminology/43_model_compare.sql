-- =============================================================================
-- 43_model_compare.sql — mapping variant (d), model-proposed (registration §4.2), step 3 of 3: score the
-- model's decisions (model_mapping_run.py) against the reviewer's blind decisions
-- (dqm_map.audit_decisions_{round}, 41_audit_apply.sql), and build variant (d)'s additions and drops
-- for the the prepare stage rerun (prepare_run.py --med-map audited --variant d).
-- What matters for the measures is whether an entry lands in a measure value set (and which one), so
-- agreement is reported two ways: "measure-equivalent" (same yes/no, and for yes the same value-set
-- membership for CMS816 and CMS871; mixes: same keep/drop) and exact product agreement.
-- Every model code is re-checked against RxNorm here (the runner already limits it to retrieved
-- candidates). Printed output: counts only.
-- Run:
--   python3 pipeline/athena/run_sql.py --sql pipeline/terminology/43_model_compare.sql \
--     --out results/model-mapping-compare-r1 --db dqm_map --param version=20261005 --param round=r1 \
--     --param model_out=s3://your-bucket/dqm/map/model-r1-<model>-<stamp> \
--     --param loc=s3://your-bucket/dqm/map/model-compare-r1-<UTC stamp>
-- =============================================================================

DROP TABLE IF EXISTS dqm_map.model_decisions_raw_{round};

CREATE EXTERNAL TABLE dqm_map.model_decisions_raw_{round} (
  item_id string, model string, decision string, rxcui string, confidence string, parse_ok string,
  rxcui_valid string, input_tokens string, output_tokens string, ms string, error string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION '{model_out}/model_decisions/'
TBLPROPERTIES ('skip.header.line.count' = '1');

DROP TABLE IF EXISTS dqm_map.model_vs_human_{round};

CREATE TABLE dqm_map.model_vs_human_{round}
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/model_vs_human/') AS
WITH vs AS (
  SELECT code AS rxcui,
         max(IF(oid = '2.16.840.1.113762.1.4.1196.393', 1, 0)) = 1 AS in_cms816,
         max(IF(oid = '2.16.840.1.113762.1.4.1196.394', 1, 0)) = 1 AS in_cms871
  FROM dqm_ref.value_set_codes_{version}
  WHERE oid IN ('2.16.840.1.113762.1.4.1196.393', '2.16.840.1.113762.1.4.1196.394')
    AND system = 'http://www.nlm.nih.gov/research/umls/rxnorm'
  GROUP BY code
),
rx AS (   -- current prescribable products (SUPPRESS N), or value-set codes
  SELECT DISTINCT c.rxcui FROM dqm_ref.rxnconso_{version} c
  WHERE c.sab = 'RXNORM' AND c.tty IN ('SCD', 'GPCK', 'SBD', 'BPCK')
    AND (c.suppress = 'N' OR c.rxcui IN (SELECT rxcui FROM vs))
),
m AS (
  SELECT r.item_id, r.model, lower(trim(r.decision)) AS decision, trim(r.rxcui) AS rxcui, r.confidence,
         r.parse_ok = '1' AS parse_ok, r.error <> '' AS errored,
         rx.rxcui IS NOT NULL AS rxcui_in_rxnorm
  FROM dqm_map.model_decisions_raw_{round} r
  LEFT JOIN rx ON rx.rxcui = trim(r.rxcui)
)
SELECT h.item_id, h.part, h.med_system, h.med_code, h.names, h.match_level, h.administrations,
       h.status AS human, h.rxcui AS human_rxcui, h.in_cms816 AS human_816, h.in_cms871 AS human_871,
       coalesce(m.decision, '') AS model, IF(m.rxcui_in_rxnorm, m.rxcui, '') AS model_rxcui, m.confidence,
       coalesce(m.parse_ok, false) AS model_parse_ok, coalesce(m.errored, false) AS model_errored,
       m.rxcui <> '' AND NOT coalesce(m.rxcui_in_rxnorm, false) AS model_rxcui_rejected,
       coalesce(v.in_cms816, false) AS model_816, coalesce(v.in_cms871, false) AS model_871,
       m.model AS model_id
FROM dqm_map.audit_decisions_{round} h
LEFT JOIN m ON m.item_id = h.item_id
LEFT JOIN vs v ON v.rxcui = IF(m.rxcui_in_rxnorm, m.rxcui, NULL);

-- A. Model run health: entries answered, unparsed, errors, codes rejected by the RxNorm check
SELECT max(model_id) AS model, count(*) AS entries,
       count_if(model IN ('map', 'possible', 'no')) AS answered,
       count_if(NOT model_parse_ok AND NOT model_errored) AS unparsed,
       count_if(model_errored) AS errors,
       count_if(model_rxcui_rejected) AS codes_rejected_by_rxnorm_check
FROM dqm_map.model_vs_human_{round};

-- B. Glucose-lowering detection, model vs reviewer, by part (human yes = map/possible; counts only)
SELECT part,
       count_if(human IN ('map', 'possible') AND model IN ('map', 'possible')) AS both_yes,
       count_if(human IN ('map', 'possible') AND model NOT IN ('map', 'possible')) AS model_missed,
       count_if(human = 'no' AND model IN ('map', 'possible')) AS model_extra,
       count_if(human = 'no' AND model = 'no') AS both_no,
       count_if(human NOT IN ('map', 'possible', 'no')) AS human_undecided
FROM dqm_map.model_vs_human_{round}
GROUP BY 1 ORDER BY 1;

-- C. Measure-equivalent and exact agreement, by part and by model confidence (counts only).
--    Measure-equivalent: both "no"; or both yes with the same CMS816 and CMS871 value-set membership;
--    mixes (E): same keep/drop.
WITH s AS (
  SELECT part, coalesce(confidence, '') AS confidence,
         CASE WHEN part LIKE 'E%' THEN (human = 'no') = (model = 'no')
              WHEN human = 'no' AND model = 'no' THEN true
              WHEN human IN ('map', 'possible') AND model IN ('map', 'possible')
                   THEN human_816 = model_816 AND human_871 = model_871
              ELSE false END AS measure_equivalent,
         human IN ('map', 'possible') AND model IN ('map', 'possible') AND human_rxcui = model_rxcui AS same_product
  FROM dqm_map.model_vs_human_{round}
  WHERE human IN ('map', 'possible', 'no')
)
SELECT part, confidence, count(*) AS entries, count_if(measure_equivalent) AS measure_equivalent,
       count_if(same_product) AS same_product
FROM s GROUP BY 1, 2 ORDER BY 1, 2;

-- D. Review by exception: if every "high"-confidence answer were accepted without review, how many
--    entries would a person still review, and how many accepted answers would change a measure
--    (not measure-equivalent), with their administrations (counts only)
WITH s AS (
  SELECT *,
         CASE WHEN part LIKE 'E%' THEN (human = 'no') = (model = 'no')
              WHEN human = 'no' AND model = 'no' THEN true
              WHEN human IN ('map', 'possible') AND model IN ('map', 'possible')
                   THEN human_816 = model_816 AND human_871 = model_871
              ELSE false END AS measure_equivalent
  FROM dqm_map.model_vs_human_{round}
  WHERE human IN ('map', 'possible', 'no')
)
SELECT count(*) AS entries,
       count_if(confidence = 'high') AS auto_accepted_high,
       count_if(confidence <> 'high' OR confidence IS NULL) AS left_for_review,
       count_if(confidence = 'high' AND NOT measure_equivalent) AS accepted_but_wrong_for_measures,
       sum(IF(confidence = 'high' AND NOT measure_equivalent AND administrations <> '<10',
              try_cast(administrations AS bigint), 0)) AS wrong_accepted_administrations_10plus
FROM s;

DROP TABLE IF EXISTS dqm_map.med_additions_model_{round};

-- 2. Variant (d): the model's "map" answers with a valid product code (no human in the loop)
CREATE TABLE dqm_map.med_additions_model_{round}
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/med_additions_model/') AS
SELECT DISTINCT 'd' AS variant, match_level, med_system, med_code,
       IF(match_level = 'names', coalesce(names, ''), '') AS names, model_rxcui AS rxcui, part
FROM dqm_map.model_vs_human_{round}
WHERE part NOT LIKE 'E%' AND model = 'map' AND model_rxcui <> '';

DROP TABLE IF EXISTS dqm_map.med_drops_model_{round};

CREATE TABLE dqm_map.med_drops_model_{round}
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/med_drops_model/') AS
SELECT DISTINCT med_system, med_code, coalesce(names, '') AS names
FROM dqm_map.model_vs_human_{round}
WHERE part LIKE 'E%' AND model = 'no';

-- E. Variant (d) size (counts only)
SELECT (SELECT count(*) FROM dqm_map.med_additions_model_{round}) AS keys_added,
       (SELECT count(DISTINCT rxcui) FROM dqm_map.med_additions_model_{round}) AS products,
       (SELECT count(*) FROM dqm_map.med_drops_model_{round}) AS mixes_dropped;
