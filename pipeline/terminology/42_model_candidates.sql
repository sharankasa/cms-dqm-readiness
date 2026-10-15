-- =============================================================================
-- 42_model_candidates.sql — mapping variant (d), model-proposed (registration §4.2), step 1 of 3: the
-- deterministic retrieval. For every entry of the medication audit worklist, the up-to-12 RxNorm
-- generic products (SCD / GPCK) containing a glucose-lowering ingredient whose names best match the
-- entry's words. The model (model_mapping_run.py) may only answer with one of these codes or "none",
-- so every proposed code is a real, current RxNorm concept by construction (checked again in 43).
-- Products offered: current prescribable RxNorm concepts only (SUPPRESS = 'N'), plus any value-set code.
-- Fixed 2026-10-13 after the first full model run: the pool had also held unquantified, non-prescribable
-- concepts (SUPPRESS = 'E', e.g. "metformin 500 MG Extended Release Oral Tablet", superseded by its
-- quantified "24 HR ..." form), which the model often chose and which no value set lists.
-- Glucose-lowering ingredients = the value-set drugs' ingredients + every ATC A10 ingredient
-- (dqm_map.drug_words_{version}, from 40_audit_worklist.sql). Candidates show the product's RxNorm
-- name and its brand names; value-set membership is stored for evaluation but never shown to the model.
-- Printed output: counts only.
-- Run:
--   python3 pipeline/athena/run_sql.py --sql pipeline/terminology/42_model_candidates.sql \
--     --out results/model-mapping-candidates --db dqm_map --param version=20261005 \
--     --param loc=s3://your-bucket/dqm/map/model-candidates-<UTC stamp>
-- =============================================================================

DROP TABLE IF EXISTS dqm_map.model_candidates_{version};

CREATE TABLE dqm_map.model_candidates_{version}
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/model_candidates/') AS
WITH conso AS (
  SELECT rxcui, tty, str, suppress FROM dqm_ref.rxnconso_{version} WHERE sab = 'RXNORM' AND suppress <> 'O'
),
tty AS (
  SELECT DISTINCT rxcui, tty FROM conso
),
edge AS (
  SELECT rxcui1 AS a, rxcui2 AS b FROM dqm_ref.rxnrel_{version} WHERE sab = 'RXNORM'
  UNION
  SELECT rxcui2 AS a, rxcui1 AS b FROM dqm_ref.rxnrel_{version} WHERE sab = 'RXNORM'
),
ing AS (
  SELECT DISTINCT rxcui FROM dqm_map.drug_words_{version}
),
scdc AS (   -- clinical drug components of the glucose-lowering ingredients
  SELECT DISTINCT e.b AS scdc, i.rxcui AS ing FROM ing i
  JOIN edge e ON e.a = i.rxcui
  JOIN tty t ON t.rxcui = e.b AND t.tty = 'SCDC'
),
scd AS (
  SELECT DISTINCT e.b AS rxcui, s.ing FROM scdc s
  JOIN edge e ON e.a = s.scdc
  JOIN tty t ON t.rxcui = e.b AND t.tty = 'SCD'
),
gpck AS (
  SELECT DISTINCT e.b AS rxcui, s.ing FROM scd s
  JOIN edge e ON e.a = s.rxcui
  JOIN tty t ON t.rxcui = e.b AND t.tty = 'GPCK'
),
pool AS (
  SELECT rxcui, ing FROM scd UNION SELECT rxcui, ing FROM gpck
),
vs_codes AS (
  SELECT DISTINCT code AS rxcui FROM dqm_ref.value_set_codes_{version}
  WHERE oid IN ('2.16.840.1.113762.1.4.1196.393', '2.16.840.1.113762.1.4.1196.394')
    AND system = 'http://www.nlm.nih.gov/research/umls/rxnorm'
),
pname AS (   -- current prescribable products (SUPPRESS N) or value-set codes
  SELECT p.rxcui, min(c.str) AS name, min(c.tty) AS tty
  FROM (SELECT DISTINCT rxcui FROM pool) p
  JOIN conso c ON c.rxcui = p.rxcui AND c.tty IN ('SCD', 'GPCK')
  LEFT JOIN vs_codes v ON v.rxcui = p.rxcui
  WHERE c.suppress = 'N' OR v.rxcui IS NOT NULL
  GROUP BY p.rxcui
),
brands AS (   -- brand names of the branded versions of each product (SBD/BPCK -> BN)
  SELECT p.rxcui, array_sort(array_distinct(array_agg(bn.str))) AS brand_names
  FROM (SELECT DISTINCT rxcui FROM pool) p
  JOIN edge e ON e.a = p.rxcui
  JOIN tty t ON t.rxcui = e.b AND t.tty IN ('SBD', 'BPCK')
  JOIN edge e2 ON e2.a = e.b
  JOIN conso bn ON bn.rxcui = e2.b AND bn.tty = 'BN'
  GROUP BY p.rxcui
),
key_words AS (   -- ingredient words of each product (from the drug-word list) + its brand-name words
  SELECT p.rxcui, array_distinct(array_agg(w.word)) AS kw
  FROM pool p JOIN dqm_map.drug_words_{version} w ON w.rxcui = p.ing
  GROUP BY p.rxcui
),
vs AS (
  SELECT code AS rxcui,
         max(IF(oid = '2.16.840.1.113762.1.4.1196.393', 1, 0)) = 1 AS in_cms816,
         max(IF(oid = '2.16.840.1.113762.1.4.1196.394', 1, 0)) = 1 AS in_cms871
  FROM dqm_ref.value_set_codes_{version}
  WHERE oid IN ('2.16.840.1.113762.1.4.1196.393', '2.16.840.1.113762.1.4.1196.394')
    AND system = 'http://www.nlm.nih.gov/research/umls/rxnorm'
  GROUP BY code
),
prod AS (
  SELECT n.rxcui, n.tty, n.name, coalesce(b.brand_names, CAST(ARRAY[] AS ARRAY(VARCHAR))) AS brand_names,
         array_distinct(k.kw || filter(regexp_split(lower(array_join(coalesce(b.brand_names, CAST(ARRAY[] AS ARRAY(VARCHAR))), ' ')),
                                                    '[^a-z0-9]+'), x -> length(x) >= 4)) AS kw,
         array_distinct(filter(regexp_split(regexp_replace(lower(n.name), '([0-9])([a-z])', '$1 $2'), '[^a-z0-9.]+'),
                               x -> length(x) >= 3 OR regexp_like(x, '^[0-9.]+$'))) AS ptoks,
         coalesce(v.in_cms816, false) AS in_cms816, coalesce(v.in_cms871, false) AS in_cms871
  FROM pname n
  JOIN key_words k ON k.rxcui = n.rxcui
  LEFT JOIN brands b ON b.rxcui = n.rxcui
  LEFT JOIN vs v ON v.rxcui = n.rxcui
),
items AS (   -- entry words; digits split from units (5mg -> 5 mg); NPH also read as isophane
  SELECT item_id, toks || IF(contains(toks, 'nph'), ARRAY['isophane'], CAST(ARRAY[] AS ARRAY(VARCHAR))) AS itoks
  FROM (
    SELECT item_id,
           array_distinct(filter(regexp_split(regexp_replace(lower(coalesce(names, '') || ' ' || coalesce(med_code, '')),
                                                             '([0-9])([a-z])', '$1 $2'), '[^a-z0-9.]+'),
                                 x -> length(x) >= 3 OR regexp_like(x, '^[0-9.]+$'))) AS toks
    FROM dqm_map.audit_worklist
  )
),
scored AS (
  SELECT i.item_id, p.rxcui, p.tty, p.name, p.brand_names, p.in_cms816, p.in_cms871,
         cardinality(array_intersect(i.itoks, p.kw)) AS key_hits,
         cardinality(array_intersect(i.itoks, p.ptoks)) AS word_hits
  FROM items i CROSS JOIN prod p
),
ranked AS (
  SELECT *, row_number() OVER (PARTITION BY item_id ORDER BY key_hits DESC, word_hits DESC, length(name), rxcui) AS candidate_rank
  FROM scored WHERE key_hits > 0
)
SELECT item_id, candidate_rank, rxcui, tty, name, array_join(brand_names, ', ') AS brands,
       key_hits, word_hits, in_cms816, in_cms871
FROM ranked WHERE candidate_rank <= 12;

-- A. Candidate pool and coverage (counts only)
SELECT count(DISTINCT rxcui) AS candidate_products_used,
       count(DISTINCT item_id) AS entries_with_candidates,
       (SELECT count(*) FROM dqm_map.audit_worklist) AS worklist_entries
FROM dqm_map.model_candidates_{version};

-- B. Entries with candidates, by worklist part (counts only)
SELECT w.part,
       CASE WHEN count(*) BETWEEN 1 AND 9 THEN '<10' ELSE CAST(count(*) AS varchar) END AS entries,
       CASE WHEN count(c.item_id) BETWEEN 1 AND 9 THEN '<10' ELSE CAST(count(c.item_id) AS varchar) END AS with_candidates
FROM dqm_map.audit_worklist w
LEFT JOIN (SELECT DISTINCT item_id FROM dqm_map.model_candidates_{version}) c ON c.item_id = w.item_id
GROUP BY 1 ORDER BY 1;
