-- =============================================================================
-- 20_medication_mapping.sql — the medication mapping step 2: map MIMIC-IV-on-FHIR medication administrations to the
-- RxNorm generic concepts the measures' value sets list. Baseline variant (a), strict
-- (decided 2026-10-09):
--   eMAR administration -> its prescription (MedicationAdministration.request -> MedicationRequest)
--   -> Medication (single product: its NDC; mix: the NDCs of its ingredients)
--   -> RxNorm concept by NDC, only NDCs RxNorm itself asserts (SAB=RXNORM, not suppressed)
--   -> generic: SCD/GPCK as is; SBD/BPCK through RxNorm's tradename_of relationship.
-- Not in the baseline: other sources' NDCs and ingredient-only matches (liberal variant c), ICU item
-- codes and entries without an NDC (pharmacist review, the medication audit).
--
-- Credentialed data: tables stay in S3 (fresh run prefix {loc}); SELECTs return counts only, small
-- counts masked as '<10'. The review-candidate table holds MIMIC drug names: it is created here but
-- never selected by this file (its contents must not leave AWS; the pharmacist gets it later).
-- Run with pipeline/athena/run_sql.py:
--   --db dqm_map --param version=20261005 --param loc=s3://your-bucket/dqm/map/<run>
-- Reads: mimic_fhir.ndjson_parts (Phase 1 catalog), dqm_ref.rxn*_{version}, dqm_ref.value_set_codes_{version},
--        mimiciv_hosp.patients (anchor year group, for coverage by era), mimiciv_icu.d_items.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS dqm_map;

DROP TABLE IF EXISTS dqm_map.med_ndc;

-- 1. Medication -> NDC. Single products carry the NDC as their code; mixes list ingredient Medications.
CREATE TABLE dqm_map.med_ndc
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/med_ndc/') AS
WITH med AS (
  SELECT source_file, json_extract_scalar(line, '$.id') AS med_id, line
  FROM mimic_fhir.ndjson_parts
  WHERE source_file IN ('MimicMedication', 'MimicMedicationMix')
),
single AS (
  SELECT m.med_id, json_extract_scalar(c, '$.code') AS ndc
  FROM med m
  CROSS JOIN UNNEST(CAST(json_extract(m.line, '$.code.coding') AS ARRAY(JSON))) AS u(c)
  WHERE m.source_file = 'MimicMedication'
    AND json_extract_scalar(c, '$.system') LIKE '%/mimic-medication-ndc'
),
mix AS (
  SELECT m.med_id, regexp_extract(json_extract_scalar(i, '$.itemReference.reference'), '[^/]+$') AS ingredient_id
  FROM med m
  CROSS JOIN UNNEST(CAST(json_extract(m.line, '$.ingredient') AS ARRAY(JSON))) AS u(i)
  WHERE m.source_file = 'MimicMedicationMix'
)
SELECT med_id, 'single' AS kind, ndc FROM single
UNION ALL
SELECT x.med_id, 'mix' AS kind, s.ndc FROM mix x JOIN single s ON s.med_id = x.ingredient_id;

DROP TABLE IF EXISTS dqm_map.ndc_generic_{version};

-- 2. NDC -> RxNorm concept -> generic concept (pinned release)
CREATE TABLE dqm_map.ndc_generic_{version}
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/ndc_generic/') AS
WITH ndc AS (
  SELECT DISTINCT atv AS ndc, rxcui FROM dqm_ref.rxnsat_{version}
  WHERE atn = 'NDC' AND sab = 'RXNORM' AND suppress = 'N'
),
tty AS (
  SELECT DISTINCT rxcui, tty FROM dqm_ref.rxnconso_{version}
  WHERE sab = 'RXNORM' AND tty IN ('SCD', 'SBD', 'GPCK', 'BPCK')
),
brand AS (   -- verified 2026-10-09: tradename_of rows hold the generic in rxcui1, the brand in rxcui2
  SELECT DISTINCT rxcui2 AS brand_rxcui, rxcui1 AS generic_rxcui FROM dqm_ref.rxnrel_{version}
  WHERE sab = 'RXNORM' AND rela = 'tradename_of'
)
SELECT n.ndc, n.rxcui, t.tty,
       CASE WHEN t.tty IN ('SCD', 'GPCK') THEN n.rxcui ELSE b.generic_rxcui END AS generic_rxcui,
       CASE WHEN t.tty IN ('SCD', 'GPCK') THEN 'direct'
            WHEN b.generic_rxcui IS NOT NULL THEN 'brand_to_generic'
            ELSE 'brand_without_generic' END AS route
FROM ndc n
JOIN tty t ON t.rxcui = n.rxcui
LEFT JOIN brand b ON b.brand_rxcui = n.rxcui AND t.tty IN ('SBD', 'BPCK');

DROP TABLE IF EXISTS dqm_map.ma_emar_path;

-- 3. Every eMAR administration with its path (one row per administration x NDC x generic)
CREATE TABLE dqm_map.ma_emar_path
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/ma_emar_path/') AS
WITH ma AS (
  SELECT json_extract_scalar(line, '$.id') AS ma_id,
         regexp_extract(json_extract_scalar(line, '$.subject.reference'), '[^/]+$') AS patient_id,
         regexp_extract(json_extract_scalar(line, '$.request.reference'), '[^/]+$') AS req_id,
         json_extract_scalar(line, '$.medicationCodeableConcept.coding[0].system') AS med_system,
         json_extract_scalar(line, '$.medicationCodeableConcept.coding[0].code')   AS med_code
  FROM mimic_fhir.ndjson_parts
  WHERE source_file = 'MimicMedicationAdministration'
),
req AS (
  SELECT json_extract_scalar(line, '$.id') AS req_id,
         regexp_extract(json_extract_scalar(line, '$.medicationReference.reference'), '[^/]+$') AS med_id
  FROM mimic_fhir.ndjson_parts
  WHERE source_file = 'MimicMedicationRequest'
)
SELECT ma.ma_id, ma.patient_id, ma.med_system, ma.med_code, ma.req_id, r.med_id, n.kind, n.ndc,
       g.rxcui, g.tty, g.generic_rxcui, g.route
FROM ma
LEFT JOIN req r ON r.req_id = ma.req_id
LEFT JOIN dqm_map.med_ndc n ON n.med_id = r.med_id
LEFT JOIN dqm_map.ndc_generic_{version} g ON g.ndc = n.ndc;

-- 4a. Coverage: administrations by path (one class per administration)
WITH per_ma AS (
  SELECT ma_id,
         CASE WHEN max(req_id) IS NULL THEN '1 no prescription link'
              WHEN max(ndc) IS NULL THEN '2 prescription without NDC'
              WHEN max(generic_rxcui) IS NOT NULL THEN '4 NDC mapped to a generic RxNorm drug'
              WHEN max(rxcui) IS NOT NULL THEN '3b NDC on a brand with no generic link'
              ELSE '3a NDC not asserted in RxNorm 2026-10-05' END AS path,
         max(kind) AS kind
  FROM dqm_map.ma_emar_path GROUP BY ma_id
)
SELECT path, kind,
       CASE WHEN count(*) < 10 THEN '<10' ELSE cast(count(*) AS varchar) END AS administrations,
       round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct
FROM per_ma GROUP BY 1, 2 ORDER BY 1, 2;

-- 4b. Glucose-lowering administrations reached through the mapping, by route, per measure value set
WITH vs AS (
  SELECT oid, code FROM dqm_ref.value_set_codes_{version}
  WHERE oid IN ('2.16.840.1.113762.1.4.1196.393', '2.16.840.1.113762.1.4.1196.394')
    AND system = 'http://www.nlm.nih.gov/research/umls/rxnorm'
),
hits AS (
  SELECT DISTINCT p.ma_id, v.oid, p.route
  FROM dqm_map.ma_emar_path p JOIN vs v ON v.code = p.generic_rxcui
)
SELECT CASE oid WHEN '2.16.840.1.113762.1.4.1196.393' THEN 'CMS816 hypoglycemics'
                ELSE 'CMS871 treatment medications' END AS value_set,
       route,
       CASE WHEN count(DISTINCT ma_id) < 10 THEN '<10' ELSE cast(count(DISTINCT ma_id) AS varchar) END AS administrations
FROM hits GROUP BY 1, 2 ORDER BY 1, 2;

-- 4c. Coverage by era: share of administrations that reach a generic RxNorm drug, by the patient's
--     anchor year group (MIMIC-IV 3.1), to see whether older NDCs map worse
WITH pid AS (
  SELECT json_extract_scalar(line, '$.id') AS patient_id,
         json_extract_scalar(line, '$.identifier[0].value') AS subject_id
  FROM mimic_fhir.ndjson_parts WHERE source_file = 'MimicPatient'
),
per_ma AS (
  SELECT ma_id, max(patient_id) AS patient_id, max(ndc) IS NOT NULL AS has_ndc,
         max(generic_rxcui) IS NOT NULL AS mapped
  FROM dqm_map.ma_emar_path GROUP BY ma_id
)
SELECT p.anchor_year_group,
       count(*) AS administrations,
       round(100.0 * count_if(m.has_ndc) / count(*), 1) AS pct_with_ndc,
       round(100.0 * count_if(m.mapped) / nullif(count_if(m.has_ndc), 0), 1) AS pct_of_ndc_mapped
FROM per_ma m
LEFT JOIN pid ON pid.patient_id = m.patient_id
LEFT JOIN mimiciv_hosp.patients p ON p.subject_id = pid.subject_id
GROUP BY 1 ORDER BY 1;

-- 4d. Distinct NDCs used by administrations: asserted in RxNorm or not
SELECT CASE WHEN g.ndc IS NOT NULL THEN 'in RxNorm 2026-10-05'
            WHEN n.ndc IN ('0', '') OR length(n.ndc) <> 11 THEN 'missing or malformed'
            ELSE 'not asserted in RxNorm 2026-10-05' END AS ndc_status,
       CASE WHEN count(DISTINCT n.ndc) < 10 THEN '<10' ELSE cast(count(DISTINCT n.ndc) AS varchar) END AS distinct_ndcs
FROM (SELECT DISTINCT ndc FROM dqm_map.ma_emar_path WHERE ndc IS NOT NULL) n
LEFT JOIN (SELECT DISTINCT ndc FROM dqm_map.ndc_generic_{version}) g ON g.ndc = n.ndc
GROUP BY 1 ORDER BY 1;

-- 4e. ICU administrations: how many, how many distinct ICU item codes (no names)
SELECT count(*) AS icu_administrations,
       count(DISTINCT json_extract_scalar(line, '$.medicationCodeableConcept.coding[0].code')) AS distinct_icu_items
FROM mimic_fhir.ndjson_parts WHERE source_file = 'MimicMedicationAdministrationICU';

-- 4f. The CMS816 value-set code that is not a current RxNorm concept (terminology only)
SELECT v.oid, v.code,
       max(c.str) AS any_name_in_release, max(c.suppress) AS suppress_flag, count(c.rxcui) AS atoms_in_release
FROM dqm_ref.value_set_codes_{version} v
LEFT JOIN dqm_ref.rxnconso_{version} c ON c.rxcui = v.code AND c.sab = 'RXNORM'
WHERE v.oid IN ('2.16.840.1.113762.1.4.1196.393', '2.16.840.1.113762.1.4.1196.394')
  AND v.system = 'http://www.nlm.nih.gov/research/umls/rxnorm'
GROUP BY 1, 2
HAVING count_if(c.suppress <> 'O') = 0;

DROP TABLE IF EXISTS dqm_map.review_candidates;

-- 5. Pharmacist review candidates (stays in S3; NOT selected here). Entries that need a human:
--    eMAR entries with no NDC path whose name looks glucose-lowering; mixes containing a value-set
--    drug; value-set-class NDCs that did not map; ICU items whose label looks glucose-lowering.
CREATE TABLE dqm_map.review_candidates
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/review_candidates/') AS
WITH names AS (
  SELECT json_extract_scalar(line, '$.id') AS med_id,
         array_join(transform(CAST(json_extract(line, '$.identifier') AS ARRAY(JSON)),
                              x -> json_extract_scalar(x, '$.value')), ' | ') AS med_names
  FROM mimic_fhir.ndjson_parts WHERE source_file IN ('MimicMedication', 'MimicMedicationMix')
),
vs AS (
  SELECT DISTINCT code FROM dqm_ref.value_set_codes_{version}
  WHERE oid IN ('2.16.840.1.113762.1.4.1196.393', '2.16.840.1.113762.1.4.1196.394')
),
emar AS (
  SELECT p.med_system, p.med_code, p.ndc, p.kind, p.route, p.generic_rxcui, nm.med_names,
         p.generic_rxcui IN (SELECT code FROM vs) AS in_value_set,
         regexp_like(lower(coalesce(nm.med_names, '') || ' ' || coalesce(p.med_code, '')),
           'insulin|glipizide|glyburide|glimepiride|repaglinide|nateglinide|tolbutamide|chlorpropamide|tolazamide|metformin|sitagliptin|linagliptin|saxagliptin|alogliptin|pioglitazone|rosiglitazone|empagliflozin|dapagliflozin|canagliflozin|ertugliflozin|liraglutide|semaglutide|dulaglutide|exenatide|lixisenatide|tirzepatide|pramlintide|acarbose|miglitol|humalog|novolog|lantus|levemir|humulin|novolin|toujeo|tresiba|basaglar|apidra|admelog|fiasp|lyumjev|glucotrol|amaryl|januvia|jardiance|farxiga|invokana|victoza|ozempic|trulicity|byetta|mounjaro|actos|avandia|glucophage|glucovance|janumet|prandin|starlix') AS name_looks_glucose_lowering
  FROM dqm_map.ma_emar_path p LEFT JOIN names nm ON nm.med_id = p.med_id
)
SELECT 'eMAR no NDC, name looks glucose-lowering' AS reason, med_system, med_code, med_names,
       CAST(NULL AS varchar) AS ndc, CAST(NULL AS varchar) AS generic_rxcui, count(*) AS administrations
FROM emar WHERE ndc IS NULL AND name_looks_glucose_lowering GROUP BY 2, 3, 4
UNION ALL
SELECT 'mix containing a value-set drug', med_system, med_code, med_names, ndc, generic_rxcui, count(*)
FROM emar WHERE kind = 'mix' AND in_value_set GROUP BY 2, 3, 4, 5, 6
UNION ALL
SELECT 'NDC not mapped, name looks glucose-lowering', med_system, med_code, med_names, ndc, CAST(NULL AS varchar), count(*)
FROM emar WHERE ndc IS NOT NULL AND generic_rxcui IS NULL AND name_looks_glucose_lowering GROUP BY 2, 3, 4, 5
UNION ALL
SELECT 'ICU item, label looks glucose-lowering', 'mimic-medication-icu', i.code, d.label, CAST(NULL AS varchar),
       CAST(NULL AS varchar), count(*)
FROM (SELECT json_extract_scalar(line, '$.medicationCodeableConcept.coding[0].code') AS code
      FROM mimic_fhir.ndjson_parts WHERE source_file = 'MimicMedicationAdministrationICU') i
JOIN mimiciv_icu.d_items d ON d.itemid = try_cast(i.code AS integer)
WHERE regexp_like(lower(d.label), 'insulin|humalog|novolog|lantus|glargine|lispro|aspart|detemir|glulisine')
GROUP BY 2, 3, 4, 5, 6;

-- 6. Size of the review list by reason (counts of distinct entries only)
SELECT reason,
       CASE WHEN count(*) < 10 THEN '<10' ELSE cast(count(*) AS varchar) END AS distinct_entries,
       CASE WHEN sum(administrations) < 10 THEN '<10' ELSE cast(sum(administrations) AS varchar) END AS administrations
FROM dqm_map.review_candidates GROUP BY 1 ORDER BY 1;
