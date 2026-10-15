-- =============================================================================
-- prep_10_map.sql — the prepare stage prepare: the baseline mapping (variant a) applied to the FHIR resources in Athena.
-- SQL translation of the open-demo mapping rules (version 3) (deviation 3), with the medication codings taken
-- from med_map (the medication mapping NDC path on credentialed data; the shim's crosswalk on the open demo).
-- Rules (nothing removed; codings and profiles are ADDED; values stay as exported):
--   Patient                  + QI-Core patient profile
--   Encounter (hospital)     by class: EMER -> 183452005; AMB/SS -> 32485007 (priority EL -> 8715000);
--                            OBSENC -> the Observation Services code; other classes untouched.
--                            Mapped ones also get: QI-Core encounter profile, reasonReference to ALL
--                            their diagnoses, hospice discharge -> the SNOMED hospice code
--   Encounter (ED)           + SNOMED 4525004, QI-Core encounter profile, reasonReference to diagnoses
--   Condition (hosp + ED)    + dotted ICD-10-CM / ICD-9-CM coding, QI-Core encounter-diagnosis profile,
--                            clinicalStatus active (if absent), onset = start of its encounter (if absent)
--   Observation (labevents)  glucose items: + LOINC, QI-Core lab profile, mg/dL unit system -> UCUM
--   Observation (chart)      glucose items: the same + the standard `laboratory` category
--   MedicationAdministration + RxNorm codings from med_map, QI-Core profile (only when mapped)
-- Rewritten resources are re-serialized by Athena (compact JSON, keys possibly reordered); untouched
-- resources keep their original bytes. Array order of added references: sorted by Condition id.
--
-- Output: mapped (source_file, rtype, id, line, changed, keep)
--   keep = the data-requirements filter decided per resource (a superset of the slice filter in
--          ../core/30_filter_hash.sql): Patient, Encounter, Medication, Location, Organization; coded
--          types with a code in a measure value set; administrations with such a code or with a
--          medicationReference. Everything else can never be in a filtered slice.
-- Placeholders: {loc}, {source_filter} (rows of src to read), {keep_filter} (TRUE, or keep),
--   {obs_code}, {obs_display}, {hospice_system}, {hospice_code}, {hospice_display}.
-- =============================================================================

CREATE TABLE enc_start
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/enc_start/') AS
SELECT json_extract_scalar(line, '$.id') AS enc_id,
       json_extract_scalar(line, '$.period.start') AS start
FROM src
WHERE source_file IN ('MimicEncounter', 'MimicEncounterED');

CREATE TABLE enc_dx
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/enc_dx/') AS
SELECT regexp_extract(json_extract_scalar(line, '$.encounter.reference'), '[^/]+$') AS enc_id,
       array_sort(array_agg('Condition/' || json_extract_scalar(line, '$.id'))) AS refs
FROM src
WHERE source_file IN ('MimicCondition', 'MimicConditionED')
  AND json_extract_scalar(line, '$.encounter.reference') IS NOT NULL
GROUP BY 1;

CREATE TABLE mapped
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/mapped/') AS
WITH
vs AS (
  SELECT map_agg(k, true) AS m
  FROM (SELECT DISTINCT system || '|' || code AS k FROM vs_codes)
),
xw AS (
  SELECT map_agg(itemid, loinc)   FILTER (WHERE source_table = 'labevents')   AS lab_loinc,
         map_agg(itemid, display) FILTER (WHERE source_table = 'labevents')   AS lab_disp,
         map_agg(itemid, loinc)   FILTER (WHERE source_table = 'chartevents') AS chart_loinc,
         map_agg(itemid, display) FILTER (WHERE source_table = 'chartevents') AS chart_disp
  FROM glucose_xw
),
s AS (
  SELECT source_file, line, json_extract_scalar(line, '$.id') AS id
  FROM src
  WHERE {source_filter}
),

-- ---------- Patient
b_patient AS (
  SELECT source_file, line,
         map_filter(MAP(ARRAY['_'], ARRAY[CAST(NULL AS JSON)]), (k, v) -> v IS NOT NULL) AS patch,
         'http://hl7.org/fhir/us/qicore/StructureDefinition/qicore-patient' AS add_profile
  FROM s WHERE source_file = 'MimicPatient'
),

-- ---------- Encounter (hospital stays)
e0 AS (
  SELECT s.source_file, s.line, d.refs,
         CASE json_extract_scalar(s.line, '$.class.code')
           WHEN 'EMER'   THEN '183452005'
           WHEN 'AMB'    THEN IF(json_extract_scalar(s.line, '$.priority.coding[0].code') = 'EL', '8715000', '32485007')
           WHEN 'SS'     THEN IF(json_extract_scalar(s.line, '$.priority.coding[0].code') = 'EL', '8715000', '32485007')
           WHEN 'OBSENC' THEN '{obs_code}' END AS tcode,
         CASE json_extract_scalar(s.line, '$.class.code')
           WHEN 'EMER'   THEN 'Emergency hospital admission'
           WHEN 'AMB'    THEN IF(json_extract_scalar(s.line, '$.priority.coding[0].code') = 'EL', 'Hospital admission, elective', 'Hospital admission')
           WHEN 'SS'     THEN IF(json_extract_scalar(s.line, '$.priority.coding[0].code') = 'EL', 'Hospital admission, elective', 'Hospital admission')
           WHEN 'OBSENC' THEN '{obs_display}' END AS tdisp,
         coalesce(CAST(json_extract(s.line, '$.type') AS ARRAY(JSON)), CAST(ARRAY[] AS ARRAY(JSON))) AS types,
         coalesce(CAST(json_extract(s.line, '$.reasonReference') AS ARRAY(JSON)), CAST(ARRAY[] AS ARRAY(JSON))) AS rr,
         json_extract(s.line, '$.hospitalization.dischargeDisposition') AS dd,
         coalesce(CAST(json_extract(s.line, '$.hospitalization.dischargeDisposition.coding') AS ARRAY(JSON)),
                  CAST(ARRAY[] AS ARRAY(JSON))) AS ddc
  FROM s LEFT JOIN enc_dx d ON d.enc_id = s.id
  WHERE s.source_file = 'MimicEncounter'
),
e1 AS (
  SELECT *, IF(cardinality(types) = 0, json_parse('{"coding":[]}'), element_at(types, 1)) AS t0
  FROM e0
),
e2 AS (
  SELECT *, coalesce(CAST(json_extract(t0, '$.coding') AS ARRAY(JSON)), CAST(ARRAY[] AS ARRAY(JSON))) AS c0
  FROM e1
),
b_enc AS (
  SELECT source_file, line,
    map_filter(MAP(ARRAY['type', 'reasonReference', 'hospitalization'], ARRAY[
      -- type: add the SNOMED encounter type to the first type's codings
      IF(tcode IS NULL, NULL, CAST(
        ARRAY[IF(any_match(c0, c -> json_extract_scalar(c, '$.system') = 'http://snomed.info/sct'
                                AND json_extract_scalar(c, '$.code') = tcode),
                 t0,
                 CAST(map_concat(CAST(t0 AS MAP(VARCHAR, JSON)),
                                 MAP(ARRAY['coding'], ARRAY[CAST(c0 || CAST(MAP(ARRAY['system', 'code', 'display'],
                                     ARRAY[CAST('http://snomed.info/sct' AS JSON), CAST(tcode AS JSON), CAST(tdisp AS JSON)]) AS JSON) AS JSON)]))
                      AS JSON))]
        || IF(cardinality(types) > 1, slice(types, 2, cardinality(types) - 1), CAST(ARRAY[] AS ARRAY(JSON)))
        AS JSON)),
      -- reasonReference: every diagnosis of the stay (MIMIC links Condition -> Encounter only)
      IF(tcode IS NULL OR refs IS NULL OR cardinality(refs) = 0, NULL, CAST(
        rr || transform(filter(refs, x -> NOT contains(transform(rr, y -> json_extract_scalar(y, '$.reference')), x)),
                        x -> CAST(MAP(ARRAY['reference'], ARRAY[CAST(x AS JSON)]) AS JSON))
        AS JSON)),
      -- hospitalization.dischargeDisposition: HOSPICE -> + the SNOMED hospice code
      IF(tcode IS NULL OR dd IS NULL OR NOT any_match(ddc, c -> json_extract_scalar(c, '$.code') = 'HOSPICE'), NULL,
        CAST(map_concat(CAST(json_extract(line, '$.hospitalization') AS MAP(VARCHAR, JSON)),
             MAP(ARRAY['dischargeDisposition'], ARRAY[
               IF(any_match(ddc, c -> json_extract_scalar(c, '$.system') = '{hospice_system}'
                                  AND json_extract_scalar(c, '$.code') = '{hospice_code}'),
                  dd,
                  CAST(map_concat(CAST(dd AS MAP(VARCHAR, JSON)),
                       MAP(ARRAY['coding'], ARRAY[CAST(ddc || CAST(MAP(ARRAY['system', 'code', 'display'],
                           ARRAY[CAST('{hospice_system}' AS JSON), CAST('{hospice_code}' AS JSON),
                                 CAST('{hospice_display}' AS JSON)]) AS JSON) AS JSON)])) AS JSON))]))
        AS JSON))
    ]), (k, v) -> v IS NOT NULL) AS patch,
    IF(tcode IS NULL, NULL, 'http://hl7.org/fhir/us/qicore/StructureDefinition/qicore-encounter') AS add_profile
  FROM e2
),

-- ---------- Encounter (ED visits)
d0 AS (
  SELECT s.source_file, s.line, d.refs,
         coalesce(CAST(json_extract(s.line, '$.type') AS ARRAY(JSON)), CAST(ARRAY[] AS ARRAY(JSON))) AS types,
         coalesce(CAST(json_extract(s.line, '$.reasonReference') AS ARRAY(JSON)), CAST(ARRAY[] AS ARRAY(JSON))) AS rr
  FROM s LEFT JOIN enc_dx d ON d.enc_id = s.id
  WHERE s.source_file = 'MimicEncounterED'
),
d1 AS (
  SELECT *, IF(cardinality(types) = 0, json_parse('{"coding":[]}'), element_at(types, 1)) AS t0 FROM d0
),
d2 AS (
  SELECT *, coalesce(CAST(json_extract(t0, '$.coding') AS ARRAY(JSON)), CAST(ARRAY[] AS ARRAY(JSON))) AS c0 FROM d1
),
b_ed AS (
  SELECT source_file, line,
    map_filter(MAP(ARRAY['type', 'reasonReference'], ARRAY[
      CAST(
        ARRAY[IF(any_match(c0, c -> json_extract_scalar(c, '$.system') = 'http://snomed.info/sct'
                                AND json_extract_scalar(c, '$.code') = '4525004'),
                 t0,
                 CAST(map_concat(CAST(t0 AS MAP(VARCHAR, JSON)),
                                 MAP(ARRAY['coding'], ARRAY[CAST(c0 || CAST(MAP(ARRAY['system', 'code', 'display'],
                                     ARRAY[CAST('http://snomed.info/sct' AS JSON), CAST('4525004' AS JSON),
                                           CAST('Emergency department patient visit' AS JSON)]) AS JSON) AS JSON)]))
                      AS JSON))]
        || IF(cardinality(types) > 1, slice(types, 2, cardinality(types) - 1), CAST(ARRAY[] AS ARRAY(JSON)))
        AS JSON),
      IF(refs IS NULL OR cardinality(refs) = 0, NULL, CAST(
        rr || transform(filter(refs, x -> NOT contains(transform(rr, y -> json_extract_scalar(y, '$.reference')), x)),
                        x -> CAST(MAP(ARRAY['reference'], ARRAY[CAST(x AS JSON)]) AS JSON))
        AS JSON))
    ]), (k, v) -> v IS NOT NULL) AS patch,
    'http://hl7.org/fhir/us/qicore/StructureDefinition/qicore-encounter' AS add_profile
  FROM d2
),

-- ---------- Condition (hospital and ED diagnoses)
k0 AS (
  SELECT s.source_file, s.line, es.start AS enc_start_txt,
         coalesce(CAST(json_extract(s.line, '$.code.coding') AS ARRAY(JSON)), CAST(ARRAY[] AS ARRAY(JSON))) AS cc
  FROM s
  LEFT JOIN enc_start es ON es.enc_id = regexp_extract(json_extract_scalar(s.line, '$.encounter.reference'), '[^/]+$')
  WHERE s.source_file IN ('MimicCondition', 'MimicConditionED')
),
k1 AS (
  SELECT *, element_at(filter(cc, c -> regexp_like(coalesce(json_extract_scalar(c, '$.system'), ''), 'icd(10|9)$')), 1) AS ic
  FROM k0
),
k2 AS (
  SELECT *,
         CASE WHEN json_extract_scalar(ic, '$.system') LIKE '%icd10' THEN 'http://hl7.org/fhir/sid/icd-10-cm'
              WHEN json_extract_scalar(ic, '$.system') LIKE '%icd9'  THEN 'http://hl7.org/fhir/sid/icd-9-cm' END AS dsys,
         coalesce(json_extract_scalar(ic, '$.code'), '') AS rawcode,
         coalesce(json_extract_scalar(ic, '$.display'), '') AS ddisp
  FROM k1
),
k3 AS (
  SELECT *,
         IF(length(rawcode) > n, substr(rawcode, 1, n) || '.' || substr(rawcode, n + 1), rawcode) AS dcode
  FROM (SELECT *, CASE WHEN dsys = 'http://hl7.org/fhir/sid/icd-9-cm' AND starts_with(rawcode, 'E') THEN 4 ELSE 3 END AS n FROM k2)
),
b_cond AS (
  SELECT source_file, line,
    map_filter(MAP(ARRAY['code', 'clinicalStatus', 'onsetDateTime'], ARRAY[
      IF(dsys IS NULL
         OR any_match(cc, c -> json_extract_scalar(c, '$.system') = dsys AND json_extract_scalar(c, '$.code') = dcode), NULL,
         CAST(map_concat(CAST(coalesce(json_extract(line, '$.code'), json_parse('{}')) AS MAP(VARCHAR, JSON)),
              MAP(ARRAY['coding'], ARRAY[CAST(cc || CAST(MAP(ARRAY['system', 'code', 'display'],
                  ARRAY[CAST(dsys AS JSON), CAST(dcode AS JSON), CAST(ddisp AS JSON)]) AS JSON) AS JSON)])) AS JSON)),
      IF(json_extract(line, '$.clinicalStatus') IS NULL,
         json_parse('{"coding":[{"system":"http://terminology.hl7.org/CodeSystem/condition-clinical","code":"active","display":"Active"}]}'),
         NULL),
      IF(enc_start_txt IS NOT NULL AND json_extract(line, '$.onsetDateTime') IS NULL, CAST(enc_start_txt AS JSON), NULL)
    ]), (k, v) -> v IS NOT NULL) AS patch,
    'http://hl7.org/fhir/us/qicore/StructureDefinition/qicore-condition-encounter-diagnosis' AS add_profile
  FROM k3
),

-- ---------- Observation: glucose from labevents and from ICU charting
o0 AS (
  SELECT s.source_file, s.line,
         IF(s.source_file = 'MimicObservationLabevents', '%mimic-d-labitems', '%mimic-chartevents-d-items') AS sys_like,
         IF(s.source_file = 'MimicObservationLabevents', x.lab_loinc, x.chart_loinc) AS lm,
         IF(s.source_file = 'MimicObservationLabevents', x.lab_disp, x.chart_disp) AS ldisp,
         coalesce(CAST(json_extract(s.line, '$.code.coding') AS ARRAY(JSON)), CAST(ARRAY[] AS ARRAY(JSON))) AS cc,
         json_extract(s.line, '$.valueQuantity') AS q,
         coalesce(CAST(json_extract(s.line, '$.category') AS ARRAY(JSON)), CAST(ARRAY[] AS ARRAY(JSON))) AS cats
  FROM s CROSS JOIN xw x
  WHERE s.source_file IN ('MimicObservationLabevents', 'MimicObservationChartevents')
),
o1 AS (
  SELECT *,
         element_at(transform(filter(cc, c -> coalesce(json_extract_scalar(c, '$.system'), '') LIKE sys_like
                                          AND element_at(lm, coalesce(json_extract_scalar(c, '$.code'), '')) IS NOT NULL),
                              c -> json_extract_scalar(c, '$.code')), 1) AS item
  FROM o0
),
o2 AS (
  SELECT *, element_at(lm, item) AS loinc, element_at(ldisp, item) AS loinc_disp FROM o1
),
b_obs AS (
  SELECT source_file, line,
    map_filter(MAP(ARRAY['code', 'valueQuantity', 'category'], ARRAY[
      IF(item IS NULL
         OR any_match(cc, c -> json_extract_scalar(c, '$.system') = 'http://loinc.org' AND json_extract_scalar(c, '$.code') = loinc), NULL,
         CAST(map_concat(CAST(json_extract(line, '$.code') AS MAP(VARCHAR, JSON)),
              MAP(ARRAY['coding'], ARRAY[CAST(cc || CAST(MAP(ARRAY['system', 'code', 'display'],
                  ARRAY[CAST('http://loinc.org' AS JSON), CAST(loinc AS JSON), CAST(loinc_disp AS JSON)]) AS JSON) AS JSON)])) AS JSON)),
      -- MIMIC codes mg/dL in its own unit system; CMS's FHIRHelpers.ToQuantity accepts only UCUM
      IF(item IS NULL OR q IS NULL OR coalesce(json_extract_scalar(q, '$.code'), '') <> 'mg/dL'
         OR coalesce(json_extract_scalar(q, '$.system'), '') = 'http://unitsofmeasure.org', NULL,
         CAST(map_concat(CAST(q AS MAP(VARCHAR, JSON)),
              MAP(ARRAY['system'], ARRAY[CAST('http://unitsofmeasure.org' AS JSON)])) AS JSON)),
      -- ICU charting uses a MIMIC "Labs" category; add the standard laboratory category
      IF(item IS NULL OR source_file <> 'MimicObservationChartevents'
         OR any_match(cats, cat -> any_match(coalesce(CAST(json_extract(cat, '$.coding') AS ARRAY(JSON)), CAST(ARRAY[] AS ARRAY(JSON))),
                                             cc2 -> json_extract_scalar(cc2, '$.system') = 'http://terminology.hl7.org/CodeSystem/observation-category'
                                                AND json_extract_scalar(cc2, '$.code') = 'laboratory')), NULL,
         CAST(cats || json_parse('{"coding":[{"system":"http://terminology.hl7.org/CodeSystem/observation-category","code":"laboratory","display":"Laboratory"}]}') AS JSON))
    ]), (k, v) -> v IS NOT NULL) AS patch,
    IF(item IS NULL, NULL, 'http://hl7.org/fhir/us/qicore/StructureDefinition/qicore-observation-lab') AS add_profile
  FROM o2
),

-- ---------- MedicationAdministration: RxNorm codings from med_map
m0 AS (
  SELECT s.source_file, s.line,
         json_extract(s.line, '$.medicationCodeableConcept') AS mcc,
         coalesce(CAST(json_extract(s.line, '$.medicationCodeableConcept.coding') AS ARRAY(JSON)), CAST(ARRAY[] AS ARRAY(JSON))) AS mc,
         CAST(json_parse(mm.codings_json) AS ARRAY(JSON)) AS add_c
  FROM s LEFT JOIN med_map mm ON mm.ma_id = s.id
  WHERE s.source_file IN ('MimicMedicationAdministration', 'MimicMedicationAdministrationICU')
),
m1 AS (
  SELECT *, (mcc IS NOT NULL AND add_c IS NOT NULL AND cardinality(add_c) > 0) AS hit FROM m0
),
b_ma AS (
  SELECT source_file, line,
    map_filter(MAP(ARRAY['medicationCodeableConcept'], ARRAY[
      IF(NOT hit, NULL,
         CAST(map_concat(CAST(mcc AS MAP(VARCHAR, JSON)),
              MAP(ARRAY['coding'], ARRAY[CAST(mc || filter(add_c, a -> NOT any_match(mc, c ->
                    json_extract_scalar(c, '$.system') = json_extract_scalar(a, '$.system')
                AND json_extract_scalar(c, '$.code') = json_extract_scalar(a, '$.code'))) AS JSON)])) AS JSON))
    ]), (k, v) -> v IS NOT NULL) AS patch,
    IF(NOT hit, NULL, 'http://hl7.org/fhir/us/qicore/StructureDefinition/qicore-medicationadministration') AS add_profile
  FROM m1
),

-- ---------- everything else: unchanged
b_other AS (
  SELECT source_file, line,
         map_filter(MAP(ARRAY['_'], ARRAY[CAST(NULL AS JSON)]), (k, v) -> v IS NOT NULL) AS patch,
         CAST(NULL AS varchar) AS add_profile
  FROM s
  WHERE source_file NOT IN ('MimicPatient', 'MimicEncounter', 'MimicEncounterED', 'MimicCondition', 'MimicConditionED',
                            'MimicObservationLabevents', 'MimicObservationChartevents',
                            'MimicMedicationAdministration', 'MimicMedicationAdministrationICU')
),

allb AS (
  SELECT * FROM b_patient UNION ALL SELECT * FROM b_enc UNION ALL SELECT * FROM b_ed UNION ALL
  SELECT * FROM b_cond UNION ALL SELECT * FROM b_obs UNION ALL SELECT * FROM b_ma UNION ALL SELECT * FROM b_other
),
outl AS (
  SELECT source_file,
         CASE WHEN add_profile IS NULL AND cardinality(patch) = 0 THEN line
              ELSE json_format(CAST(map_concat(
                     CAST(json_parse(line) AS MAP(VARCHAR, JSON)),
                     patch,
                     map_filter(MAP(ARRAY['meta'], ARRAY[IF(add_profile IS NULL, NULL,
                       CAST(map_concat(CAST(coalesce(json_extract(line, '$.meta'), json_parse('{}')) AS MAP(VARCHAR, JSON)),
                            MAP(ARRAY['profile'], ARRAY[CAST(
                              IF(contains(coalesce(CAST(json_extract(line, '$.meta.profile') AS ARRAY(VARCHAR)), CAST(ARRAY[] AS ARRAY(VARCHAR))), add_profile),
                                 coalesce(CAST(json_extract(line, '$.meta.profile') AS ARRAY(VARCHAR)), CAST(ARRAY[] AS ARRAY(VARCHAR))),
                                 coalesce(CAST(json_extract(line, '$.meta.profile') AS ARRAY(VARCHAR)), CAST(ARRAY[] AS ARRAY(VARCHAR))) || add_profile)
                              AS JSON)])) AS JSON))]), (k, v) -> v IS NOT NULL)
                   ) AS JSON)) END AS line,
         NOT (add_profile IS NULL AND cardinality(patch) = 0) AS changed
  FROM allb
),
typed AS (
  SELECT o.source_file, json_extract_scalar(o.line, '$.resourceType') AS rtype, json_extract_scalar(o.line, '$.id') AS id,
         o.line, o.changed, vs.m AS vsm
  FROM outl o CROSS JOIN vs
),
flagged AS (
  SELECT source_file, rtype, id, line, changed,
         CASE
           WHEN rtype IN ('Patient', 'Encounter', 'Medication', 'Location', 'Organization') THEN true
           WHEN rtype IN ('Observation', 'Condition', 'Procedure', 'ServiceRequest') THEN
             any_match(coalesce(CAST(json_extract(line, '$.code.coding') AS ARRAY(JSON)), CAST(ARRAY[] AS ARRAY(JSON))),
                       c -> element_at(vsm, coalesce(json_extract_scalar(c, '$.system'), '') || '|' || coalesce(json_extract_scalar(c, '$.code'), '')) IS NOT NULL)
           WHEN rtype = 'MedicationAdministration' THEN
             json_extract(line, '$.medicationReference') IS NOT NULL
             OR any_match(coalesce(CAST(json_extract(line, '$.medicationCodeableConcept.coding') AS ARRAY(JSON)), CAST(ARRAY[] AS ARRAY(JSON))),
                          c -> element_at(vsm, coalesce(json_extract_scalar(c, '$.system'), '') || '|' || coalesce(json_extract_scalar(c, '$.code'), '')) IS NOT NULL)
           ELSE false END AS keep
  FROM typed
)
SELECT source_file, rtype, id, line, changed, keep
FROM flagged
WHERE {keep_filter};
