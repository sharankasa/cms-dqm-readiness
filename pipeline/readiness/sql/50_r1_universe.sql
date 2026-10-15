-- =============================================================================
-- 50_r1_universe.sql — R1/R2 readiness scenarios: the records the two measures use in the 2017-2019 cohort
-- (corrected baseline = the prepare stage mapping a, {b4db}), each with its role, its baseline JSON and its JSON as
-- exported by MIMIC-IV-on-FHIR (before our mapping), plus the stays whose slices hold it.
--   r1_record_stays  (rtype, id, unit_id)  records of the cohort's slices (Observation, MedicationAdministration,
--                                          Encounter, Condition)
--   r1_universe      (role, rtype, id, j, ma_event, is_emar, dx_encounter)
--       role glucose_result      Observation with a code in Glucose Lab Test Mass Per Volume (both measures)
--            medication_cms816   MedicationAdministration with a code in Hypoglycemics Severe Hypoglycemia
--            medication_cms871   MedicationAdministration with a code in Hypoglycemics Treatment Medications
--            diabetes_diagnosis  Condition with a code in Diabetes (CMS871)
--            encounter           the stay's own hospital Encounter
--       (a record in both medication value sets appears once per role)
--       ma_event   the eMAR event (dosage.method) of eMAR administrations; is_emar: from the eMAR table
--       dx_encounter  the Encounter a Condition belongs to (Condition.encounter)
--   r1_exported      (rtype, id, j)  the same records as exported (raw MIMIC-IV-on-FHIR lines, {raw})
--   r1_generic_brand (generic, brand) RxNorm tradename_of: one current branded product per generic (for the
--                                     term-type injection)
--   status_map       the reviewed eMAR event -> FHIR status table (crosswalk/medication_status_map.csv)
-- Counts only are printed.
-- =============================================================================

-- part build
CREATE EXTERNAL TABLE status_map (
  emar_event_txt string, proposed_fhir_status string, dose_event string, counts_cms816 string, counts_cms871 string,
  confidence string, rationale string, reviewer_status string, reviewer_note string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
STORED AS TEXTFILE LOCATION '{loc}/status_map/'
TBLPROPERTIES ('skip.header.line.count' = '1');

CREATE TABLE r1_record_stays
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/r1_record_stays/') AS
SELECT DISTINCT s.rtype, s.id, s.unit_id
FROM {b4db}.slice s
JOIN dqm_cohort.stay_years c ON c.encounter_id = s.unit_id
WHERE c.cohort_group = '1 primary 2017-2019'
  AND s.rtype IN ('Observation', 'MedicationAdministration', 'Encounter', 'Condition');

CREATE TABLE r1_universe
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/r1_universe/') AS
WITH recs AS (
  SELECT DISTINCT rtype, id FROM r1_record_stays
),
roles AS (
  SELECT DISTINCT r.rtype, r.id,
         CASE v.oid WHEN '2.16.840.1.113762.1.4.1248.34' THEN 'glucose_result'
                    WHEN '2.16.840.1.113762.1.4.1196.393' THEN 'medication_cms816'
                    WHEN '2.16.840.1.113762.1.4.1196.394' THEN 'medication_cms871'
                    ELSE 'diabetes_diagnosis' END AS role
  FROM recs r
  JOIN {b4db}.res_codes c ON c.rtype = r.rtype AND c.id = r.id
  JOIN {b4db}.vs_members v ON v.system = c.system AND v.code = c.code
  WHERE (r.rtype = 'Observation' AND v.oid = '2.16.840.1.113762.1.4.1248.34')
     OR (r.rtype = 'MedicationAdministration' AND v.oid IN ('2.16.840.1.113762.1.4.1196.393', '2.16.840.1.113762.1.4.1196.394'))
     OR (r.rtype = 'Condition' AND v.oid = '2.16.840.1.113883.3.464.1003.103.12.1001')
  UNION ALL
  SELECT DISTINCT rtype, id, 'encounter' FROM r1_record_stays WHERE rtype = 'Encounter' AND id = unit_id
)
SELECT ro.role, ro.rtype, ro.id, r.json AS j,
       trim(e.method) AS ma_event, e.ma_id IS NOT NULL AS is_emar,
       regexp_extract(json_extract_scalar(r.json, '$.encounter.reference'), '[^/]+$') AS dx_encounter
FROM roles ro
JOIN {b4db}.res r ON r.rtype = ro.rtype AND r.id = ro.id
LEFT JOIN dqm_map.ma_emar_method e ON ro.rtype = 'MedicationAdministration' AND e.ma_id = ro.id;

CREATE TABLE r1_exported
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/r1_exported/') AS
WITH u AS (SELECT DISTINCT rtype, id FROM r1_universe),
x AS (
  SELECT json_extract_scalar(line, '$.resourceType') AS rtype, json_extract_scalar(line, '$.id') AS id, line
  FROM {raw}
  WHERE source_file IN ('MimicObservationLabevents', 'MimicObservationChartevents', 'MimicMedicationAdministration',
                        'MimicMedicationAdministrationICU', 'MimicEncounter', 'MimicEncounterED', 'MimicCondition', 'MimicConditionED')
)
SELECT u.rtype, u.id, x.line AS j
FROM u JOIN x ON x.rtype = u.rtype AND x.id = u.id;

CREATE TABLE r1_generic_brand
WITH (format = 'PARQUET', external_location = '{loc}/r1_generic_brand/') AS
SELECT rel.rxcui1 AS generic, min(rel.rxcui2) AS brand
FROM dqm_ref.rxnrel_{version} rel
JOIN dqm_ref.rxnconso_{version} b ON b.rxcui = rel.rxcui2 AND b.sab = 'RXNORM' AND b.tty IN ('SBD', 'BPCK') AND b.suppress = 'N'
WHERE rel.sab = 'RXNORM' AND rel.rela = 'tradename_of'
GROUP BY rel.rxcui1;

-- part summary
SELECT u.role, count(*) AS records, count(x.id) AS with_exported_version,
       count_if(u.is_emar) AS emar_records, count_if(u.ma_event IS NOT NULL) AS with_emar_event
FROM r1_universe u LEFT JOIN r1_exported x ON x.rtype = u.rtype AND x.id = u.id
GROUP BY u.role ORDER BY u.role;

SELECT count(DISTINCT unit_id) AS cohort_stays_with_slices, count(*) AS record_stay_pairs FROM r1_record_stays;

SELECT count(*) AS status_table_rows, count_if(reviewer_status = 'agree') AS reviewed_agree FROM status_map;
