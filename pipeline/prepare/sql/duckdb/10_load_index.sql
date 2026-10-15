-- =============================================================================
-- 10_load_index.sql — BACKEND ADAPTER (DuckDB). Loads FHIR NDJSON and builds the typed index the
-- portable core (../core/*.sql) works on. The Athena adapter (the two-backend slicing check) must produce the same three
-- tables with the same columns and the same values:
--
--   res        (rtype, id, json)                          one row per resource, raw JSON text
--   res_index  (rtype, id, patient_id, encounter_id, part_of_id, med_ref_id, is_unit, t_start, t_end)
--   res_codes  (rtype, id, system, code)                  codings used by the optional filter
--
-- Times: every FHIR dateTime is converted to a UTC timestamp. For each resource, t_start/t_end are
-- the earliest/latest of ALL its known time fields (a superset of what any measure reads).
-- Partial dates (YYYY, YYYY-MM, YYYY-MM-DD) carry no time zone: widened to the whole period
-- +/- 14 h (the widest UTC offsets), so the slice is a superset whatever the zone.
-- Units (one slice each) = hospital stays: Encounters carrying an identifier with system
-- {unit_identifier_system} (MIMIC: .../identifier/encounter-hosp, i.e. one per hadm_id). This rule
-- is data-source specific; a hospital sets its own (e.g. its inpatient encounter identifier).
-- 'ALL' makes every Encounter a unit (used for CMS's test cases, which carry no such identifier).
-- Placeholders: {input_glob}, {unit_identifier_system} (set by the driver).
-- =============================================================================

SET TimeZone = 'UTC';

CREATE OR REPLACE MACRO fhir_lo(s) AS CASE
  WHEN s IS NULL THEN NULL
  WHEN length(s) = 4  THEN CAST(s || '-01-01' AS TIMESTAMP) - INTERVAL 14 HOUR
  WHEN length(s) = 7  THEN CAST(s || '-01' AS TIMESTAMP) - INTERVAL 14 HOUR
  WHEN length(s) = 10 THEN CAST(s AS TIMESTAMP) - INTERVAL 14 HOUR
  ELSE CAST(CAST(s AS TIMESTAMPTZ) AS TIMESTAMP) END;

CREATE OR REPLACE MACRO fhir_hi(s) AS CASE
  WHEN s IS NULL THEN NULL
  WHEN length(s) = 4  THEN CAST(s || '-01-01' AS TIMESTAMP) + INTERVAL 1 YEAR + INTERVAL 14 HOUR
  WHEN length(s) = 7  THEN CAST(s || '-01' AS TIMESTAMP) + INTERVAL 1 MONTH + INTERVAL 14 HOUR
  WHEN length(s) = 10 THEN CAST(s AS TIMESTAMP) + INTERVAL 1 DAY + INTERVAL 14 HOUR
  ELSE CAST(CAST(s AS TIMESTAMPTZ) AS TIMESTAMP) END;

CREATE OR REPLACE MACRO ref_id(r, prefix) AS
  CASE WHEN starts_with(r, prefix || '/') THEN substr(r, length(prefix) + 2) END;

CREATE OR REPLACE TABLE res AS
SELECT json_extract_string(json, '$.resourceType') AS rtype,
       json_extract_string(json, '$.id')           AS id,
       CAST(json AS VARCHAR)                        AS json
FROM read_ndjson_objects('{input_glob}');

-- All time fields of the resource types in MIMIC-IV-on-FHIR that carry time (FHIR R4 paths).
CREATE OR REPLACE TABLE res_times AS
SELECT rtype, id, t FROM (
  SELECT rtype, id, unnest([
    json_extract_string(json, '$.period.start'),               json_extract_string(json, '$.period.end'),
    json_extract_string(json, '$.effectiveDateTime'),          json_extract_string(json, '$.effectiveInstant'),
    json_extract_string(json, '$.effectivePeriod.start'),      json_extract_string(json, '$.effectivePeriod.end'),
    json_extract_string(json, '$.performedDateTime'),
    json_extract_string(json, '$.performedPeriod.start'),      json_extract_string(json, '$.performedPeriod.end'),
    json_extract_string(json, '$.authoredOn'),
    json_extract_string(json, '$.occurrenceDateTime'),
    json_extract_string(json, '$.occurrencePeriod.start'),     json_extract_string(json, '$.occurrencePeriod.end'),
    json_extract_string(json, '$.whenPrepared'),               json_extract_string(json, '$.whenHandedOver'),
    json_extract_string(json, '$.dateAsserted'),
    json_extract_string(json, '$.collection.collectedDateTime'),
    json_extract_string(json, '$.collection.collectedPeriod.start'),
    json_extract_string(json, '$.collection.collectedPeriod.end')
  ]) AS t
  FROM res
  WHERE rtype NOT IN ('Patient', 'Condition', 'Coverage', 'Location', 'Organization', 'Medication')
) WHERE t IS NOT NULL;

CREATE OR REPLACE TABLE res_index AS
SELECT r.rtype, r.id,
       CASE WHEN r.rtype = 'Patient' THEN r.id
            ELSE coalesce(ref_id(json_extract_string(r.json, '$.subject.reference'), 'Patient'),
                          ref_id(json_extract_string(r.json, '$.patient.reference'), 'Patient'),
                          ref_id(json_extract_string(r.json, '$.beneficiary.reference'), 'Patient')) END AS patient_id,
       coalesce(ref_id(json_extract_string(r.json, '$.encounter.reference'), 'Encounter'),
                ref_id(json_extract_string(r.json, '$.context.reference'), 'Encounter'))         AS encounter_id,
       ref_id(json_extract_string(r.json, '$.partOf.reference'), 'Encounter')                    AS part_of_id,
       ref_id(json_extract_string(r.json, '$.medicationReference.reference'), 'Medication')      AS med_ref_id,
       (r.rtype = 'Encounter'
        AND ('{unit_identifier_system}' = 'ALL'
             OR list_contains(json_extract_string(r.json, '$.identifier[*].system'), '{unit_identifier_system}'))) AS is_unit,
       t.t_start, t.t_end
FROM res r
LEFT JOIN (SELECT rtype, id, min(fhir_lo(t)) AS t_start, max(fhir_hi(t)) AS t_end
           FROM res_times GROUP BY rtype, id) t
  ON t.rtype = r.rtype AND t.id = r.id;

-- Codings for the optional data-requirements filter: the primary code path of each coded retrieve
-- in the two measures (Library.dataRequirement codeFilter: Observation/Condition/Procedure/
-- ServiceRequest.code, MedicationAdministration.medication[x]).
CREATE OR REPLACE TABLE res_codes AS
SELECT rtype, id,
       json_extract_string(c, '$.system') AS system,
       json_extract_string(c, '$.code')   AS code
FROM (
  SELECT rtype, id, unnest(CAST(coalesce(json_extract(json, '$.code.coding'),
                                         json_extract(json, '$.medicationCodeableConcept.coding')) AS JSON[])) AS c
  FROM res
  WHERE rtype IN ('Observation', 'Condition', 'Procedure', 'ServiceRequest', 'MedicationAdministration')
);
