-- =============================================================================
-- 10_load_index.sql — BACKEND ADAPTER (Athena engine v3 / Trino). Hand-written counterpart of
-- ../duckdb/10_load_index.sql: must produce the same tables with the same values.
--
--   raw_ndjson (line)                                     every NDJSON line, raw text (TEXTFILE)
--   vs_codes   (system, code)                             value-set codes (TSV uploaded by the runner)
--   res        (rtype, id, json)                          json = the raw line, byte for byte
--   res_times  (rtype, id, t)
--   res_index  (rtype, id, patient_id, encounter_id, part_of_id, med_ref_id, is_unit, t_start, t_end)
--   res_codes  (rtype, id, system, code)
--
-- Run by athena_run.py in the query context of database {db}. Every CTAS writes to a fresh run
-- prefix ({loc}/<table>/), so nothing is overwritten and nothing needs deleting.
-- Times: UTC timestamps, same rules as DuckDB (partial dates widened by +/- 14 h).
-- Placeholders: {data_location}, {loc}, {unit_identifier_system}.
-- =============================================================================

CREATE EXTERNAL TABLE raw_ndjson (line string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '{data_location}';

CREATE EXTERNAL TABLE vs_codes (system string, code string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '{loc}/vs_codes/';

CREATE TABLE res
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/res/') AS
SELECT json_extract_scalar(line, '$.resourceType') AS rtype,
       json_extract_scalar(line, '$.id')           AS id,
       line                                        AS json
FROM raw_ndjson
WHERE trim(line) <> '';

CREATE TABLE res_times
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/res_times/') AS
SELECT r.rtype, r.id, u.t
FROM res r
CROSS JOIN UNNEST(ARRAY[
    json_extract_scalar(r.json, '$.period.start'),               json_extract_scalar(r.json, '$.period.end'),
    json_extract_scalar(r.json, '$.effectiveDateTime'),          json_extract_scalar(r.json, '$.effectiveInstant'),
    json_extract_scalar(r.json, '$.effectivePeriod.start'),      json_extract_scalar(r.json, '$.effectivePeriod.end'),
    json_extract_scalar(r.json, '$.performedDateTime'),
    json_extract_scalar(r.json, '$.performedPeriod.start'),      json_extract_scalar(r.json, '$.performedPeriod.end'),
    json_extract_scalar(r.json, '$.authoredOn'),
    json_extract_scalar(r.json, '$.occurrenceDateTime'),
    json_extract_scalar(r.json, '$.occurrencePeriod.start'),     json_extract_scalar(r.json, '$.occurrencePeriod.end'),
    json_extract_scalar(r.json, '$.whenPrepared'),               json_extract_scalar(r.json, '$.whenHandedOver'),
    json_extract_scalar(r.json, '$.dateAsserted'),
    json_extract_scalar(r.json, '$.collection.collectedDateTime'),
    json_extract_scalar(r.json, '$.collection.collectedPeriod.start'),
    json_extract_scalar(r.json, '$.collection.collectedPeriod.end')
  ]) AS u(t)
WHERE r.rtype NOT IN ('Patient', 'Condition', 'Coverage', 'Location', 'Organization', 'Medication')
  AND u.t IS NOT NULL;

CREATE TABLE res_index
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/res_index/') AS
WITH tt AS (
  SELECT rtype, id,
    min(CASE
      WHEN length(t) = 4  THEN CAST(from_iso8601_date(t || '-01-01') AS timestamp) - INTERVAL '14' HOUR
      WHEN length(t) = 7  THEN CAST(from_iso8601_date(t || '-01') AS timestamp) - INTERVAL '14' HOUR
      WHEN length(t) = 10 THEN CAST(from_iso8601_date(t) AS timestamp) - INTERVAL '14' HOUR
      ELSE CAST(from_iso8601_timestamp(t) AT TIME ZONE 'UTC' AS timestamp) END) AS t_start,
    max(CASE
      WHEN length(t) = 4  THEN CAST(from_iso8601_date(t || '-01-01') AS timestamp) + INTERVAL '1' YEAR + INTERVAL '14' HOUR
      WHEN length(t) = 7  THEN CAST(from_iso8601_date(t || '-01') AS timestamp) + INTERVAL '1' MONTH + INTERVAL '14' HOUR
      WHEN length(t) = 10 THEN CAST(from_iso8601_date(t) AS timestamp) + INTERVAL '1' DAY + INTERVAL '14' HOUR
      ELSE CAST(from_iso8601_timestamp(t) AT TIME ZONE 'UTC' AS timestamp) END) AS t_end
  FROM res_times
  GROUP BY rtype, id
),
refs AS (
  SELECT rtype, id, json,
         json_extract_scalar(json, '$.subject.reference')             AS subj,
         json_extract_scalar(json, '$.patient.reference')             AS pat,
         json_extract_scalar(json, '$.beneficiary.reference')         AS ben,
         json_extract_scalar(json, '$.encounter.reference')           AS enc,
         json_extract_scalar(json, '$.context.reference')             AS ctx,
         json_extract_scalar(json, '$.partOf.reference')              AS partof,
         json_extract_scalar(json, '$.medicationReference.reference') AS medref
  FROM res
)
SELECT r.rtype, r.id,
       CASE WHEN r.rtype = 'Patient' THEN r.id
            ELSE coalesce(CASE WHEN starts_with(r.subj, 'Patient/') THEN substr(r.subj, 9) END,
                          CASE WHEN starts_with(r.pat,  'Patient/') THEN substr(r.pat, 9) END,
                          CASE WHEN starts_with(r.ben,  'Patient/') THEN substr(r.ben, 9) END) END      AS patient_id,
       coalesce(CASE WHEN starts_with(r.enc, 'Encounter/') THEN substr(r.enc, 11) END,
                CASE WHEN starts_with(r.ctx, 'Encounter/') THEN substr(r.ctx, 11) END)                AS encounter_id,
       CASE WHEN starts_with(r.partof, 'Encounter/') THEN substr(r.partof, 11) END                    AS part_of_id,
       CASE WHEN starts_with(r.medref, 'Medication/') THEN substr(r.medref, 12) END                   AS med_ref_id,
       (r.rtype = 'Encounter'
        AND ('{unit_identifier_system}' = 'ALL'
             OR coalesce(any_match(CAST(json_extract(r.json, '$.identifier') AS ARRAY(JSON)),
                                   x -> json_extract_scalar(x, '$.system') = '{unit_identifier_system}'), false))) AS is_unit,
       tt.t_start, tt.t_end
FROM refs r
LEFT JOIN tt ON tt.rtype = r.rtype AND tt.id = r.id;

CREATE TABLE res_codes
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/res_codes/') AS
SELECT r.rtype, r.id,
       json_extract_scalar(u.c, '$.system') AS system,
       json_extract_scalar(u.c, '$.code')   AS code
FROM res r
CROSS JOIN UNNEST(CAST(coalesce(json_extract(r.json, '$.code.coding'),
                                json_extract(r.json, '$.medicationCodeableConcept.coding')) AS ARRAY(JSON))) AS u(c)
WHERE r.rtype IN ('Observation', 'Condition', 'Procedure', 'ServiceRequest', 'MedicationAdministration');
