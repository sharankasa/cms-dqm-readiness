-- =============================================================================
-- 20_reference.sql — the pilot/the equivalence run: the sample's patients, and (after the the prepare stage mapping SQL has run with
-- {keep_filter} = TRUE restricted to them) their resources keyed by patient.
-- The UNMODIFIED reference evaluation reads each sampled patient's FULL, UNFILTERED, mapped record
-- (same mapping as the scaled side, no data-requirements filter, no slicing) plus every resource
-- without a patient (Medication, Location, Organization), as in the demo equivalence check.
-- Run in two parts by sample_run.py: part 1 before the mapping, part 2 after.
-- =============================================================================

-- part 1
CREATE TABLE sample_patients
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/sample_patients/') AS
SELECT patient_id, min(shard) AS shard, count(*) AS sampled_stays
FROM sample_stays GROUP BY patient_id;

-- part 2
CREATE TABLE ref_lines
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/ref_lines/') AS
SELECT CASE WHEN rtype = 'Patient' THEN id
            ELSE regexp_extract(coalesce(json_extract_scalar(line, '$.subject.reference'),
                                         json_extract_scalar(line, '$.patient.reference'),
                                         json_extract_scalar(line, '$.beneficiary.reference')), '^Patient/(.+)$', 1) END AS patient_id,
       source_file, rtype, id, line
FROM mapped;
