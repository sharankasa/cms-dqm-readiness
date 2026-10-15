-- =============================================================================
-- 30_filter_hash.sql — PORTABLE CORE: optional data-requirements filter, then a hash per stay.
--
-- Filter (when the driver runs it; otherwise slice = members). From the two measures'
-- Library.dataRequirement:
--   * types retrieved: Patient, Encounter, Observation, MedicationAdministration, Medication,
--     Condition, Procedure, ServiceRequest. Everything else is dropped.
--   * coded retrieves (Observation, Condition, Procedure, ServiceRequest on `code`): keep only
--     records with a coding in the measures' value sets; a coded retrieve cannot return others.
--   * MedicationAdministration (coded on medication[x]): keep if its drug coding is in a value set,
--     or if it names the drug by reference (cannot be judged from its own record; the engine
--     resolves it through Medication).
--   * Medication: keep if referenced by a kept MedicationAdministration of the same slice.
--   * Patient, Encounter: all (Encounter also has uncoded retrieves).
-- vs_codes(system, code) is loaded by the driver from the measure bundles + the local VSAC cache.
-- Same idea as the speed filter validated in the preliminary work gate (run_fqm_demo.js), extended to
-- every coded retrieve. Sliced + filtered must equal unsliced + unfiltered (the demo equivalence check, S1).
--
-- Hashes per stay, compared across backends in the two-backend slicing check:
--   slice_sha256   SHA-256 over the sorted "Type/id" keys: which resources are in the slice;
--   content_sha256 SHA-256 over the sorted "Type/id:sha256(raw JSON line)": the exact bytes the
--                  engine will read (catches any backend that re-serializes the JSON).
-- Written with list()/array_to_string() so SQLGlot translates it to ARRAY_AGG/ARRAY_JOIN.
-- =============================================================================

CREATE TABLE slice AS
SELECT m.unit_id, m.rtype, m.id
FROM members m
WHERE {filter_off}
   OR m.rtype IN ('Patient', 'Encounter')
   OR (m.rtype IN ('Observation', 'Condition', 'Procedure', 'ServiceRequest') AND EXISTS (
         SELECT 1 FROM res_codes c JOIN vs_codes v ON v.system = c.system AND v.code = c.code
         WHERE c.rtype = m.rtype AND c.id = m.id))
   OR (m.rtype = 'MedicationAdministration' AND (
         EXISTS (SELECT 1 FROM res_index i WHERE i.rtype = m.rtype AND i.id = m.id AND i.med_ref_id IS NOT NULL)
         OR EXISTS (SELECT 1 FROM res_codes c JOIN vs_codes v ON v.system = c.system AND v.code = c.code
                    WHERE c.rtype = m.rtype AND c.id = m.id)))
   OR (m.rtype = 'Medication' AND EXISTS (
         SELECT 1 FROM members m2 JOIN res_index i2 ON i2.rtype = m2.rtype AND i2.id = m2.id
         WHERE m2.unit_id = m.unit_id AND m2.rtype = 'MedicationAdministration' AND i2.med_ref_id = m.id));

CREATE TABLE slice_hash AS
SELECT s.unit_id,
       count(*) AS n_resources,
       lower(sha256(array_to_string(list(s.rtype || '/' || s.id ORDER BY s.rtype || '/' || s.id), chr(10)))) AS slice_sha256,
       lower(sha256(array_to_string(list(s.rtype || '/' || s.id || ':' || lower(sha256(r.json))
                                         ORDER BY s.rtype || '/' || s.id), chr(10)))) AS content_sha256
FROM slice s
JOIN res r ON r.rtype = s.rtype AND r.id = s.id
GROUP BY s.unit_id;
