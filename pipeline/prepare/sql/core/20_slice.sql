-- =============================================================================
-- 20_slice.sql — PORTABLE CORE: one slice per hospital stay. Written for DuckDB, translated to
-- Athena (Trino) with SQLGlot in the two-backend slicing check; must give identical slices on both (hash per stay).
-- Input: res_index (from the backend adapter). Placeholders: {lookback_hours}, {lookahead_hours}.
--
-- Rule (derived from the CQL of CMS816FHIRHHHypo and CMS871FHIRHHHyper, 2025 content, commit 5f840c8):
--   H = hospitalizationWithObservation(E) = [start of the last finished "Observation Services"
--       encounter ending 1 h or less on or before E's start (else E's start), extended to the
--       start of the last finished "Emergency Department Visit" ending 1 h or less on or before
--       that start; end of E].
--   Data the measures read for a stay, relative to H:
--     CMS816  glucose results during H; hypoglycemic administrations starting during H or up to
--             24 h before a low result in H  -> look-back 24 h before start of H
--     CMS871  glucose results from 1 h before start of H to end of H; administrations during H;
--             comfort-care procedures/requests starting during H; hospice disposition on E;
--             diabetes conditions starting ANY time before end of H -> all Conditions
--   So: window = [lower bound of start of H - {lookback_hours} h, end of E + {lookahead_hours} h].
--
-- The slice is a SUPERSET of what the measures can read (extra resources outside the measures'
-- windows cannot change a stay's result; a missing one can). Hence:
--   * the start of H is bounded from below WITHOUT value sets or status: ANY encounter of the
--     patient (not only ED/observation, not only finished) ending within 1 h before the start
--     counts, in the same two steps as the CQL. The true start of H is never earlier.
--   * every resource whose time interval overlaps the window is kept, plus resources linked to
--     the stay, resources with no time at all, and all Patient / Condition / Coverage resources.
--   * Location and Organization go to one shared file added to every slice; Medications are
--     added when a slice member references them.
-- Other encounters inside the window come along (e.g. a stay ending just before this one); they
-- form their own episodes in the engine and are ignored: only the target stay's result is read.
-- =============================================================================

CREATE TABLE units AS
SELECT id AS unit_id, patient_id, t_start AS enc_start, t_end AS enc_end
FROM res_index
WHERE is_unit AND patient_id IS NOT NULL;

CREATE TABLE unit_bound AS
WITH enc AS (
  SELECT id, patient_id, t_start, t_end FROM res_index WHERE rtype = 'Encounter'
),
step1 AS (   -- stand-in for the observation encounter: any encounter ending <= 1 h before E's start
  SELECT u.unit_id, u.patient_id, u.enc_start, u.enc_end,
         least(u.enc_start, coalesce(min(e.t_start), u.enc_start)) AS v_lo
  FROM units u
  LEFT JOIN enc e
    ON e.patient_id = u.patient_id AND e.id <> u.unit_id
   AND e.t_end >= u.enc_start - INTERVAL '1' HOUR AND e.t_end <= u.enc_start
  GROUP BY u.unit_id, u.patient_id, u.enc_start, u.enc_end
),
step2 AS (   -- stand-in for the ED visit: any encounter ending <= 1 h before the visit start
  SELECT s.unit_id, s.patient_id, s.enc_start, s.enc_end, s.v_lo,
         least(s.v_lo, coalesce(min(e.t_start), s.v_lo)) AS h_lo
  FROM step1 s
  LEFT JOIN enc e
    ON e.patient_id = s.patient_id AND e.id <> s.unit_id
   AND e.t_end >= s.v_lo - INTERVAL '1' HOUR AND e.t_end <= s.enc_start
  GROUP BY s.unit_id, s.patient_id, s.enc_start, s.enc_end, s.v_lo
)
SELECT unit_id, patient_id, enc_start, enc_end, v_lo, h_lo,
       h_lo    - INTERVAL '{lookback_hours}' HOUR  AS w_lo,
       enc_end + INTERVAL '{lookahead_hours}' HOUR AS w_hi
FROM step2;

CREATE TABLE members AS
SELECT DISTINCT b.unit_id, i.rtype, i.id
FROM unit_bound b
JOIN res_index i ON i.patient_id = b.patient_id
WHERE i.rtype IN ('Patient', 'Condition', 'Coverage')
   OR (i.rtype = 'Encounter' AND i.id = b.unit_id)
   OR i.encounter_id = b.unit_id
   OR i.part_of_id = b.unit_id
   OR i.t_start IS NULL
   OR b.w_lo IS NULL OR b.w_hi IS NULL
   OR (i.t_start <= b.w_hi AND i.t_end >= b.w_lo);

-- Medications referenced by slice members (MedicationAdministration.medicationReference etc.)
INSERT INTO members
SELECT DISTINCT m.unit_id, 'Medication', i.med_ref_id
FROM members m
JOIN res_index i ON i.rtype = m.rtype AND i.id = m.id
JOIN res_index med ON med.rtype = 'Medication' AND med.id = i.med_ref_id
WHERE i.med_ref_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM members x
                  WHERE x.unit_id = m.unit_id AND x.rtype = 'Medication' AND x.id = i.med_ref_id);

CREATE TABLE shared AS
SELECT rtype, id FROM res_index WHERE rtype IN ('Location', 'Organization');
