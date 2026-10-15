-- =============================================================================
-- prep_40_manifest.sql — the prepare stage prepare: one row per hospital stay with its slice hashes, measurement year,
-- worker shard and the pre-registered workload metric (registration §4.3, S2 "Workload").
--   CMS816 work = G x M816      G    = glucose results in the slice ("Glucose Lab Test Mass Per Volume")
--                               M816 = administrations in the slice with a CMS816 hypoglycemic code
--   CMS871 work = (3d + 1) x G + M871
--                               d    = evaluated hospital days: whole 24-hour periods in
--                                      [start of hospitalization, min(end, start + 10 days)], so at most 10
--                                      (same day rule as the independent check V, which agrees with the
--                                      engine on eligible days; the registration text says "at most 11")
--                               M871 = administrations with a CMS871 treatment-medication code
--   Start of hospitalization = the slice's lower bound h_lo (never later than CMS's true start).
--   "In scope" = the stay's filtered slice, as registered for the scaled pipeline.
-- shard = patient-level hash bucket ({shards} buckets): all stays of a patient go to the same worker.
-- mp_year = calendar year of the recorded discharge (the measurement period each stay is run in).
-- =============================================================================

CREATE TABLE stay_manifest
WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{loc}/stay_manifest/') AS
WITH sc AS (
  SELECT DISTINCT s.unit_id, s.rtype, s.id, v.oid
  FROM slice s
  JOIN res_codes c ON c.rtype = s.rtype AND c.id = s.id
  JOIN vs_members v ON v.system = c.system AND v.code = c.code
  WHERE s.rtype IN ('Observation', 'MedicationAdministration')
    AND v.oid IN ('2.16.840.1.113762.1.4.1248.34', '2.16.840.1.113762.1.4.1196.393', '2.16.840.1.113762.1.4.1196.394')
),
w AS (
  SELECT unit_id,
         count(DISTINCT IF(rtype = 'Observation' AND oid = '2.16.840.1.113762.1.4.1248.34', id)) AS g,
         count(DISTINCT IF(rtype = 'MedicationAdministration' AND oid = '2.16.840.1.113762.1.4.1196.393', id)) AS m816,
         count(DISTINCT IF(rtype = 'MedicationAdministration' AND oid = '2.16.840.1.113762.1.4.1196.394', id)) AS m871
  FROM sc GROUP BY unit_id
),
base AS (
  SELECT b.unit_id, b.patient_id,
         mod(mod(from_big_endian_64(xxhash64(to_utf8(b.patient_id))), {shards}) + {shards}, {shards}) AS shard,
         substr(json_extract_scalar(r.json, '$.period.end'), 1, 4) AS mp_year,
         b.enc_start, b.enc_end, b.h_lo, b.w_lo, b.w_hi,
         coalesce(h.n_resources, 0) AS n_resources, h.slice_sha256, h.content_sha256,
         coalesce(w.g, 0) AS g_glucose, coalesce(w.m816, 0) AS m_cms816, coalesce(w.m871, 0) AS m_cms871,
         greatest(0, least(10, CAST(floor(date_diff('second', b.h_lo, b.enc_end) / 86400.0) AS integer))) AS d_days
  FROM unit_bound b
  LEFT JOIN slice_hash h ON h.unit_id = b.unit_id
  LEFT JOIN w ON w.unit_id = b.unit_id
  JOIN res r ON r.rtype = 'Encounter' AND r.id = b.unit_id
)
SELECT *,
       g_glucose * m_cms816 AS work_cms816,
       (3 * coalesce(d_days, 0) + 1) * g_glucose + m_cms871 AS work_cms871
FROM base;
