-- =============================================================================
-- 55_r1_checks.sql — R1 readiness checks (registration §4.4), per record, generated from what the measures need
-- (pipeline/readiness/checks/measure_requirements.json: CMS's engine's data requirements + the elements
-- the CQL reads). The src CTE = the records to check: (role, rtype, id, j, injected, ma_event, is_emar,
-- dx_encounter), from r1_universe (baseline), r1_exported (as exported) or an injected copy (r1_run.py).
-- Checks (applies -> flagged):
--   profile           every record: meta.profile lacks the QI-Core profile the measure retrieves by
--   code_mapped       every record: no code in the measure's value set for its role (standard system);
--                     stay encounters: no SNOMED encounter type at all (an observation-typed stay is mapped)
--   info_typed_not_inpatient  (information, not a problem) stay encounters whose type is not in Encounter
--                     Inpatient (MIMIC class OBSENC -> observation services: the measures do not count them)
--   term_type         administrations with an RxNorm code: none of its RxNorm codes has a term type the value
--                     set lists (the value sets list generic SCD/GPCK; a brand or ingredient code never matches)
--   value_comparable  glucose results: no valueQuantity in a UCUM mass-per-volume unit (compared with mg/dL)
--   status_usable     status missing or "unknown" (results, administrations, encounters); diagnoses: neither
--                     clinically active (active/recurrence/relapse) nor abated (prevalence undefined)
--   timing            results: no effective time; CMS816 administrations: no start; CMS871 administrations: no
--                     closed period or time (an open period never lies in the stay); encounters: no start or
--                     end; diagnoses: no onset
--   subject           no reference to a Patient
--   diagnosis_link    stay encounters with a diabetes diagnosis recorded for them: the encounter references
--                     none of those diagnoses (reasonReference / diagnosis)
--   status_has_event  eMAR administrations: no recorded eMAR event to confirm the status (MIMIC-specific)
--   status_matches_event  eMAR administrations with an event: status differs from the reviewed event -> status
--                     table (e.g. "Not Given" recorded as completed)
-- Output: per check and role: records it applies to, flagged, injected, flagged and injected.
-- =============================================================================

WITH src AS (
  {src}
),
vs AS (
  SELECT oid, system, code FROM {b4db}.vs_members
  WHERE oid IN ('2.16.840.1.113762.1.4.1248.34', '2.16.840.1.113762.1.4.1196.393', '2.16.840.1.113762.1.4.1196.394',
                '2.16.840.1.113883.3.464.1003.103.12.1001', '2.16.840.1.113883.3.666.5.307')
),
role_vs AS (
  SELECT * FROM (VALUES ('glucose_result', '2.16.840.1.113762.1.4.1248.34'),
                        ('medication_cms816', '2.16.840.1.113762.1.4.1196.393'),
                        ('medication_cms871', '2.16.840.1.113762.1.4.1196.394'),
                        ('diabetes_diagnosis', '2.16.840.1.113883.3.464.1003.103.12.1001'),
                        ('encounter', '2.16.840.1.113883.3.666.5.307')) AS t(role, oid)
),
codings AS (
  SELECT s.role, s.rtype, s.id, json_extract_scalar(u.c, '$.system') AS system, json_extract_scalar(u.c, '$.code') AS code
  FROM src s
  CROSS JOIN UNNEST(
    IF(s.rtype = 'Encounter',
       flatten(transform(CAST(coalesce(json_extract(s.j, '$.type'), json_parse('[]')) AS ARRAY(JSON)),
                         t -> CAST(coalesce(json_extract(t, '$.coding'), json_parse('[]')) AS ARRAY(JSON)))),
       CAST(coalesce(json_extract(s.j, '$.code.coding'), json_extract(s.j, '$.medicationCodeableConcept.coding'),
                     json_parse('[]')) AS ARRAY(JSON)))) AS u(c)
),
in_vs AS (
  SELECT DISTINCT c.role, c.rtype, c.id
  FROM codings c JOIN role_vs r ON r.role = c.role JOIN vs v ON v.oid = r.oid AND v.system = c.system AND v.code = c.code
),
vs_tty AS (
  SELECT DISTINCT r.role, x.tty
  FROM role_vs r JOIN vs v ON v.oid = r.oid
  JOIN dqm_ref.rxnconso_{version} x ON x.rxcui = v.code AND x.sab = 'RXNORM'
  WHERE r.role LIKE 'medication%' AND x.tty IN ('SCD', 'SBD', 'GPCK', 'BPCK', 'IN', 'PIN', 'MIN', 'SCDC', 'SBDC', 'BN', 'SCDF', 'SBDF', 'SCDG', 'SBDG')
),
enc_sct AS (
  SELECT DISTINCT role, rtype, id FROM codings WHERE role = 'encounter' AND system = 'http://snomed.info/sct'
),
rx AS (
  SELECT c.role, c.rtype, c.id, bool_or(t.tty IS NOT NULL) AS tty_ok
  FROM codings c
  LEFT JOIN dqm_ref.rxnconso_{version} x ON x.rxcui = c.code AND x.sab = 'RXNORM'
       AND x.tty IN ('SCD', 'SBD', 'GPCK', 'BPCK', 'IN', 'PIN', 'MIN', 'SCDC', 'SBDC', 'BN', 'SCDF', 'SBDF', 'SCDG', 'SBDG')
  LEFT JOIN vs_tty t ON t.role = c.role AND t.tty = x.tty
  WHERE c.role LIKE 'medication%' AND c.system = 'http://www.nlm.nih.gov/research/umls/rxnorm'
  GROUP BY 1, 2, 3
),
dxl AS (
  SELECT dx_encounter AS enc_id, array_agg(DISTINCT 'Condition/' || id) AS dx_refs
  FROM src WHERE role = 'diabetes_diagnosis' AND dx_encounter IS NOT NULL GROUP BY 1
),
rec AS (
  SELECT s.role, s.rtype, s.id, s.injected, s.is_emar, s.ma_event,
         iv.id IS NOT NULL AS has_vs_code, es.id IS NOT NULL AS has_sct_type, rx.id IS NOT NULL AS has_rx, coalesce(rx.tty_ok, false) AS tty_ok,
         coalesce(try(CAST(json_extract(s.j, '$.meta.profile') AS ARRAY(VARCHAR))), CAST(ARRAY[] AS ARRAY(VARCHAR))) AS profiles,
         json_extract_scalar(s.j, '$.status') AS status,
         json_extract_scalar(s.j, '$.valueQuantity.system') AS q_system, json_extract_scalar(s.j, '$.valueQuantity.code') AS q_code,
         json_extract_scalar(s.j, '$.valueQuantity.value') AS q_value,
         coalesce(json_extract_scalar(s.j, '$.effectiveDateTime'), json_extract_scalar(s.j, '$.effectiveInstant')) AS eff_time,
         json_extract_scalar(s.j, '$.effectivePeriod.start') AS eff_start, json_extract_scalar(s.j, '$.effectivePeriod.end') AS eff_end,
         json_extract_scalar(s.j, '$.period.start') AS p_start, json_extract_scalar(s.j, '$.period.end') AS p_end,
         coalesce(json_extract_scalar(s.j, '$.onsetDateTime'), json_extract_scalar(s.j, '$.onsetPeriod.start'),
                  json_extract_scalar(s.j, '$.onsetString'), json_extract_scalar(s.j, '$.onsetAge.value')) AS onset,
         json_extract_scalar(s.j, '$.clinicalStatus.coding[0].code') AS clinical_status,
         coalesce(json_extract_scalar(s.j, '$.abatementDateTime'), json_extract_scalar(s.j, '$.abatementPeriod.start'),
                  json_extract_scalar(s.j, '$.abatementString')) AS abatement,
         coalesce(json_extract_scalar(s.j, '$.subject.reference'), json_extract_scalar(s.j, '$.patient.reference'), '') AS subj,
         filter(transform(CAST(coalesce(json_extract(s.j, '$.reasonReference'), json_parse('[]')) AS ARRAY(JSON)), x -> json_extract_scalar(x, '$.reference'))
                || transform(CAST(coalesce(json_extract(s.j, '$.diagnosis'), json_parse('[]')) AS ARRAY(JSON)), x -> json_extract_scalar(x, '$.condition.reference')),
                x -> x IS NOT NULL) AS refs,
         d.dx_refs, m.proposed_fhir_status AS event_status
  FROM src s
  LEFT JOIN in_vs iv ON iv.role = s.role AND iv.rtype = s.rtype AND iv.id = s.id
  LEFT JOIN rx ON rx.role = s.role AND rx.rtype = s.rtype AND rx.id = s.id
  LEFT JOIN enc_sct es ON es.role = s.role AND es.rtype = s.rtype AND es.id = s.id
  LEFT JOIN dxl d ON s.role = 'encounter' AND d.enc_id = s.id
  LEFT JOIN (SELECT trim(emar_event_txt) AS ev, max(proposed_fhir_status) AS proposed_fhir_status
             FROM dqm_readiness.status_map GROUP BY 1) m ON m.ev = s.ma_event
),
chk AS (
  -- three parallel arrays (Athena unnests them side by side): check name, applies, flagged
  SELECT r.role, r.injected, k.check_id, k.applies, k.flagged
  FROM rec r
  CROSS JOIN UNNEST(
    ARRAY[
    'profile',
    'code_mapped',
    'term_type',
    'value_comparable',
    'status_usable',
    'timing',
    'subject',
    'diagnosis_link',
    'status_has_event',
    'status_matches_event',
    'info_typed_not_inpatient'],
    ARRAY[
    coalesce(true, false),
    coalesce(true, false),
    coalesce(r.role LIKE 'medication%' AND r.has_rx, false),
    coalesce(r.role = 'glucose_result', false),
    coalesce(true, false),
    coalesce(true, false),
    coalesce(true, false),
    coalesce(r.role = 'encounter' AND r.dx_refs IS NOT NULL, false),
    coalesce(r.role LIKE 'medication%' AND r.is_emar, false),
    coalesce(r.role LIKE 'medication%' AND r.is_emar AND r.event_status IS NOT NULL, false),
    coalesce(r.role = 'encounter', false)],
    ARRAY[
    coalesce(NOT any_match(r.profiles, p -> p = CASE r.role
                 WHEN 'glucose_result' THEN 'http://hl7.org/fhir/us/qicore/StructureDefinition/qicore-observation-lab'
                 WHEN 'encounter' THEN 'http://hl7.org/fhir/us/qicore/StructureDefinition/qicore-encounter'
                 WHEN 'diabetes_diagnosis' THEN 'http://hl7.org/fhir/us/qicore/StructureDefinition/qicore-condition-encounter-diagnosis'
                 ELSE 'http://hl7.org/fhir/us/qicore/StructureDefinition/qicore-medicationadministration' END
               OR (r.role = 'diabetes_diagnosis' AND p = 'http://hl7.org/fhir/us/qicore/StructureDefinition/qicore-condition-problems-health-concerns')), false),
    coalesce(IF(r.role = 'encounter', NOT r.has_sct_type, NOT r.has_vs_code), false),
    coalesce(NOT r.tty_ok, false),
    coalesce(try_cast(r.q_value AS double) IS NULL OR coalesce(r.q_system, '') <> 'http://unitsofmeasure.org'
             OR coalesce(r.q_code, '') NOT IN ('mg/dL', 'mg/L', 'g/L', 'g/dL', 'mg/mL', 'ug/mL', 'ug/dL', 'ng/mL'), false),
    coalesce(IF(r.role = 'diabetes_diagnosis',
                coalesce(r.clinical_status, '') NOT IN ('active', 'recurrence', 'relapse') AND r.abatement IS NULL,
                coalesce(r.status, 'unknown') = 'unknown'), false),
    coalesce(CASE r.role
               WHEN 'glucose_result' THEN r.eff_time IS NULL AND r.eff_start IS NULL
               WHEN 'medication_cms816' THEN r.eff_time IS NULL AND r.eff_start IS NULL
               WHEN 'medication_cms871' THEN r.eff_time IS NULL AND (r.eff_start IS NULL OR r.eff_end IS NULL)
               WHEN 'encounter' THEN r.p_start IS NULL OR r.p_end IS NULL
               ELSE r.onset IS NULL END, false),
    coalesce(NOT starts_with(r.subj, 'Patient/'), false),
    coalesce(NOT any_match(coalesce(r.dx_refs, CAST(ARRAY[] AS ARRAY(VARCHAR))), x -> coalesce(contains(r.refs, x), false)), false),
    coalesce(r.ma_event IS NULL OR r.ma_event = '', false),
    coalesce(coalesce(r.status, '') <> r.event_status, false),
    coalesce(NOT r.has_vs_code, false)]
  ) AS k(check_id, applies, flagged)
)
SELECT check_id, role,
       count_if(applies) AS records, count_if(applies AND flagged) AS flagged,
       count_if(applies AND injected) AS injected, count_if(applies AND flagged AND injected) AS flagged_injected
FROM chk GROUP BY 1, 2 ORDER BY 1, 2
