#!/usr/bin/env python3
"""
r1_run.py — readiness checks (R1) and the readiness scenarios' engine inputs (R2), in Athena. Row-level
data stays in S3; only counts come back (1-9 shown as <10).

  cd ".../project2-dqm-infrastructure"; export AWS_PROFILE=your-aws-profile
  python3 pipeline/readiness/r1_run.py universe          # once: the records the measures use (cohort, mapping a)
  python3 pipeline/readiness/r1_run.py checks            # natural problems: baseline vs as exported, measure level
  python3 pipeline/readiness/r1_run.py inject            # injected problems: 7 types x 1/10/50%, detection
  python3 pipeline/readiness/r1_run.py scenario-inputs --scenarios status-lower status-upper units@10 natural:profiles

Universe (sql/50_r1_universe.sql): the 2017-2019 cohort's slice records of the roles the measures retrieve
(glucose results, CMS816/CMS871 administrations, diabetes diagnoses, the stay encounter), each with its
baseline JSON (the prepare stage mapping a) and its JSON as exported by MIMIC-IV-on-FHIR.
Checks (sql/55_r1_checks.sql): one row per check and role; generated from the measures' requirements
(checks/measure_requirements.json, made by checks/make_requirements.js from CMS's bundles).

Injected problems (registration §4.4; seeds dqm-r1-inject-<type>-20261014; rates nested: the 1% records are
inside the 10%, the 10% inside the 50%), each on copies of the baseline records of its roles:
  units           glucose result in mg/dL rewritten in mmol/L (value / 18.016): valid UCUM, not comparable
                  with the measures' mg/dL thresholds                                   target check value_comparable
  profiles        QI-Core profile removed from meta.profile (results, administrations)      profile
  unmapped-codes  standard coding removed (LOINC from results, RxNorm from administrations) code_mapped
  term-type       each generic RxNorm code of an administration replaced by its branded product
                  (RxNorm tradename_of); only administrations whose every RxNorm code has one  term_type
  missing-links   reasonReference / diagnosis removed from stay encounters with a diabetes
                  diagnosis                                                              diagnosis_link
  status          status set to "unknown" (results, administrations)                     status_usable
  timing          effective time removed (results, administrations)                      timing
Detection = recall (injected records the target check flags) and precision (flagged records that were
injected), per type and rate; every other check's flags are saved too (cross-talk).

Scenarios for engine runs (changed stays only: stays whose slice holds a changed record; every other
stay's result is the baseline result):
  <type>@<rate>      an injected copy as above (rate 1, 10 or 50)
  natural:<problem>  the problem at its full, real extent: that element as MIMIC-IV-on-FHIR exports it, for
                     every record (units: valueQuantity; profiles: meta; codes: code / medication; links:
                     reasonReference / diagnosis) — the 100% point of the curve
  observation-as-inpatient  exploratory (deviation 6): stays MIMIC classes as observation (OBSENC) typed as an
                     inpatient hospital admission (SNOMED 32485007) instead of observation services
  icu-glucose-unit-assumed  repair: ICU (chartevents) glucose results with a number but no unit get mg/dL in
                     UCUM, the unit of their item in MIMIC's item dictionary (baseline: values as exported,
                     so these readings cannot be compared with any mg/dL threshold)
  glucose-lab-only   registered sensitivity (registration §7, glucose source: lab only vs lab + ICU finger-sticks): ICU
                     (chartevents) glucose results lose their LOINC coding, so the measures' glucose value set no
                     longer retrieves them; laboratory results unchanged. Baseline = lab + bedside as recorded
  status-upper / status-lower  corrected medication status (deviation 2): eMAR administrations get the
                     status of their recorded event from the reviewed table; administrations without an event
                     keep "completed" (upper bound) or become "unknown", counted by neither measure (lower)
Each scenario becomes an engine input like build_inputs.py's (inputs.json, inputs_env.py):
dqm/readiness/<stamp>/scenario-<name>/export/, slices = baseline slices with the changed records replaced.
"""
import argparse
import csv
import datetime
import glob
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(ROOT, "pipeline", "prepare"))
sys.path.insert(0, HERE)
from athena_run import Athena, aws  # noqa: E402
from build_inputs import BUCKET, COLS, S1_RUN, YEAR_OK, local_sql, mask  # noqa: E402

DB = "dqm_readiness"
B4DB = "dqm_prep_vaf"
VERSION = "20261005"
RAW = "mimic_fhir.ndjson_parts"
RX = "http://www.nlm.nih.gov/research/umls/rxnorm"
TYPES = ["units", "profiles", "unmapped-codes", "term-type", "missing-links", "status", "timing"]
TARGET = {"units": "value_comparable", "profiles": "profile", "unmapped-codes": "code_mapped", "term-type": "term_type",
          "missing-links": "diagnosis_link", "status": "status_usable", "timing": "timing"}
NATURAL_FIELDS = {"units": ["valueQuantity"], "profiles": ["meta"],
                  "codes": ["code", "medicationCodeableConcept", "medicationReference"],
                  "links": ["reasonReference", "diagnosis"]}
NATURAL_ROLES = {"units": "u.role = 'glucose_result'",
                 "profiles": "u.role IN ('glucose_result', 'medication_cms816', 'medication_cms871')",
                 "codes": "u.role IN ('glucose_result', 'medication_cms816', 'medication_cms871')",
                 "links": "u.role = 'encounter'"}
RESULTS_MEDS = "u.role IN ('glucose_result', 'medication_cms816', 'medication_cms871')"
M = "CAST(json_parse(u.j) AS MAP(VARCHAR, JSON))"


def put(key, value):
    return f"json_format(CAST(map_concat({M}, MAP(ARRAY['{key}'], ARRAY[{value}])) AS JSON))"


def drop(keys):
    ks = ", ".join(f"'{k}'" for k in keys)
    return f"json_format(CAST(map_filter({M}, (k, v) -> k NOT IN ({ks})) AS JSON))"


def sub_coding(field, system_like):
    return put(field, f"""CAST(map_concat(CAST(json_extract(u.j, '$.{field}') AS MAP(VARCHAR, JSON)),
        MAP(ARRAY['coding'], ARRAY[CAST(filter(CAST(json_extract(u.j, '$.{field}.coding') AS ARRAY(JSON)),
            c -> strpos(coalesce(json_extract_scalar(c, '$.system'), ''), '{system_like}') = 0) AS JSON)])) AS JSON)""")


TRANSFORM = {
    "units": put("valueQuantity", """CAST(MAP(ARRAY['value', 'unit', 'system', 'code'], ARRAY[
        CAST(round(CAST(json_extract_scalar(u.j, '$.valueQuantity.value') AS double) / 18.016, 2) AS JSON),
        CAST('mmol/L' AS JSON), CAST('http://unitsofmeasure.org' AS JSON), CAST('mmol/L' AS JSON)]) AS JSON)"""),
    "profiles": put("meta", """CAST(map_concat(CAST(json_extract(u.j, '$.meta') AS MAP(VARCHAR, JSON)),
        MAP(ARRAY['profile'], ARRAY[CAST(filter(CAST(json_extract(u.j, '$.meta.profile') AS ARRAY(VARCHAR)),
            p -> strpos(p, '/qicore-') = 0) AS JSON)])) AS JSON)"""),
    "unmapped-codes": f"IF(u.rtype = 'Observation', {sub_coding('code', 'loinc.org')}, {sub_coding('medicationCodeableConcept', 'rxnorm')})",
    "term-type": put("medicationCodeableConcept", """CAST(map_concat(CAST(json_extract(u.j, '$.medicationCodeableConcept') AS MAP(VARCHAR, JSON)),
        MAP(ARRAY['coding'], ARRAY[CAST(tt.coding AS JSON)])) AS JSON)"""),
    "missing-links": drop(["reasonReference", "diagnosis"]),
    "status": put("status", "CAST('unknown' AS JSON)"),
    "timing": drop(["effectiveDateTime", "effectiveInstant", "effectivePeriod"]),
}
ELIGIBLE = {
    "units": "u.role = 'glucose_result' AND json_extract_scalar(u.j, '$.valueQuantity.code') = 'mg/dL' "
             "AND try_cast(json_extract_scalar(u.j, '$.valueQuantity.value') AS double) IS NOT NULL",
    "profiles": f"{RESULTS_MEDS} AND strpos(coalesce(json_format(json_extract(u.j, '$.meta.profile')), ''), '/qicore-') > 0",
    "unmapped-codes": "((u.role = 'glucose_result' AND strpos(coalesce(json_format(json_extract(u.j, '$.code.coding')), ''), 'loinc.org') > 0) "
                      "OR (u.role LIKE 'medication%' AND strpos(coalesce(json_format(json_extract(u.j, '$.medicationCodeableConcept.coding')), ''), 'rxnorm') > 0))",
    "term-type": "u.rtype = 'MedicationAdministration' AND tt.id IS NOT NULL",
    "missing-links": "u.role = 'encounter' AND json_extract(u.j, '$.reasonReference') IS NOT NULL "
                     f"AND u.id IN (SELECT dx_encounter FROM {DB}.r1_universe WHERE role = 'diabetes_diagnosis')",
    "status": f"{RESULTS_MEDS} AND coalesce(json_extract_scalar(u.j, '$.status'), 'unknown') <> 'unknown'",
    "timing": f"{RESULTS_MEDS} AND (json_extract(u.j, '$.effectiveDateTime') IS NOT NULL OR json_extract(u.j, '$.effectiveInstant') IS NOT NULL "
              "OR json_extract(u.j, '$.effectivePeriod') IS NOT NULL)",
}
TT_CTE = f"""tt AS (
  SELECT u.rtype, u.id,
         array_agg(IF(b.brand IS NOT NULL, CAST(MAP(ARRAY['system', 'code'], ARRAY['{RX}', b.brand]) AS JSON), x.c) ORDER BY x.n) AS coding
  FROM (SELECT DISTINCT rtype, id, j FROM {DB}.r1_universe WHERE rtype = 'MedicationAdministration') u
  CROSS JOIN UNNEST(CAST(coalesce(json_extract(u.j, '$.medicationCodeableConcept.coding'), json_parse('[]')) AS ARRAY(JSON))) WITH ORDINALITY AS x(c, n)
  LEFT JOIN {DB}.r1_generic_brand b ON json_extract_scalar(x.c, '$.system') = '{RX}' AND b.generic = json_extract_scalar(x.c, '$.code')
  GROUP BY u.rtype, u.id
  HAVING count_if(json_extract_scalar(x.c, '$.system') = '{RX}') > 0
     AND count(b.brand) = count_if(json_extract_scalar(x.c, '$.system') = '{RX}')
)"""


def hash_lt(seed, bp):
    return (f"mod(mod(from_big_endian_64(xxhash64(to_utf8(u.rtype || '/' || u.id || '|{seed}'))), 10000) + 10000, 10000) < {bp}")


def injected_records(t, rate):
    """(ctes, select of rtype, id, j_new) for injection type t at rate (percent)."""
    seed = f"dqm-r1-inject-{t}-20261014"
    ctes = [TT_CTE] if t == "term-type" else []
    join = "LEFT JOIN tt ON tt.rtype = u.rtype AND tt.id = u.id" if t == "term-type" else ""
    sel = (f"SELECT DISTINCT u.rtype, u.id, {TRANSFORM[t]} AS j_new FROM {DB}.r1_universe u {join} "
           f"WHERE {ELIGIBLE[t]} AND {hash_lt(seed, int(round(rate * 100)))}")
    return ctes, sel


def natural_records(p):
    keep = ", ".join(f"'{k}'" for k in NATURAL_FIELDS[p])
    new = (f"json_format(CAST(map_concat(map_filter({M}, (k, v) -> k NOT IN ({keep})), "
           f"map_filter(CAST(json_parse(x.j) AS MAP(VARCHAR, JSON)), (k, v) -> k IN ({keep}))) AS JSON))")
    differs = " OR ".join(f"coalesce(json_format(json_extract(u.j, '$.{k}')), '') <> coalesce(json_format(json_extract(x.j, '$.{k}')), '')"
                          for k in NATURAL_FIELDS[p])
    roles = NATURAL_ROLES[p] + (" AND u.id IN (SELECT dx_encounter FROM " + DB + ".r1_universe WHERE role = 'diabetes_diagnosis')"
                                if p == "links" else "")
    return [], (f"SELECT DISTINCT u.rtype, u.id, {new} AS j_new FROM {DB}.r1_universe u "
                f"JOIN {DB}.r1_exported x ON x.rtype = u.rtype AND x.id = u.id WHERE {roles} AND ({differs})")


def status_records(bound):
    missing = "json_extract_scalar(u.j, '$.status')" if bound == "upper" else "'unknown'"
    new = (f"CASE WHEN coalesce(u.ma_event, '') = '' THEN {missing} "
           f"ELSE coalesce(m.proposed_fhir_status, json_extract_scalar(u.j, '$.status')) END")
    return [], (f"SELECT rtype, id, json_format(CAST(map_concat(CAST(json_parse(j) AS MAP(VARCHAR, JSON)), "
                f"MAP(ARRAY['status'], ARRAY[CAST(new_status AS JSON)])) AS JSON)) AS j_new "
                f"FROM (SELECT DISTINCT u.rtype, u.id, u.j, {new} AS new_status FROM {DB}.r1_universe u "
                f"LEFT JOIN (SELECT trim(emar_event_txt) AS ev, max(proposed_fhir_status) AS proposed_fhir_status "
                f"FROM {DB}.status_map GROUP BY 1) m ON m.ev = u.ma_event "
                f"WHERE u.rtype = 'MedicationAdministration' AND u.is_emar) z "
                f"WHERE z.new_status IS NOT NULL AND z.new_status <> coalesce(json_extract_scalar(z.j, '$.status'), '')")


def icu_unit_records():
    """ICU (chartevents) glucose results whose valueQuantity has a number but no unit: the item dictionary's unit
    (mg/dL for every chartevents glucose item in the crosswalk) written as UCUM. A repair scenario, not part of
    the baseline (baseline rule: values stay as exported)."""
    q = ("CAST(MAP(ARRAY['value', 'unit', 'system', 'code'], ARRAY[json_extract(u.j, '$.valueQuantity.value'), "
         "CAST('mg/dL' AS JSON), CAST('http://unitsofmeasure.org' AS JSON), CAST('mg/dL' AS JSON)]) AS JSON)")
    return [], (f"SELECT DISTINCT u.rtype, u.id, {put('valueQuantity', q)} AS j_new FROM {DB}.r1_universe u "
                f"WHERE u.role = 'glucose_result' AND try_cast(json_extract_scalar(u.j, '$.valueQuantity.value') AS double) IS NOT NULL "
                f"AND json_extract_scalar(u.j, '$.valueQuantity.code') IS NULL AND json_extract_scalar(u.j, '$.valueQuantity.unit') IS NULL "
                f"AND strpos(coalesce(json_format(json_extract(u.j, '$.code.coding')), ''), 'mimic-chartevents-d-items') > 0")


def lab_only_records():
    """Registered sensitivity (registration §7, glucose source): lab only vs lab + ICU finger-sticks. Every ICU (chartevents)
    glucose result loses its standard (LOINC) coding, so the glucose value set of both measures no longer retrieves it:
    for the measures the bedside results are gone, while the record stays in the slice. Laboratory results unchanged."""
    return [], (f"SELECT DISTINCT u.rtype, u.id, {sub_coding('code', 'loinc.org')} AS j_new FROM {DB}.r1_universe u "
                f"WHERE u.role = 'glucose_result' "
                f"AND strpos(coalesce(json_format(json_extract(u.j, '$.code.coding')), ''), 'mimic-chartevents-d-items') > 0 "
                f"AND strpos(coalesce(json_format(json_extract(u.j, '$.code.coding')), ''), 'loinc.org') > 0")


OBS_CODE = "448951000124107"     # SNOMED observation services: the type the baseline gives MIMIC class OBSENC stays
ADMIT_CODE, ADMIT_DISPLAY = "32485007", "Hospital admission"   # what the baseline gives AMB/SS stays (in Encounter Inpatient)


def observation_records():
    """Exploratory (added 2026-10-14, deviation 6): cohort stays that MIMIC classes as observation (OBSENC) typed as an
    inpatient hospital admission instead: the observation-services coding of the stay encounter's type is replaced by
    SNOMED 32485007, as for AMB/SS stays. Changed stays = those stays plus any cohort stay whose slice holds one of these
    encounters (an observation encounter just before an admission extends that admission's start in CMS's logic)."""
    retype = (f"CAST(transform(CAST(json_extract(u.j, '$.type') AS ARRAY(JSON)), t -> CAST(map_concat(CAST(t AS MAP(VARCHAR, JSON)), "
              f"MAP(ARRAY['coding'], ARRAY[CAST(transform(CAST(coalesce(json_extract(t, '$.coding'), json_parse('[]')) AS ARRAY(JSON)), "
              f"c -> IF(json_extract_scalar(c, '$.system') = 'http://snomed.info/sct' AND json_extract_scalar(c, '$.code') = '{OBS_CODE}', "
              f"CAST(MAP(ARRAY['system', 'code', 'display'], ARRAY['http://snomed.info/sct', '{ADMIT_CODE}', '{ADMIT_DISPLAY}']) AS JSON), c)) "
              f"AS JSON)])) AS JSON)) AS JSON)")
    return [], (f"SELECT DISTINCT u.rtype, u.id, {put('type', retype)} AS j_new FROM {DB}.r1_universe u "
                f"WHERE u.role = 'encounter' AND strpos(coalesce(json_format(json_extract(u.j, '$.type')), ''), '{OBS_CODE}') > 0")


def records_for(scenario):
    if scenario == "observation-as-inpatient":
        return observation_records()
    if scenario == "icu-glucose-unit-assumed":
        return icu_unit_records()
    if scenario == "glucose-lab-only":
        return lab_only_records()
    if scenario in ("status-lower", "status-upper"):
        return status_records(scenario.split("-")[1])
    if scenario.startswith("natural:"):
        p = scenario.split(":", 1)[1]
        if p not in NATURAL_FIELDS:
            sys.exit(f"natural:<problem> must be one of {sorted(NATURAL_FIELDS)}")
        return natural_records(p)
    t, _, r = scenario.partition("@")
    if t not in TYPES or r not in ("1", "10", "50"):
        sys.exit(f"bad scenario {scenario}: <type>@<1|10|50>, natural:<problem>, status-lower, status-upper, icu-glucose-unit-assumed, glucose-lab-only, observation-as-inpatient")
    return injected_records(t, float(r))


def checks_sql(src):
    return local_sql_r1("55_r1_checks.sql", src=src, b4db=B4DB, version=VERSION)


def local_sql_r1(rel, **params):
    text = open(os.path.join(HERE, "sql", rel)).read()
    for k, v in params.items():
        text = text.replace("{" + k + "}", str(v))
    return "\n".join(l for l in text.splitlines() if not l.strip().startswith("--")).strip().rstrip(";")


def src_with(ctes, sel):
    w = ", ".join(ctes + [f"sel AS ({sel})"])
    return (f"WITH {w} SELECT u.role, u.rtype, u.id, coalesce(s.j_new, u.j) AS j, s.id IS NOT NULL AS injected, "
            f"u.ma_event, u.is_emar, u.dx_encounter FROM {DB}.r1_universe u LEFT JOIN sel s ON s.rtype = u.rtype AND s.id = u.id")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("step", choices=["universe", "checks", "inject", "scenario-inputs"])
    ap.add_argument("--types", nargs="+", default=TYPES)
    ap.add_argument("--rates", nargs="+", type=float, default=[1, 10, 50])
    ap.add_argument("--scenarios", nargs="+", default=[])
    ap.add_argument("--shards", type=int, default=64)
    ap.add_argument("--min-stays-per-shard", type=int, default=150)
    ap.add_argument("--light-factor", type=float, default=0.35)
    ap.add_argument("--batch-max-res", type=int, default=400)
    ap.add_argument("--workgroup", default="primary")
    a = ap.parse_args()
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = os.path.join(ROOT, "results", f"r1-{a.step}-{stamp}")
    os.makedirs(out, exist_ok=True)
    print(f"AWS identity: {aws('sts', 'get-caller-identity')['Arn']}")
    wg = aws("athena", "get-work-group", "--work-group", a.workgroup)["WorkGroup"]
    cfg = wg.get("Configuration", {})
    out_loc = None if cfg.get("ResultConfiguration", {}).get("OutputLocation") else f"s3://{BUCKET}/dqm/athena-results/"
    at = Athena(a.workgroup, out_loc)
    t0 = time.time()
    run = lambda sql, label: at.run(sql, db=DB, label=label)   # noqa: E731
    at.run(f"CREATE DATABASE IF NOT EXISTS {DB}", label=f"database {DB}")

    def show(sql, label, fname):
        hdr, rows = at.rows(run(sql, label))
        shown = [[mask(c) if c is not None and c.isdigit() else c for c in r] for r in rows]
        with open(os.path.join(out, fname), "w", newline="") as f:
            csv.writer(f).writerows([hdr] + shown)
        print("     " + " | ".join(hdr))
        for r in shown[:60]:
            print("     " + " | ".join("" if c is None else c for c in r))
        return [dict(zip(hdr, r)) for r in rows]

    if a.step == "universe":
        loc = f"s3://{BUCKET}/dqm/readiness/r1-{stamp}"
        subprocess.run(["aws", "s3", "cp", os.path.join(ROOT, "crosswalk", "medication_status_map.csv"),
                        f"{loc}/status_map/medication_status_map.csv", "--only-show-errors"], check=True)
        for t in ["status_map", "r1_record_stays", "r1_universe", "r1_exported", "r1_generic_brand"]:
            run(f"DROP TABLE IF EXISTS {t}", f"drop table (metadata only) {t}")
        p = local_sql("50_r1_universe.sql", loc=loc, b4db=B4DB, raw=RAW, version=VERSION)
        for s in p["part build"]:
            run(s, "universe: " + " ".join(s.split()[:4]))
        for i, s in enumerate(p["part summary"]):
            show(s, f"universe summary {i + 1}", f"universe_{i + 1}.csv")
        json.dump({"loc": loc, "b4db": B4DB, "stamp": stamp}, open(os.path.join(out, "universe.json"), "w"), indent=2)

    elif a.step == "checks":
        req = json.load(open(os.path.join(HERE, "checks", "measure_requirements.json")))
        print("measure level: value sets the logic declares whose expansion the measure bundle does not carry:")
        for m, v in req["measures"].items():
            miss = [f"{x['name']} ({x['oid']})" for x in v["value_sets"] if not x["expansion_in_bundle"]]
            print(f"  {m}: {', '.join(miss) or 'none'}")
        base = f"SELECT role, rtype, id, j, false AS injected, ma_event, is_emar, dx_encounter FROM {DB}.r1_universe"
        exported = (f"SELECT u.role, u.rtype, u.id, x.j, true AS injected, u.ma_event, u.is_emar, u.dx_encounter "
                    f"FROM {DB}.r1_universe u JOIN {DB}.r1_exported x ON x.rtype = u.rtype AND x.id = u.id")
        print("\nchecks on the corrected baseline (mapping a):")
        show(checks_sql(base), "checks: baseline", "checks_baseline.csv")
        print("\nchecks on the same records as exported by MIMIC-IV-on-FHIR (natural problems):")
        show(checks_sql(exported), "checks: as exported", "checks_as_exported.csv")
        fam = lambda src: f"""WITH src AS ({src})
            SELECT role, family, count(*) AS records FROM (
              SELECT role, CASE WHEN any_match(p, x -> x LIKE '%/us/qicore/%') THEN 'QI-Core'
                                WHEN any_match(p, x -> x LIKE '%/us/core/%') THEN 'US Core'
                                WHEN any_match(p, x -> x LIKE '%quality-core%') THEN 'US Quality Core'
                                WHEN cardinality(p) > 0 THEN 'other profile only' ELSE 'no profile' END AS family
              FROM (SELECT role, coalesce(try(CAST(json_extract(j, '$.meta.profile') AS ARRAY(VARCHAR))), CAST(ARRAY[] AS ARRAY(VARCHAR))) AS p FROM src))
            GROUP BY 1, 2 ORDER BY 1, 2"""  # noqa: E731
        print("\ndeclared profiles per role (registered profile check), baseline and as exported:")
        show(fam(base), "profiles: baseline", "profiles_baseline.csv")
        show(fam(exported), "profiles: as exported", "profiles_as_exported.csv")

    elif a.step == "inject":
        summary = []
        for t in a.types:
            if t not in TYPES:
                sys.exit(f"unknown type {t}")
            for r in a.rates:
                ctes, sel = injected_records(t, r)
                rows = show(checks_sql(src_with(ctes, sel)), f"inject {t} {r:g}%", f"inject_{t}_{r:g}.csv")
                tg = [x for x in rows if x["check_id"] == TARGET[t]]
                inj = sum(int(x["injected"]) for x in tg)
                fl = sum(int(x["flagged"]) for x in tg)
                fi = sum(int(x["flagged_injected"]) for x in tg)
                rec = {"type": t, "rate_pct": r, "target_check": TARGET[t], "injected": inj, "flagged": fl, "flagged_injected": fi,
                       "recall": round(fi / inj, 4) if inj else None, "precision": round(fi / fl, 4) if fl else None}
                summary.append(rec)
                print(f"  -> {t} {r:g}%: target check {TARGET[t]}: injected {mask(inj)}, flagged {mask(fl)}, "
                      f"recall {rec['recall']}, precision {rec['precision']}")
        with open(os.path.join(out, "detection.csv"), "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(summary[0]))
            w.writeheader()
            w.writerows([{k: (mask(v) if k in ("injected", "flagged", "flagged_injected") else v) for k, v in s.items()}
                         for s in summary])
        print("\ndetection (target check per problem type):")
        for s in summary:
            print(f"  {s['type']:<15} {s['rate_pct']:>4g}%  recall {s['recall']}  precision {s['precision']}  (injected {mask(s['injected'])})")

    else:
        if not a.scenarios:
            sys.exit("--scenarios is required")
        made = []
        for sc in a.scenarios:
            name = sc.replace(":", "-").replace("@", "-")
            label = f"scenario-{name}"
            root = f"s3://{BUCKET}/dqm/readiness/{stamp}/{label}"
            xp = f"{root}/export"
            tag = name.replace("-", "_") + "_" + stamp.lower()
            ov, chg, table = f"scen_override_{tag}", f"scen_stays_{tag}", f"set_{label.replace('-', '_')}_{stamp.lower()}"
            for t in (ov, chg, table):
                run(f"DROP TABLE IF EXISTS {t}", f"drop (metadata only) {t}")
            ctes, sel = records_for(sc)
            w = ("WITH " + ", ".join(ctes) + " ") if ctes else ""
            run(f"CREATE TABLE {ov} WITH (format = 'PARQUET', write_compression = 'SNAPPY', external_location = '{root}/overrides/') AS "
                f"{w}SELECT rtype, id, max(j_new) AS j FROM ({sel}) GROUP BY rtype, id", f"{label}: changed records")
            run(f"CREATE TABLE {chg} WITH (format = 'PARQUET', external_location = '{root}/changed_stays/') AS "
                f"SELECT DISTINCT rs.unit_id FROM {DB}.r1_record_stays rs JOIN {ov} o ON o.rtype = rs.rtype AND o.id = rs.id",
                f"{label}: changed stays")
            cnt = show(f"SELECT (SELECT count(*) FROM {ov}) AS changed_records, (SELECT count(*) FROM {chg}) AS changed_stays",
                       f"{label}: counts", f"{label}_counts.csv")[0]
            n = int(cnt["changed_stays"])
            if n == 0:
                print(f"  {label}: no stay changes: nothing to export")
                made.append({"set": label, "stays": 0})
                continue
            selection = (f"SELECT {COLS}, '{label}' AS reason FROM {B4DB}.stay_manifest m "
                         f"JOIN {chg} c ON c.unit_id = m.unit_id WHERE {YEAR_OK}")
            shards = max(1, min(a.shards, -(-n // a.min_stays_per_shard)))
            p = local_sql("20_select_and_shard.sql", table=table, root=f"{root}/set-table", selection=selection, shards=shards,
                          batch_max_res=a.batch_max_res, light_factor=a.light_factor)
            for s in p["part select"]:
                run(s, f"{label}: select + shards")
            tot = show(p["part summary"][0], f"{label}: totals", f"{label}_totals.csv")[0]
            if not (tot["stays"] == tot["distinct_stays"] and tot["shards"] == tot["distinct_shards"]):
                sys.exit(f"CHECK FAILED ({label})")
            x = local_sql("40_scenario_export.sql", table=table, b4db=B4DB, xp=xp, overrides=f"{DB}.{ov}")
            for i, s in enumerate(x["part export"]):
                run(s, f"{label}: UNLOAD {['manifest', 'slices', 'shared'][i]}")
            nsh = int(tot["shards"])
            for sub in ["manifest", "slices"]:
                r = subprocess.run(["aws", "s3", "ls", f"{xp}/{sub}/"], capture_output=True, text=True)
                got = sum(1 for line in r.stdout.splitlines() if "PRE shard=" in line)
                if got != nsh:
                    sys.exit(f"CHECK FAILED ({label}): {sub} has {got} shard folders, expected {nsh}")
            subprocess.run(["aws", "s3", "cp", "--recursive", f"{S1_RUN}/engine/", f"{root}/engine/", "--only-show-errors"], check=True)
            made.append({"set": label, "kind": "scenario", "scenario": sc, "prep_database": B4DB, "medication_mapping": "a (final)",
                         "input": xp, "v_input": None, "root": root, "athena_table": f"{DB}.{table}", "overrides": f"{DB}.{ov}",
                         "stays": int(tot["stays"]), "patients": tot["patients"], "shards": nsh,
                         "changed_records": int(cnt["changed_records"]),
                         "predicted_hours_one_stay_per_call": tot["predicted_hours"]})
            print(f"  {label}: {mask(n)} changed stays, {nsh} shards; INPUT={xp}")
        rdir = os.path.join(ROOT, "results", f"readiness-inputs-{stamp}")
        os.makedirs(rdir, exist_ok=True)
        json.dump({"stamp": stamp, "kind": "scenarios", "base": "cohort-a (mapping a, final)", "sets": made},
                  open(os.path.join(rdir, "inputs.json"), "w"), indent=2)
        print(f"inputs recorded: {rdir}/inputs.json (eval \"$(python3 pipeline/readiness/inputs_env.py)\")")

    print(f"Athena: {at.n} queries, {at.scanned / 1e9:.1f} GB scanned (~${at.scanned / 1e12 * 5:.2f}); {time.time() - t0:.0f} s; "
          f"saved (counts only): {out}")


if __name__ == "__main__":
    main()
