#!/usr/bin/env python3
"""
make_examples.py: pick example records from the mapped OPEN demonstration data (MIMIC-IV Clinical Database
Demo on FHIR, mapped as in the study) that carry each readiness problem found in R1/R2, plus controls, for
validation against the QI-Core 6.0.0 profiles with the HL7 FHIR validator (validate_examples.sh).

Question answered: would a FHIR profile validator flag the records whose problems changed the measure
results? Open data only; nothing credentialed is read. Each example keeps only its QI-Core profile in
meta.profile, so the validator checks QI-Core and nothing else.

  python3 pipeline/readiness/conformance/make_examples.py [--demo work/demo-mapped-v3] [--out DIR]
"""
import argparse
import copy
import gzip
import json
import os
import time

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
QI = "http://hl7.org/fhir/us/qicore/StructureDefinition/"
UCUM = "http://unitsofmeasure.org"


def rows(demo, name):
    with gzip.open(os.path.join(demo, f"{name}.ndjson.gz"), "rt") as f:
        for line in f:
            yield json.loads(line)


def qicore_only(r):
    r = copy.deepcopy(r)
    prof = [p for p in r.get("meta", {}).get("profile", []) if p.startswith(QI)]
    r.setdefault("meta", {})["profile"] = prof
    return r


def has_profile(r, suffix):
    return any(p == QI + suffix for p in r.get("meta", {}).get("profile", []))


def first(it, pred):
    for r in it:
        if pred(r):
            return r
    return None


def enc_type_codes(r):
    return {c.get("code") for t in r.get("type", []) for c in t.get("coding", [])}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--demo", default=os.path.join(ROOT, "work", "demo-mapped-v3"))
    ap.add_argument("--out")
    a = ap.parse_args()
    out = a.out or os.path.join(ROOT, "results", "conformance-" + time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()))
    ex_dir = os.path.join(out, "examples")
    os.makedirs(ex_dir, exist_ok=True)
    d = a.demo

    picks = {}
    picks["patient"] = first(rows(d, "MimicPatient"), lambda r: has_profile(r, "qicore-patient"))
    picks["glucose-bedside-no-unit"] = first(
        rows(d, "MimicObservationChartevents"),
        lambda r: has_profile(r, "qicore-observation-lab") and "value" in r.get("valueQuantity", {})
        and not r["valueQuantity"].get("code") and not r["valueQuantity"].get("unit"))
    picks["control-glucose-lab-ucum"] = first(
        rows(d, "MimicObservationLabevents"),
        lambda r: has_profile(r, "qicore-observation-lab") and r.get("valueQuantity", {}).get("system") == UCUM)
    picks["encounter-typed-observation"] = first(
        rows(d, "MimicEncounter"),
        lambda r: has_profile(r, "qicore-encounter") and "448951000124107" in enc_type_codes(r))
    linked = first(
        rows(d, "MimicEncounter"),
        lambda r: has_profile(r, "qicore-encounter") and "183452005" in enc_type_codes(r)
        and (r.get("reasonReference") or r.get("diagnosis")))
    picks["control-encounter-inpatient-linked"] = linked
    if linked is not None:
        unlinked = copy.deepcopy(linked)
        unlinked.pop("reasonReference", None)
        unlinked.pop("diagnosis", None)
        unlinked["id"] = linked["id"][:-4] + "0000"
        picks["encounter-inpatient-diagnosis-links-removed"] = unlinked
    picks["medication-administration-completed"] = first(
        rows(d, "MimicMedicationAdministration"),
        lambda r: has_profile(r, "qicore-medicationadministration") and r.get("status") == "completed")
    picks["condition-encounter-diagnosis"] = first(
        rows(d, "MimicCondition"), lambda r: has_profile(r, "qicore-condition-encounter-diagnosis"))

    manifest = []
    for name, r in picks.items():
        if r is None:
            print(f"  {name}: no example found in {d}")
            continue
        r = qicore_only(r)
        with open(os.path.join(ex_dir, f"{name}.json"), "w") as f:
            json.dump(r, f, indent=1)
        manifest.append({"example": name, "resourceType": r["resourceType"], "profiles": r["meta"]["profile"],
                         "valueQuantity": r.get("valueQuantity")})
        print(f"  {name}: {r['resourceType']} {r['meta']['profile']}")
    with open(os.path.join(out, "examples.json"), "w") as f:
        json.dump({"demo": os.path.relpath(d, ROOT), "examples": manifest}, f, indent=1)
    print(out)


if __name__ == "__main__":
    main()
