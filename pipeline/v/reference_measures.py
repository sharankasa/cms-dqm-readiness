#!/usr/bin/env python3
"""
reference_measures.py — V (registration §4.6): an independent implementation of CMS816 and CMS871, written
from CMS's published human-readable specification (Measure.description and population descriptions in
the measure bundles), NOT run through any CQL engine. Compared stay by stay with fqm-execution
(compare_v.py); every disagreement is traced to its cause: CMS content, engine, or this reference.

  python3 reference_measures.py --prep <slice_stays.py --whole-patient output> --out <file.jsonl> \
      --bundles <CMS816 bundle> <CMS871 bundle> --vs-cache <dir> [--period-year 2026] \
      [--exclusion-rule narrative|first-test] [--diagnosis-rule narrative|linked] [--med-interval-rule narrative|strict] [--prevalence-rule narrative|qicore]

Reads each patient's FULL record (patients/<id>.ndjson.gz + shared_all.ndjson.gz) and the stays in
manifest.csv; each stay is evaluated in the calendar year of its discharge (or --period-year).

Spec text -> rule (where the narrative is silent or ambiguous, the choice is stated; the options name
the places where the narrative and CMS's CQL logic visibly differ):

 Both measures
  - Inpatient hospitalization: Encounter.type in "Encounter Inpatient", status finished, patient >= 18
    on the start date, discharge (period end) on a date in the measurement year.
  - "The encounter" includes ED and observation time immediately before admission (CMS816/CMS871 text:
    "including the emergency department and observation"): an observation encounter ending 1 h or
    less before the admission extends the start; then an ED visit ending 1 h or less before that
    start extends it again (latest-ending one each time).
  - Glucose result: Observation code in "Glucose Lab Test Mass Per Volume", status final/amended/
    corrected, value in mg/dL (unit string; the unit's code system is not checked), time = effective
    start.
 CMS816 (Severe Hypoglycemia)
  - Initial population = denominator: a hypoglycemic medication administration (status completed)
    STARTS during the encounter.
  - Numerator: a glucose < 40 mg/dL during the encounter, with a hypoglycemic administration starting
    24 h or less before (or at) it, and no other glucose > 80 mg/dL more than 0 and at most 5 minutes
    after it (also during the encounter).
 CMS871 (Severe Hyperglycemia)
  - Initial population: (a) a diabetes diagnosis that starts before or during the encounter, or (b) an
    administration of a hypoglycemic/treatment medication (status completed or in-progress) during the
    encounter, or (c) a glucose >= 200 mg/dL during the encounter.
      --diagnosis-rule narrative (default): ANY diabetes Condition of the patient whose onset starts
        before the end of the encounter (the text does not restrict which encounter recorded it).
      --diagnosis-rule linked: as CMS's logic: an encounter diagnosis referenced by THIS encounter
        (reasonReference) or a diabetes reasonCode on the encounter, or a problem-list diabetes
        condition (verification status empty or confirmed/unconfirmed/provisional/differential).
      Onset = onsetDateTime / onsetPeriod.start / onsetAge is not used; no onset -> does not count.
      --prevalence-rule narrative (default): a diagnosis with an onset before the end counts.
      --prevalence-rule qicore: as CMS's QICoreCommon library: the condition must also be clinically
        active (active/recurrence/relapse) or have an abatement; otherwise its prevalence interval is
        null and it never counts (found 2026-10-09: MIMIC diagnoses carry no clinical status).
  - Exclusions (denominator and numerator): glucose > 600 mg/dL between 1 h before and 6 h after the
    start; comfort measures (Procedure completed / ServiceRequest active|completed, "Comfort Measures")
    starting during the encounter; discharge disposition in either hospice value set.
      --exclusion-rule narrative (default): ANY glucose > 600 in that window counts.
      --exclusion-rule first-test: as CMS's logic: only if it is the FIRST glucose test in the window.
  - Medication "during the encounter" (b):
      --med-interval-rule narrative (default): the administration time(s) fall in the encounter; an
        administration period with no recorded end is treated as an instant at its start.
      --med-interval-rule strict: as CMS's logic: the whole period must lie in the encounter; a period
        with no end never does.
  - Days: the first min(10 days, length of stay) after the start, cut into whole 24-hour periods
    (a final period shorter than 24 h is dropped). Eligible days = days 2 onward ("minus the first 24
    hours"). Event day: a glucose > 300, or no glucose test that day while each of the two preceding
    days had a glucose >= 200. Numerator = at least one event day. Observations: eligible days and
    event days (0 when excluded).
Output: JSON lines like run_engine.js (measure, unit_id, episode, pops by fqm populationId, obs).
"""
import argparse
import csv
import datetime as dt
import glob
import gzip
import json
import os
import sys

OID = {
    "inpatient": "2.16.840.1.113883.3.666.5.307", "ed": "2.16.840.1.113883.3.117.1.7.1.292",
    "obs_services": "2.16.840.1.113762.1.4.1111.143", "glucose": "2.16.840.1.113762.1.4.1248.34",
    "hypo816": "2.16.840.1.113762.1.4.1196.393", "treat871": "2.16.840.1.113762.1.4.1196.394",
    "diabetes": "2.16.840.1.113883.3.464.1003.103.12.1001", "comfort": "1.3.6.1.4.1.33895.1.3.0.45",
    "hospice_home": "2.16.840.1.113883.3.117.1.7.1.209", "hospice_facility": "2.16.840.1.113883.3.117.1.7.1.207",
}
OK_OBS = {"final", "amended", "corrected"}
H1, H6, H24, M5 = dt.timedelta(hours=1), dt.timedelta(hours=6), dt.timedelta(hours=24), dt.timedelta(minutes=5)


def load_vs(bundles, vs_cache):
    vs = {}
    for p in bundles:
        for e in json.load(open(p))["entry"]:
            r = e["resource"]
            if r["resourceType"] == "ValueSet":
                vs[r["url"].split("/")[-1]] = {(c.get("system"), c.get("code")) for c in r.get("expansion", {}).get("contains", [])}
    for oid in OID.values():
        if oid not in vs:
            f = os.path.join(vs_cache or "", f"{oid}.json")
            if not os.path.exists(f):
                sys.exit(f"value set {oid} not in the bundles or the VSAC cache")
            vs[oid] = {(c.get("system"), c.get("code")) for c in json.load(open(f)).get("expansion", {}).get("contains", [])}
    return {k: vs[o] for k, o in OID.items()}


def t(s):
    if not s:
        return None
    if len(s) == 10:
        return dt.datetime.fromisoformat(s).replace(tzinfo=dt.timezone.utc)
    d = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    return d if d.tzinfo else d.replace(tzinfo=dt.timezone.utc)


def codings(cc):
    return {(c.get("system"), c.get("code")) for c in (cc or {}).get("coding", [])}


def in_vs(cc, s):
    return bool(codings(cc) & s)


def ref(r, field):
    return ((r.get(field) or {}).get("reference") or "").split("/")[-1] or None


def mg_dl(o):
    q = o.get("valueQuantity") or {}
    if q.get("value") is None or (q.get("code") or q.get("unit")) != "mg/dL":
        return None
    return float(q["value"])


def eff_start(r):
    return t(r.get("effectiveDateTime") or r.get("effectiveInstant") or (r.get("effectivePeriod") or {}).get("start")
             or (r.get("effectivePeriod") or {}).get("end"))


def eff_interval(r):
    if r.get("effectiveDateTime"):
        x = t(r["effectiveDateTime"])
        return x, x
    p = r.get("effectivePeriod") or {}
    return t(p.get("start")), t(p.get("end")) or t(p.get("start"))


class Patient:
    def __init__(self, resources, shared):
        self.by_type = {}
        for r in resources:
            self.by_type.setdefault(r["resourceType"], []).append(r)
        self.meds_by_id = {r["id"]: r for r in shared if r["resourceType"] == "Medication"}
        self.meds_by_id.update({r["id"]: r for r in self.by_type.get("Medication", [])})
        p = (self.by_type.get("Patient") or [{}])[0]
        self.birth = dt.date.fromisoformat(p["birthDate"][:10]) if p.get("birthDate") else None

    def of(self, rtype):
        return self.by_type.get(rtype, [])

    def med_in(self, ma, s):
        if in_vs(ma.get("medicationCodeableConcept"), s):
            return True
        mid = ref(ma, "medicationReference")
        return bool(mid and mid in self.meds_by_id and in_vs(self.meds_by_id[mid].get("code"), s))


def age_on(birth, d):
    return d.year - birth.year - ((d.month, d.day) < (birth.month, birth.day))


def hospitalization(pt, enc, vs):
    start, end = t(enc["period"].get("start")), t(enc["period"].get("end"))

    def last_ending_before(s, valueset):
        best = None
        for e in pt.of("Encounter"):
            if e["id"] == enc["id"] or e.get("status") != "finished" or not any(in_vs(x, valueset) for x in e.get("type", [])):
                continue
            pe = t((e.get("period") or {}).get("end"))
            if pe and s - H1 <= pe <= s and (best is None or pe > best[1]):
                best = (t(e["period"].get("start")), pe)
        return best

    obs = last_ending_before(start, vs["obs_services"])
    visit_start = (obs[0] if obs and obs[0] else start)
    ed = last_ending_before(visit_start, vs["ed"])
    return (ed[0] if ed and ed[0] else visit_start), end


def qualifying(pt, enc, vs, year):
    per = enc.get("period") or {}
    if enc.get("status") != "finished" or not per.get("start") or not per.get("end") or not pt.birth:
        return False
    if not any(in_vs(x, vs["inpatient"]) for x in enc.get("type", [])):
        return False
    return age_on(pt.birth, dt.date.fromisoformat(per["start"][:10])) >= 18 and per["end"][:4] == str(year)


def glucose(pt, vs):
    out = []
    for o in pt.of("Observation"):
        if in_vs(o.get("code"), vs["glucose"]) and o.get("status") in OK_OBS:
            tm = eff_start(o)
            if tm:
                out.append((tm, mg_dl(o), o["id"]))
    return sorted(out, key=lambda x: x[0])


def cms816(pt, enc, vs, year):
    if not qualifying(pt, enc, vs, year):
        return None
    hs, he = hospitalization(pt, enc, vs)
    meds = [eff_interval(m)[0] for m in pt.of("MedicationAdministration")
            if m.get("status") == "completed" and pt.med_in(m, vs["hypo816"])]
    meds = [m for m in meds if m]
    if not any(hs <= m <= he for m in meds):
        return None
    gl = [g for g in glucose(pt, vs) if hs <= g[0] <= he]
    numer = False
    for tm, v, gid in gl:
        if v is None or not v < 40:
            continue
        if not any(tm - H24 <= m <= tm for m in meds):
            continue
        if any(t2 > tm and t2 - tm <= M5 and v2 is not None and v2 > 80 and g2 != gid for t2, v2, g2 in gl):
            continue
        numer = True
        break
    return {"pops": {"InitialPopulation_1": True, "Denominator_1": True, "Numerator_1": numer}, "obs": {}}


def onset_start(c):
    return t(c.get("onsetDateTime") or (c.get("onsetPeriod") or {}).get("start"))


def category(c):
    return {cd for cat in c.get("category", []) for _, cd in codings(cat)}


def cms871(pt, enc, vs, year, exclusion_rule, diagnosis_rule, med_rule, prevalence_rule):
    if not qualifying(pt, enc, vs, year):
        return None
    hs, he = hospitalization(pt, enc, vs)
    gl = glucose(pt, vs)
    # (a) diabetes diagnosis
    diab = False
    if diagnosis_rule == "linked" and any(in_vs(rc, vs["diabetes"]) for rc in enc.get("reasonCode", [])):
        diab = True
    linked = {ref({"r": rr}, "r") for rr in enc.get("reasonReference", [])}
    for c in pt.of("Condition"):
        if not in_vs(c.get("code"), vs["diabetes"]):
            continue
        on = onset_start(c)
        if not on or not on < he:
            continue
        if prevalence_rule == "qicore":
            # CMS's QICoreCommon prevalenceInterval(): only active/recurrence/relapse conditions, or ones
            # with an abatement, have an interval; otherwise it is null and the condition never counts
            status = {cd for _, cd in codings(c.get("clinicalStatus"))}
            has_abatement = any(k.startswith("abatement") for k in c)
            if not (status & {"active", "recurrence", "relapse"} or has_abatement):
                continue
        if diagnosis_rule == "narrative":
            diab = True
        else:
            cats = category(c)
            vstat = codings(c.get("verificationStatus"))
            v_ok = not vstat or any(cd in {"confirmed", "unconfirmed", "provisional", "differential"} for _, cd in vstat)
            if ("encounter-diagnosis" in cats and c["id"] in linked) or \
               (cats & {"problem-list-item", "health-concern"} and v_ok):
                diab = True
    # (b) medication during the encounter
    def within(m):
        if med_rule == "strict":   # CMS's logic: the whole administration period, with a known end
            p = m.get("effectivePeriod")
            s, e = (t(m["effectiveDateTime"]),) * 2 if m.get("effectiveDateTime") else (t((p or {}).get("start")), t((p or {}).get("end")))
            return bool(s and e and hs <= s and e <= he)
        return all(x and hs <= x <= he for x in eff_interval(m))   # no end recorded: treated as an instant
    med = any(m.get("status") in ("completed", "in-progress") and pt.med_in(m, vs["treat871"]) and within(m)
              for m in pt.of("MedicationAdministration"))
    # (c) glucose >= 200 during the encounter
    hi = any(hs <= tm <= he and v is not None and v >= 200 for tm, v, _ in gl)
    if not (diab or med or hi):
        return None
    # exclusions
    win = [(tm, v) for tm, v, _ in gl if hs - H1 <= tm <= hs + H6]
    if exclusion_rule == "narrative":
        early600 = any(v is not None and v > 600 for _, v in win)
    else:
        early600 = any(v is not None and v > 600 and not any(t2 < tm for t2, _ in win) for tm, v in win)
    comfort = False
    for p in pt.of("Procedure"):
        if p.get("status") == "completed" and in_vs(p.get("code"), vs["comfort"]):
            s = t(p.get("performedDateTime") or (p.get("performedPeriod") or {}).get("start"))
            comfort |= bool(s and hs <= s <= he)
    for sr in pt.of("ServiceRequest"):
        if sr.get("status") in ("active", "completed") and in_vs(sr.get("code"), vs["comfort"]):
            s = t(sr.get("authoredOn"))
            comfort |= bool(s and hs <= s <= he)
    dispo = (enc.get("hospitalization") or {}).get("dischargeDisposition")
    hospice = in_vs(dispo, vs["hospice_home"]) or in_vs(dispo, vs["hospice_facility"])
    excluded = early600 or comfort or hospice
    # days
    rel_end = min(he, hs + dt.timedelta(days=10))
    n_days = int((rel_end - hs).total_seconds() // 86400)
    days = []
    for i in range(1, n_days + 1):
        d0, d1 = hs + H24 * (i - 1), hs + H24 * i
        vals = [v for tm, v, _ in gl if d0 <= tm < d1]
        days.append({"severe": any(v is not None and v > 300 for v in vals),
                     "elevated": any(v is not None and v >= 200 for v in vals),
                     "no_test": not vals})
    eligible = max(0, n_days - 1)
    events = 0
    for i in range(2, n_days + 1):
        d = days[i - 1]
        prev_ok = i >= 3 and days[i - 2]["elevated"] and days[i - 3]["elevated"]
        events += bool(d["severe"] or (d["no_test"] and prev_ok))
    return {"pops": {"InitialPopulation_1": True, "Denominator_1": True, "DenominatorExclusion_1": excluded,
                     "Numerator_1": events > 0, "NumeratorExclusion_1": excluded and events > 0},
            "obs": {"MeasureObservation_1_1": [0 if excluded else eligible],
                    "MeasureObservation_1_2": [0 if excluded else events]}}


def read_gz(p):
    with gzip.open(p, "rt", encoding="utf-8") as f:
        return [json.loads(l) for l in f if l.strip()]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prep", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--bundles", nargs="+", required=True)
    ap.add_argument("--vs-cache")
    ap.add_argument("--period-year")
    ap.add_argument("--exclusion-rule", choices=["narrative", "first-test"], default="narrative")
    ap.add_argument("--diagnosis-rule", choices=["narrative", "linked"], default="narrative")
    ap.add_argument("--med-interval-rule", choices=["narrative", "strict"], default="narrative")
    ap.add_argument("--prevalence-rule", choices=["narrative", "qicore"], default="narrative")
    a = ap.parse_args()
    vs = load_vs(a.bundles, a.vs_cache)
    units = list(csv.DictReader(open(os.path.join(a.prep, "manifest.csv"))))
    shared = read_gz(os.path.join(a.prep, "shared_all.ndjson.gz"))
    by_pid = {}
    for u in units:
        by_pid.setdefault(u["patient_id"], []).append(u)
    os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
    n = 0
    with open(a.out, "w") as out:
        for pid, us in sorted(by_pid.items()):
            pt = Patient(read_gz(os.path.join(a.prep, "patients", f"{pid}.ndjson.gz")), shared)
            encs = {e["id"]: e for e in pt.of("Encounter")}
            for u in us:
                year = a.period_year or u["mp_year"]
                enc = encs[u["unit_id"]]
                for mid, fn in (("CMS816FHIRHHHypo", lambda: cms816(pt, enc, vs, year)),
                                ("CMS871FHIRHHHyper", lambda: cms871(pt, enc, vs, year, a.exclusion_rule, a.diagnosis_rule, a.med_interval_rule, a.prevalence_rule))):
                    r = fn()
                    out.write(json.dumps({"mode": "reference-implementation", "measure": mid, "unit_id": u["unit_id"],
                                          "patient_id": pid, "mp_year": year, "episode": r is not None,
                                          "pops": r["pops"] if r else {}, "obs": r["obs"] if r else {}, "error": None,
                                          "rules": {"exclusion": a.exclusion_rule, "diagnosis": a.diagnosis_rule, "med_interval": a.med_interval_rule, "prevalence": a.prevalence_rule}}) + "\n")
                n += 1
    print(f"reference implementation: {n} stays x 2 measures -> {a.out} (rules: exclusion={a.exclusion_rule}, diagnosis={a.diagnosis_rule}, med_interval={a.med_interval_rule}, prevalence={a.prevalence_rule})")


if __name__ == "__main__":
    main()
