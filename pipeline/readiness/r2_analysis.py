#!/usr/bin/env python3
"""
r2_analysis.py — R2 (registration §4.5, §6): for every mapping variant and readiness scenario, the change in each
measure's result against the corrected baseline (2017-2019 cohort, mapping a), in Athena. Row-level results
stay in S3; only aggregates come back; counts 1-9 are shown as <10, and a rate whose event count is 1-9 is
suppressed.

  cd ".../project2-dqm-infrastructure"; export AWS_PROFILE=your-aws-profile
  python3 pipeline/readiness/r2_analysis.py                      # every set with finished results
  python3 pipeline/readiness/r2_analysis.py --only CHANGED_B SCENARIO_STATUS_LOWER

Inputs: the baseline engine run (COHORT_A: <root>/runs/engine-sliced, all 88,234 cohort stays) and each set's run
on its changed stays only (CHANGED_B/C/D, SCENARIO_*); a stay without a scenario result has the baseline result
(the engine is deterministic and its input did not change).
Per set and measure:
  rates      CMS816 (proportion): numerator / denominator. CMS871 (ratio): hyperglycemic event days / eligible
             days over the measure population (denominator minus exclusions), per 100 eligible days. Point estimates
             on all stays; 95% CI of each rate and of the difference (percentage points) by a stay-level PAIRED
             bootstrap, 2,000 resamples (Poisson bootstrap: each stay gets the same Poisson(1) weight in the baseline
             and the scenario within a resample; seed dqm-r2-bootstrap-20261014). CMS816 rates also get a Wilson 95%
             interval (registration §6).
  membership stays entering and leaving each population (initial population, denominator, denominator exclusion,
             numerator, numerator exclusion), with the baseline size; CMS871: stays whose eligible or event days
             change
  components among stays whose result moved: which of the measure's named expressions (engine output `defs`)
             changed, and in how many of them (the measure components responsible, registration §4.5)
Writes results/r2-analysis-<stamp>/ (CSV per section + summary.json).
"""
import argparse
import csv
import datetime
import glob
import json
import math
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(ROOT, "pipeline", "prepare"))
from athena_run import Athena, aws  # noqa: E402

BUCKET = "your-bucket"
DB = "dqm_readiness"
B = 2000
SEED = "dqm-r2-bootstrap-20261014"
KIND = {
    "CHANGED_B": "registered: medication mapping (b) conservative", "CHANGED_C": "registered: medication mapping (c) liberal",
    "CHANGED_D": "registered: medication mapping (d) model-proposed",
    "SCENARIO_STATUS_LOWER": "registered sensitivity: corrected medication status, lower bound (deviation 2)",
    "SCENARIO_STATUS_UPPER": "registered sensitivity: corrected medication status, upper bound (deviation 2)",
    "SCENARIO_OBSERVATION_AS_INPATIENT": "exploratory (deviation 6): observation stays counted as inpatient",
    "SCENARIO_ICU_GLUCOSE_UNIT_ASSUMED": "exploratory (deviation 6): ICU glucose unit from the item dictionary",
    "SCENARIO_GLUCOSE_LAB_ONLY": "registered sensitivity (registration §7): glucose source, laboratory results only",
}


def kind_of(name):
    if name in KIND:
        return KIND[name]
    if name.startswith("SCENARIO_NATURAL_"):
        return "registered curve, 100% (as exported): " + name[len("SCENARIO_NATURAL_"):].lower()
    if name.startswith("SCENARIO_"):
        t, _, r = name[len("SCENARIO_"):].rpartition("_")
        return f"registered curve, injected {r}%: " + t.lower().replace("_", "-")
    return "?"


def mask(v):
    try:
        n = int(v)
    except (TypeError, ValueError):
        return v
    return "<10" if 1 <= n <= 9 else str(n)


def sets():
    found = {}
    for f in sorted(glob.glob(os.path.join(ROOT, "results", "readiness-inputs-*", "inputs.json"))):
        for s in json.load(open(f)).get("sets", []):
            if s.get("stays") and (s["set"].startswith(("cohort-", "changed-", "scenario-"))):
                found[s["set"].upper().replace("-VS-A", "").replace("-", "_")] = s
    return found


def parsed(name, table):
    return f"""
  SELECT '{name}' AS scenario, measure, unit_id, err, defs,
         ep AND coalesce(element_at(pops, 'InitialPopulation_1'), false) AS ip,
         ep AND coalesce(element_at(pops, 'Denominator_1'), false) AS den,
         ep AND coalesce(element_at(pops, 'DenominatorExclusion_1'), false) AS dex,
         ep AND coalesce(element_at(pops, 'Numerator_1'), false) AS num,
         ep AND coalesce(element_at(pops, 'NumeratorExclusion_1'), false) AS nex,
         IF(ep AND coalesce(element_at(pops, 'Denominator_1'), false) AND NOT coalesce(element_at(pops, 'DenominatorExclusion_1'), false),
            reduce(coalesce(try(CAST(element_at(obs, 'MeasureObservation_1_1') AS ARRAY(DOUBLE))), CAST(ARRAY[] AS ARRAY(DOUBLE))), 0e0, (s, x) -> s + coalesce(x, 0e0), s -> s), 0e0) AS elig,
         IF(ep AND coalesce(element_at(pops, 'Denominator_1'), false) AND NOT coalesce(element_at(pops, 'DenominatorExclusion_1'), false),
            reduce(coalesce(try(CAST(element_at(obs, 'MeasureObservation_1_2') AS ARRAY(DOUBLE))), CAST(ARRAY[] AS ARRAY(DOUBLE))), 0e0, (s, x) -> s + coalesce(x, 0e0), s -> s), 0e0) AS ev
  FROM (
    SELECT IF(json_extract_scalar(j, '$.measure') LIKE 'CMS816%', 'CMS816', 'CMS871') AS measure,
           json_extract_scalar(j, '$.unit_id') AS unit_id, json_extract_scalar(j, '$.error') AS err,
           coalesce(try(CAST(json_extract(j, '$.episode') AS boolean)), false) AS ep,
           coalesce(try(CAST(json_extract(j, '$.pops') AS MAP(VARCHAR, BOOLEAN))), CAST(MAP() AS MAP(VARCHAR, BOOLEAN))) AS pops,
           coalesce(try(CAST(json_extract(j, '$.obs') AS MAP(VARCHAR, JSON))), CAST(MAP() AS MAP(VARCHAR, JSON))) AS obs,
           json_format(json_extract(j, '$.defs')) AS defs,
           row_number() OVER (PARTITION BY json_extract_scalar(j, '$.measure'), json_extract_scalar(j, '$.unit_id') ORDER BY 1) AS rn
    FROM (SELECT try(json_parse(line)) AS j FROM {table} WHERE trim(line) <> '') WHERE j IS NOT NULL
  ) WHERE rn = 1"""


def joined(t, s, m):
    return f"""j AS (
  SELECT b.unit_id,
         b.ip AS b_ip, b.den AS b_den, b.dex AS b_dex, b.num AS b_num, b.nex AS b_nex, b.elig AS b_elig, b.ev AS b_ev, b.defs AS b_defs,
         coalesce(x.ip, b.ip) AS s_ip, coalesce(x.den, b.den) AS s_den, coalesce(x.dex, b.dex) AS s_dex,
         coalesce(x.num, b.num) AS s_num, coalesce(x.nex, b.nex) AS s_nex, coalesce(x.elig, b.elig) AS s_elig,
         coalesce(x.ev, b.ev) AS s_ev, coalesce(x.defs, b.defs) AS s_defs, x.unit_id IS NOT NULL AS rerun
  FROM {t} b LEFT JOIN {t} x ON x.scenario = '{s}' AND x.measure = b.measure AND x.unit_id = b.unit_id
  WHERE b.scenario = 'COHORT_A' AND b.measure = '{m}'
)"""


def contrib(m, side):
    if m == "CMS816":
        return (f"IF({side}_den AND NOT {side}_dex AND {side}_num, 1e0, 0e0)", f"IF({side}_den AND NOT {side}_dex, 1e0, 0e0)")
    return (f"{side}_ev", f"{side}_elig")


def wilson(k, n):
    if n == 0:
        return None, None
    z, p = 1.959964, k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return c - h, c + h


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", nargs="+", default=[])
    ap.add_argument("--workgroup", default="primary")
    a = ap.parse_args()
    found = sets()
    if "COHORT_A" not in found:
        sys.exit("no cohort-a set recorded (build_inputs.py)")
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = os.path.join(ROOT, "results", f"r2-analysis-{stamp}")
    os.makedirs(out, exist_ok=True)
    print(f"AWS identity: {aws('sts', 'get-caller-identity')['Arn']}")
    wg = aws("athena", "get-work-group", "--work-group", a.workgroup)["WorkGroup"]
    cfg = wg.get("Configuration", {})
    out_loc = None if cfg.get("ResultConfiguration", {}).get("OutputLocation") else f"s3://{BUCKET}/dqm/athena-results/"
    at = Athena(a.workgroup, out_loc)
    t0 = time.time()
    run = lambda sql, label: at.run(sql, db=DB, label=label)   # noqa: E731

    def q(sql, label):
        hdr, rows = at.rows(run(sql, label))
        return [dict(zip(hdr, r)) for r in rows]

    names = ["COHORT_A"] + [n for n in sorted(found) if n != "COHORT_A" and (not a.only or n in a.only)]
    have = []
    for n in names:
        res = found[n]["input"].rsplit("/", 1)[0] + "/runs/engine-sliced/results/"
        r = subprocess.run(["aws", "s3", "ls", res], capture_output=True, text=True)
        if r.returncode == 0 and r.stdout.strip():
            have.append(n)
        else:
            print(f"  {n}: no results yet under {res} (skipped)")
    if "COHORT_A" not in have:
        sys.exit("the baseline (COHORT_A) has no results yet")
    for n in have:
        res = found[n]["input"].rsplit("/", 1)[0] + "/runs/engine-sliced/results/"
        run(f"DROP TABLE IF EXISTS r2raw_{n.lower()}", f"drop (metadata only) r2raw_{n.lower()}")
        run(f"""CREATE EXTERNAL TABLE r2raw_{n.lower()} (line string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\001'
                LINES TERMINATED BY '\\n' STORED AS TEXTFILE LOCATION '{res}'""", f"results of {n}")
    t = f"r2_results_{stamp.lower()}"
    run(f"DROP TABLE IF EXISTS {t}", f"drop (metadata only) {t}")
    run(f"CREATE TABLE {t} WITH (format = 'PARQUET', write_compression = 'SNAPPY', "
        f"external_location = 's3://{BUCKET}/dqm/readiness/r2-analysis-{stamp}/results/') AS "
        + " UNION ALL ".join(parsed(n, f"r2raw_{n.lower()}") for n in have), "normalised results (all sets)")
    chk = q(f"""SELECT scenario, measure, count(*) AS stays, count(err) AS errors FROM {t} GROUP BY 1, 2 ORDER BY 1, 2""",
            "results per set")
    print("\nresults per set (stays with a result; engine errors):")
    for c in chk:
        print(f"  {c['scenario']:<38} {c['measure']}  {mask(c['stays']):>8}  errors {mask(c['errors'])}")

    rates, members, comps = [], [], []
    for s in have:
        for m in ("CMS816", "CMS871"):
            bn, bd = contrib(m, "b")
            sn, sd = contrib(m, "s")
            jj = joined(t, s, m)
            pt = q(f"""WITH {jj} SELECT sum({bn}) AS b_num, sum({bd}) AS b_den, sum({sn}) AS s_num, sum({sd}) AS s_den,
                       count_if(rerun) AS rerun_stays FROM j""", f"{s} {m}: rates")[0]
            bnum, bden, snum, sden = (float(pt[k] or 0) for k in ("b_num", "b_den", "s_num", "s_den"))
            row = {"set": s, "kind": kind_of(s), "measure": m, "rerun_stays": int(pt["rerun_stays"])}
            scale = 100.0
            row.update({"baseline_numerator": bnum, "baseline_denominator": bden, "scenario_numerator": snum, "scenario_denominator": sden,
                        "baseline_rate": (100 * bnum / bden) if bden else None, "scenario_rate": (100 * snum / sden) if sden else None})
            if True:   # the baseline row bootstraps its own CIs (scenario = baseline, difference 0)
                bs = q(f"""WITH {jj},
                    rel AS (SELECT unit_id, {bn} AS bn, {bd} AS bd, {sn} AS sn, {sd} AS sd FROM j WHERE {bd} > 0 OR {sd} > 0),
                    w AS (SELECT r.rep, rel.*, CASE
                             WHEN u < 0.367879441 THEN 0 WHEN u < 0.735758882 THEN 1 WHEN u < 0.919698603 THEN 2
                             WHEN u < 0.981011843 THEN 3 WHEN u < 0.996340153 THEN 4 WHEN u < 0.999405815 THEN 5 ELSE 6 END AS wt
                          FROM rel CROSS JOIN UNNEST(sequence(1, {B})) AS r(rep)
                          CROSS JOIN UNNEST(ARRAY[CAST(mod(mod(from_big_endian_64(xxhash64(to_utf8(rel.unit_id || '|' || CAST(r.rep AS varchar) || '|{SEED}'))), 1000000) + 1000000, 1000000) AS double) / 1e6]) AS uu(u)),
                    agg AS (SELECT rep, {scale} * sum(wt * bn) / nullif(sum(wt * bd), 0) AS br, {scale} * sum(wt * sn) / nullif(sum(wt * sd), 0) AS sr
                            FROM w GROUP BY rep)
                    SELECT element_at(array_sort(array_agg(sr - br)), 50) AS d_lo, element_at(array_sort(array_agg(sr - br)), 1951) AS d_hi,
                           element_at(array_sort(array_agg(br)), 50) AS b_lo, element_at(array_sort(array_agg(br)), 1951) AS b_hi,
                           element_at(array_sort(array_agg(sr)), 50) AS s_lo, element_at(array_sort(array_agg(sr)), 1951) AS s_hi,
                           count(*) AS reps FROM agg WHERE br IS NOT NULL AND sr IS NOT NULL""", f"{s} {m}: paired bootstrap")[0]
                row.update({k: (float(bs[k]) if bs[k] is not None else None) for k in ("d_lo", "d_hi", "b_lo", "b_hi", "s_lo", "s_hi")})
                row["bootstrap_resamples"] = int(bs["reps"])
            if s != "COHORT_A":
                mem = q(f"""WITH {jj}
                    SELECT pop, count_if(b) AS baseline_stays, count_if(s) AS scenario_stays,
                           count_if(s AND NOT b) AS entering, count_if(b AND NOT s) AS leaving
                    FROM j CROSS JOIN UNNEST(ARRAY['initial population', 'denominator', 'denominator exclusion', 'numerator', 'numerator exclusion'],
                                             ARRAY[b_ip, b_den, b_dex, b_num, b_nex], ARRAY[s_ip, s_den, s_dex, s_num, s_nex]) AS u(pop, b, s)
                    GROUP BY pop
                    UNION ALL
                    SELECT 'observation values changed (CMS871 days)', count_if(b_elig > 0), count_if(s_elig > 0),
                           count_if(b_elig <> s_elig OR b_ev <> s_ev), NULL FROM j WHERE '{m}' = 'CMS871'""", f"{s} {m}: membership")
                for x in mem:
                    members.append({"set": s, "measure": m, **x})
                cp = q(f"""WITH {jj},
                    moved AS (SELECT * FROM j WHERE rerun AND (b_ip <> s_ip OR b_den <> s_den OR b_dex <> s_dex OR b_num <> s_num
                                                             OR b_nex <> s_nex OR b_elig <> s_elig OR b_ev <> s_ev)),
                    kv AS (SELECT m2.unit_id, k, json_format(element_at(bm, k)) AS bv, json_format(element_at(sm, k)) AS sv
                           FROM (SELECT unit_id,
                                        coalesce(try(CAST(json_parse(b_defs) AS MAP(VARCHAR, JSON))), CAST(MAP() AS MAP(VARCHAR, JSON))) AS bm,
                                        coalesce(try(CAST(json_parse(s_defs) AS MAP(VARCHAR, JSON))), CAST(MAP() AS MAP(VARCHAR, JSON))) AS sm
                                 FROM moved) m2
                           CROSS JOIN UNNEST(array_distinct(map_keys(bm) || map_keys(sm))) AS kk(k))
                    SELECT k AS definition, count_if(coalesce(bv, '') <> coalesce(sv, '')) AS moved_stays_where_changed,
                           (SELECT count(*) FROM moved) AS moved_stays
                    FROM kv GROUP BY k HAVING count_if(coalesce(bv, '') <> coalesce(sv, '')) > 0
                    ORDER BY 2 DESC LIMIT 12""", f"{s} {m}: components")
                for x in cp:
                    comps.append({"set": s, "measure": m, **x})
            if m == "CMS816":
                for side in ("baseline", "scenario"):
                    lo, hi = wilson(row[f"{side}_numerator"], row[f"{side}_denominator"])
                    row[f"{side}_wilson_lo"], row[f"{side}_wilson_hi"] = (100 * lo, 100 * hi) if lo is not None else (None, None)
            rates.append(row)

    def show_rate(r, side):
        k = r[f"{side}_numerator"]
        if r["measure"] == "CMS816" and 1 <= k <= 9:
            return "suppressed (<10 events)"
        v = r[f"{side}_rate"]
        return "n/a" if v is None else f"{v:.3f}"

    print("\nR2 rates (CMS816 % of denominator stays with a severe hypoglycemic event; CMS871 hyperglycemic event days per 100 "
          "eligible days); difference in percentage points with 95% paired bootstrap CI:")
    small = lambda r: r["measure"] == "CMS816" and any(1 <= r[f"{x}_numerator"] <= 9 for x in ("baseline", "scenario"))  # noqa: E731
    for r in rates:
        d = "" if r["set"] == "COHORT_A" or r.get("d_lo") is None else "  diff suppressed (<10 events)" if small(r) else \
            (f"  diff {((r['scenario_rate'] or 0) - (r['baseline_rate'] or 0)):+.3f} (95% CI {r['d_lo']:+.3f} to {r['d_hi']:+.3f})")
        print(f"  {r['set']:<38} {r['measure']}  base {show_rate(r, 'baseline'):>10}  scenario {show_rate(r, 'scenario'):>10}"
              f"  (den {mask(int(r['scenario_denominator']))}){d}  re-run {mask(r['rerun_stays'])}")
    print("\nmembership (stays entering / leaving each population; counts 1-9 as <10):")
    for x in members:
        print(f"  {x['set']:<38} {x['measure']} {x['pop']:<42} base {mask(x['baseline_stays']):>7}  scen {mask(x['scenario_stays']):>7}  "
              f"+{mask(x['entering'])} -{mask(x['leaving']) if x['leaving'] is not None else ''}")
    print("\ncomponents changed in moved stays (top definitions):")
    for x in comps:
        print(f"  {x['set']:<38} {x['measure']} {x['definition'][:60]:<60} {mask(x['moved_stays_where_changed']):>7} of {mask(x['moved_stays'])}")

    def write(name, rows):
        if not rows:
            return
        keys = list(dict.fromkeys(k for r in rows for k in r))
        with open(os.path.join(out, name), "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            for r in rows:
                w.writerow({k: (mask(r.get(k)) if isinstance(r.get(k), str) and r.get(k, "").isdigit() else r.get(k)) for k in keys})
    safe_rates = []
    for r in rates:
        r2 = dict(r)
        if small(r):
            for k in ("d_lo", "d_hi", "b_lo", "b_hi", "s_lo", "s_hi"):
                r2[k] = "suppressed"
        for side in ("baseline", "scenario"):
            if r["measure"] == "CMS816" and 1 <= r[f"{side}_numerator"] <= 9:
                for k in (f"{side}_rate", f"{side}_numerator", f"{side}_wilson_lo", f"{side}_wilson_hi"):
                    r2[k] = "suppressed"
        safe_rates.append(r2)
    write("rates.csv", safe_rates)
    write("membership.csv", members)
    write("components.csv", comps)
    json.dump({"stamp": stamp, "results_table": f"{DB}.{t}", "sets": have, "bootstrap": {"resamples": B, "kind": "Poisson, paired by stay", "seed": SEED},
               "athena": {"queries": at.n, "data_scanned_mb": round(at.scanned / 1e6, 1)}, "wall_seconds": round(time.time() - t0, 1)},
              open(os.path.join(out, "summary.json"), "w"), indent=2)
    print(f"\nAthena: {at.n} queries, {at.scanned / 1e9:.1f} GB scanned (~${at.scanned / 1e12 * 5:.2f}); saved (aggregates only): {out}")


if __name__ == "__main__":
    main()
