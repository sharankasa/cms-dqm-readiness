#!/usr/bin/env python3
"""
test_balance.py — offline test of 70_balance.sql on SYNTHETIC data (no MIMIC, no AWS): the Athena SQL
is translated to DuckDB with SQLGlot and run on made-up patients, pilot timings and value sets.
Checks: the runtime fit recovers a known exponent despite a fixed start-up cost (which flattens a fit on
all groups: the first real run's problem), and its check on the pilot is within 2x per band; every stay lands in exactly one shard; shards are
numbered without gaps; the highest-work groups (and any above an even share) sit alone, heaviest =
shard 0, even when the model sees no difference between groups; the rest are
balanced; a patient split across shards gets their record in each shard (reference export join).

  python3 pipeline/s1/test_balance.py        (needs duckdb + sqlglot)
"""
import json
import math
import os
import random
import re
import sys

import duckdb
import sqlglot

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from rebalance_run import makespan  # noqa: E402

GLU, HYPO = "2.16.840.1.113762.1.4.1248.34", "2.16.840.1.113762.1.4.1196.393"
SHARDS = 16
ALONE = 3
FIT_MIN_WORK = 1000
FIT_BINS = 10


def parts():
    text = open(os.path.join(HERE, "sql", "70_balance.sql")).read()
    text = text.replace("{bal}", "s3://test/bal").replace("{pilot_db}", "pilot").replace("{shards}", str(SHARDS))
    text = text.replace("{alone}", str(ALONE)).replace("{fit_min_work}", str(FIT_MIN_WORK)).replace("{fit_bins}", str(FIT_BINS))
    out, buf, part = {}, [], None
    for line in text.splitlines():
        s = line.strip()
        if s.startswith("-- part "):
            part = s[3:]
            continue
        if s.startswith("--"):
            continue
        buf.append(line)
        if line.rstrip().endswith(";"):
            out.setdefault(part, []).append("\n".join(buf).strip().rstrip(";"))
            buf = []
    return out


def to_duck(sql):
    sql = re.sub(r"\s*WITH \(format = [^)]*\) AS", " AS", sql)     # Athena table properties: not needed here
    return sqlglot.transpile(sql, read="athena", write="duckdb")[0]


def make_records(con, schema, rng, patients):
    """ref_lines rows: Observation (glucose) and MedicationAdministration (hypoglycemic) per patient."""
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(f"CREATE TABLE {schema}.vs_members (oid VARCHAR, system VARCHAR, code VARCHAR)")
    con.execute(f"INSERT INTO {schema}.vs_members VALUES ('{GLU}', 'http://loinc.org', '2345-7'),"
                f" ('{HYPO}', 'http://www.nlm.nih.gov/research/umls/rxnorm', '253182'),"
                f" ('other', 'http://loinc.org', '0000-0')")
    rows = []
    for pid, (g, m) in patients.items():
        for i in range(g):
            rows.append((pid, "Observation", f"{pid}-o{i}", json.dumps(
                {"resourceType": "Observation", "code": {"coding": [{"system": "http://loinc.org", "code": "2345-7"}]}})))
        for i in range(m):
            rows.append((pid, "MedicationAdministration", f"{pid}-m{i}", json.dumps(
                {"resourceType": "MedicationAdministration", "medicationCodeableConcept": {"coding": [
                    {"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "253182"}]}})))
        rows.append((pid, "Observation", f"{pid}-x", json.dumps(
            {"resourceType": "Observation", "code": {"coding": [{"system": "http://loinc.org", "code": "0000-0"}]}})))
    rows.append((None, "Medication", "shared-1", json.dumps({"resourceType": "Medication"})))
    con.execute(f"CREATE TABLE {schema}.ref_lines (patient_id VARCHAR, rtype VARCHAR, id VARCHAR, line VARCHAR)")
    con.executemany(f"INSERT INTO {schema}.ref_lines VALUES (?, ?, ?, ?)", rows)


def main():
    rng = random.Random(7)
    con = duckdb.connect()
    p = parts()

    # ---- pilot: 1,500 patient-year groups (about 250 in the fit range, as in the real pilot), seconds = 2 (start-up) + 0.01 x work x lognormal noise (true k = 1),
    #      the noise spreading out as work grows (as in the real pilot: a fit on log times then tracks the
    #      typical group and underpredicts the mean of the heavy bands)
    pilot = {}
    for i in range(1500):
        z = rng.random()
        g, m = ((0, 0) if z < 0.5 else (rng.randint(1, 30), rng.randint(1, 30)) if z < 0.83
                else (rng.randint(30, 600), rng.randint(30, 300)))
        pilot[f"pp{i:04d}"] = (g, m)
    make_records(con, "pilot", rng, pilot)
    res = []
    for pid, (g, m) in pilot.items():
        work = g * m
        for measure, frac in [("CMS816FHIRHHHypo", 0.8), ("CMS871FHIRHHHyper", 0.2)]:
            sd = 0.4 + 0.5 * max(0.0, math.log10(max(work, 1)) - 3)
            base = 2.0 if work == 0 else 2.0 + 0.01 * work * math.exp(rng.gauss(0, sd) - sd * sd / 2)
            res.append(json.dumps({"measure": measure, "patient_id": pid, "mp_year": "2015", "unit_id": f"{pid}-s",
                                   "seconds": round(base * frac, 4)}))
    con.execute("CREATE TABLE pilot.res_ref (line VARCHAR)")
    con.executemany("INSERT INTO pilot.res_ref VALUES (?)", [(r,) for r in res])

    # ---- S1 sample: 300 patients, 1-3 discharge years each, a heavy tail incl. 4 very heavy patients
    s1 = {}
    for i in range(300):
        z = rng.random()
        s1[f"sp{i:04d}"] = ((0, 0) if z < 0.6 else (rng.randint(1, 50), rng.randint(1, 30)) if z < 0.9
                            else (rng.randint(100, 500), rng.randint(50, 400)))
    for j, (g, m) in enumerate([(3000, 2000), (2500, 1500), (2000, 1200), (1500, 900)]):
        s1[f"sp{j:04d}"] = (g, m)
    make_records(con, "main", rng, s1)
    stays = []
    for pid in s1:
        years = rng.sample(["2012", "2013", "2014", "2015"], rng.choice([1, 1, 1, 2, 3]))
        for y in years:
            for k in range(rng.choice([1, 1, 2])):
                stays.append((f"{pid}-{y}-{k}", pid, y, "random", False, False, False, False, -1))
    stays.append(("sp0299-null", "sp0299", None, "random", False, False, False, False, -1))   # mp_year missing
    con.execute("CREATE TABLE sample_stays (unit_id VARCHAR, patient_id VARCHAR, mp_year VARCHAR, reason VARCHAR, "
                "g2_start_extended BOOLEAN, g2_linked_ed BOOLEAN, g3_readmit_24h BOOLEAN, g4_spans_year BOOLEAN, shard INTEGER)")
    con.executemany("INSERT INTO sample_stays VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", stays)

    # ---- run 70_balance.sql (translated)
    for part in ["part fit", "part groups", "part assign"]:
        for s in p[part]:
            con.execute(to_duck(s))
    fit = con.execute("SELECT k, a, smear, n_fit, base0, n_base0, base1, n_base1, n_bins FROM runtime_fit").fetchone()
    naive = con.execute("SELECT regr_slope(ln(s), ln(work)) FROM pilot_groups WHERE work > 0").fetchone()[0]
    print(f"fit: k={fit[0]:.3f} (a fit on all groups would give {naive:.3f}) a={fit[1]:.3f} smear={fit[2]:.3f} "
          f"n_fit={fit[3]} base0={fit[4]:.2f} n_base0={fit[5]} base1={fit[6]:.2f} n_base1={fit[7]}")
    assert abs(fit[0] - 1.0) < 0.25 and fit[8] == FIT_BINS, "exponent of the mean not recovered"
    logfit = con.execute(f"SELECT regr_slope(ln(s), ln(work)) FROM pilot_groups WHERE work >= {FIT_MIN_WORK}").fetchone()[0]
    print(f"   (a fit on log times over the same range: k={logfit:.3f})")
    assert naive < fit[0] - 0.1, "start-up cost should flatten the all-groups fit (the case this guards against)"
    assert abs(fit[4] - 2.0) < 1e-6 and fit[3] + fit[5] + fit[7] == 1500

    summ = [con.execute(to_duck(s)).fetchall() for s in p["part summary"]]
    chk = summ[0][0]
    print("checks:", chk)
    n_st, n_bal, n_dist, n_grp, n_asg, n_sh, n_max, n_own, n_pairs, n_pat = chk
    assert n_st == n_bal == n_dist == len(stays), "every stay exactly once"
    assert n_grp == n_asg and n_sh == n_max <= SHARDS, "shards without gaps"
    sh = con.execute("SELECT shard, count(*), sum(pred_s), bool_or(heavy), bool_and(heavy) FROM group_shard "
                     "GROUP BY 1 ORDER BY 1").fetchall()
    heavy = [r for r in sh if r[3]]
    assert all(r[1] == 1 and r[4] for r in heavy), "heavy groups alone"
    assert [r[0] for r in heavy] == list(range(len(heavy))), "heavy groups first"
    assert all(heavy[i][2] >= heavy[i + 1][2] for i in range(len(heavy) - 1)), "heaviest = shard 0"
    light = [r[2] for r in sh if not r[3]]
    total = sum(r[2] for r in sh)
    print(f"{len(heavy)} groups alone; {len(light)} shared shards; light loads {min(light):.0f}-{max(light):.0f} s "
          f"(target {sum(light) / len(light):.0f} s); total {total:.0f} s")
    assert max(light) <= 1.5 * sum(light) / len(light), "light shards balanced"
    assert n_sh == SHARDS, "all shards used"
    split = con.execute("SELECT count(*) FROM (SELECT patient_id FROM sample_patients_bal GROUP BY 1 HAVING count(*) > 1)").fetchone()[0]
    ref_rows = con.execute("SELECT count(*) FROM ref_lines l JOIN sample_patients_bal p ON p.patient_id = l.patient_id").fetchone()[0]
    own_rows = con.execute("SELECT count(*) FROM ref_lines WHERE patient_id IS NOT NULL").fetchone()[0]
    print(f"patients split across shards: {split}; reference lines exported {ref_rows} (unsplit would be {own_rows})")
    assert split > 0 and ref_rows > own_rows
    nul = con.execute("SELECT shard FROM sample_stays_bal WHERE unit_id = 'sp0299-null'").fetchall()
    assert len(nul) == 1, "stay without mp_year kept"
    loads = [r[2] / 3600 for r in sh]
    for pods in [2, 4, 8]:
        print(f"  {pods} pods: wall {makespan(loads, pods):.2f} h (lower bound {max(max(loads), sum(loads) / pods):.2f} h)")
        assert makespan(loads, pods) <= 1.34 * max(max(loads), sum(loads) / pods) + 1e-9
    print("per-shard summary rows:", len(summ[2]), "| first:", summ[2][0])
    print("model check on the pilot (band, groups, obs mean, pred mean, obs h, pred h):")
    for r in summ[3]:
        print("   ", r)
        if r[1] != "<10" and r[0] >= "2":
            assert 0.5 <= r[5] / r[4] <= 2.0, f"model check band {r[0]}"
    print("S1 bands:", summ[4])
    top = con.execute("SELECT g.shard FROM group_shard g JOIN group_pred p USING (patient_id, mp_year) "
                      f"ORDER BY p.work DESC, p.patient_id LIMIT {ALONE}").fetchall()
    assert sorted(r[0] for r in top) == list(range(ALONE)), "highest-work groups in shards 0..ALONE-1"

    # ---- the model sees no difference (all predictions equal): the highest-work groups still go alone, first
    con.execute("CREATE OR REPLACE TABLE group_pred AS SELECT patient_id, mp_year, n_stays, work, 5.0 AS pred_s FROM group_pred")
    for tname in ["sample_patients_bal", "sample_stays_bal", "group_shard"]:
        con.execute(f"DROP TABLE {tname}")
    for s in p["part assign"]:
        con.execute(to_duck(s))
    own = con.execute("SELECT count_if(heavy) FROM group_shard").fetchone()[0]
    top = con.execute("SELECT g.shard, g.heavy FROM group_shard g JOIN group_pred p USING (patient_id, mp_year) "
                      f"ORDER BY p.work DESC, p.patient_id LIMIT {ALONE}").fetchall()
    print(f"flat predictions: {own} groups alone; highest-work groups in shards {[r[0] for r in top]}")
    assert own == ALONE and sorted(r[0] for r in top) == list(range(ALONE)) and all(r[1] for r in top)
    print("balance test: PASS")


if __name__ == "__main__":
    main()
