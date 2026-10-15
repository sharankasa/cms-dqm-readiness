#!/usr/bin/env python3
"""
run_sql.py — run a SQL file in Athena statement by statement (AWS CLI + standard library only) and
print / save the results of every SELECT. Uses the AWS credentials already in the shell.

  python3 pipeline/athena/run_sql.py --sql <file.sql> --out <dir> [--db <database>] \
      [--param key=value ...] [--workgroup primary] [--bucket your-bucket]

{key} placeholders in the SQL are filled from --param. Statements end with ';' at line end.
SELECT results go to <out>/<n>_<first words>.csv and are printed (first 40 rows). Every run writes
<out>/run.json (statements, data scanned, engine time). Results are aggregates by design: only run
SQL whose SELECTs return counts or terminology, never patient rows.
"""
import argparse
import csv
import datetime
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "prepare"))
from athena_run import Athena, aws  # noqa: E402


def statements(path, params):
    text = open(path).read()
    for k, v in params.items():
        text = text.replace("{" + k + "}", v)
    left = re.findall(r"\{[a-z_]+\}", text)
    if left:
        sys.exit(f"unfilled placeholders: {sorted(set(left))}")
    out, buf = [], []
    for line in text.splitlines():
        if line.strip().startswith("--"):
            continue
        buf.append(line)
        if line.rstrip().endswith(";"):
            out.append("\n".join(buf).strip().rstrip(";"))
            buf = []
    if "".join(buf).strip():
        out.append("\n".join(buf).strip())
    return [s for s in out if s]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sql", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--db")
    ap.add_argument("--param", action="append", default=[])
    ap.add_argument("--workgroup", default="primary")
    ap.add_argument("--bucket", default="your-bucket")
    a = ap.parse_args()
    params = dict(p.split("=", 1) for p in a.param)
    stmts = statements(a.sql, params)
    os.makedirs(a.out, exist_ok=True)

    wg = aws("athena", "get-work-group", "--work-group", a.workgroup)["WorkGroup"]
    has_loc = bool(wg.get("Configuration", {}).get("ResultConfiguration", {}).get("OutputLocation"))
    at = Athena(a.workgroup, None if has_loc else f"s3://{a.bucket}/dqm/athena-results/")
    log = []
    for i, s in enumerate(stmts, 1):
        words = " ".join(s.split()[:5])
        qid = at.run(s, db=a.db, label=f"{i:02d} {words}")
        entry = {"n": i, "statement_start": words, "query_id": qid}
        if re.match(r"^\s*(SELECT|WITH|SHOW|DESCRIBE)\b", s, re.I):
            head, rows = at.rows(qid)
            name = re.sub(r"[^a-z0-9]+", "_", words.lower()).strip("_")[:40]
            f = os.path.join(a.out, f"{i:02d}_{name}.csv")
            with open(f, "w", newline="") as fh:
                csv.writer(fh).writerows([head] + rows)
            entry["rows"] = len(rows)
            print("     " + " | ".join(head))
            for r in rows[:40]:
                print("     " + " | ".join("" if x is None else str(x) for x in r))
            if len(rows) > 40:
                print(f"     ... {len(rows)} rows, see {f}")
        log.append(entry)
    json.dump({"sql": a.sql, "params": params, "db": a.db, "finished_utc":
               datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
               "queries": at.n, "data_scanned_mb": round(at.scanned / 1e6, 1),
               "engine_seconds": round(at.engine_ms / 1000, 1), "statements": log},
              open(os.path.join(a.out, "run.json"), "w"), indent=2)
    print(f"done: {at.n} queries, {at.scanned / 1e6:.1f} MB scanned -> {a.out}")


if __name__ == "__main__":
    main()
