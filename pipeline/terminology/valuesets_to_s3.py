#!/usr/bin/env python3
"""
valuesets_to_s3.py — put the measures' value-set codes (oid, system, code) into S3 as one TSV, for
Athena joins (mapping coverage, term types). Sources: the CMS measure bundles (expansions shipped by
CMS) and the local VSAC cache for value sets the bundles declare but do not ship. VSAC content is
licensed: it goes only to your own bucket, never into the repo.

  python3 pipeline/terminology/valuesets_to_s3.py --bundles <b1> <b2> --vs-cache <dir> \
      [--bucket your-bucket]
Prints the S3 prefix to pass to the SQL as vsloc.
"""
import argparse
import base64
import datetime
import json
import os
import re
import subprocess
import tempfile

ap = argparse.ArgumentParser()
ap.add_argument("--bundles", nargs="+", required=True)
ap.add_argument("--vs-cache", required=True)
ap.add_argument("--bucket", default="your-bucket")
a = ap.parse_args()

rows, present, declared = set(), set(), set()
for p in a.bundles:
    for e in json.load(open(p))["entry"]:
        r = e["resource"]
        if r["resourceType"] == "ValueSet":
            oid = r["url"].split("/")[-1]
            present.add(oid)
            for c in r.get("expansion", {}).get("contains", []):
                rows.add((oid, c.get("system", ""), c.get("code", "")))
        if r["resourceType"] == "Library":
            for c in r.get("content", []):
                if c.get("contentType") == "text/cql" and c.get("data"):
                    declared.update(u.split("/")[-1] for u in re.findall(
                        r"'(http://cts\.nlm\.nih\.gov/fhir/ValueSet/[0-9.]+)'", base64.b64decode(c["data"]).decode()))
for oid in sorted(declared - present):
    f = os.path.join(a.vs_cache, f"{oid}.json")
    if not os.path.exists(f):
        raise SystemExit(f"value set {oid} missing from the VSAC cache")
    for c in json.load(open(f)).get("expansion", {}).get("contains", []):
        rows.add((oid, c.get("system", ""), c.get("code", "")))

stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
dest = f"s3://{a.bucket}/dqm/ref/valuesets/{stamp}"
with tempfile.TemporaryDirectory() as d:
    f = os.path.join(d, "value_set_codes.tsv")
    with open(f, "w") as fh:
        for r in sorted(rows):
            fh.write("\t".join(r) + "\n")
    subprocess.run(["aws", "s3", "cp", f, f"{dest}/value_set_codes.tsv", "--only-show-errors"], check=True)
print(f"{len(rows)} codes from {len({r[0] for r in rows})} value sets -> {dest}")
print(f"vsloc={dest}")
