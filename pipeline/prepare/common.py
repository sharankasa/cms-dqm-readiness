"""common.py — shared by the DuckDB driver (slice_stays.py) and the Athena runner (athena_run.py).
Standard library only (the Athena runner must work without DuckDB installed)."""
import base64
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))


def sql_file(rel, **params):
    """Read sql/<rel>, fill {placeholders}, split into statements (';' at line end; '--' comments dropped)."""
    with open(os.path.join(HERE, "sql", rel)) as f:
        text = f.read()
    for k, v in params.items():
        text = text.replace("{" + k + "}", str(v))
    stmts, buf = [], []
    for line in text.splitlines():
        if line.strip().startswith("--"):
            continue
        buf.append(line)
        if line.rstrip().endswith(";"):
            stmts.append("\n".join(buf).strip().rstrip(";"))
            buf = []
    if "".join(buf).strip():
        stmts.append("\n".join(buf).strip())
    return [s for s in stmts if s]


def vs_members_from(bundles, vs_cache):
    """(oid, system, code) for every value set the measures declare: bundle expansions + local VSAC cache
    (the prepare stage workload metric needs to know which value set a code belongs to). Same completeness check."""
    rows, declared, present = set(), set(), set()
    for path in bundles:
        b = json.load(open(path))
        for e in b["entry"]:
            r = e["resource"]
            if r["resourceType"] == "ValueSet":
                present.add(r["url"])
                for c in r.get("expansion", {}).get("contains", []):
                    rows.add((r["url"].split("/")[-1], c.get("system"), c.get("code")))
            if r["resourceType"] == "Library":
                for c in r.get("content", []):
                    if c.get("contentType") == "text/cql" and c.get("data"):
                        cql = base64.b64decode(c["data"]).decode()
                        declared.update(re.findall(r"'(http://cts\.nlm\.nih\.gov/fhir/ValueSet/[0-9.]+)'", cql))
    for url in sorted(declared - present):
        p = os.path.join(vs_cache or "", url.split("/")[-1] + ".json")
        if not (vs_cache and os.path.exists(p)):
            sys.exit(f"value set missing: {url} (pass --vs-cache)")
        for c in json.load(open(p)).get("expansion", {}).get("contains", []):
            rows.add((url.split("/")[-1], c.get("system"), c.get("code")))
    return sorted(r for r in rows if r[1] and r[2])


def vs_codes_from(bundles, vs_cache):
    """(system, code) pairs of every value set the measures declare: bundle expansions + local VSAC cache.
    Exits if a declared value set is missing (a filter without it could drop needed records)."""
    codes, declared, present = set(), set(), set()
    for path in bundles:
        b = json.load(open(path))
        for e in b["entry"]:
            r = e["resource"]
            if r["resourceType"] == "ValueSet":
                present.add(r["url"])
                for c in r.get("expansion", {}).get("contains", []):
                    codes.add((c.get("system"), c.get("code")))
            if r["resourceType"] == "Library":
                for c in r.get("content", []):
                    if c.get("contentType") == "text/cql" and c.get("data"):
                        cql = base64.b64decode(c["data"]).decode()
                        declared.update(re.findall(r"'(http://cts\.nlm\.nih\.gov/fhir/ValueSet/[0-9.]+)'", cql))
    added = []
    for url in sorted(declared - present):
        p = os.path.join(vs_cache or "", url.split("/")[-1] + ".json")
        if vs_cache and os.path.exists(p):
            for c in json.load(open(p)).get("expansion", {}).get("contains", []):
                codes.add((c.get("system"), c.get("code")))
            added.append(url)
    still = sorted(declared - present - set(added))
    if still:
        sys.exit(f"value sets missing for the filter: {still} (pass --vs-cache)")
    return sorted(c for c in codes if c[0] and c[1]), added
