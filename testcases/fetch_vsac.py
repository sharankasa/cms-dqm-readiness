#!/usr/bin/env python3
"""
fetch_vsac.py — download expansions of the value sets a measure declares but CMS's bundle does not ship,
from NLM's VSAC FHIR terminology service, into a local cache. Requires your UMLS API key.

  export UMLS_API_KEY=...        # typed in your own terminal, from 1Password; never pasted into chat or files
  python3 fetch_vsac.py --repo vendor/dqm-content --measure CMS871FHIRHHHyper --cache vendor/vsac-cache
  python3 fetch_vsac.py --repo vendor/dqm-content --measure CMS816FHIRHHHypo  --cache vendor/vsac-cache

UMLS license: the cache is for the licensee's own use. It must never be committed or redistributed
vendor/ is kept out of the repository.
Prints only OIDs, names, code counts and the expansion timestamp (no codes).
"""
import argparse
import base64
import json
import os
import re
import sys
import urllib.error
import urllib.request

VSAC = "https://cts.nlm.nih.gov/fhir"


def declared_and_missing(repo, measure):
    bundle = json.load(open(os.path.join(repo, "bundles", "measure", measure, f"{measure}-bundle.json")))
    declared, present = set(), set()
    for e in bundle["entry"]:
        r = e["resource"]
        if r["resourceType"] == "ValueSet":
            present.add(r.get("url"))
        if r["resourceType"] == "Library":
            for c in r.get("content", []):
                if c.get("contentType") == "text/cql" and c.get("data"):
                    cql = base64.b64decode(c["data"]).decode("utf-8", "replace")
                    declared |= set(re.findall(r"'(http://cts\.nlm\.nih\.gov/fhir/ValueSet/[0-9.]+)'", cql))
    return sorted(declared - present)


def expand(oid, key):
    url = f"{VSAC}/ValueSet/{oid}/$expand"
    auth = base64.b64encode(f"apikey:{key}".encode()).decode()
    req = urllib.request.Request(url, headers={"Authorization": f"Basic {auth}", "Accept": "application/fhir+json"})
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True)
    ap.add_argument("--measure", required=True)
    ap.add_argument("--cache", required=True)
    a = ap.parse_args()
    key = os.environ.get("UMLS_API_KEY")
    if not key:
        sys.exit("Set UMLS_API_KEY in your terminal first (from 1Password).")
    os.makedirs(a.cache, exist_ok=True)
    missing = declared_and_missing(a.repo, a.measure)
    print(f"{a.measure}: {len(missing)} value set(s) declared in CQL but not in CMS's bundle")
    for url in missing:
        oid = url.rsplit("/", 1)[-1]
        try:
            vs = expand(oid, key)
        except urllib.error.HTTPError as e:
            print(f"  {oid}: HTTP {e.code} ({'check the API key' if e.code in (401, 403) else 'see VSAC'})")
            continue
        vs["url"] = url          # VSAC returns the canonical; keep it identical to the CQL declaration
        vs["id"] = oid
        n = len(vs.get("expansion", {}).get("contains", []))
        json.dump(vs, open(os.path.join(a.cache, f"{oid}.json"), "w"))
        print(f"  {oid}: '{vs.get('name') or vs.get('title')}', {n} codes, "
              f"expansion {vs.get('expansion', {}).get('timestamp')}, version {vs.get('version')}")


if __name__ == "__main__":
    main()
