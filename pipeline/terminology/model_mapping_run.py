#!/usr/bin/env python3
"""
model_mapping_run.py — mapping variant (d), model-proposed (registration §4.2), step 2 of 3: an open-weight
model in the study's own AWS account (Amazon Bedrock, us-west-2, invocation logging off) decides every
entry of the medication audit worklist, choosing only among the RxNorm candidates retrieved by
42_model_candidates.sql. Same question and rules as the human review (the medication audit guide, not distributed).

Data handling (registration §9; agreed 2026-10-13): worklist entries (MIMIC drug names, code level, no
patients) are read from Athena into memory, sent only to Bedrock in this account, and the answers are
written to S3. Nothing with drug names is printed or written to disk; the local output folder gets
counts, the prompt text (no data) and run metadata only. The model never sees the reviewer's decisions,
the worklist flags or value-set membership.

  python3 pipeline/terminology/model_mapping_run.py --round r1 \
      --model openai.gpt-oss-120b-1:0 --workers 4 [--limit 20]

Writes s3://<bucket>/dqm/map/model-<round>-<model>-<UTC stamp>/model_decisions.csv
(item_id, model, decision, rxcui, confidence, parse_ok, rxcui_valid, input_tokens, output_tokens, ms);
compare with the reviewer's decisions in 43_model_compare.sql. Standard library + AWS CLI only.
"""
import argparse
import concurrent.futures as cf
import csv
import datetime
import hashlib
import io
import json
import os
import re
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(ROOT, "pipeline", "prepare"))
from athena_run import Athena, aws  # noqa: E402

PROMPT_VERSION = "medmap-v1-2026-10-13"
SYSTEM = """You map hospital medication entries to RxNorm for two CMS hospital quality measures about blood sugar \
(severe hypoglycemia and severe hyperglycemia). For each entry, decide whether it is a glucose-lowering drug: \
insulin of any kind, sulfonylurea, meglitinide, metformin, DPP-4 inhibitor, SGLT2 inhibitor, GLP-1 receptor agonist, \
thiazolidinedione, alpha-glucosidase inhibitor, amylin analog, or another drug used to lower blood glucose.

Answer with exactly one JSON object and nothing else:
{"decision": "map" | "possible" | "no", "rxcui": "<one candidate rxcui>" | "none", "confidence": "high" | "medium" | "low"}

Rules:
- "map": it is a glucose-lowering drug and the product is clear. Choose the candidate that matches its \
ingredient(s), strength and dose form. If no candidate matches, use "rxcui": "none".
- "possible": probably glucose-lowering but the entry is ambiguous (combination product, unclear strength or \
form, infusion of unclear concentration). Give your best candidate.
- "no": not a glucose-lowering drug. Use "rxcui": "none".
- For a mixture already mapped through an ingredient (shown as "already mapped to"): "map" keeps it as a dose \
of that drug; "no" drops it. "rxcui" may be "none".
- Only use an rxcui from the candidate list. Never invent codes."""

PART_CONTEXT = {
    "A": "a medication code from the hospital's medication administration record (not yet mapped to any measure drug)",
    "B": "a medication code from the hospital's medication administration record (not yet mapped to any measure drug)",
    "C": "a medication administration record entry whose NDC code did not map to RxNorm",
    "D": "a medication administration record entry without an NDC code",
    "E": "a mixture (e.g. an infusion bag) from the medication administration record",
    "F": "an ICU charting item for a medication infusion or injection",
}
PRICE_PER_M = {"input": 0.15, "output": 0.60}   # USD per million tokens, gpt-oss-120b list price (assumption; check)


def athena_csv(at, sql, db):
    """Run a SELECT and read its full result CSV from S3 into memory (no local file)."""
    qid = at.run(sql, db=db, label=" ".join(sql.split()[:6]))
    loc = aws("athena", "get-query-execution", "--query-execution-id", qid)["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
    r = subprocess.run(["aws", "s3", "cp", loc, "-"], capture_output=True, text=True, check=True)
    return list(csv.DictReader(io.StringIO(r.stdout)))


def prompt_for(item, cands):
    part = item["part"][:1]
    lines = [f"Entry type: {PART_CONTEXT.get(part, 'a medication entry')}",
             f"Entry: {item['names'] or ''} | local code: {item['med_code'] or ''}"]
    if item.get("ndc_path_drugs"):
        lines.append(f"Already mapped to: {item['ndc_path_drugs']}")
    if cands:
        lines.append("Candidates (RxNorm generic products):")
        for c in cands:
            brand = f" (brands: {c['brands']})" if c.get("brands") else ""
            lines.append(f"- rxcui {c['rxcui']}: {c['name']}{brand}")
    else:
        lines.append("Candidates: none found by name.")
    return "\n".join(lines)


def parse(text):
    m = re.search(r"\{[^{}]*\}", text or "", re.S)
    if not m:
        return None
    try:
        d = json.loads(m.group(0))
    except json.JSONDecodeError:
        return None
    dec = str(d.get("decision", "")).strip().lower()
    rx = str(d.get("rxcui", "")).strip().lower()
    conf = str(d.get("confidence", "")).strip().lower()
    if dec not in ("map", "possible", "no"):
        return None
    return {"decision": dec, "rxcui": "" if rx in ("", "none", "null") else rx,
            "confidence": conf if conf in ("high", "medium", "low") else ""}


def converse(model, region, user_text, max_tokens, retries=6):
    args = ["aws", "bedrock-runtime", "converse", "--region", region, "--model-id", model,
            "--system", json.dumps([{"text": SYSTEM}]),
            "--messages", json.dumps([{"role": "user", "content": [{"text": user_text}]}]),
            "--inference-config", json.dumps({"maxTokens": max_tokens, "temperature": 0}),
            "--output", "json"]
    delay = 2
    for attempt in range(retries):
        r = subprocess.run(args, capture_output=True, text=True)
        if r.returncode == 0:
            out = json.loads(r.stdout)
            text = " ".join(c.get("text", "") for c in out["output"]["message"]["content"] if "text" in c)
            u = out.get("usage", {})
            return text, u.get("inputTokens", 0), u.get("outputTokens", 0), out.get("metrics", {}).get("latencyMs", 0)
        err = r.stderr
        if any(k in err for k in ("Throttling", "ServiceUnavailable", "ModelNotReady", "InternalServer", "timed out")):
            time.sleep(delay); delay = min(delay * 2, 60)
            continue
        raise RuntimeError(err.strip().splitlines()[-1][:300] if err.strip() else "converse failed")
    raise RuntimeError("converse: retries exhausted (throttling)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--round", default="r1")
    ap.add_argument("--model", default="openai.gpt-oss-120b-1:0")
    ap.add_argument("--version", default="20261005")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--max-tokens", type=int, default=2000)
    ap.add_argument("--limit", type=int, default=0, help="test: only the first N entries (by sort order)")
    ap.add_argument("--region", default="us-west-2")
    ap.add_argument("--db", default="dqm_map")
    ap.add_argument("--bucket", default="your-bucket")
    ap.add_argument("--workgroup", default="primary")
    a = ap.parse_args()

    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    tag = re.sub(r"[^a-z0-9]+", "-", a.model.lower()).strip("-")
    s3_out = f"s3://{a.bucket}/dqm/map/model-{a.round}-{tag}-{stamp}"
    local = os.path.join(ROOT, "results", f"model-mapping-{a.round}-{tag}-{stamp}")
    os.makedirs(local, exist_ok=True)
    print(f"AWS identity: {aws('sts', 'get-caller-identity')['Arn']}")
    logcfg = aws("bedrock", "get-model-invocation-logging-configuration", "--region", a.region)
    if logcfg and logcfg.get("loggingConfig"):
        sys.exit("Bedrock invocation logging is ON in this account: stopping (entries must not be logged).")
    print("Bedrock invocation logging: off")

    wg = aws("athena", "get-work-group", "--work-group", a.workgroup)["WorkGroup"]
    out_loc = None if wg.get("Configuration", {}).get("ResultConfiguration", {}).get("OutputLocation") \
        else f"s3://{a.bucket}/dqm/athena-results/"
    at = Athena(a.workgroup, out_loc)
    items = athena_csv(at, "SELECT item_id, part, names, med_code, ndc_path_drugs FROM dqm_map.audit_worklist "
                           "ORDER BY sort_order" + (f" LIMIT {a.limit}" if a.limit else ""), a.db)
    cand_rows = athena_csv(at, f"SELECT item_id, candidate_rank, rxcui, name, brands FROM dqm_map.model_candidates_{a.version} "
                               "ORDER BY item_id, candidate_rank", a.db)
    cands = {}
    for c in cand_rows:
        cands.setdefault(c["item_id"], []).append(c)
    print(f"entries: {len(items)}; entries with candidates: {sum(1 for i in items if i['item_id'] in cands)}; model {a.model}")

    def work(item):
        cs = cands.get(item["item_id"], [])
        t0 = time.time()
        try:
            text, tin, tout, ms = converse(a.model, a.region, prompt_for(item, cs), a.max_tokens)
            p = parse(text)
            err = ""
        except RuntimeError as e:
            p, tin, tout, ms, err = None, 0, 0, int((time.time() - t0) * 1000), str(e)
        ok = p is not None
        rx = p["rxcui"] if ok else ""
        valid = (rx == "") or any(c["rxcui"] == rx for c in cs)
        return {"item_id": item["item_id"], "model": a.model, "decision": p["decision"] if ok else "",
                "rxcui": rx if valid else "", "confidence": p["confidence"] if ok else "",
                "parse_ok": int(ok), "rxcui_valid": int(valid), "input_tokens": tin, "output_tokens": tout,
                "ms": ms, "error": err[:200]}

    results, t0, done = [], time.time(), 0
    with cf.ThreadPoolExecutor(max_workers=a.workers) as ex:
        for r in ex.map(work, items):
            results.append(r)
            done += 1
            if done % 50 == 0 or done == len(items):
                print(f"  {done}/{len(items)} entries, {time.time() - t0:.0f} s")

    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=list(results[0].keys()))
    w.writeheader(); w.writerows(results)
    subprocess.run(["aws", "s3", "cp", "-", f"{s3_out}/model_decisions/model_decisions.csv", "--only-show-errors"],
                   input=buf.getvalue(), text=True, check=True)

    def masked(n):
        return "<10" if 0 < n < 10 else str(n)
    parts = sorted({i["part"] for i in items})
    by_part = {p: {} for p in parts}
    part_of = {i["item_id"]: i["part"] for i in items}
    for r in results:
        k = r["decision"] or ("error" if r["error"] else "unparsed")
        by_part[part_of[r["item_id"]]][k] = by_part[part_of[r["item_id"]]].get(k, 0) + 1
    tin = sum(r["input_tokens"] for r in results); tout = sum(r["output_tokens"] for r in results)
    summary = {"model": a.model, "prompt_version": PROMPT_VERSION,
               "prompt_sha256": hashlib.sha256(SYSTEM.encode()).hexdigest()[:16],
               "entries": len(results), "parse_ok": sum(r["parse_ok"] for r in results),
               "rxcui_invalid_dropped": sum(1 - r["rxcui_valid"] for r in results),
               "errors": sum(1 for r in results if r["error"]),
               "decisions_by_part": {p: {k: masked(v) for k, v in d.items()} for p, d in by_part.items()},
               "input_tokens": tin, "output_tokens": tout,
               "estimated_cost_usd": round(tin / 1e6 * PRICE_PER_M["input"] + tout / 1e6 * PRICE_PER_M["output"], 3),
               "wall_seconds": round(time.time() - t0, 1), "s3_output": s3_out,
               "athena_scanned_mb": round(at.scanned / 1e6, 1)}
    json.dump(summary, open(os.path.join(local, "run.json"), "w"), indent=2)
    open(os.path.join(local, "system_prompt.txt"), "w").write(SYSTEM + "\n")
    print(json.dumps(summary, indent=1))
    print(f"counts and prompt: {local}")


if __name__ == "__main__":
    main()
