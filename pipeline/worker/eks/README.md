# EKS worker (the pilot pilot, the equivalence run S1 sample, the readiness inputs timing runs)

One Kubernetes **Indexed Job per side** (sliced or reference); each pod evaluates one shard with CMS's
reference engine, unmodified (`pipeline/s1/run_engine.js`), reading its input from S3 and keeping its
result file in S3. A pod or node that dies is replaced and the replacement **resumes**: every
(measure, stay) is written exactly once (the same mechanism as the local fault test, with S3 instead of a local folder).

| File | Role |
|---|---|
| `worker_s3.js` | S3 in/out wrapper. `SHARD_OFFSET` (default 0) is added to the completion index, so a second job can run shards N.. with other pod sizes (the equivalence run). Each pod's status file records its container memory peak and limit (cgroup). `MODE=sliced` / `reference`: downloads the shard, runs `run_engine.js`, uploads results every 30 s and on exit (resumable). `MODE=v`: runs V (`pipeline/v/reference_measures.py`, the readings that match CMS's logic) on the reference input. `MODE=compare-v`: one pod compares the engine's reference-side results with V (`pipeline/v/compare_v.py`; `NO_POP_COUNTS=1` for the pilot) and writes the summary to S3 |
| `package.json`, `package-lock.json` | AWS SDK for JavaScript v3 (S3 client), pinned |
| `Dockerfile` | Worker image: engine (pinned lock) + runner + wrapper + python3 and V; no data, no value sets, no credentials |
| `job-eks.yaml` | Service account `dqm-worker` + Indexed Job template (per-index retry limit 3) |
| `run_eks_job.sh` | Build/push the image to ECR if this code version is new; run the Job; optional fault test (`KILL_PODS`, `KILL_NODE=1`); save job/pod status (no data) locally, pod logs to S3 |
| `test_local.sh` | The image on a workstation with Docker on open demo data, S3 replaced by a folder: S1 must hold |
| `profile_run.js`, `profile_summary.js` | `MODE=profile` (exploratory, the equivalence run): re-runs one patient-year of a shard on the reference side under V8's CPU profiler (in-process, stops after `PROFILE_SECONDS`), then summarizes CPU time by package, source file (cql-execution's files are named after operator families) and function. Writes `<OUT>/profile/{profile,summary}_<shard>.*`: function names and timings only, no data. Tested on the open demo 2026-10-11 |
| `run_progress.sh`, `finish_job.sh`, `trim_idle_nodes.sh` | Progress snapshot (no data); end-of-job steps for detached jobs; cordon and terminate idle nodes once nothing is queued (dry run by default) |
| Options added 2026-10-14 | `run_eks_job.sh`: `NODEGROUP` (pods stay on one node group: S2 timing runs on `dqm-eks-workers`, readiness runs on `dqm-eks-burst`), `BATCH` / `BATCH_MAX_RES` (sliced side: up to N stays per engine call, distinct patients, same year; slices over 400 resources alone; default 1 = as in S1; checked identical to one-per-call on the open demo, 550/550 lines incl. `defs`), `ENGINE_PREFIX=sliced_` (compare-v against a sliced job), `JOB_TAG`, `NO_BUILD=1` + `IMAGE_TAG`. Result lines of the sliced side now carry `defs` (size of each named expression of the measure's main library: R2's "components responsible"); status files carry `cpu_seconds` (cgroup) and the batch setting |
| `status_summary.py` | Pod hours per shard from a job's status files in S3 (no data), e.g. after the the equivalence run calibration run; prints the `--measured` argument for `pipeline/s1/rebalance_run.py --plan-only` |

**Credentials.** EKS Pod Identity: only pods using the service account `dqm-worker` get the worker role
(`infra/eks/main.tf`): read the run prefixes, write only under `<run>/runs/`. Nodes get no S3 access.

**Input layout** (written by `pipeline/s1/sample_run.py`, or by `pipeline/prepare/prepare_run.py` for
the full data): `<run>/export/{manifest,slices,reference}/shard=<n>/`, `<run>/export/{shared,shared_all}/`,
`<run>/engine/{bundles,vsac-cache}/`. Output: `<run>/runs/<job>/results/<mode>_<shard>.jsonl`,
`<run>/runs/<job>/status/`, `<run>/runs/<job>/logs/`.

**Comparison** (in Athena, counts only): `pipeline/s1/compare_run.py`. Pilot: no population counts.

**Tested (2026-10-10, open demo, S3 replaced by a folder):** shard 12 (16 stays).
- Sandbox: sliced and reference S1 16/16 on both measures; results equal the Athena-slice engine run;
  resume from a cut-off result file (torn last line) finishes exactly once; V agrees with the engine
  16/16 on both measures, and the pilot summary carries no population counts.
- The image on a workstation (`test_local.sh`, Docker): S1 16/16 on both measures, exactly once, resume
  rerun does nothing; V agrees with the engine 16/16 on both measures; compare-v writes its summary
  without population counts.

**Pilot order (the pilot):** `MODE=sliced`, `MODE=reference`, `MODE=v` (each `run_eks_job.sh` with the pilot's
INPUT), then `compare_run.py --sample pilot` (S1) and `MODE=compare-v NO_POP_COUNTS=1 LABEL=pilot`
(V vs engine, CMS816 is the registered check).
