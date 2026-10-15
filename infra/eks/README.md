# infra/eks: short-lived EKS cluster

A first test (does EKS work on the account?) and the cluster for the study runs. Created and
destroyed per run; nothing always-on. Default VPC public subnets with public IPs on the nodes
(no NAT gateway); the API endpoint only accepts your IP.

**Profiles.** `apply`/`destroy` use `personal-admin`, as in `aws-foundation` (the daily role
`your-role` denies all IAM actions, including the `iam:PassRole` EKS needs). Everything else
(kubectl, checks, data) uses the daily role, which gets cluster-admin on this cluster only through an
EKS access entry. No change to the daily role's permissions.

**Cost of the first cluster test:** EKS control plane $0.10/h + one `m7i-flex.large` node (Free Plan eligible)
+ a 20 GB disk, roughly $0.20 for an hour, from credits. Destroy as soon as the test passes.

## first cluster test run (about 30 to 40 minutes, most of it waiting)

Terminal commands have no `#` comments (zsh would choke on them).

**0. Tools and your IP (read-only)**
```
cd infra/eks
terraform version
kubectl version --client
curl -s https://checkip.amazonaws.com
```

**1. Guardrails and a clean start (daily role, read-only)**
```
eval "$(aws configure export-credentials --profile your-aws-profile --format env)"
export AWS_DEFAULT_REGION=us-west-2
aws budgets describe-budgets --account-id 123456789012 --query 'Budgets[].[BudgetName,BudgetLimit.Amount,TimeUnit]' --output table
bash check_leftovers.sh
```

**2. Create (admin profile; first clear the daily role's exported keys so Terraform uses the profile)**
```
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
terraform init
terraform validate
terraform plan -var "my_ip_cidr=$(curl -s https://checkip.amazonaws.com)/32" -out tfplan
terraform apply tfplan
```
About 12 to 15 minutes. If it fails, copy the error: a Free Plan restriction would show up here.

**3. Test (daily role)**
```
eval "$(aws configure export-credentials --profile your-aws-profile --format env)"
export AWS_DEFAULT_REGION=us-west-2
aws eks update-kubeconfig --name dqm-eks-g3 --region us-west-2 --alias dqm-eks-g3
kubectl get nodes -o wide
kubectl run g3-hello --image=public.ecr.aws/docker/library/busybox:stable --restart=Never -- echo "hello from EKS"
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/g3-hello --timeout=180s
kubectl logs g3-hello
kubectl config use-context docker-desktop
```

**4. Destroy (admin profile)**
```
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
terraform destroy -var "my_ip_cidr=$(curl -s https://checkip.amazonaws.com)/32"
```
Type `yes` when asked. About 10 minutes.

**5. Confirm nothing is left (daily role)**
```
eval "$(aws configure export-credentials --profile your-aws-profile --format env)"
export AWS_DEFAULT_REGION=us-west-2
bash check_leftovers.sh
kubectl config delete-context dqm-eks-g3
```

**The test passes when:** apply succeeds, the node is `Ready`, the pod prints `hello from EKS`, destroy
succeeds and `check_leftovers.sh` lists nothing. If EKS is blocked: fallback k3s on EC2.

## Cost guards (added 2026-10-11)

Three layers, from fastest to slowest:

| Layer | Where | What it does |
|---|---|---|
| Job cap | `pipeline/worker/eks/run_eks_job.sh` (`JOB_COST_CAP`, default $5) | Live estimate (nodes × node price + EKS fee, from job start); warning at 80%; at the cap: ERROR, the job is stopped (finished results kept, a rerun resumes), exit code 3. Writes `cost_estimate.json` per job. Needs the laptop running |
| Cluster watchdog | `watchdog.tf` + `watchdog/watchdog.py` (`-var cost_cap_usd`, `-var max_hours`) | Lambda every 10 min, independent of the laptop: running estimate of the cluster's cost; SNS email at 80%; at the cap or max age it scales every node group to 0 and emails again (every 6 h until the cluster is destroyed). If the watchdog itself fails, Lambda emails the failure. Node price read from the AWS Price List API. Created and destroyed with the cluster |
| Account | AWS Budgets | Cost Explorer gross spend (credits not subtracted) vs the registration cap ($150 for the whole study), monthly per-service caps and a daily cap, and which whole-study alert levels ($50, $90) are passed; exit 1 = warning, 2 = over. Run before and after every AWS session. AWS Budgets (gross usage) email without the laptop: whole study at $50/$90/$120/$150 (`dqm-study-total`), plus the low account tripwires ($2/day, $25/month, $10/service). Budget data lags up to a day |

Offline test of the watchdog logic: `python3 infra/eks/watchdog/test_watchdog.py`.
Set the caps per run from the run plan, e.g. `-var cost_cap_usd=8 -var max_hours=12`.

## Notes for the study runs

- Node type: `m7i.large` since the paid-plan upgrade (2026-10-11): 2 vCPU, 8 GiB, full CPU sustained
  (the pilot ran on `m7i-flex.large`, same shape but a 40% CPU baseline). The registered scaling points
  (1/2/4/8 nodes) run on these 2-vCPU nodes; the vCPU quota (32) allows up to 16 of them.
- **Worker S3 access (added 2026-10-10):** through EKS Pod Identity, not the node role. The stack now
  also creates the `eks-pod-identity-agent` add-on, a worker role (read `dqm/s1/*`, `dqm/pilot/*`,
  `dqm/prep/*`; write only `<run>/runs/*` under them) and its association with the Kubernetes service
  account `dqm-worker` (created by `pipeline/worker/eks/job-eks.yaml`). Nodes and other pods get no S3
  access. If the data bucket uses a KMS key (check: `aws s3api get-bucket-encryption --bucket
  your-bucket`), the worker role also needs `kms:Decrypt` on it: add it to the worker role's policy.
- **Extra nodes for a phase (`-var burst_count=<n>`, added 2026-10-11):** a second node group `<cluster>-burst`, same
  instance type, added and removed (`burst_count=0`) without touching the main group's nodes, so long pods on
  them keep running. Always repeat every other `-var` of the running cluster (cluster name, node count, node type):
  a changed node type would replace the main node group. The plan must show no replace/destroy of `workers`.
- study runs: `-var cluster_name=dqm-eks -var node_count=<n>`; `run_eks_job.sh` only runs when the
  kubectl context name contains `dqm-eks`. The worker image lives in the ECR repository `dqm-worker`
  (created on first use; storage about $0.02 a month).
- `kubectl` context: `update-kubeconfig` switches the current context to the cluster; the local
  `run_local_job.sh` refuses to run unless the context is `docker-desktop`, so a local test can
  never land on the cloud cluster by mistake.
