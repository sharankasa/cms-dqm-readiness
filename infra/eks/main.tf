# Minimal, short-lived EKS cluster. first cluster test now; the same stack, with more nodes and S3
# access for the workers, runs the study jobs. Networking: the default VPC's public subnets with
# public IPs on the nodes, so no NAT gateway (always-on cost). The API endpoint is public but only
# reachable from my_ip_cidr.

data "aws_caller_identity" "me" {}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "public" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
  filter {
    name   = "availability-zone"
    values = var.eks_azs
  }
}

# --- IAM roles (created and destroyed with the stack) ---------------------------------

resource "aws_iam_role" "cluster" {
  name = "${var.cluster_name}-cluster"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "eks.amazonaws.com" }, Action = ["sts:AssumeRole", "sts:TagSession"] }]
  })
}

resource "aws_iam_role_policy_attachment" "cluster" {
  role       = aws_iam_role.cluster.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
}

resource "aws_iam_role" "node" {
  name = "${var.cluster_name}-node"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "ec2.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy_attachment" "node" {
  for_each = toset([
    "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy",
    "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy",
    "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
  ])
  role       = aws_iam_role.node.name
  policy_arn = each.value
}

# --- Cluster ------------------------------------------------------------------------------

resource "aws_eks_cluster" "this" {
  name     = var.cluster_name
  version  = var.kubernetes_version
  role_arn = aws_iam_role.cluster.arn

  vpc_config {
    subnet_ids              = data.aws_subnets.public.ids
    endpoint_public_access  = true
    endpoint_private_access = true
    public_access_cidrs     = [var.my_ip_cidr]
  }

  access_config {
    authentication_mode                         = "API"
    bootstrap_cluster_creator_admin_permissions = true
  }

  # EKS defaulted to EXTENDED support in the first cluster test (2026-10-09). Extended support costs $0.60/h
  # instead of $0.10/h once a version leaves standard support; STANDARD makes AWS upgrade instead.
  upgrade_policy {
    support_type = "STANDARD"
  }

  depends_on = [aws_iam_role_policy_attachment.cluster]
}

# The daily role runs kubectl (cluster-admin on this cluster only).
resource "aws_eks_access_entry" "daily" {
  cluster_name  = aws_eks_cluster.this.name
  principal_arn = "arn:aws:iam::${data.aws_caller_identity.me.account_id}:role/${var.daily_role_name}"
}

resource "aws_eks_access_policy_association" "daily_admin" {
  cluster_name  = aws_eks_cluster.this.name
  principal_arn = aws_eks_access_entry.daily.principal_arn
  policy_arn    = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"
  access_scope {
    type = "cluster"
  }
}

# --- Workers ------------------------------------------------------------------------------

resource "aws_eks_node_group" "workers" {
  cluster_name    = aws_eks_cluster.this.name
  node_group_name = "${var.cluster_name}-workers"
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = data.aws_subnets.public.ids
  instance_types  = [var.node_instance_type]
  ami_type        = "AL2023_x86_64_STANDARD"
  capacity_type   = "ON_DEMAND"
  disk_size       = 20

  scaling_config {
    desired_size = var.node_count
    min_size     = 0
    max_size     = max(var.node_count, 1)
  }

  depends_on = [aws_iam_role_policy_attachment.node]
}

# Extra nodes for a run phase (the equivalence run, 2026-10-11): a separate node group, so it can be added and removed
# (-var burst_count=<n> / 0) without touching the main group's nodes or the long pods running on them.
# Same instance type, so the watchdog's node price holds for both groups (it scales both to 0 at the cap).
resource "aws_eks_node_group" "burst" {
  count           = var.burst_count > 0 ? 1 : 0
  cluster_name    = aws_eks_cluster.this.name
  node_group_name = "${var.cluster_name}-burst"
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = data.aws_subnets.public.ids
  instance_types  = [var.node_instance_type]
  ami_type        = "AL2023_x86_64_STANDARD"
  capacity_type   = "ON_DEMAND"
  disk_size       = 20

  scaling_config {
    desired_size = var.burst_count
    min_size     = 0
    max_size     = var.burst_count
  }

  depends_on = [aws_iam_role_policy_attachment.node]
}

# --- workers: S3 access for the worker pods only (EKS Pod Identity) -----------------
# Pods using the Kubernetes service account `dqm-worker` (namespace default) get this role; nodes and
# other pods do not. Read: the run prefixes that hold worker input; write: only `<run>/runs/*`.

resource "aws_eks_addon" "pod_identity" {
  cluster_name = aws_eks_cluster.this.name
  addon_name   = "eks-pod-identity-agent"
  depends_on   = [aws_eks_node_group.workers]
}

resource "aws_iam_role" "worker" {
  name = "${var.cluster_name}-worker"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "pods.eks.amazonaws.com" }, Action = ["sts:AssumeRole", "sts:TagSession"] }]
  })
}

resource "aws_iam_role_policy" "worker_s3" {
  name = "s3-run-prefixes"
  role = aws_iam_role.worker.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "ListRunPrefixes"
        Effect    = "Allow"
        Action    = "s3:ListBucket"
        Resource  = "arn:aws:s3:::${var.data_bucket}"
        Condition = { StringLike = { "s3:prefix" = [for p in var.worker_prefixes : "${p}/*"] } }
      },
      {
        Sid      = "ReadRunPrefixes"
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = [for p in var.worker_prefixes : "arn:aws:s3:::${var.data_bucket}/${p}/*"]
      },
      {
        Sid      = "WriteResultsOnly"
        Effect   = "Allow"
        Action   = "s3:PutObject"
        Resource = [for p in var.worker_prefixes : "arn:aws:s3:::${var.data_bucket}/${p}/*/runs/*"]
      }
    ]
  })
}

resource "aws_eks_pod_identity_association" "worker" {
  cluster_name    = aws_eks_cluster.this.name
  namespace       = "default"
  service_account = "dqm-worker"
  role_arn        = aws_iam_role.worker.arn
}
