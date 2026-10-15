variable "region" {
  type    = string
  default = "us-west-2"
}

variable "aws_profile" {
  description = "Profile for apply/destroy. personal-admin, as in aws-foundation: the daily role denies all IAM actions (incl. PassRole), which EKS needs."
  type        = string
  default     = "personal-admin"
}

variable "cluster_name" {
  type    = string
  default = "dqm-eks-g3"
}

variable "purpose" {
  type    = string
  default = "g3-eks-gate-test"
}

variable "kubernetes_version" {
  description = "null = the EKS default version (in standard support, $0.10/h; extended support costs $0.60/h)."
  type        = string
  default     = null
}

variable "node_instance_type" {
  description = "Paid plan since 2026-10-11: m7i.large (2 vCPU, 8 GiB, full CPU sustained; same shape as the pilot's m7i-flex.large, which has a 40% CPU baseline). The Free Plan allowed only t3/t4g micro/small and the flex types."
  type        = string
  default     = "m7i.large"
}

variable "node_count" {
  type    = number
  default = 1
}

variable "burst_count" {
  description = "Nodes in the separate 'burst' node group (0 = no group). Add/remove extra nodes for a run phase without touching the main group's nodes."
  type        = number
  default     = 0
}

variable "my_ip_cidr" {
  description = "Your public IP as x.x.x.x/32: the only address allowed to reach the cluster API."
  type        = string
}

variable "daily_role_name" {
  description = "Role used for kubectl and data work (gets cluster-admin on this cluster only)."
  type        = string
  default     = "your-role"
}

variable "eks_azs" {
  description = "AZs for the cluster subnets (us-west-2d is avoided: EKS control-plane AZ support varies)."
  type        = list(string)
  default     = ["us-west-2a", "us-west-2b", "us-west-2c"]
}

variable "data_bucket" {
  description = "Bucket holding the credentialed data and run prefixes (workers)."
  type        = string
  default     = "your-bucket"
}

variable "worker_prefixes" {
  description = "Prefixes the worker pods may read (and write under <prefix>/<run>/runs/). dqm/readiness added 2026-10-14 (R2/R1 engine inputs)."
  type        = list(string)
  default     = ["dqm/s1", "dqm/pilot", "dqm/prep", "dqm/readiness"]
}

# --- Cost guard (watchdog.tf) ----------------------------------------------------------------

variable "cost_cap_usd" {
  description = "Estimated spend (EKS fee + nodes) at which the watchdog scales the nodes to 0 and alerts; warning at 80%. Set per run from the run plan."
  type        = number
  default     = 5
}

variable "max_hours" {
  description = "Cluster age at which the watchdog scales the nodes to 0 and alerts, whatever the spend."
  type        = number
  default     = 6
}

variable "node_hour_usd" {
  description = "On-demand price of one node per hour. 0 = read it from the AWS Price List API."
  type        = number
  default     = 0
}

variable "alerts_topic_name" {
  description = "Existing SNS topic for alerts (aws-foundation account stack; email subscription)."
  type        = string
  default     = "dqm-alerts"
}
