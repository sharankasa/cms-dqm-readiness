output "cluster_name" {
  value = aws_eks_cluster.this.name
}

output "kubernetes_version" {
  value = aws_eks_cluster.this.version
}

output "subnets_used" {
  value = data.aws_subnets.public.ids
}

output "kubeconfig_command" {
  description = "Run with the daily role's credentials exported in the shell (see README)."
  value       = "aws eks update-kubeconfig --name ${aws_eks_cluster.this.name} --region ${var.region} --alias ${aws_eks_cluster.this.name}"
}

output "worker_role_arn" {
  value = aws_iam_role.worker.arn
}

output "node_hour_usd" {
  description = "Node price used by the cost guards (watchdog and run_eks_job.sh)."
  value       = local.node_hour_usd
}

output "cost_guard" {
  value = "watchdog: nodes to 0 at an estimated USD ${var.cost_cap_usd} or ${var.max_hours} h (warning at 80%); alerts to ${var.alerts_topic_name}"
}
