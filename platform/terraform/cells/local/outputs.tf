output "cluster_name" {
  description = "Name of the created kind cluster."
  value       = kind_cluster.onezox_dev.name
}

output "kubeconfig_path" {
  description = "Path to the kubeconfig file kind exported for this cluster (also merged into ~/.kube/config)."
  value       = kind_cluster.onezox_dev.kubeconfig_path
}

output "endpoint" {
  description = "Kubernetes API server endpoint."
  value       = kind_cluster.onezox_dev.endpoint
}
