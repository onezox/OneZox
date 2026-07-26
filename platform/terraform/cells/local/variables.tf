variable "cluster_name" {
  description = "Name of the local kind cluster standing in for one region, one cell (Phase-00, Part N)."
  type        = string
  default     = "onezox-dev"
}

variable "worker_node_count" {
  description = "Number of kind worker nodes, in addition to the single control-plane node."
  type        = number
  default     = 2
}
