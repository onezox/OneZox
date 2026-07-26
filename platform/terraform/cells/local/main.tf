resource "kind_cluster" "onezox_dev" {
  name = var.cluster_name

  # Must be false: with the default CNI disabled below, kubelet cannot report
  # the control-plane Ready until Cilium is installed (next Phase-00 step).
  # wait_for_ready=true would block until its internal 5-minute timeout.
  wait_for_ready = false

  kind_config {
    kind        = "Cluster"
    api_version = "kind.x-k8s.io/v1alpha4"

    node {
      role = "control-plane"
    }

    dynamic "node" {
      for_each = range(var.worker_node_count)
      content {
        role = "worker"
      }
    }

    # Default CNI (kindnet) is disabled so Cilium (eBPF CNI + mTLS) can be
    # installed as the cluster's only CNI in the next Phase-00 step.
    networking {
      disable_default_cni = true
    }
  }
}
