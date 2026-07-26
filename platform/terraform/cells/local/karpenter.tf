# Karpenter (node autoscaler) — DECLARED, NOT DEPLOYED, in this local cell.
#
# Why: Karpenter watches for unschedulable pods and calls a cloud provider's
# API (AWS EC2 Fleet, GCP MIG, Azure VMSS) to provision real compute to fit
# them, then bin-packs and consolidates nodes as load changes. A kind
# cluster's "nodes" are fixed-size Docker containers on this machine, created
# once by Step 8B — there is no cloud API underneath them for Karpenter to
# call, so its controller would run but could never do its actual job
# (spot-aware bin-packing against real instance types). Installing it here
# would just be a permanently-idle pod, matching neither the roadmap's intent
# nor the Part N testing requirements, which are about a real cell.
#
# This file keeps Karpenter present in the Terraform tree — mirroring how the
# roadmap keeps Qdrant/ScyllaDB/Redpanda/Temporal "declared in Terraform but
# not yet deployed" this phase — so the module's shape already matches a real
# cloud cell. See platform/karpenter/values-cloud.yaml for the Helm values
# that will be used once this is turned on.

variable "enable_karpenter" {
  description = <<-EOT
    Deploy the Karpenter node-autoscaler controller. Requires a real cloud
    provider node-pool API (AWS/GCP/Azure) underneath the cluster; always
    false for the local kind cell, since kind nodes are static Docker
    containers with nothing for Karpenter to autoscale against.
  EOT
  type    = bool
  default = false
}

# Deploying Karpenter for real (cloud cells only) will look like this, added
# once this module targets a cloud-managed cluster (EKS/GKE/AKS) instead of
# kind, and the `helm` + `kubernetes` providers are wired to that cluster's
# credentials instead of the kind_cluster resource:
#
#   resource "helm_release" "karpenter" {
#     count      = var.enable_karpenter ? 1 : 0
#     name       = "karpenter"
#     repository = "oci://public.ecr.aws/karpenter"
#     chart      = "karpenter"
#     namespace  = "karpenter"
#     create_namespace = true
#     values     = [file("${path.module}/../../../karpenter/values-cloud.yaml")]
#   }
#
# It is left commented rather than instantiated at count = 0 so this module
# does not need to declare/configure a helm provider before anything in the
# cell actually uses Helm-via-Terraform — that provider gets added properly
# at the step that first needs it.
