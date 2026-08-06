#!/usr/bin/env bash
# Phase-04 Step Q: Vault admin setup for data-plane's OWN independent
# manifest signature verification. USER-RUN ONLY — needs the Vault root
# token, which must never pass through an agent's tool output/transcript
# (same boundary vault-init-unseal.sh and vault-setup-control-plane.sh
# both established).
#
# Sets up:
#   1. A least-privilege policy (data-plane-manifest-verify): ONLY
#      transit/verify on model-manifest-signing — deliberately NOT
#      transit/sign. control-plane's own policy (Step E) gets both; this
#      one gets read-only verify capability alone, so that even a fully
#      compromised data-plane pod cannot forge a new signed manifest, only
#      check existing signatures. Same key, asymmetric capability.
#   2. A Kubernetes auth role binding specifically the data-plane
#      ServiceAccount in the default namespace to that policy — a NEW
#      role, not reusing control-plane's, and a NEW dedicated
#      ServiceAccount (added to data-plane's Deployment alongside this
#      script) rather than the implicit "default" SA data-plane used
#      before this step.
#
# Kubernetes auth itself is already enabled cluster-wide
# (vault-setup-control-plane.sh, Step E) — this script does not re-enable
# it, only adds data-plane's own policy + role on top.
#
# Idempotent: safe to re-run.
#
# Usage: scripts/vault-setup-data-plane.sh
# Run from a machine with `kubectl` access to the onezox-dev kind cluster.

set -euo pipefail

NAMESPACE="default"
POD="vault-0"
SIGNING_KEY="model-manifest-signing"
POLICY_NAME="data-plane-manifest-verify"
K8S_AUTH_ROLE="data-plane"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl not found on PATH" >&2
  exit 1
fi

read -r -s -p "Vault root token: " ROOT_TOKEN
echo >&2
if [[ -z "$ROOT_TOKEN" ]]; then
  echo "No token entered — aborting." >&2
  exit 1
fi

vexec() {
  kubectl exec -n "$NAMESPACE" -i "$POD" -- env VAULT_TOKEN="$ROOT_TOKEN" VAULT_ADDR="http://127.0.0.1:8200" "$@"
}

echo "=== 1/3: confirm the signing key already exists (created by vault-setup-control-plane.sh) ===" >&2
if ! vexec vault read "transit/keys/${SIGNING_KEY}" >/dev/null 2>&1; then
  echo "ERROR: transit/keys/${SIGNING_KEY} does not exist yet." >&2
  echo "Run scripts/vault-setup-control-plane.sh first (Step E) — it owns" >&2
  echo "creating this key; this script only adds a read-only verify policy" >&2
  echo "for it, it does not create keys." >&2
  exit 1
fi
echo "confirmed: ${SIGNING_KEY} exists." >&2

echo >&2
echo "=== 2/3: least-privilege READ-ONLY policy (${POLICY_NAME}) — verify only, no sign ===" >&2
vexec sh -c "cat <<'EOF' | vault policy write ${POLICY_NAME} -
path \"transit/verify/${SIGNING_KEY}\" {
  capabilities = [\"update\"]
}
EOF"

echo >&2
echo "=== 3/3: Kubernetes auth role (${K8S_AUTH_ROLE}) ===" >&2
vexec vault write "auth/kubernetes/role/${K8S_AUTH_ROLE}" \
  bound_service_account_names=data-plane \
  bound_service_account_namespaces="${NAMESPACE}" \
  policies="${POLICY_NAME}" \
  ttl=15m

unset ROOT_TOKEN

echo >&2
echo "=== Verification (policy content only, no secret material) ===" >&2
kubectl exec -n "$NAMESPACE" -i "$POD" -- vault policy read "${POLICY_NAME}" 2>&1 || true
echo >&2
echo "Done. data-plane's pods (ServiceAccount data-plane, namespace" >&2
echo "default) can now log in via Vault's Kubernetes auth method and VERIFY" >&2
echo "with ${SIGNING_KEY} — read-only, cannot sign, cannot read provider" >&2
echo "secrets or anything else." >&2
