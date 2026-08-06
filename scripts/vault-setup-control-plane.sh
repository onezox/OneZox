#!/usr/bin/env bash
# Phase-04 Step E: Vault admin setup for control-plane's manifest signing.
# USER-RUN ONLY — needs the Vault root token, which must never pass
# through an agent's tool output/transcript (same boundary
# vault-init-unseal.sh established).
#
# Sets up:
#   1. Transit secrets engine + a dedicated ed25519 signing key
#      (model-manifest-signing) — the user's own explicit choice over
#      reusing cosign/Sigstore: manifest authorship and build provenance
#      are different trust questions, so this key is independent of the
#      Phase-00 cosign keypair.
#   2. Kubernetes auth method, configured to trust this cluster's own API
#      server (Vault auto-detects its own pod's CA/SA token when running
#      in-cluster and neither is explicitly passed — the modern supported
#      approach; vault-server-binding already grants Vault's own
#      ServiceAccount system:auth-delegator, confirmed present before
#      writing this script).
#   3. A least-privilege policy: only transit sign/verify on
#      model-manifest-signing, nothing else.
#   4. A Kubernetes auth role binding specifically the control-plane
#      ServiceAccount in the default namespace (not any pod using
#      "default") to that policy — this is what makes "the cluster
#      vouches for this identity" real rather than nominal.
#
# Idempotent: safe to re-run (each step checks/overwrites its own config
# without erroring on "already exists").
#
# Usage: scripts/vault-setup-control-plane.sh
# Run from a machine with `kubectl` access to the onezox-dev kind cluster.

set -euo pipefail

NAMESPACE="default"
POD="vault-0"
SIGNING_KEY="model-manifest-signing"
POLICY_NAME="control-plane-manifest-signing"
K8S_AUTH_ROLE="control-plane"

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

echo "=== 1/4: Transit secrets engine + signing key ===" >&2
if vexec vault secrets list -format=json 2>/dev/null | grep -q '"transit/"'; then
  echo "transit/ already enabled — skipping enable." >&2
else
  vexec vault secrets enable transit
fi

if vexec vault read "transit/keys/${SIGNING_KEY}" >/dev/null 2>&1; then
  echo "${SIGNING_KEY} already exists — skipping create (not overwritten; a" >&2
  echo "genuinely new key would need rotation, not silent replacement)." >&2
else
  vexec vault write -f "transit/keys/${SIGNING_KEY}" type=ed25519
fi

echo >&2
echo "=== 2/4: Kubernetes auth method ===" >&2
if vexec vault auth list -format=json 2>/dev/null | grep -q '"kubernetes/"'; then
  echo "kubernetes/ auth already enabled — skipping enable." >&2
else
  vexec vault auth enable kubernetes
fi
vexec vault write auth/kubernetes/config \
  kubernetes_host="https://kubernetes.default.svc:443"

echo >&2
echo "=== 3/4: least-privilege policy (${POLICY_NAME}) ===" >&2
vexec sh -c "cat <<'EOF' | vault policy write ${POLICY_NAME} -
path \"transit/sign/${SIGNING_KEY}\" {
  capabilities = [\"update\"]
}
path \"transit/verify/${SIGNING_KEY}\" {
  capabilities = [\"update\"]
}
EOF"

echo >&2
echo "=== 4/4: Kubernetes auth role (${K8S_AUTH_ROLE}) ===" >&2
vexec vault write "auth/kubernetes/role/${K8S_AUTH_ROLE}" \
  bound_service_account_names=control-plane \
  bound_service_account_namespaces="${NAMESPACE}" \
  policies="${POLICY_NAME}" \
  ttl=15m

unset ROOT_TOKEN

echo >&2
echo "=== Verification (config only, no secret material) ===" >&2
kubectl exec -n "$NAMESPACE" -i "$POD" -- vault read "transit/keys/${SIGNING_KEY}" 2>&1 || true
echo >&2
echo "Done. control-plane's pods (ServiceAccount control-plane, namespace" >&2
echo "default) can now log in via Vault's Kubernetes auth method and sign/" >&2
echo "verify with ${SIGNING_KEY} — nothing else." >&2
