#!/usr/bin/env bash
# Phase-04 Step I: loads the 5 active providers' REAL API keys into Vault
# (secret/provider/{name}, KV v2 — Phase-04.txt's own DATABASE TABLES
# REQUIRED / Vault section) and grants control-plane read access to them.
#
# USER-RUN ONLY — needs the Vault root token (enabling a secrets engine
# and writing policy both need root, same as vault-setup-control-plane.sh)
# AND the 5 providers' real API keys. Neither the root token nor any key
# value may ever pass through an agent's tool output/transcript — same
# boundary vault-init-unseal.sh and create-provider-secret.sh both
# established, applied here to both a root token and five more secrets at
# once.
#
# control-plane, not provider-gateway, reads these paths: per the
# approved Phase-04 design, control-plane mediates Vault
# (IssueProviderToken is control-plane's own RPC, Step J) — provider-
# gateway never talks to Vault directly, so this policy is bound to
# control-plane's existing Kubernetes-auth role, NOT a new one for
# provider-gateway. Attached as a SEPARATE policy
# (control-plane-provider-secrets) from control-plane-manifest-signing
# (vault-setup-control-plane.sh) — signing manifests and reading provider
# credentials are different capabilities with different blast radii, kept
# as independently-revocable policies even though both currently attach
# to the same role.
#
# Idempotent: safe to re-run. Leave any prompt blank to skip that
# provider (same "proceed with what's available" pattern
# create-provider-secret.sh already established) — re-running with a
# provider left blank does NOT delete an already-loaded key for it, only
# a real entry (non-blank) overwrites that provider's secret.
#
# Usage: scripts/vault-load-provider-keys.sh

set -euo pipefail

NAMESPACE="default"
POD="vault-0"
K8S_AUTH_ROLE="control-plane"
POLICY_NAME="control-plane-provider-secrets"

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

echo "=== 1/4: KV v2 secrets engine at secret/ ===" >&2
if vexec vault secrets list -format=json 2>/dev/null | grep -q '"secret/"'; then
  echo "secret/ already enabled — skipping enable." >&2
else
  vexec vault secrets enable -path=secret -version=2 kv
fi

echo >&2
echo "=== 2/4: least-privilege policy (${POLICY_NAME}) — read-only, provider secrets only ===" >&2
vexec sh -c "cat <<'EOF' | vault policy write ${POLICY_NAME} -
path \"secret/data/provider/*\" {
  capabilities = [\"read\"]
}
EOF"

echo >&2
echo "=== 3/4: attach ${POLICY_NAME} to the existing control-plane K8s-auth role ===" >&2
echo "(alongside control-plane-manifest-signing — a role may hold multiple" >&2
echo "policies; each stays independently scoped/revocable)" >&2
vexec vault write "auth/kubernetes/role/${K8S_AUTH_ROLE}" \
  bound_service_account_names=control-plane \
  bound_service_account_namespaces="${NAMESPACE}" \
  policies="control-plane-manifest-signing,${POLICY_NAME}" \
  ttl=15m

echo >&2
echo "=== 4/4: load provider keys (leave blank to skip/deactivate) ===" >&2

load_key() {
  local provider_name="$1"
  local display_name="$2"
  local key=""

  read -r -s -p "Enter ${display_name} API key (leave blank to skip): " key
  echo >&2

  if [[ -z "$key" ]]; then
    echo "Skipping ${display_name} (no value entered)." >&2
    unset key
    return
  fi

  # kv-v2 write via stdin JSON, not a CLI positional arg — a key typed as
  # `vault kv put secret/provider/x api_key=$key` would land in this
  # script's own process listing and shell history; piping through stdin
  # avoids both, same reasoning create-provider-secret.sh's --from-file
  # (over --from-literal) already established for the K8s Secret path.
  printf '{"data":{"api_key":%s}}' "$(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$key")" \
    | kubectl exec -n "$NAMESPACE" -i "$POD" -- env VAULT_TOKEN="$ROOT_TOKEN" VAULT_ADDR="http://127.0.0.1:8200" \
      vault write "secret/data/provider/${provider_name}" -
  unset key
  echo "${display_name} loaded into secret/provider/${provider_name}." >&2
}

load_key "openai" "OpenAI"
load_key "anthropic" "Anthropic"
load_key "grok" "Grok (xAI)"
load_key "glm" "GLM (Zhipu / z.ai)"
load_key "kimi" "Kimi (Moonshot)"

unset ROOT_TOKEN

echo >&2
echo "=== Verification (paths only, values never printed) ===" >&2
kubectl exec -n "$NAMESPACE" -i "$POD" -- vault kv list secret/provider 2>&1 || true

echo >&2
echo "Done. control-plane can now read secret/provider/{name} via its" >&2
echo "existing K8s-auth login — no static Vault token, no key value ever" >&2
echo "touched this terminal's scrollback beyond your own typed input." >&2
