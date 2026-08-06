#!/usr/bin/env bash
# Phase-04 Step B: initializes and unseals the 3-node Vault Raft cluster,
# then enables audit logging. USER-RUN ONLY — never execute this via an
# agent session. The unseal keys and root token this produces must never
# pass through an agent's tool output/transcript, the same boundary the
# create-provider-secret.sh credential-script incident established for
# provider API keys, applied here to something even more sensitive (root
# token = full Vault access).
#
# This script does NOT persist the unseal keys or root token to any file —
# `vault operator init` prints them straight to your own terminal (stdout,
# never redirected here) and it is YOUR responsibility to record them
# immediately in a real secrets manager (password manager, etc.), not in
# this repo, not in a plaintext file on this host. Once recorded, they are
# gone from this script's memory — nothing here caches them.
#
# Unseal key threshold: 3-of-5 (Vault's own default `-key-shares=5
# -key-threshold=3`). Each of the 3 pods must be unsealed independently
# (Shamir seal state is per-node even under Raft integrated storage) — this
# script prompts for 3 keys per pod, 3 times total, hidden input each time
# (`read -s`, matching create-provider-secret.sh's pattern).
#
# Usage: scripts/vault-init-unseal.sh
# Run from a machine with `kubectl` access to the onezox-dev kind cluster.

set -euo pipefail

NAMESPACE="default"
PODS=(vault-0 vault-1 vault-2)

if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl not found on PATH" >&2
  exit 1
fi

vexec() {
  local pod="$1"
  shift
  kubectl exec -n "$NAMESPACE" -i "$pod" -- "$@"
}

echo "=== Step 1: checking vault-0 init status ===" >&2
if vexec vault-0 vault status -format=json 2>/dev/null | grep -q '"initialized": true'; then
  echo "vault-0 already initialized — skipping init. If you still have your" >&2
  echo "unseal keys, proceed straight to unsealing (Step 2) manually with" >&2
  echo "'kubectl exec -it vault-0 -- vault operator unseal'." >&2
  exit 0
fi

echo >&2
echo "=== Step 2: vault operator init (vault-0) ===" >&2
echo "This prints 5 unseal key shares and a root token to YOUR terminal" >&2
echo "below. Record them now in a real secrets manager. They will not be" >&2
echo "shown again, and this script does not save them anywhere." >&2
echo >&2
kubectl exec -n "$NAMESPACE" -it vault-0 -- vault operator init

echo >&2
echo "=== Step 3: unseal vault-0 ===" >&2
echo "Enter 3 of the 5 unseal keys you just recorded, one at a time." >&2
for i in 1 2 3; do
  read -r -s -p "Unseal key ${i}/3 for vault-0: " key
  echo >&2
  kubectl exec -n "$NAMESPACE" -i vault-0 -- vault operator unseal "$key" >/dev/null
  unset key
done
echo "vault-0 unsealed." >&2

echo >&2
echo "=== Step 4: join vault-1 and vault-2 to the Raft cluster, then unseal ===" >&2
for pod in vault-1 vault-2; do
  echo "Joining ${pod}..." >&2
  # Deliberately NOT swallowing a failed join with '|| echo ...continuing':
  # an earlier version of this script did exactly that, which silently
  # walked straight into the unseal loop below on a node that was never
  # actually joined (unseal then fails with "not initialized", but by then
  # the real join error is already gone). If join fails, check first
  # whether it's already joined (a real "already joined" error is fine to
  # skip past); anything else must stop the script, not be papered over.
  join_output="$(vexec "$pod" vault operator raft join "http://vault-0.vault-internal:8200" 2>&1)" || {
    if echo "$join_output" | grep -qi "already"; then
      echo "${pod}: $join_output" >&2
      echo "(already joined — continuing to unseal)" >&2
    else
      echo "${pod}: raft join failed:" >&2
      echo "$join_output" >&2
      exit 1
    fi
  }

  echo "Enter 3 of the 5 unseal keys for ${pod} (same keys as vault-0)." >&2
  for i in 1 2 3; do
    read -r -s -p "Unseal key ${i}/3 for ${pod}: " key
    echo >&2
    kubectl exec -n "$NAMESPACE" -i "$pod" -- vault operator unseal "$key" >/dev/null
    unset key
  done
  echo "${pod} unsealed." >&2
done

echo >&2
echo "=== Step 5: enable audit logging ===" >&2
echo "Enter the ROOT TOKEN from Step 2 to enable the audit device." >&2
read -r -s -p "Root token: " root_token
echo >&2
kubectl exec -n "$NAMESPACE" -i vault-0 -- env VAULT_TOKEN="$root_token" \
  vault audit enable file file_path=/vault/audit/vault_audit.log
unset root_token

echo >&2
echo "=== Verification (status only, no secret material) ===" >&2
for pod in "${PODS[@]}"; do
  echo "--- ${pod} ---" >&2
  vexec "$pod" vault status || true
done

echo >&2
echo "Done. All 3 pods should show Sealed: false, HA Mode consistent with" >&2
echo "one active + two standby (or all 'active' if using autopilot), and" >&2
echo "audit logging enabled. Kubernetes auth method setup (for" >&2
echo "control-plane's own IssueProviderToken RPC) is a later step, not part" >&2
echo "of this script." >&2
