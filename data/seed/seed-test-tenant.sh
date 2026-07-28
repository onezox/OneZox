#!/usr/bin/env bash
# Phase-01, Deployment Step 4 — seed one test tenant + one hashed API key.
#
# The raw key is generated here, hashed with SHA-256 (hex-encoded, lowercase
# — the same convention edge-gateway's auth module uses to hash an incoming
# key before comparing it against api_keys.hash), and only the hash is
# stored. The raw key is printed once to stdout and is not recoverable
# afterward (CLAUDE.md: store API-key hashes, never raw keys).
#
# Idempotent: reuses the existing "phase01-test-tenant" tenant if present,
# and does not mint a second key if this tenant already has an active
# (non-revoked) one.
set -euo pipefail

POD="${1:-onezox-crdb-0}"
NAMESPACE="default"
TENANT_NAME="phase01-test-tenant"

sql() {
  kubectl exec -n "$NAMESPACE" "$POD" -- cockroach sql --insecure --format=csv --execute "$1" | tail -n +2
}

ORG_ID="$(sql "SELECT org_id FROM tenants WHERE name = '${TENANT_NAME}';")"
if [[ -z "$ORG_ID" ]]; then
  ORG_ID="$(sql "INSERT INTO tenants (name) VALUES ('${TENANT_NAME}') RETURNING org_id;")"
  echo "Created tenant ${TENANT_NAME} (org_id=${ORG_ID})"
else
  echo "Reusing existing tenant ${TENANT_NAME} (org_id=${ORG_ID})"
fi

EXISTING_KEY_ID="$(sql "SELECT key_id FROM api_keys WHERE org_id = '${ORG_ID}' AND revoked_at IS NULL LIMIT 1;")"
if [[ -n "$EXISTING_KEY_ID" ]]; then
  echo "An active API key already exists for this tenant (key_id=${EXISTING_KEY_ID})."
  echo "Raw keys are shown only once at creation and are not recoverable; revoke it and re-run to issue a new one."
  exit 0
fi

RANDOM_HEX="$(sql "SELECT encode(gen_random_bytes(32), 'hex');")"
RAW_KEY="oz_test_${RANDOM_HEX}"
HASH="$(printf '%s' "$RAW_KEY" | sha256sum | cut -d' ' -f1)"

KEY_ID="$(sql "INSERT INTO api_keys (org_id, hash, scopes) VALUES ('${ORG_ID}', '${HASH}', ARRAY['chat.completions','responses','embeddings','models']) RETURNING key_id;")"

echo
echo "=== Test API key created — shown once, not recoverable ==="
echo "org_id:  ${ORG_ID}"
echo "key_id:  ${KEY_ID}"
echo "raw key: ${RAW_KEY}"
echo "============================================================"
