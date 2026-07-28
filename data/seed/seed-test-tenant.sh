#!/usr/bin/env bash
# Phase-01, Deployment Step 4 — seed one test tenant, one hashed API key,
# and (Step D1) that tenant's rate_limit_policy row.
#
# The raw key is generated here, hashed with SHA-256 (hex-encoded, lowercase
# — the same convention edge-gateway's auth module uses to hash an incoming
# key before comparing it against api_keys.hash), and only the hash is
# stored. The raw key is printed once to stdout and is not recoverable
# afterward (CLAUDE.md: store API-key hashes, never raw keys).
#
# Idempotent: reuses the tenant if it already exists, does not mint a
# second key if it already has an active (non-revoked) one, and does not
# overwrite an existing rate_limit_policy row. rpm=60/tpm=100000/
# concurrency=10 are general-purpose test defaults, not tuned to trip on
# the first few requests — a step that specifically wants to exercise
# "rate limit exceeded" (Step H) adjusts rpm for its own test scope rather
# than relying on a low standing default here.
#
# Usage: seed-test-tenant.sh [pod] [tenant-name]
# Defaults to the standing "phase01-test-tenant" used throughout Phase-01;
# pass a second tenant name (Step H2) to seed an independent tenant for
# cross-tenant isolation testing.
set -euo pipefail

POD="${1:-onezox-crdb-0}"
NAMESPACE="default"
TENANT_NAME="${2:-phase01-test-tenant}"
DEFAULT_RPM=60
DEFAULT_TPM=100000
DEFAULT_CONCURRENCY=10

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

EXISTING_POLICY_ID="$(sql "SELECT policy_id FROM rate_limit_policy WHERE org_id = '${ORG_ID}';")"
if [[ -z "$EXISTING_POLICY_ID" ]]; then
  POLICY_ID="$(sql "INSERT INTO rate_limit_policy (org_id, rpm, tpm, concurrency) VALUES ('${ORG_ID}', ${DEFAULT_RPM}, ${DEFAULT_TPM}, ${DEFAULT_CONCURRENCY}) RETURNING policy_id;")"
  echo "Created rate_limit_policy (policy_id=${POLICY_ID}, rpm=${DEFAULT_RPM})"
else
  echo "Reusing existing rate_limit_policy (policy_id=${EXISTING_POLICY_ID})"
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
