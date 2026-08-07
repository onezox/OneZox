#!/usr/bin/env bash
# Phase-05 Step C — seed an admin_user row + its own hashed credential.
#
# Mirrors data/seed/seed-test-tenant.sh's own pattern exactly (Phase-01):
# generate a raw token, hash it with the same SHA-256 hex convention
# edge-gateway's hash_api_key already established, store only the hash,
# print the raw token once (not recoverable afterward). No root/Vault
# credential is needed — admin_user is a plain CockroachDB table, same
# insecure-mode access every other seed script in this project already
# uses directly.
#
# Deliberately the ONLY way an admin_user row is created this phase — see
# the Phase-05 plan's Decision 1: no in-panel/admin-api RPC can mint a new
# admin account, to avoid an unscoped privilege-escalation surface. This
# script IS that provisioning path, run directly against the cluster.
#
# Idempotent per (org, role): if an active (non-revoked) credential already
# exists for the given org+role, prints its user_id and does not mint a
# second one — same "an active key already exists, revoke and re-run"
# discipline as seed-test-tenant.sh.
#
# Admin operators are tied to a dedicated internal org (default
# "onezox-internal"), not any customer test tenant — admin_user.org_id
# exists in Phase-05.txt's own schema for future multi-org admin scoping,
# but conflating it with data-plane's own phase01-test-tenant fixture would
# make admin identity accidentally coupled to unrelated tenant test data.
#
# Usage: seed-admin-user.sh [pod] [role: viewer|admin] [org-name]
set -euo pipefail

POD="${1:-onezox-crdb-0}"
ROLE="${2:-admin}"
ORG_NAME="${3:-onezox-internal}"
NAMESPACE="default"

if [[ "$ROLE" != "viewer" && "$ROLE" != "admin" ]]; then
  echo "role must be 'viewer' or 'admin', got: ${ROLE}" >&2
  exit 1
fi

sql() {
  kubectl exec -n "$NAMESPACE" "$POD" -- cockroach sql --insecure --format=csv --execute "$1" | tail -n +2
}

ORG_ID="$(sql "SELECT org_id FROM tenants WHERE name = '${ORG_NAME}';")"
if [[ -z "$ORG_ID" ]]; then
  ORG_ID="$(sql "INSERT INTO tenants (name) VALUES ('${ORG_NAME}') RETURNING org_id;")"
  echo "Created org ${ORG_NAME} (org_id=${ORG_ID})"
else
  echo "Reusing existing org ${ORG_NAME} (org_id=${ORG_ID})"
fi

EXISTING_USER_ID="$(sql "SELECT user_id FROM admin_user WHERE org_id = '${ORG_ID}' AND role = '${ROLE}' AND revoked_at IS NULL LIMIT 1;")"
if [[ -n "$EXISTING_USER_ID" ]]; then
  echo "An active ${ROLE} admin_user already exists for this org (user_id=${EXISTING_USER_ID})."
  echo "Raw tokens are shown only once at creation and are not recoverable; revoke it and re-run to issue a new one."
  exit 0
fi

RANDOM_HEX="$(sql "SELECT encode(gen_random_bytes(32), 'hex');")"
RAW_TOKEN="oz_admin_${ROLE}_${RANDOM_HEX}"
HASH="$(printf '%s' "$RAW_TOKEN" | sha256sum | cut -d' ' -f1)"

USER_ID="$(sql "INSERT INTO admin_user (org_id, role, credential_hash) VALUES ('${ORG_ID}', '${ROLE}', '${HASH}') RETURNING user_id;")"

echo
echo "=== Admin credential created (role=${ROLE}) — shown once, not recoverable ==="
echo "org_id:    ${ORG_ID}"
echo "user_id:   ${USER_ID}"
echo "raw token: ${RAW_TOKEN}"
echo "==============================================================================="
