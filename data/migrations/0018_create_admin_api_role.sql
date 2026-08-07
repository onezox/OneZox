-- Phase-05 Step B — admin_api DB role: the least-privilege identity
-- admin-api's own service code connects as (Step E onward), mirroring
-- control_plane's own role exactly (migration 0012) — a plain, non-admin
-- CockroachDB user, not root/admin, so GRANT/REVOKE actually constrains it
-- (root/admin bypasses GRANT restrictions entirely, same superuser
-- semantics 0012 already documented).
--
-- This is the DB-layer half of the Phase-05 plan's EC4 no-bypass proof
-- (Decision 3, sub-test 1): admin_api gets NO grant at all on
-- model_manifest, model_active, policy, or pricing — Phase-04's own
-- registry tables, unchanged. CockroachDB grants no privileges to a new
-- role by default (0012's own point), so simply never granting anything
-- on those four tables is sufficient; there is nothing to revoke. Even a
-- fully compromised admin-api process cannot touch live model state
-- directly — every mutation to it must go through control-plane's own
-- RPCs (RegisterModelManifest, rollout/'s AdvanceStage/Rollback), which
-- enforce the staged-rollout state machine admin-api itself cannot skip.
--
-- Privilege shape:
--   audit_log:   SELECT + INSERT only. No UPDATE, no DELETE — immutable
--     append, the same mechanism model_manifest uses (0008/0012). This is
--     the adversarial property Step Q's verification proves.
--   rollout:     SELECT + INSERT + UPDATE. No DELETE — a rollout's own
--     stage/status genuinely progresses over its lifetime (0016's own
--     comment), but its history is never erased.
--   admin_user:  SELECT only. No INSERT/UPDATE/DELETE — admin_user
--     provisioning stays scripts/seed-admin-user.sh's job (run directly
--     against the cluster, not through admin-api), never a capability
--     admin-api's own running code has.
--   api_keys:    SELECT + INSERT + UPDATE (createApiKey inserts,
--     revokeApiKey sets revoked_at, listApiKeys reads) — reusing Phase-01's
--     table as Phase-05.txt specifies. No DELETE: a revoked key stays a
--     row, it is never removed.
--   model_manifest, model_active, policy, pricing: NO GRANTS. See above.
--
-- Idempotent: safe to run repeatedly against the same cluster.

CREATE USER IF NOT EXISTS admin_api;

GRANT SELECT, INSERT ON audit_log TO admin_api;
GRANT SELECT, INSERT, UPDATE ON rollout TO admin_api;
GRANT SELECT ON admin_user TO admin_api;
GRANT SELECT, INSERT, UPDATE ON api_keys TO admin_api;

INSERT INTO schema_migrations (migration_id)
VALUES ('0018_create_admin_api_role')
ON CONFLICT (migration_id) DO NOTHING;
