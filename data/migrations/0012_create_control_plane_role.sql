-- Phase-04 Step C — control_plane DB role: the least-privilege identity
-- control-plane's own service code will connect as (Step D onward),
-- instead of root/admin.
--
-- This is what makes model_manifest's storage-layer immutability (0008)
-- real rather than nominal: CockroachDB's root user is a member of the
-- built-in admin role, which holds ALL PRIVILEGES on every object and is
-- NOT subject to GRANT/REVOKE restrictions (same superuser-bypass
-- semantics as PostgreSQL) — connecting as root and "revoking" UPDATE/
-- DELETE from root would enforce nothing. control_plane is a plain,
-- non-admin user; CockroachDB tables grant NO privileges to a new role by
-- default, so simply never granting UPDATE/DELETE on model_manifest is
-- sufficient (no separate REVOKE needed — there is nothing to revoke).
-- Insecure-mode auth (Phase-00's cluster posture) trusts the connecting
-- username with no password, same as every other user on this cluster.
--
-- Privilege shape:
--   model_manifest: SELECT + INSERT only. No UPDATE, no DELETE — a direct
--     SQL UPDATE/DELETE against a published row, issued as control_plane,
--     must fail. This is the adversarial property Step C's verification
--     proves.
--   model_active:   SELECT + INSERT + UPDATE — the mutable active-version
--     pointer is meant to change.
--   policy, pricing: SELECT + INSERT + UPDATE + DELETE — ordinary mutable
--     config tables, no immutability requirement was specified for these.
--
-- Idempotent: safe to run repeatedly against the same cluster.

CREATE USER IF NOT EXISTS control_plane;

GRANT SELECT, INSERT ON model_manifest TO control_plane;
GRANT SELECT, INSERT, UPDATE ON model_active TO control_plane;
GRANT SELECT, INSERT, UPDATE, DELETE ON policy TO control_plane;
GRANT SELECT, INSERT, UPDATE, DELETE ON pricing TO control_plane;

INSERT INTO schema_migrations (migration_id)
VALUES ('0012_create_control_plane_role')
ON CONFLICT (migration_id) DO NOTHING;
