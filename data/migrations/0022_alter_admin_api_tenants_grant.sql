-- Phase-05 Step S fix — admin_api needs SELECT on tenants.
--
-- Live-caught during Step T's setup: CreateApiKey's INSERT INTO api_keys
-- fails with "user admin_api does not have SELECT privilege on relation
-- tenants (SQLSTATE 42501)" — api_keys.org_id REFERENCES tenants(org_id)
-- (migration 0004), and CockroachDB/Postgres FK enforcement requires the
-- inserting role to have SELECT on the REFERENCED table to check the
-- constraint at all, not just the table being written to.
--
-- This is a functional grant, not a security loosening: SELECT-only,
-- read-only, and it does not touch model_manifest/model_active/policy/
-- pricing (the P04 control-plane tables Decision 3's EC4 proof requires
-- admin_api to have zero access to, unaffected by this migration).
-- tenants is P01's own tenant registry — org names/ids, nothing
-- security-sensitive, and admin-api already legitimately handles a
-- client-supplied org_id for every CreateApiKey call.
--
-- Idempotent: safe to run repeatedly against the same cluster.

GRANT SELECT ON tenants TO admin_api;

INSERT INTO schema_migrations (migration_id)
VALUES ('0022_alter_admin_api_tenants_grant')
ON CONFLICT (migration_id) DO NOTHING;
