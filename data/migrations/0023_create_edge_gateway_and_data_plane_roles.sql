-- Post-M2 audit fix C1 — edge_gateway and data_plane DB roles.
--
-- Until now these two services connected to CockroachDB as ROOT
-- (services/edge-gateway/src/auth/store.rs, services/data-plane/main.py).
-- root is a superuser and bypasses GRANT/REVOKE entirely — the same
-- semantics migrations 0012 and 0018 already documented as the whole
-- reason the control_plane and admin_api roles exist. The consequence,
-- found by the post-M2 audit and confirmed live: either service's
-- connection could UPDATE model_active (mutating the live model pointer
-- outside signed manifests + rollout) or UPDATE/DELETE audit_log —
-- making EC4's "no path exists" and EC3's "audit_log is immutable"
-- false outside the admin_api-scoped paths Step T actually proved.
--
-- These roles close that. Both get EXACTLY what their code does and
-- nothing more; the grant list below was derived by reading every SQL
-- statement in each service, not from the audit's summary (which was
-- wrong in two places — see the two NOTE blocks).
--
-- Privilege shape:
--   edge_gateway:
--     api_keys           SELECT  (auth/store.rs: lookup_by_hash)
--     rate_limit_policy  SELECT  (ratelimit/store.rs: rpm lookup)
--     Nothing else. edge-gateway issues no INSERT/UPDATE/DELETE at all;
--     its readiness probe is a table-less `SELECT 1`.
--     NOTE: it does NOT need SELECT on tenants. api_keys and
--     rate_limit_policy both carry org_id FKs to tenants, but foreign-key
--     validation only runs on INSERT/UPDATE — a pure reader never
--     triggers it.
--
--   data_plane:
--     request_log   INSERT  (request_log/__init__.py)
--     usage_event   INSERT  (usage_event/__init__.py)
--     health_probe  INSERT  (main.py write_health_probe, called at boot)
--     tenants       SELECT
--     NOTE: health_probe was missing from the audit's recommended list;
--     omitting it would have broken data-plane's boot. NOTE: tenants
--     SELECT is required NOT because data-plane ever queries that table
--     (it never does) but because request_log.org_id and
--     usage_event.org_id are NOT NULL REFERENCES tenants (org_id), and
--     CockroachDB requires SELECT on the REFERENCED table to validate a
--     foreign key on INSERT. This is exactly the failure migration 0022
--     had to fix for admin_api -> api_keys -> tenants; the same rule
--     applies here and is applied up front rather than after a live
--     failure.
--     data_plane gets NO UPDATE and NO DELETE on anything: every write it
--     makes is an append (a request log line, a usage event, a boot
--     probe row). It has no legitimate reason to modify or remove a row
--     anywhere in the schema, so it cannot.
--
--   Both roles get NO GRANT AT ALL on model_manifest, model_active,
--   rollout, audit_log, admin_user, policy or pricing. CockroachDB grants
--   a new role nothing by default (0012's own point), so never granting
--   is sufficient — there is nothing to revoke.
--
-- IMPORTANT — this migration alone is NOT sufficient. CockroachDB runs
-- --insecure in this cluster, so any client that can REACH port 26257
-- connects as whatever user it names, including root, with no
-- credential. Constraining the roles matters only once the datastores
-- are also network-restricted (audit fix C2, the CiliumNetworkPolicies).
-- C1 and C2 together close the finding; neither alone does.
--
-- Idempotent: safe to run repeatedly against the same cluster.

CREATE USER IF NOT EXISTS edge_gateway;
CREATE USER IF NOT EXISTS data_plane;

GRANT SELECT ON api_keys TO edge_gateway;
GRANT SELECT ON rate_limit_policy TO edge_gateway;

GRANT INSERT ON request_log TO data_plane;
GRANT INSERT ON usage_event TO data_plane;
GRANT INSERT ON health_probe TO data_plane;
GRANT SELECT ON tenants TO data_plane;

INSERT INTO schema_migrations (migration_id)
VALUES ('0023_create_edge_gateway_and_data_plane_roles')
ON CONFLICT (migration_id) DO NOTHING;
