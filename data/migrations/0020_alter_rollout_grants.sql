-- Phase-05 Step L fix — rollout table grants corrected to match what
-- Step L's actual design turned out to need, caught live: control-plane's
-- own rollout/ module (CockroachStore) writes rollout rows directly and
-- had NO grant on the table at all — migration 0018 never anticipated
-- control-plane needing it, only admin_api. First real CreateRollout call
-- failed with "user control_plane does not have SELECT privilege on
-- relation rollout" (SQLSTATE 42501).
--
-- Fixing this properly, not just adding the missing grant: migration
-- 0018's own admin_api grant on rollout (SELECT/INSERT/UPDATE) turns out
-- to be UNUSED — Step L's actual implementation has admin-api call
-- control-plane's CreateRollout/PromoteRollout/AbortRollout/
-- GetRolloutStatus RPCs exclusively; no admin-api code path ever opens a
-- direct SQL query against rollout. Leaving that grant in place would be
-- unnecessary excess privilege undermining the exact EC4 story Step T's
-- own DB-layer adversarial proof will make: after this migration,
-- admin_api's ENTIRE DB footprint is audit_log (SELECT+INSERT) +
-- api_keys (SELECT+INSERT+UPDATE) + admin_user (SELECT) — zero grants on
-- rollout, model_manifest, model_active, policy, or pricing. Even a fully
-- compromised admin-api process cannot touch registry or rollout state
-- directly, only through control-plane's own audited, state-machine-
-- enforcing RPCs.
--
-- Idempotent: safe to run repeatedly against the same cluster.

GRANT SELECT, INSERT, UPDATE ON rollout TO control_plane;
REVOKE SELECT, INSERT, UPDATE ON rollout FROM admin_api;

INSERT INTO schema_migrations (migration_id)
VALUES ('0020_alter_rollout_grants')
ON CONFLICT (migration_id) DO NOTHING;
