-- Phase-05 Step L — rollout.stable_version_id: the version_id that was
-- active for model_ref at the MOMENT this rollout started, captured once
-- at CreateRollout and immutable for the rollout's own lifetime.
--
-- Why this is needed, not derivable later: an in-progress canary's etcd
-- envelope (data/migrations/0016's own K-added sibling concept) carries
-- BOTH stable and canary version_ids at every intermediate stage
-- (canary_1/10/50) — writing that envelope requires knowing what "stable"
-- currently is. model_active (0009) only ever stores the CURRENT value,
-- never history, and by design (Step H's own bootstrap-vs-rollout fix)
-- model_active is untouched for the entire duration of a canary in
-- progress — only a full promotion (stage='stable') or the original
-- bootstrap ever writes it. So there is no other place to read "what was
-- stable when this rollout began" from once the rollout is under way;
-- capturing it once, at creation, is the only correct source.
--
-- This is also exactly what AbortRollout/an automatic rollback reverts
-- TO: canary_percent back to 0, stable unchanged from whatever it was
-- before this rollout ever touched anything — never re-derived, always
-- this column's own captured value.
--
-- Idempotent: safe to run repeatedly against the same cluster.

ALTER TABLE rollout ADD COLUMN IF NOT EXISTS stable_version_id STRING NOT NULL DEFAULT '';

INSERT INTO schema_migrations (migration_id)
VALUES ('0019_alter_rollout_add_stable_version')
ON CONFLICT (migration_id) DO NOTHING;
