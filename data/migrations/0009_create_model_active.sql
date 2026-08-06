-- Phase-04 Step C — model_active table: the MUTABLE active-version pointer
-- per model_ref, distinct from model_manifest's immutable version rows
-- (0008). This is the table that changes on every rollout/activation —
-- "which version is live," not the version's own content. Also the row
-- RegisterModelManifest/an explicit activation call updates after a new
-- model_manifest version is published, and what etcd's
-- /onezox/active/{model_ref} key (Step R) mirrors for edge/data-plane
-- consumers.
--
-- Idempotent: safe to run repeatedly against the same cluster.

CREATE TABLE IF NOT EXISTS model_active (
    model_ref         STRING PRIMARY KEY,
    active_version_id UUID NOT NULL REFERENCES model_manifest (version_id),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO schema_migrations (migration_id)
VALUES ('0009_create_model_active')
ON CONFLICT (migration_id) DO NOTHING;
