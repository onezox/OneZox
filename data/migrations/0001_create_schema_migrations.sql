-- Phase-00, Deployment Step 8 — migration ledger.
-- Every later migration file records its own application here; this table
-- must be created first since it's what the others insert into.
--
-- Idempotent: safe to run repeatedly against the same cluster.

CREATE TABLE IF NOT EXISTS schema_migrations (
    migration_id STRING PRIMARY KEY,
    applied_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO schema_migrations (migration_id)
VALUES ('0001_create_schema_migrations')
ON CONFLICT (migration_id) DO NOTHING;
