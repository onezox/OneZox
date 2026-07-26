-- Phase-00, Deployment Step 8 — tenants table.
-- Schema bootstrap only per the roadmap: created here, populated and owned
-- by later phases (Part I: Website Backend / control plane). org_id uses a
-- random UUID default rather than a sequential key — the standard
-- CockroachDB idiom to avoid hotspotting a single range under concurrent
-- inserts in a distributed cluster.
--
-- Idempotent: safe to run repeatedly against the same cluster.

CREATE TABLE IF NOT EXISTS tenants (
    org_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name       STRING NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    status     STRING NOT NULL DEFAULT 'active'
);

INSERT INTO schema_migrations (migration_id)
VALUES ('0002_create_tenants')
ON CONFLICT (migration_id) DO NOTHING;
