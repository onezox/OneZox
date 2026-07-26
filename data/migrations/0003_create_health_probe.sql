-- Phase-00, Deployment Step 8 — health_probe table.
-- Proves DB connectivity from each language runtime (Rust edge-stub, Python
-- dataplane-stub, Go provider-stub): each stub writes one row here on boot.
--
-- Idempotent: safe to run repeatedly against the same cluster.

CREATE TABLE IF NOT EXISTS health_probe (
    probe_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    service  STRING NOT NULL,
    ts       TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO schema_migrations (migration_id)
VALUES ('0003_create_health_probe')
ON CONFLICT (migration_id) DO NOTHING;
