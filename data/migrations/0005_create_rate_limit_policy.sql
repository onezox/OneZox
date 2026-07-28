-- Phase-01, Deployment Step 1 — rate_limit_policy table.
-- One policy row per tenant; edge-gateway's ratelimit module reads this to
-- size the Redis token bucket for a given org_id (tenant-scoped keys).
--
-- Idempotent: safe to run repeatedly against the same cluster.

CREATE TABLE IF NOT EXISTS rate_limit_policy (
    policy_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id      UUID NOT NULL REFERENCES tenants (org_id),
    rpm         INT4 NOT NULL,
    tpm         INT4 NOT NULL,
    concurrency INT4 NOT NULL,

    UNIQUE (org_id)
);

INSERT INTO schema_migrations (migration_id)
VALUES ('0005_create_rate_limit_policy')
ON CONFLICT (migration_id) DO NOTHING;
