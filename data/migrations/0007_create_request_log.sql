-- Phase-03, Deployment Step 1 — request_log table: one row per Submit
-- request, independent of usage_event (a request can fail before any
-- provider usage exists at all, and still needs a log row).
--
-- Idempotent: safe to run repeatedly against the same cluster.

CREATE TABLE IF NOT EXISTS request_log (
    request_id STRING PRIMARY KEY,
    org_id     UUID NOT NULL REFERENCES tenants (org_id),
    path       STRING NOT NULL,
    status     STRING NOT NULL,
    latency_ms INT4 NOT NULL,
    ts         TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO schema_migrations (migration_id)
VALUES ('0007_create_request_log')
ON CONFLICT (migration_id) DO NOTHING;
