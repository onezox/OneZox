-- Phase-03, Deployment Step 1 — usage_event table: the billing substrate
-- (EC2). One row per completed (or failed) Submit request.
--
-- tokens_in/tokens_out/orch_tokens/usd_cost are NULLABLE, not NOT NULL
-- DEFAULT 0: this mirrors proto/provider's own Delta.input_tokens/
-- output_tokens presence semantics (Phase-03 Step A) — NULL means "not
-- known," which is a genuinely different fact than "zero," and must not
-- be conflated with it. Step H (real metering) is what decides exactly
-- when a row gets written with some fields NULL versus fully populated;
-- this migration only needs to allow that state to be representable.
--
-- Idempotent: safe to run repeatedly against the same cluster.

CREATE TABLE IF NOT EXISTS usage_event (
    event_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id      UUID NOT NULL REFERENCES tenants (org_id),
    request_id  STRING NOT NULL,
    tokens_in   INT4,
    tokens_out  INT4,
    orch_tokens INT4,
    usd_cost    DECIMAL,
    model_ref   STRING NOT NULL,
    ts          TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO schema_migrations (migration_id)
VALUES ('0006_create_usage_event')
ON CONFLICT (migration_id) DO NOTHING;
