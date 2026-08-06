-- Phase-04 Step C — pricing table: per-model unit costs, control-plane
-- owned (Phase-04.txt's own DATABASE TABLES REQUIRED section).
--
-- Scope note (explicit call, not an oversight): this migration provides
-- the pricing DATA only. It does NOT wire usage_event.usd_cost (still
-- NULL, deferred since Phase-03 Step H) to read from this table — that
-- would be metering-path scope, not control-plane scope, and is being
-- held as a separate, conscious follow-on rather than pulled in just
-- because the data now exists.
--
-- Idempotent: safe to run repeatedly against the same cluster.

CREATE TABLE IF NOT EXISTS pricing (
    pricing_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    model_ref       STRING NOT NULL,
    unit_costs_json JSONB NOT NULL,
    effective_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    INDEX pricing_model_ref_idx (model_ref)
);

INSERT INTO schema_migrations (migration_id)
VALUES ('0011_create_pricing')
ON CONFLICT (migration_id) DO NOTHING;
