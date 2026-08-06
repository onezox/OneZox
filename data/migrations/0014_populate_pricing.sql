-- Phase-04 Step H — populate pricing for the 5 active providers
-- (openai, anthropic, grok, glm, kimi — CLAUDE.md's own Phase-04 section:
-- "The 5 ACTIVE providers ... become registry entries", same model_ref
-- values used for model_manifest, Step T).
--
-- unit_costs_json shape: {"input_per_million_tokens": X,
-- "output_per_million_tokens": Y, "currency": "USD"} — matches
-- pricing.UnitCosts (services/control-plane/internal/pricing/pricing.go).
--
-- APPROXIMATE REFERENCE FIGURES, not live-fetched from any provider's own
-- pricing page — same "placeholder, not tuned against real data" posture
-- already established elsewhere in this codebase (e.g. provider-gateway's
-- QUOTA_LIMIT_PER_MINUTE). Representative cheap/default-tier pricing per
-- provider as of this phase's own general knowledge cutoff:
--   openai    (gpt-4o-mini-class):      $0.15 in / $0.60 out per 1M tokens
--   anthropic (claude-3-5-haiku-class): $0.80 in / $4.00 out per 1M tokens
--   grok      (xAI mini-tier):          $0.20 in / $0.50 out per 1M tokens
--   glm       (Zhipu flash-tier):       $0.10 in / $0.10 out per 1M tokens
--   kimi      (Moonshot 8k-tier):       $0.15 in / $0.15 out per 1M tokens
-- grok/glm/kimi figures in particular carry LOW confidence — verify
-- against each provider's own current pricing page before this feeds any
-- real billing decision (P04 explicitly does not wire usage_event.usd_cost
-- from this table; that gap is exactly why an approximate figure here is
-- acceptable for now and must not be trusted blindly later).
--
-- Not idempotent in the strict re-run sense (each run appends a new row
-- per provider, matching pricing's own "new row = price change over time"
-- convention, data/migrations/0011) — guarded by schema_migrations like
-- every other migration, so a normal re-run of run-migrations.sh is still
-- a no-op.

INSERT INTO pricing (model_ref, unit_costs_json) VALUES
    ('openai',    '{"input_per_million_tokens": 0.15, "output_per_million_tokens": 0.60, "currency": "USD"}'),
    ('anthropic', '{"input_per_million_tokens": 0.80, "output_per_million_tokens": 4.00, "currency": "USD"}'),
    ('grok',      '{"input_per_million_tokens": 0.20, "output_per_million_tokens": 0.50, "currency": "USD"}'),
    ('glm',       '{"input_per_million_tokens": 0.10, "output_per_million_tokens": 0.10, "currency": "USD"}'),
    ('kimi',      '{"input_per_million_tokens": 0.15, "output_per_million_tokens": 0.15, "currency": "USD"}');

INSERT INTO schema_migrations (migration_id)
VALUES ('0014_populate_pricing')
ON CONFLICT (migration_id) DO NOTHING;
