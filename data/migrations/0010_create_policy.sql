-- Phase-04 Step C — policy table: per-org policy rules, control-plane
-- owned (Phase-04.txt's own DATABASE TABLES REQUIRED section). No
-- immutability constraint here — that requirement is scoped explicitly to
-- model_manifest (0008) only; policy rows are ordinary mutable config.
--
-- Idempotent: safe to run repeatedly against the same cluster.

CREATE TABLE IF NOT EXISTS policy (
    policy_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id     UUID NOT NULL REFERENCES tenants (org_id),
    rules_json JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    INDEX policy_org_id_idx (org_id)
);

INSERT INTO schema_migrations (migration_id)
VALUES ('0010_create_policy')
ON CONFLICT (migration_id) DO NOTHING;
