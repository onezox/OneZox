-- Phase-01, Deployment Step 1 — api_keys table.
-- Stores a HASH of each developer API key, never the raw key (CLAUDE.md hard
-- rule; Part O tenant isolation begins at the edge). edge-gateway's auth
-- module hashes an incoming key and looks it up by hash, resolving org_id.
--
-- Idempotent: safe to run repeatedly against the same cluster.

CREATE TABLE IF NOT EXISTS api_keys (
    key_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id     UUID NOT NULL REFERENCES tenants (org_id),
    hash       STRING NOT NULL,
    scopes     STRING[] NOT NULL DEFAULT ARRAY[]::STRING[],
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at TIMESTAMPTZ,

    UNIQUE (hash)
);

INSERT INTO schema_migrations (migration_id)
VALUES ('0004_create_api_keys')
ON CONFLICT (migration_id) DO NOTHING;
