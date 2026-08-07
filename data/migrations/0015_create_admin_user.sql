-- Phase-05 Step B — admin_user table: the identity/role source for the
-- admin surface (Part R). Deliberately a SEPARATE table and credential
-- namespace from api_keys (Phase-01) — see the Phase-05 plan's Decision 1.
-- A tenant api_keys.hash and an admin_user.credential_hash can never
-- collide into a false authentication, because they are different rows in
-- different tables, looked up by different code paths (edge-gateway vs
-- admin-api) — not merely "different code," genuinely disjoint data.
--
-- credential_hash: SHA-256 of the raw admin token, same hash function
-- edge-gateway's own hash_api_key already proved (P01 Step H1) — reused
-- pattern, not reinvented crypto. The raw token is never stored; it is
-- printed once by scripts/seed-admin-user.sh at creation time, the same
-- "shown once" discipline Phase-05.txt itself requires for createApiKey.
--
-- role: plain STRING, not an enum type — 'viewer' or 'admin' this phase
-- (see the plan's blast-radius table). A STRING keeps adding a finer role
-- later a data change, not a migration.
--
-- revoked_at: mirrors api_keys.revoked_at exactly — instant revocability
-- without deleting the audit trail's own actor reference (audit_log.actor
-- points at user_id, which must keep resolving even after revocation).
--
-- No self-service admin_user creation RPC exists this phase (see the plan:
-- admin_user provisioning is the one action with no in-panel path at all,
-- deliberately, to avoid an unscoped privilege-escalation surface) — this
-- table is populated only by scripts/seed-admin-user.sh.
--
-- Idempotent: safe to run repeatedly against the same cluster.

CREATE TABLE IF NOT EXISTS admin_user (
    user_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id          UUID NOT NULL REFERENCES tenants (org_id),
    role            STRING NOT NULL,
    credential_hash STRING NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at      TIMESTAMPTZ,

    UNIQUE (credential_hash),
    CONSTRAINT role_is_known CHECK (role IN ('viewer', 'admin'))
);

INSERT INTO schema_migrations (migration_id)
VALUES ('0015_create_admin_user')
ON CONFLICT (migration_id) DO NOTHING;
