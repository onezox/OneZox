-- Phase-04 Step C — model_manifest table: the registry's immutable,
-- versioned, signed manifest rows (Architecture Part G.2, Phase-04.txt's
-- own DATABASE TABLES REQUIRED section).
--
-- Column set matches Phase-04.txt exactly: version_id PK, model_ref,
-- spec_json, signature, created_by, created_at, status.
--
-- signature holds the Vault Transit signature (Step G) over spec_json —
-- the dedicated signing key the user specified in place of cosign/Sigstore,
-- since manifest authorship and build provenance are different trust
-- questions. status is set once at INSERT time (e.g. which validation
-- state a version was published in) and is never updated afterward — a
-- version that needs to stop being served is handled by model_active's
-- pointer moving away from it (0009), not by mutating this row.
--
-- IMMUTABILITY IS ENFORCED AT THE STORAGE LAYER, not only in the
-- RegisterModelManifest RPC's own app-level "always INSERT, never UPDATE"
-- behavior: 0012_create_control_plane_role.sql grants control-plane's own
-- DB role SELECT + INSERT on this table but never UPDATE or DELETE — so a
-- direct-SQL bypass using that role cannot mutate a published row. This
-- migration only creates the table; the role/grants live in 0012 because
-- they need every table (0008-0011) to exist first.
--
-- Idempotent: safe to run repeatedly against the same cluster.

CREATE TABLE IF NOT EXISTS model_manifest (
    version_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    model_ref  STRING NOT NULL,
    spec_json  JSONB NOT NULL,
    signature  STRING NOT NULL,
    created_by STRING NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    status     STRING NOT NULL DEFAULT 'published',

    INDEX model_manifest_model_ref_idx (model_ref)
);

INSERT INTO schema_migrations (migration_id)
VALUES ('0008_create_model_manifest')
ON CONFLICT (migration_id) DO NOTHING;
