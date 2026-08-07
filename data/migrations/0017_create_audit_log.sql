-- Phase-05 Step B — audit_log table: an immutable append-only record of
-- every admin action (Part R / SECURITY IMPLEMENTATION). Same storage-
-- layer-immutability discipline as model_manifest (Phase-04 migrations
-- 0008/0012): the mechanism is the admin_api DB role's own GRANTs
-- (migration 0018), not application code — SELECT+INSERT only, no UPDATE,
-- no DELETE, so a direct SQL mutation against a written row fails at the
-- database itself, adversarially proven in Step Q the same way Phase-04
-- Step F proved model_manifest's immutability.
--
-- actor references admin_user.user_id — every row, success OR denial,
-- traces to a real identity (Step H/J/R: denied RBAC attempts are audited
-- too, not just successful mutations).
--
-- before_json/after_json ARE JSONB here, unlike model_manifest.spec_json
-- or rollout.strategy_json: nothing in this project ever signs, hashes, or
-- byte-compares an audit_log row's JSON — it exists to be queried and
-- displayed (the panel's Audit section, Model Studio's diff view), which
-- is exactly JSONB's own strength. Migration 0013's byte-exactness lesson
-- applies specifically to JSON that gets signed/verified; this isn't that.
--
-- Idempotent: safe to run repeatedly against the same cluster.

CREATE TABLE IF NOT EXISTS audit_log (
    audit_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    actor       UUID NOT NULL REFERENCES admin_user (user_id),
    action      STRING NOT NULL,
    target      STRING NOT NULL,
    before_json JSONB,
    after_json  JSONB,
    ts          TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO schema_migrations (migration_id)
VALUES ('0017_create_audit_log')
ON CONFLICT (migration_id) DO NOTHING;
