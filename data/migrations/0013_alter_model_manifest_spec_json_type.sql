-- Phase-04 Step E fix — model_manifest.spec_json: JSONB -> STRING.
--
-- Live bug found during Step E's own end-to-end verification against real
-- Vault Transit: CockroachDB's JSONB column type does NOT preserve input
-- text verbatim — it parses to a structured value and reserializes
-- deterministically on read. Confirmed live: inserting
-- spec_json='{"provider":"test"}' (no space, exactly what was signed)
-- came back from a SELECT as '{"provider": "test"}' (a space after the
-- colon that was never in the signed bytes). RegisterModelManifest signs
-- the exact input bytes BEFORE insert (data/migrations/0008's own
-- reasoning: the signature must cover the real content); GetModelManifest
-- re-verifies against whatever bytes come back FROM the table — a JSONB
-- round-trip reformatting even one space breaks that byte-for-byte match,
-- so a manifest this service itself just signed failed its own
-- verification. STRING preserves exactly what was written, which is what
-- a signed payload actually needs: byte-identical round-trip, not "the
-- same JSON up to formatting."
--
-- Loses JSONB's own "this is valid JSON" enforcement at the DB layer —
-- compensated in application code instead (registry.RegisterModelManifest
-- now validates spec_json with encoding/json.Valid before signing/
-- inserting), matching this project's "validate at the boundary" default.
--
-- pricing.unit_costs_json and policy.rules_json (0010, 0011) are
-- deliberately NOT changed here — neither is ever signed/byte-verified,
-- so JSONB's reformatting is harmless for them; this fix is scoped to the
-- one column that's actually broken by it.
--
-- Idempotent: safe to run repeatedly against the same cluster (re-running
-- ALTER ... TYPE STRING on an already-STRING column is a no-op; the
-- schema_migrations insert below is the actual idempotency guard
-- run-migrations.sh relies on).

ALTER TABLE model_manifest ALTER COLUMN spec_json TYPE STRING USING spec_json::STRING;

INSERT INTO schema_migrations (migration_id)
VALUES ('0013_alter_model_manifest_spec_json_type')
ON CONFLICT (migration_id) DO NOTHING;
