-- Phase-05 Step B — rollout table: the staged-canary history/state Phase-
-- 05.txt's own schema specifies (rollout_id PK, model_ref, version_id,
-- strategy_json, stage, status, started_at, ended_at). Owned and mutated
-- exclusively by control-plane's rollout/ module (Step L) — Argo Rollouts
-- and admin-api never write here directly (see the plan's Decision 3 /
-- EC4 no-bypass design: admin-api's own DB role gets no grant on this
-- table's sibling model_manifest/model_active, and the reverse holds too —
-- only control-plane's rollout/ module advances a row's own stage/status).
--
-- Column semantics (Phase-05.txt names the columns, not their values —
-- fixed here explicitly so the meaning doesn't drift across the phase):
--   stage    — which staged step this rollout is currently at:
--              'pending' | 'canary_1' | 'canary_10' | 'canary_50' | 'stable'.
--              Driven ONLY by control-plane's AdvanceStage, reacting to the
--              Argo Rollouts controller's own step + AnalysisRun signal —
--              never client-suppliable (the EC4 API-parameter-layer proof
--              target).
--   status   — the row's own lifecycle outcome: 'running' | 'promoted' |
--              'rolled_back' | 'aborted'.
-- strategy_json is stored as STRING, not JSONB, for the same reason
-- model_manifest.spec_json is (migration 0013): it is read back and
-- compared/logged verbatim by the rollout controller and the panel's own
-- diff view; JSONB's silent reformatting is exactly the class of bug that
-- migration fixed, so every JSON blob this project treats as
-- read-verbatim-not-just-queried stays STRING by the same discipline.
--
-- No immutability constraint here (unlike model_manifest) — a rollout
-- genuinely progresses through stages over its own lifetime; that mutation
-- is the whole point of this table.
--
-- Idempotent: safe to run repeatedly against the same cluster.

CREATE TABLE IF NOT EXISTS rollout (
    rollout_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    model_ref     STRING NOT NULL,
    version_id    UUID NOT NULL,
    strategy_json STRING NOT NULL,
    stage         STRING NOT NULL DEFAULT 'pending',
    status        STRING NOT NULL DEFAULT 'running',
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at      TIMESTAMPTZ,

    CONSTRAINT stage_is_known CHECK (
        stage IN ('pending', 'canary_1', 'canary_10', 'canary_50', 'stable')
    ),
    CONSTRAINT status_is_known CHECK (
        status IN ('running', 'promoted', 'rolled_back', 'aborted')
    )
);

INSERT INTO schema_migrations (migration_id)
VALUES ('0016_create_rollout')
ON CONFLICT (migration_id) DO NOTHING;
