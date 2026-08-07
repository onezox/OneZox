# OneZox — Agent Working Instructions

## What this repo is
Implementation of the OneZox AI orchestration engine. The authoritative design
is in `docs/OneZox-v2-Architecture.md`. The build is split into phases in
`docs/OneZox Implementation Roadmap/` (Roadmap.txt + Phase-00..14 +
Dependencies.txt). Phase-00 (Foundation), Phase-01 (Edge Gateway), Phase-02
(Provider Gateway), Phase-03 (Data Plane Fast Path — SHIPPABLE MILESTONE M1), and
Phase-04 (Control Plane: Registry/Manifests/etcd/Vault) are complete and verified.
A between-phase provider task (added Grok/GLM/Kimi via OpenAI-compatible endpoints,
deactivated Gemini/F13) is also complete. We are currently building **Phase-05**.

## Hard rules
- NEVER modify `docs/OneZox-v2-Architecture.md`. It is the design source of truth.
- Implement the current phase EXACTLY as written in its Phase file. Do not
  redesign, optimize, or swap technologies. If something seems wrong, STOP and
  ask — do not "improve" it silently.
- Stay within the current phase's scope. Do not build components that belong to
  a later phase (see `docs/OneZox Implementation Roadmap/Dependencies.txt`).
- Work in small, independently-verifiable increments. One component at a time.
  Show me the plan and get agreement BEFORE writing files.
- Output full files, not diffs, when creating something new.
- Every increment must be runnable/testable before moving on. Commit after each
  verified step (commits are phase checkpoints).

## Local build reality
- We run on a local kind cluster (onezox-dev) in WSL2 Ubuntu 24.04, with MinIO
  standing in for object storage. Everything ships via GitOps: manifest -> git ->
  Argo CD sync. Do NOT apply stub/service or CiliumNetworkPolicy manifests
  directly with kubectl; go through the "onezox-stubs" Argo CD Application.
- Terraform cloud-provisioning and Karpenter are DEFERRED to a cloud phase —
  their files exist in `platform/` per the folder structure, but do not try to
  make them functional locally, and do not mark those as complete in a local build.
- Store API-key HASHES, never raw keys. Provider credentials live in Vault (F9,
  Phase-04) — provider-gateway authenticates via K8s-auth and short-lived scoped
  tokens; the old K8s Secret is deleted. NEVER run commands that print Secret or
  Vault VALUES (e.g. `kubectl get secret -o yaml/json` decodes them; grpcurl on a
  token RPC prints the token) — verify field NAMES / response SHAPE only. Base64
  and token exposures happened twice via careless queries; do not repeat.
- Vault is HA (3-node Raft) in the `default` namespace. On a WSL2 host restart Vault
  RESEALS — followers auto-rejoin (durable retry_join), but the cluster needs
  UNSEALING (3-of-5 keys x 3 pods) before control-plane/data-plane/edge-gateway can
  verify manifests and provider-gateway can fetch creds. Unseal keys live in
  encrypted storage; the root token has been revoked (all 4 consumers use K8s-auth;
  regenerable from unseal keys only if a future root op is needed).
- Known data note: `health_probe` and `tenants` contain troubleshooting rows
  from Phase-00 failed boots. Do not assume those tables are empty.
- Session-start ritual: this local cluster (kind on WSL2) loses Redis Cluster
  gossip state on host restart, and Vault reseals. Run
  `scripts/recover-redis-cluster.sh` (idempotent) AND confirm Vault is unsealed
  (`kubectl get pods -A | grep -i vault` -> all 1/1; unseal if sealed) at the start
  of every session before building. Recovery/seal drift has surfaced sideways
  mid-build multiple times when skipped.

## Current phase
**Phase-05 — Admin Panel, Model Studio, Canary Rollouts (TypeScript/Next.js RSC).
MILESTONE M2 — Safe Model Ops.** Build the operator control surface over the
Phase-04 registry: an admin panel (Next.js RSC) with a Model Studio to author +
version virtual-model manifests, and Argo Rollouts canary deployment that promotes
new manifest versions by staged traffic (1% -> 10% -> 50% -> 100%) with metric-based
AUTOMATIC ROLLBACK on SLO regression. Implements Architecture Parts R, G.2, A, N.5,
C. Completes build stage 3. This phase CONSUMES Phase-04's control plane — it does
NOT rebuild registry/signing/etcd logic.

New services: admin-panel (Next.js RSC web), admin-api (gRPC + GraphQL gateway
bridging panel <-> control-plane). New contract: proto/admin + GraphQL schema.
New CockroachDB tables: rollout, audit_log (immutable append), admin_user.
(api_keys from P01 is reused for the key-management UI.) Argo Rollouts deployed
this phase (declared in Terraform since P00). admin-api mTLS to control-plane.

Phase-05 decisions to resolve in the plan (surface up front):
- AUTHORIZATION MODEL — highest-stakes decision. Admin RBAC via admin_user.role,
  least-privilege actions, DISTINCT from tenant API-key auth. The admin surface can
  publish manifests, start rollouts, and manage keys — whoever reaches it changes
  what real traffic hits. Prove adversarially: a NON-admin cannot publish/rollout
  (EC3), positive-controlled (prove the check CAN reject before trusting it passes).
- CANARY + AUTO-ROLLBACK — the reason P04 manifests are immutable + versioned. A new
  version stages 1/10/50/100% via Argo Rollouts; analysis templates read SLO metrics
  from Prometheus; regression triggers automatic rollback. Prove a rollback ACTUALLY
  reverts live traffic (EC2) by INDUCING a regression, not just asserting the config.
- This is a CONSUMER of P04: createModelDraft/publishModelVersion -> control-plane
  RegisterModelManifest; startRollout -> Argo Rollouts. Do NOT re-implement registry,
  signing, or etcd logic.
- SCOPE: build admin-panel + admin-api + canary only. The panel's traces/cost/eval
  data panels are SCAFFOLDED-BUT-INERT this phase (F11 — traces/cost populate in P13,
  eval in P12); build the shells, do NOT wire data sources that belong to later
  phases. Do NOT build the deliberate path/LLM planner (P06), multi-agent (P07), or
  chat/memory (P08/P09).
- First TypeScript/web service in the stack — apply the same rigor bar as the
  backend services: structured logging (success path too) + telemetry from day one,
  GitOps/Argo deploy, immutable/digest tags, mesh-identity-safe, admin-api mTLS to
  control-plane, panel over TLS. Read the frontend-design skill before building UI.
- Carried deferrals still open: F13 (Gemini, suspended), F2 (KEDA -> P13), provider-
  gateway HPA (tracked), usd_cost wiring (-> P06), Redis working-memory isolation
  model (documented), F11 (panel trace/cost/eval panels inert until P12/P13).

Exit criteria (Phase-05 is NOT done until all pass — GATES MILESTONE M2; verified
verbatim against `docs/OneZox Implementation Roadmap/Phase-05.txt`):
1. A new virtual-model version is authored, published, and canaried to 100%.
2. An induced regression triggers automatic rollback.
3. All admin actions are audited; RBAC enforced.
4. No path exists to mutate a live model outside signed manifests + rollout.

Implementation plan (authorization model, canary traffic-split mechanism, and
the EC2/EC4 adversarial proofs, all resolved before coding started) is at
`/home/aasif/.claude/plans/wiggly-riding-muffin.md`.

## At each phase transition
Update the "Current phase" section above to the new phase and paste in that
phase's scope + exit criteria before starting it.

<!-- nx configuration start-->
<!-- Leave the start & end comments to automatically receive updates. -->

# General Guidelines for working with Nx

- For navigating/exploring the workspace, invoke the `nx-workspace` skill first - it has patterns for querying projects, targets, and dependencies
- When running tasks (for example build, lint, test, e2e, etc.), always prefer running the task through `nx` (i.e. `nx run`, `nx run-many`, `nx affected`) instead of using the underlying tooling directly
- Prefix nx commands with the workspace's package manager (e.g., `pnpm nx build`, `npm exec nx test`) - avoids using globally installed CLI
- You have access to the Nx MCP server and its tools, use them to help the user
- For Nx plugin best practices, check `node_modules/@nx/<plugin>/PLUGIN.md`. Not all plugins have this file - proceed without it if unavailable.
- NEVER guess CLI flags - always check nx_docs or `--help` first when unsure

## Scaffolding & Generators

- For scaffolding tasks (creating apps, libs, project structure, setup), ALWAYS invoke the `nx-generate` skill FIRST before exploring or calling MCP tools

## When to use nx_docs

- USE for: advanced config options, unfamiliar flags, migration guides, plugin configuration, edge cases
- DON'T USE for: basic generator syntax (`nx g @nx/react:app`), standard commands, things you already know
- The `nx-generate` skill handles generator discovery internally - don't call nx_docs just to look up generator syntax

<!-- nx configuration end-->