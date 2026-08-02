# OneZox — Agent Working Instructions

## What this repo is
Implementation of the OneZox AI orchestration engine. The authoritative design
is in `docs/OneZox-v2-Architecture.md`. The build is split into phases in
`docs/OneZox Implementation Roadmap/` (Roadmap.txt + Phase-00..14 +
Dependencies.txt). Phase-00 (Foundation) and Phase-01 (Edge Gateway) are
complete and verified. We are currently building **Phase-02**.

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
- Store API-key HASHES, never raw keys. Secrets via Kubernetes Secrets until
  Vault arrives in Phase-04. Private signing keys stay gitignored (CI uses the
  COSIGN_KEY / COSIGN_PASSWORD repo secrets).
- Known data note: `health_probe` and `tenants` contain troubleshooting rows
  from Phase-00 failed boots. Do not assume those tables are empty.

## Current phase
**Phase-02 — Provider Gateway (Go).** Build the dedicated Go service that owns
all provider concerns, replacing the throwaway `provider-stub`: provider adapters
(OpenAI/Anthropic/Google) behind one internal contract, a fleet-wide quota
governor (shared Redis counters, not per-pod), per-provider circuit breakers with
fallback signaling, request coalescing, streaming passthrough with backpressure,
and prefix-cache handle passthrough. Called by the data plane (Phase-03) via
gRPC — for THIS phase a test harness drives it directly; do NOT build the Phase-03
data plane. Contract: `proto/provider` (Invoke / InvokeEmbedding / ProviderHealth).
No relational tables this phase. Redis keys: `provider:{name}:quota:{window}`,
`provider:{name}:breaker`.

Phase-02 scoping decisions and deferrals:
- Provider testing is split: the resilience logic (breaker, quota, coalescing,
  fan-out cap) is tested against a controllable FAKE provider (fail/throttle/
  slow-stream on command) — a real provider can't be made to fail on demand and
  hammering it costs money. ONE real streamed call per provider proves adapter
  wire-format correctness (EC1). A breaker that never trips looks identical to a
  healthy one; the fake is how we prove it CAN fire.
- Credentials: real provider API key(s) in a K8s Secret mounted ONLY to
  provider-gateway (Dependencies.txt F9). Vault-issued tokens are Phase-04 — do
  NOT stand up Vault. EC3's "credentials never leak" test runs against this setup.
- Scaling: basic HPA only. Token-aware KEDA + fleet-governor scaling is
  F2-deferred to Phase-13 — do NOT build the full governor-driven autoscaling.
- Egress: default-deny with an allow-list for approved provider endpoints (built
  on kind+Cilium this phase). "Can a pod reach the real provider through the
  egress policy" is a step to VERIFY, not assume.
- Network hazard: this network has a TLS-intercepting proxy (confirmed Phase-00,
  broke cosign's Rekor upload). A real HTTPS provider call may hit it — detect/
  handle, don't discover mid-test.
- Deployment via the onezox-stubs Argo Application. Prefer immutable/digest image
  tags from the start to avoid Phase-01's same-tag-no-diff manual rollout-restart
  workaround.

Exit criteria (Phase-02 is NOT done until all four pass — full text in
`docs/OneZox Implementation Roadmap/Phase-02.txt`):
1. Streamed completion from a real provider via Invoke, end-to-end, metered.
2. Breaker + fallback signal + fleet-wide quota all demonstrated under fault
   injection (against the fake provider).
3. Credential isolation and egress allow-list verified.
4. Gateway telemetry (per-provider latency, breaker state, headroom) visible.

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