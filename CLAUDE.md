# OneZox — Agent Working Instructions

## What this repo is
Implementation of the OneZox AI orchestration engine. The authoritative design
is in `docs/OneZox-v2-Architecture.md`. The build is split into phases in
`docs/OneZox Implementation Roadmap/` (Roadmap.txt + Phase-00..14 +
Dependencies.txt). Phase-00 (Foundation) is complete and verified. We are
currently building **Phase-01**.

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
**Phase-01 — Edge Gateway (Rust).** Replace the throwaway `edge-stub` with the
real `edge-gateway`: API-key (hashed) + JWT auth, Redis rate limiting, admission
(accept/queue/shed), normalize to internal proto, meter (span with token/cost
fields), and SSE streaming relay with backpressure + clean disconnect handling.
The edge forwards DOWNSTREAM to the existing Phase-00 `dataplane-stub` — the real
data plane is Phase-03; do NOT build it. New tables: `api_keys`,
`rate_limit_policy`. New infrastructure this phase: an ingress/load balancer
(kind has no cloud LB — a local equivalent must be chosen in the plan).

Exit criteria (Phase-01 is NOT done until all four pass — full text in
`docs/OneZox Implementation Roadmap/Phase-01.txt`):
1. Authenticated, rate-limited, metered streamed request flows
   edge -> stub -> client over SSE.
2. Security tests pass (no raw key leakage, tenant isolation at edge,
   unauth rejected).
3. Tail-latency test shows no GC-style spikes under sustained streaming.
   (On local kind, demonstrate stability/no-leak under sustained load; absolute
   p99 is a cloud-phase measurement, not a laptop number.)
4. All edge telemetry visible in the tracing/metrics backend.

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