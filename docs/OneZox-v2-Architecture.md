# OneZox v2 — Production AI Orchestration Platform
## Principal-Level Architecture (10-Year, Billions-of-Requests, Multi-Region)

> **Mandate:** This is a ground-up redesign, not a review. Where v1 made a weak call, it is replaced and the replacement is defended. Target: a platform on par with the best AI orchestration companies — millions of developers, billions of requests, multi-region active-active, enterprise + on-prem, and forward-compatible with models that don't exist yet.

**What OneZox is:** a *control layer above the frontier labs*. It does not train foundation models. It exposes composable "virtual models" that plan, route, decompose, execute tools, verify, and synthesize across OpenAI / Anthropic / Google (and self-hosted) workers, with correctness, cost, and latency as first-class SLOs.

---

# PART A — WHAT CHANGES FROM v1, AND WHY

Before the new design, here is the honest teardown. Each v1 decision that dies, and why.

| v1 decision | Why it's not ideal | v2 replacement |
|---|---|---|
| Go gateway + Python engine, gRPC between | Correct instinct, but under-specified. No control/data-plane split, no admission control, no backpressure design, single hop | Keep Go edge + Python brain, but split into **control plane / data plane / execution plane**; add a Rust-based inference-adjacent hot path for token streaming and KV/prefix cache | 
| "Virtual model = a DB row, callable instantly" | Good idea, unsafe as stated. Live-editing a model that's serving billions of requests with no versioning = outages | **Immutable, versioned model manifests** with staged rollout, canary, instant rollback (like a config-as-code deploy, not a DB UPDATE) |
| Orchestrator = "custom, no framework" | Right to avoid LangChain bloat, but "custom" hand-waves the hardest part | A **typed workflow IR (intermediate representation)** + a deterministic **planner/compiler/scheduler** separation. The plan is a DAG artifact, not imperative code |
| litellm as the provider layer | Fine for a prototype, a liability at scale. Single-process abstraction, weak on streaming backpressure, rate-limit coordination, and provider-specific cache handles | A dedicated **Provider Gateway service** (own deployment) with per-provider circuit breakers, global token-bucket quota in a shared store, request coalescing, and a normalized streaming protocol |
| Redis for "rate limits + config + workflow state + prefix cache" | Overloading one store with four different consistency/durability needs | Split by need: **Redis** (rate limit, ephemeral), **etcd/Consul** (config/service discovery), **durable workflow store** (FoundationDB or Postgres+outbox), **dedicated prefix/KV cache tier** |
| "HPA on RPS / queue depth" | RPS is the wrong signal for LLM workloads — cost is token- and latency-bound, not request-bound | **Token-aware, SLO-driven autoscaling** using concurrency + tokens-in-flight + provider headroom, via KEDA custom metrics |
| Kafka *or* Redis Streams "for async" | Vague. Different jobs need different delivery semantics | **Two buses**: an **event bus** (Kafka/Redpanda, durable, replayable, audit + analytics) and a **work queue** (NATS JetStream or Temporal for durable workflow execution) |
| No evaluation system | Fatal gap. You cannot run an orchestrator in prod without continuous quality measurement | Full **eval + replay + shadow-traffic + LLM-judge + regression-gate** subsystem |
| No disaster recovery, no multi-region | Not production-grade | **Active-active multi-region**, cell-based architecture, tiered RTO/RPO, chaos-tested failover |
| Sandbox = "gVisor/Firecracker" (mentioned, not designed) | Correct primitives, no design | Full **execution plane**: Firecracker microVMs, egress-controlled, ephemeral, per-tenant isolation, seccomp + no-network-by-default |
| Trace viewer as an admin feature | Undersold. Observability is infrastructure, not a panel | **OpenTelemetry-native**, every span carries token/cost/provider attributes; traces are the substrate the eval system and the router both learn from |
| No memory architecture beyond "workflow state in Redis" | Missing entire tier | **Three-tier memory**: working (in-request), episodic (session), semantic (long-term, indexed), with explicit promotion/eviction |

The through-line: **v1 conflated planes.** A world-class engine strictly separates *control* (what should happen), *data/orchestration* (deciding the plan), and *execution* (doing the work + calling providers + running tools). Every redesign below flows from that separation.

---

# PART B — THE THREE-PLANE ARCHITECTURE

The single most important structural decision. Everything hangs off this.

```
┌───────────────────────────────────────────────────────────────────────────┐
│                              CONTROL PLANE                                  │
│  Model registry · config-as-code · rollout/canary · quotas · policy ·       │
│  provider-key vault · tenant/RBAC · feature flags · pricing catalog         │
│  (source of truth; low QPS; strong consistency; never on the hot path)      │
└───────────────────────────────┬───────────────────────────────────────────┘
                                 │ pushes signed, versioned manifests
                                 ▼  (etcd watch / config stream — cached at edge)
┌───────────────────────────────────────────────────────────────────────────┐
│                       DATA / ORCHESTRATION PLANE                            │
│  API gateway · admission control · planner · scheduler · context manager ·  │
│  retrieval · aggregator/verifier · streaming fan-in                         │
│  (high QPS; the "brain"; latency-critical; horizontally sharded by tenant)  │
│  (calls Memory Service + Chat Service for durable slices — owns neither)     │
└───────────────────────────────┬───────────────────────────────────────────┘
                                 │ dispatches typed work items
                                 ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                            EXECUTION PLANE                                  │
│  Provider Gateway (OpenAI/Anthropic/Google/self-host) · Tool Sandbox        │
│  (Firecracker) · embedding/rerank workers · GPU inference (self-hosted) ·   │
│  KV/prefix cache servers · Memory Service (owns AI memory) ·                 │
│  Chat Service (owns chat history)                                          │
│  (does the actual expensive work; isolated; independently scaled)          │
└───────────────────────────────────────────────────────────────────────────┘
```

**Why three planes, not one engine:**
1. **Blast radius.** A bad model manifest can't take down request serving — the control plane is off the hot path; the data plane runs on last-known-good cached config even if control is fully down.
2. **Independent scaling.** Provider calls (execution) scale with token volume; planning (data) scales with request count; config (control) barely scales at all. Coupling them wastes money and creates false bottlenecks.
3. **Security isolation.** Provider keys and tenant secrets live only in the control-plane vault; the execution plane gets short-lived scoped credentials. A compromised sandbox never sees a raw provider key.
4. **Independent deploy cadence.** You ship router logic 20×/day and touch the vault once a month. Different risk, different pipelines.

**Trade-off:** three planes = more services, more inter-service contracts, more operational surface. Mitigated by strict typed contracts (protobuf + buf schema registry) and a service mesh. For a platform at this scale, the coordination cost is far smaller than the coupling cost of a monolith-engine.

---

# PART C — LANGUAGE STRATEGY (and why each)

v1's "Go + Python" was directionally right but incomplete. The correct answer is **three languages, each where it dominates**, and a hard rule: *no language crosses into a layer it's bad at.*

| Layer | Language | Why this one, not the others |
|---|---|---|
| **Edge / API gateway / streaming relay / hot-path proxy** | **Rust** (was Go) | The single hottest path handles every byte of every streamed token for billions of requests. Rust gives predictable tail latency (no GC pauses — Go's GC shows up in p99 under sustained streaming load), zero-cost async (Tokio), and memory safety. Go is *fine* here; Rust is *better* at the p99 that defines a premium API. |
| **Orchestration / planner / scheduler / context / retrieval** | **Python 3.12+** | This is where the AI ecosystem, tokenizers, and iteration speed live. Correctness and velocity beat raw speed here because these components are I/O-bound (waiting on providers), not CPU-bound. Use `asyncio` + `uvloop`; offload CPU-heavy chunking to Rust extensions (via PyO3) where profiling demands it. |
| **Provider Gateway / quota coordinator / circuit breakers** | **Go** | Massive fan-out of concurrent outbound HTTP/2 streams to providers. Go's goroutine model and mature HTTP/2 stack make this the sweet spot: high concurrency, simple code, good-enough latency (it's provider-bound anyway). |
| **Tool sandbox supervisor** | **Rust** | Manages Firecracker microVMs, seccomp, cgroups. Systems-level, security-critical, must not leak. |
| **Admin panel / dashboards** | **TypeScript / Next.js (RSC)** | Obvious. Real-time, SSR, huge ecosystem. |
| **Eval harness / data pipelines / analytics** | **Python + SQL (dbt)** | Data-science ergonomics. |

**Rule enforced:** CPU-bound + latency-critical → Rust. Concurrency-bound + I/O → Go. AI logic + iteration → Python. This is not language fashion; each choice is defended by the workload's bottleneck.

---

# PART D — REQUEST TOPOLOGY (full path)

```
                          ┌────────────────────────────────────────┐
                          │        GLOBAL ANYCAST (Cloudflare/      │
   Developer / SDK ──────►│        AWS Global Accelerator)          │
                          │        DDoS · TLS term · WAF · geo-route │
                          └───────────────────┬────────────────────┘
                                              │ routes to nearest healthy REGION
        ┌─────────────────────────────────────┼─────────────────────────────────────┐
        ▼ REGION us-east (CELL 1..N)           ▼ REGION eu-west             ▼ REGION ap-south
┌──────────────────────────────────────────────────────────────────────────────────────┐
│  CELL = fully independent stack slice (blast-radius unit). A tenant maps to a cell.    │
│                                                                                        │
│  ┌────────────────────────────────────────────────────────────────────────────────┐  │
│  │  RUST EDGE GATEWAY  (stateless, per-cell fleet)                                  │  │
│  │  authN (mTLS/JWT) · API-key verify · rate-limit · admission control ·           │  │
│  │  request normalize (OpenAI/Anthropic/OZ-native compat) · idempotency ·          │  │
│  │  SSE/WebSocket streaming relay · usage metering start                           │  │
│  └───────────────────────────────┬────────────────────────────────────────────────┘  │
│                                   │ protobuf over service mesh (mTLS, retries budgeted)│
│  ┌────────────────────────────────▼───────────────────────────────────────────────┐  │
│  │  DATA PLANE (Python)                                                             │  │
│  │  ┌──────────┐  ┌─────────┐  ┌──────────┐  ┌───────────┐  ┌──────────────────┐   │  │
│  │  │ Context  │─►│ Planner │─►│Scheduler │─►│ Aggregator│─►│ Synthesizer/     │   │  │
│  │  │ Manager  │  │(compile │  │(admit,   │  │ /Verifier │  │ Stream fan-in    │   │  │
│  │  │          │  │ DAG IR) │  │ place)   │  │           │  │                  │   │  │
│  │  └──────────┘  └─────────┘  └────┬─────┘  └───────────┘  └──────────────────┘   │  │
│  └────────────────────────────────┼───────────────────────────────────────────────┘  │
│         dispatch typed work items │ (Temporal activities / NATS)                       │
│  ┌────────────────────────────────▼───────────────────────────────────────────────┐  │
│  │  EXECUTION PLANE                                                                 │  │
│  │  ┌───────────────┐ ┌────────────────┐ ┌──────────────┐ ┌──────────────────────┐ │  │
│  │  │ Provider GW   │ │ Tool Sandbox   │ │ Embed/Rerank │ │ Self-host GPU        │ │  │
│  │  │ (Go, circuit  │ │ (Firecracker   │ │ workers      │ │ inference (vLLM/     │ │  │
│  │  │ breakers,     │ │ microVMs)      │ │ (GPU pool)   │ │ TensorRT-LLM)        │ │  │
│  │  │ quota, cache) │ │                │ │              │ │ + KV/prefix cache    │ │  │
│  │  └───────┬───────┘ └────────────────┘ └──────────────┘ └──────────────────────┘ │  │
│  └──────────┼──────────────────────────────────────────────────────────────────────┘  │
│             │ HTTPS/2                                                                   │
└─────────────┼──────────────────────────────────────────────────────────────────────────┘
              ▼
   OpenAI · Anthropic · Google · Azure OpenAI · Bedrock · self-host
```

**Cell-based architecture (critical for scale):** a *cell* is a complete, independent slice of the stack (edge → data → execution + its own caches and queues) sized to a bounded number of tenants (say, 50–200k). Cells are the unit of blast radius, deploy, and capacity. This is how hyperscalers avoid "one bad tenant/deploy takes down everyone." Regions contain many cells; a tenant is pinned to a home cell with a failover cell.

**Why anycast + regional cells:** global anycast gives DDoS absorption and nearest-region routing; cells inside a region give fault isolation and linear capacity scaling — you add cells, you don't scale a shared fleet vertically.

---

# PART E — AI ORCHESTRATION CORE

This is the heart. v1 said "custom orchestrator, no framework" and stopped. That hand-waves the hardest engineering. Here is the real design.

## E.1 The Workflow IR (Intermediate Representation)

**Why:** an orchestrator that builds plans as imperative Python is unobservable, untestable, un-cacheable, and un-replayable. The fix borrows from compilers: **the planner emits a typed DAG artifact (the IR); the scheduler executes the IR.** Plan and execution are separated.

```
Planner  ──►  Workflow IR (a signed, serializable DAG)  ──►  Scheduler  ──►  Execution
             ▲                                              │
             └────────── cache / replay / eval / audit ─────┘
```

An IR node:

```json
{
  "node_id": "n3",
  "kind": "worker_call",              // worker_call | tool_call | retrieve | verify | reduce | branch | map
  "worker_ref": "role:coder",         // resolved to a concrete provider model at schedule time
  "inputs": ["n1.output", "ctx:auth_chunks"],
  "access_list": ["n1"],              // information isolation — what this node may see
  "budget": { "tokens_in": 180000, "tokens_out": 8000, "usd": 0.35, "deadline_ms": 9000 },
  "retry": { "max": 2, "backoff": "exp", "fallback_ref": "role:coder.fallback" },
  "cache_key": "sha256(...)",
  "on_fail": "degrade|abort|reroute"
}
```

**Why this is 10/10:**
- **Cacheable at the node level** — identical sub-DAGs skip execution entirely.
- **Replayable** — feed a stored IR + recorded provider outputs to reproduce any request exactly (this powers the eval + debugging systems).
- **Statically analyzable** — budget/deadline checks *before* spending a dollar; detect cycles, over-budget plans, illegal access-list leaks.
- **Portable** — the IR is provider-agnostic; `role:coder` binds to a concrete model at schedule time based on current health/price/latency.

**Trade-off:** an IR + compiler is more upfront work than "just call the models in a loop." Payoff: everything downstream (caching, eval, cost control, audit, multi-region replay) becomes possible. At this scale it's non-negotiable.

## E.2 Planner (the "what should happen" brain)

Two-tier, matching the fast/ultra split but done properly:

```
Request ──► Complexity Classifier (small fast model / heuristics)
                 │
      ┌──────────┴───────────┐
      ▼ simple               ▼ complex
  FAST PLAN               DELIBERATE PLAN
  (template DAG,          (LLM-generated workflow:
   1 worker, no LLM        subtasks, roles, access
   planning cost)          lists, parallelism, verify)
```

- **Fast path** never pays for planning: a classifier picks a **pre-compiled template DAG**. This keeps `onezox` latency within a whisker of a raw provider call.
- **Deliberate path** uses a planner model to emit a custom IR, then the IR is **validated and cost-gated** before execution. If the plan exceeds budget, it's automatically simplified (fewer agents, tighter retrieval).

**Missing-from-v1 additions:** plan cost-gating, plan caching (same request shape → same template), and a **plan critic** (a cheap second pass that rejects pathological plans, e.g. 5 agents each reading 1M tokens).

## E.3 Scheduler (the "make it happen efficiently" brain)

v1 had no real scheduler. This is a dedicated component and a major bottleneck-remover.

Responsibilities:
- **Admission control** — reject/queue when the cell is saturated (protect SLOs > accept-everything).
- **Placement** — bind each `role:*` to a concrete worker using live signals: provider health, current rate-limit headroom, price tier, p95 latency, and cache locality (route to the provider where the prefix is already cached).
- **Parallelism** — run independent IR branches concurrently; respect per-provider global concurrency caps.
- **Deadline propagation** — each node inherits a shrinking deadline; a slow node can't blow the whole request's SLO.
- **Backpressure** — when execution plane is hot, the scheduler slows dispatch instead of piling on (prevents metastable failure).

```
        ┌──────────────────────────────────────────────┐
        │ SCHEDULER                                     │
        │  admit? ──► place role→model ──► dispatch      │
        │    ▲            ▲                    │         │
        │    │            │ live signals       ▼         │
        │  SLO budget   provider health    concurrency   │
        │               rate headroom       governor     │
        │               price/latency                    │
        └──────────────────────────────────────────────┘
```

**Why separate planner and scheduler:** the planner decides *shape* (needs quality signals + LLM reasoning); the scheduler decides *binding + timing* (needs real-time infra signals). Different inputs, different change cadence, different failure modes. Fusing them (v1) means every infra hiccup forces a re-plan — wasteful and slow.

## E.4 Durable Workflow Execution — **Temporal**

**Why introduce Temporal (biggest new infra piece):** multi-agent workflows are long-running, involve external calls that fail, need retries, timeouts, human-in-the-loop, and must survive a pod crash mid-flight. Hand-rolling this on a queue (v1's "Redis Streams/Kafka") reinvents durable execution badly.

- Each request's IR runs as a **Temporal workflow**; each node is an **activity** with automatic retry/timeout/heartbeat.
- Crash mid-request? Temporal resumes from the last completed activity — no lost work, no double-charging providers (activities are idempotent via `cache_key`).
- Gives free **visibility, replay, and exactly-once semantics** for the orchestration layer.

**Trade-off:** Temporal is heavy operational infra and adds latency per activity (~ms). For the *fast path* we bypass it (direct dispatch — templates don't need durability). For *ultra/long-running* workflows the durability is worth it. **Hybrid: fast path = direct; deliberate path = Temporal.**

## E.5 Multi-Agent Communication & Information Isolation

Directly addresses the Fugu "orchestration collapse" problem the source doc raised.

- **Access lists in the IR** are enforced by the scheduler — an agent physically cannot receive context outside its access list. This is a *security + quality* control (prevents one agent's wrong trajectory from contaminating all others).
- **Blackboard pattern** for shared findings: agents write structured claims (with provenance) to a per-request blackboard; the aggregator reads it. Agents don't talk peer-to-peer (avoids N² prompt bloat and cascade errors).
- **Verifier is adversarial** — it's given claims + evidence and told to *disprove*, not confirm. Claims without traceable provenance are dropped.


---

# PART F — CONTEXT, RETRIEVAL, MEMORY (the layer v1 under-built)

The source doc was right that this is where most real engineering time goes. v1 treated it as one "Context Manager" box. Here it's three distinct subsystems.

## F.1 Context Management Architecture

```
Raw input (repo / PDFs / chat / DB / 1M tokens)
   │
   ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ Ingest       │─►│ Parse (typed │─►│ Chunk        │─►│ Dedupe       │
│ (stream,     │  │ per source:  │  │ (semantic-   │  │ (hash exact, │
│  MIME detect)│  │ AST/layout)  │  │  boundary)   │  │  MinHash near)│
└──────────────┘  └──────────────┘  └──────────────┘  └──────┬───────┘
                                                              ▼
                        ┌─────────────────────────────────────────────────┐
                        │ MULTI-GRANULARITY STORE                          │
                        │ L0 corpus manifest · L1 file summaries ·         │
                        │ L2 section summaries · L3 raw chunks · L4 spans  │
                        └─────────────────────────────────────────────────┘
```

- **Async ingestion off the hot path.** A 1M-token repo upload goes to the execution plane's ingest workers via the work queue; the request either waits on a ready-signal or streams partial answers as indexing completes. v1's inline indexing would blow the latency budget.
- **Parse is typed per source** — code → Tree-sitter AST + symbol/import graph; PDF → layout-aware (blocks, tables, reading order); DB → schema introspection + stats (never raw rows).
- **Chunking respects hard boundaries** (never split a function/symbol) with parent-context overlap (headings, imports, table headers).

## F.2 Retrieval Architecture — Hybrid, multi-channel

v1 listed channels; here's the production design with the reranking stack that actually determines quality.

```
Query
  ├─► Lexical (BM25, OpenSearch)        ─┐
  ├─► Dense (embeddings, vector DB)      ├─► Candidate union (top-K each)
  ├─► Structural (code/call/import graph)├─►        │
  ├─► Recency (latest turns/edits)      ─┘          ▼
  └─► Tool-aware (failing tests, traces)      ┌──────────────┐
                                              │ RERANK       │
                                              │ cross-encoder│  ← GPU rerank worker
                                              │ + MMR divers.│
                                              └──────┬───────┘
                                                     ▼
                                              ┌──────────────┐
                                              │ BUDGET PACK  │  ← optimize relevance
                                              │ (knapsack)   │     under token budget
                                              └──────────────┘
```

**New/upgraded vs v1:**
- **Cross-encoder reranker on GPU** (e.g. a fine-tuned reranker) — the single biggest lever on retrieval quality. Bi-encoder recall + cross-encoder precision.
- **Contextual retrieval** (prepend a short LLM-generated context blurb to each chunk before embedding) — measurably cuts retrieval failures; done once at index time, cached.
- **Budget packing = knapsack** — maximize Σrelevance s.t. Σtokens ≤ worker budget, with required-chunk constraints and diversity floor.

## F.3 Vector Database Choice — **Qdrant (primary) / Turbopuffer (scale-out tier)**

**Why not pgvector as the endgame** (v1's fallback): pgvector is great to *start* but couples your OLTP DB to vector load and struggles past tens of millions of vectors with high QPS. Decision:
- **Start:** pgvector (one less system).
- **Scale:** **Qdrant** — purpose-built, HNSW, payload filtering, horizontal sharding, quantization for memory efficiency. Or **Turbopuffer** for cost-efficient object-storage-backed vectors at billions-scale with acceptable latency.

**Why Qdrant over Pinecone/Weaviate/Milvus:** Qdrant gives best-in-class filtering performance (critical — you always filter by tenant + corpus), scalar/binary quantization to slash memory cost at billions of vectors, and self-host + cloud parity (matters for enterprise/on-prem).

## F.4 Embeddings

- **Embedding models are pluggable via the Provider Gateway** (same abstraction as chat workers) — you may use a frontier embedding API *or* self-host (e.g. an open embedding model on your GPU pool) for cost/privacy.
- **Binary/scalar quantization** on stored vectors → 4–32× memory reduction, rerank restores precision.
- **Embedding cache** keyed by `(chunk_hash, model_version)` — never re-embed unchanged content.

## F.5 Memory Architecture — a dedicated Memory Service (owns the three tiers)

v1 had "workflow state in Redis." Real agents need a memory hierarchy — AND that hierarchy must have a single owner. **Memory is a first-class domain, so it gets its own service.** The three-tier model below is unchanged; what changes is *who owns it*: a dedicated **Memory Service**, not the AI engine and not the website.

**Ownership contract (strict):**
- The **AI Engine remains completely stateless** — it owns no memory. It only *asks* the Memory Service for relevant memories and *submits* new ones asynchronously.
- The **Website Backend** owns accounts, authentication, billing, API keys, projects, and chat history — and does **not** touch AI memory.
- The **Memory Service is the single owner of AI memory**: storage, retrieval, embeddings, the vector database, indexing, updates, deletion, decay, and tenant isolation all live here and nowhere else.
- **PostgreSQL stores memory text (source of truth); the Vector DB (Qdrant) stores embeddings.** Both live behind the Memory Service.

```
                 ┌──────────────────────────────────────────────────┐
   AI ENGINE ───►│              MEMORY SERVICE (Python)             │
  (stateless)    │  single owner of AI memory                       │
   retrieve(     │  ├─ retrieve: hybrid search, tenant-filtered      │
   identity,     │  ├─ write: embed + index (ASYNC, off hot path)    │
   query) ◄──────┤  ├─ update / delete / decay                       │
   relevant      │  ├─ embeddings (pluggable via Provider Gateway)   │
   slice         │  └─ tenant isolation (org_id/user_id/project_id)  │
                 │                                                    │
   submit(       │  INTERNAL three-tier model (unchanged):           │
   memory) ─────►│  ┌────────────────────────────────────────────┐  │
   (async, via   │  │ WORKING (in-request, ephemeral)             │  │
    NATS)        │  │  transcript · workflow DAG state · tools    │  │
                 │  │  store: in-process + Redis · TTL=request     │  │
                 │  └───────────────┬────────────────────────────┘  │
                 │       promote (summarize+extract) │               │
                 │  ┌───────────────▼────────────────────────────┐  │
                 │  │ EPISODIC (per-session / per-user)           │  │
                 │  │  turn summaries · decisions · hypotheses     │  │
                 │  │  store: FoundationDB + vector index          │  │
                 │  └───────────────┬────────────────────────────┘  │
                 │       consolidate │                               │
                 │  ┌───────────────▼────────────────────────────┐  │
                 │  │ SEMANTIC (long-term, cross-session, indexed)│  │
                 │  │  repo facts · entities · prefs · project     │  │
                 │  │  store: Postgres(text=truth) + Qdrant + graph│  │
                 │  └────────────────────────────────────────────┘  │
                 └──────────────────────────────────────────────────┘
```

- **Working memory** stays request-local (still Redis/in-process); it is the only tier the engine holds transiently *during* a request and it evaporates at request end — statelessness preserved.
- **Retrieval returns a relevant slice, never everything** — the engine passes identity + query; the Memory Service runs tenant-filtered hybrid search (dense + graph) and returns only what matters.
- **Writes are asynchronous** — the engine submits new memories over NATS; embedding + indexing happen off the request hot path so remembering never slows answering.
- **Promotion is explicit and summarized**, never raw dumping (prevents context rot).
- **Isolation:** every read is filtered by `org_id/user_id/project_id` at the Memory Service's data-access boundary — one chokepoint to secure and audit; cross-tenant leakage is a P0 boundary.
- **Forgetting is a feature** — TTLs + relevance decay + explicit user delete (GDPR-aligned), all executed by the Memory Service.

## F.6 Cache Architecture (7 tiers, each justified)

| Tier | Key | Why it exists |
|---|---|---|
| Raw-content | content hash | skip re-upload/parse of identical blobs |
| Parse | hash + parser version | AST/layout extraction is expensive |
| Embedding | chunk hash + model version | embedding compute is $$ at scale |
| Summary | chunk hash + summarizer + prompt ver | summaries reused across requests |
| Retrieval | query fingerprint + corpus hash | identical queries skip the whole retrieval stack |
| **Prefix/prompt** | model + provider + prefix hash | **biggest cost lever** — shared system prompts/repo manifests hit provider prompt caches; route to the provider instance holding the prefix |
| **Semantic response** | embedding-similarity of request | near-duplicate requests return cached answers (with freshness policy) |

**Prefix-cache-aware routing** is a headline optimization: the scheduler prefers the worker/instance that already holds the KV prefix, turning cold prefills into cache hits. For self-hosted models this is real KV reuse (vLLM/SGLang RadixAttention); for provider APIs it's their prompt-cache handle.

## F.7 Chat History — a dedicated Chat Service (sibling to Memory Service)

Chat history has a different shape from every other store: write-heavy, append-only, read in narrow recent slices per conversation, and unbounded growth. Keeping it in the website's relational store would let billions of message inserts contend with critical account/billing queries. So — exactly like memory — **chat history becomes its own domain with its own owner and its own store.**

**Ownership contract (strict):**
- The **Website Backend no longer stores chat history directly.** It still owns accounts, auth, billing, API keys, and projects, and it sends all chat operations to the Chat Service.
- The **Chat Service is the single owner of chat history**: conversations, messages, attachments, pagination, archiving, retention, and conversation deletion.
- The Chat Service has **its own database, separate from the website database.** Primary store = **ScyllaDB** (partitioned by `conversation_id`, messages time-ordered within the partition); **Redis** fronts it as the hot-conversation cache.
- The **AI Engine receives only the required conversation slice** from the Chat Service — a bounded recent window, never the whole conversation, never a cross-conversation scan.

```
┌──────────────┐   all chat ops    ┌────────────────────────────────────┐
│   WEBSITE     │ ────────────────► │        CHAT SERVICE (Go)           │
│   BACKEND     │  create conv,      │  single owner of chat history      │
│  (no chat     │  append msg,       │  conversations · messages ·        │
│   rows)       │  paginate, delete  │  attachments · pagination ·        │
└──────────────┘                    │  archiving · retention · deletion  │
                                    │                                    │
┌──────────────┐  "last N msgs of   │  ┌──────────────────────────────┐  │
│  AI ENGINE    │   conv 555"        │  │ Redis  — hot recent (0–30d)  │  │
│ (stateless)   │ ─────────────────► │  ├──────────────────────────────┤  │
│               │ ◄───────────────── │  │ ScyllaDB — warm, partition   │  │
│  gets bounded │   recent slice     │  │   by conversation_id, time-  │  │
│  recent slice │                    │  │   ordered (primary store)    │  │
└──────────────┘                    │  ├──────────────────────────────┤  │
                                    │  │ S3 — cold archive (1y+),     │  │
                                    │  │   compressed, restore-on-dmd │  │
                                    │  └──────────────────────────────┘  │
                                    └────────────────────────────────────┘
```

**Why this store, this shape:**
- **Partition by `conversation_id`** → all of one conversation's messages live together, time-ordered; "load the last N messages of conv 555" is a single fast partition read (the dominant query).
- **ScyllaDB** gives linear write scaling (add nodes for throughput) with no single-primary write bottleneck — the correct fit for append-heavy, billions-of-rows chat. Redis in front absorbs the constant reads of active threads.
- **Attachments** are stored as references to object storage (S3), not inline blobs — Scylla holds the message + a pointer, S3 holds the bytes.
- **Retention / archiving / deletion** are owned end-to-end by the Chat Service: a background job rolls cold conversations to compressed object storage; deletion is a single authoritative path (GDPR-aligned), symmetric with the Memory Service.
- **Independent failure domain** — a chat write storm scales Scylla, never touches the website's Postgres; a chat degradation never blocks logins or payments.

**Symmetry:** Chat Service and Memory Service are siblings — each owns exactly one durable domain, each is queried by the stateless engine for a *relevant slice*, and neither lives inside the website or the engine.


---

# PART G — PROVIDER ABSTRACTION, MODEL REGISTRY, ROUTING

## G.1 Provider Gateway (dedicated Go service — replaces litellm)

**Why litellm dies at scale:** it's an in-process library — every data-plane pod independently hammers providers with no shared view of rate limits, no global circuit breaking, weak streaming backpressure. A dedicated service centralizes provider concerns.

```
┌──────────────────────────────────────────────────────────────────┐
│ PROVIDER GATEWAY (Go, own deployment, per region)                 │
│                                                                    │
│  ┌────────────┐  Normalized OZ protocol in ──► provider-native out │
│  │ Adapter:   │  OpenAI / Anthropic / Google / Azure / Bedrock /   │
│  │ per-provider│  self-host (vLLM)                                  │
│  └─────┬──────┘                                                    │
│        ▼                                                            │
│  ┌────────────┐ ┌───────────────┐ ┌──────────────┐ ┌────────────┐ │
│  │ Global     │ │ Circuit       │ │ Request      │ │ Streaming  │ │
│  │ quota /    │ │ breaker /     │ │ coalescing / │ │ normalize  │ │
│  │ token bkt  │ │ health probe  │ │ dedup        │ │ + backpr.  │ │
│  │ (shared    │ │ per provider  │ │              │ │            │ │
│  │ Redis)     │ │               │ │              │ │            │ │
│  └────────────┘ └───────────────┘ └──────────────┘ └────────────┘ │
│        │ prefix-cache handle registry                             │
└────────┼───────────────────────────────────────────────────────┬─┘
         ▼                                                        ▼
   provider APIs                                          self-hosted GPU pool
```

- **Global token-bucket quota** in shared Redis → never exceed a provider's org-level rate limit across the whole fleet (a fleet-wide correctness property litellm cannot provide).
- **Per-provider circuit breakers** with active health probes → a degraded provider is ejected in seconds; scheduler reroutes to fallback.
- **Streaming normalization** → all providers' SSE/event shapes become one internal streaming protocol with proper backpressure (slow client must not OOM the gateway).
- **Request coalescing** → identical concurrent requests (same cache key) share one provider call.

## G.2 Model Registry & Config-as-Code (replaces "DB row, callable instantly")

**Why v1's live DB-edit is dangerous:** mutating a model that serves billions of requests with an UPDATE = uncontrolled global change, no rollback, no canary. Real platforms treat model configs like *deployable artifacts.*

```
Admin edits in panel ──► generates a MODEL MANIFEST (versioned, immutable)
        │
        ▼
   Validation + policy check + cost simulation
        │
        ▼
   Staged rollout:  canary 1% ──► 10% ──► 50% ──► 100%
        │                 │ auto-rollback on SLO/quality regression
        ▼
   Control plane publishes signed manifest ──► etcd ──► edge caches
```

Manifest (immutable, versioned):

```json
{
  "id": "onezox-ultra",
  "version": "v42",
  "immutable": true,
  "mode": "deliberate",
  "roles": {
    "coder":   { "primary": "anthropic/claude-opus-4.8", "fallback": ["openai/gpt-5.5"] },
    "reasoner":{ "primary": "openai/gpt-5.5",            "fallback": ["google/gemini-3.1-pro"] },
    "longctx": { "primary": "google/gemini-3.1-pro",     "fallback": ["anthropic/claude-opus-4.8"] },
    "verifier":{ "primary": "anthropic/claude-opus-4.8", "fallback": [] }
  },
  "topology": "conductor",
  "max_agents": 3,
  "budgets": { "usd_per_req": 0.90, "p95_latency_ms": 14000, "tokens": 1000000 },
  "tools": ["shell","web","tests"],
  "verification": true,
  "cost_tier_threshold": 272000,
  "rollout": { "state": "canary", "percent": 10, "auto_rollback": true }
}
```

**"Create a model" from the admin panel** now means: the wizard generates a *manifest*, simulates its cost/latency on a replay set, and stages a canary rollout — instant to author, safe to ship. Instant callability is preserved (new manifest → new model ID) *without* the outage risk of live mutation.

## G.3 Model Routing — decoupled from the registry

Routing decides *which concrete model serves a role right now*. Three layers:
1. **Static** (manifest) — declared primary/fallback per role.
2. **Dynamic** (scheduler) — live health/price/latency/cache-locality override the static primary.
3. **Learned** (optimizer, later) — a bandit/router model trained on trace outcomes picks the best worker per request *class* to optimize quality-per-dollar. This is the "learned selection head" from the Fugu paper, but trained on *your* production traces.

---

# PART H — EXECUTION PLANE DETAIL

## H.1 Tool Sandbox (Firecracker microVMs)

```
Scheduler ──► Sandbox Supervisor (Rust) ──► Firecracker microVM (ephemeral, per tool-call)
                                              │  no network by default
                                              │  seccomp-BPF syscall filter
                                              │  cgroup CPU/mem/pids caps
                                              │  read-only rootfs + tmpfs scratch
                                              │  egress via allow-listed proxy only
                                              └► destroyed after use (no persistence)
```

**Why Firecracker not containers:** running *arbitrary agent-generated code* in a shared-kernel container is a kernel-exploit away from cross-tenant compromise. Firecracker gives VM-level isolation with ~125ms boot and tiny overhead — the right security/performance point. Egress is default-deny through an allow-listed proxy (prevents data exfiltration + SSRF).

## H.2 Self-Hosted GPU Inference (new — for cost, privacy, and the router/rerank/embed models)

```
┌────────────────────────────────────────────────────────────┐
│ GPU POOL (Kubernetes, node pools by GPU class)              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐        │
│  │ vLLM /       │  │ Rerank       │  │ Embedding    │        │
│  │ SGLang       │  │ cross-encoder│  │ model        │        │
│  │ (open models,│  │ (GPU)        │  │ (GPU)        │        │
│  │ RadixAttn KV │  │              │  │              │        │
│  │ prefix cache)│  │              │  │              │        │
│  └──────────────┘  └──────────────┘  └──────────────┘        │
│  scheduling: MIG partitioning · fractional GPU · bin-pack    │
│  autoscale: KEDA on tokens-in-flight + queue depth           │
└────────────────────────────────────────────────────────────┘
```

**Why self-host at all** (you call frontier APIs, so why GPUs?): the *supporting* models — router, reranker, embedder, classifier, cheap summarizer, plan critic — run millions of times per hour. Paying frontier API prices for these is wasteful. Self-hosting them on your own GPUs (with vLLM's continuous batching + RadixAttention prefix cache) is dramatically cheaper and lower-latency, and keeps sensitive intermediate data in-house. Frontier *worker* models still come from the labs.

**Serving engine: vLLM or SGLang** — continuous batching + PagedAttention/RadixAttention give the throughput and KV-cache efficiency that naive serving can't. **TensorRT-LLM** for the highest-QPS fixed models where the compile cost pays off.


---

# PART I — DATA STORES (every choice defended)

| Store | Technology | Why this, not alternatives |
|---|---|---|
| **Relational / OLTP** | **CockroachDB** (or Postgres+Citus) | Multi-region, horizontally scalable, strongly consistent SQL. Chosen over single-node Postgres because tenants/billing/models must survive a region loss with no data loss (RPO≈0). Cockroach's geo-partitioning pins tenant data to home region for latency + data-residency compliance. **Website Backend** uses it for accounts/billing/keys/projects (no chat rows). The **Memory Service** uses its own relational store here as the **source of truth for memory text** (embeddings live in Qdrant). |
| **Durable workflow / high-write KV** | **FoundationDB** | Backs Temporal + episodic memory + workflow state. Chosen for extreme write throughput, strict serializability, and proven scale (it underpins other large platforms). Where FDB ops cost is too high early, start on Cockroach and migrate. |
| **Cache / ephemeral / rate-limit** | **Redis (Cluster) / Dragonfly** | Sub-ms. Dragonfly as a drop-in when Redis memory/throughput ceilings hit (multi-threaded, higher throughput per node). Strictly ephemeral data only. |
| **Vector** | **Qdrant** (Turbopuffer at billions-scale) | See F.3 — filtering + quantization + self-host parity. **Owned by the Memory Service** for AI-memory embeddings (and by the context layer for corpus retrieval). |
| **Chat history** | **ScyllaDB** (+ Redis hot cache, + S3 cold archive) | **Owned by the Chat Service.** Partitioned by `conversation_id`, messages time-ordered within partition. Chosen for linear write scaling and single-partition recent reads — the exact shape of append-heavy, billions-of-rows chat. Kept out of the website's OLTP store so chat writes never contend with account/billing queries. See F.7. |
| **Lexical search** | **OpenSearch** | Mature BM25, hybrid search, self-host. |
| **Graph** | **Neo4j / Memgraph** | Code call-graphs, entity/knowledge relationships, memory links. Graph queries (k-hop neighborhoods) are first-class here; doing them in SQL is painful. Memgraph if in-memory speed matters. |
| **Object store** | **S3 / GCS** (+ MinIO on-prem) | Raw corpora, traces, eval artifacts, backups. Infinite, cheap, durable. |
| **Analytics / OLAP** | **ClickHouse** | Usage, cost, latency, token analytics at billions of rows. Chosen over warehouse-only (Snowflake/BigQuery) because it's fast enough for near-real-time admin dashboards *and* cheap batch analytics. Traces + metrics land here. |
| **Time-series / metrics** | **Prometheus + Thanos / Mimir** | Long-retention, multi-region metric aggregation. |
| **Config / service discovery** | **etcd** | Backs the control plane's manifest distribution + service coordination. |
| **Secrets** | **HashiCorp Vault** | Provider keys, tenant secrets, dynamic short-lived credentials. |

**Guiding rule:** one store per consistency/durability/latency profile. v1's "Redis for everything" violated this and would have caused correctness bugs (rate limits need ephemeral speed; workflow state needs durability — different stores).

## I.1 Core Data Model (ER)

```
┌────────────┐      ┌──────────────┐      ┌─────────────────┐
│  ORG       │1    *│  USER        │1    *│  API_KEY        │
│  id        ├──────┤  id, org_id  ├──────┤  id, hash,scope │
│  plan,tier │      │  role        │      │  rate_limits    │
└─────┬──────┘      └──────────────┘      └─────────────────┘
      │1                                          
      │*                                           
┌─────▼────────────┐   ┌────────────────────┐   ┌────────────────────┐
│ MODEL_MANIFEST   │   │ PROVIDER_KEY       │   │ USAGE_RECORD       │
│ id, version      │   │ id, provider,      │   │ req_id, model,     │
│ immutable, roles │   │ key(Vault ref),    │   │ tokens_in/out,     │
│ topology,budgets │   │ quota, health      │   │ orch_tokens,       │
│ rollout_state    │   │                    │   │ cache_hits, usd,   │
└─────┬────────────┘   └────────────────────┘   │ latency, cell,region│
      │1                                         └─────────┬──────────┘
      │*                                                   │* 
┌─────▼────────────┐   ┌────────────────────┐    ┌─────────▼──────────┐
│ WORKFLOW_RUN     │1 *│ NODE_EXECUTION     │    │ TRACE (OTel→CH)    │
│ req_id, ir_hash  ├───┤ node_id, worker,   │    │ span tree, tokens, │
│ status, cost     │   │ tokens, latency,   │    │ cost, provider,    │
│ tenant, region   │   │ status, provenance │    │ eval_scores        │
└──────────────────┘   └────────────────────┘    └────────────────────┘
```

---

# PART J — EVENT BUS & QUEUES (two systems, two jobs)

**Why two, not one** (v1 said "Kafka *or* Redis Streams"): durable event streaming and durable task execution are different problems with different delivery semantics.

| System | Tech | Purpose | Semantics |
|---|---|---|---|
| **Event bus** | **Redpanda** (Kafka API) | Every request emits events → usage, billing, analytics (ClickHouse), audit, eval sampling, router training data. Replayable log. | at-least-once, ordered per key, long retention |
| **Durable workflow queue** | **Temporal** (backed by FDB/Cockroach) | Orchestration execution: activities, retries, timeouts, resumption. | exactly-once effect (idempotent activities) |
| **Fast fan-out work** | **NATS JetStream** | Ingest/embed/summarize jobs, sandbox dispatch — low-latency, lightweight. | at-least-once, fast |

**Why Redpanda over Kafka:** Kafka-compatible API but no ZooKeeper/JVM, far lower tail latency and ops burden — same ecosystem, better operations. **Why NATS for fast work:** millisecond dispatch for internal jobs where Temporal's durability would be overkill and Kafka's throughput-orientation adds latency.

---

# PART K — API ARCHITECTURE

Three API surfaces, each intentional:

1. **Ingress API (public)** — OpenAI-compatible (`/v1/chat/completions`, `/v1/responses`, `/v1/embeddings`, `/v1/models`) **+** an OZ-native API exposing orchestration primitives (submit IR, inspect workflow, stream node-level events). OpenAI-compat = instant ecosystem; OZ-native = your differentiation.
2. **Internal API (service-to-service)** — **gRPC + protobuf**, schema-governed by **buf** (breaking-change detection in CI). Chosen over REST/JSON internally for speed, typed contracts, and streaming.
3. **Admin/Control API** — gRPC + a GraphQL gateway for the panel (flexible reads for dashboards).

**Idempotency** is first-class: every mutating request carries an idempotency key; retries never double-charge or double-execute.

**Streaming protocol:** SSE for OpenAI-compat; WebSocket/gRPC-stream for OZ-native (carries node-level events — "agent 2 started", "tool ran", token deltas — enabling rich client UIs). Backpressure enforced end-to-end.

---

# PART L — OBSERVABILITY (infrastructure, not a panel)

```
Every service ──► OpenTelemetry SDK ──► OTel Collector (per cell)
                                            │
        ┌───────────────────┬───────────────┼──────────────────┐
        ▼ traces            ▼ metrics        ▼ logs             ▼ analytics events
   Tempo/Jaeger        Prometheus+Mimir    Loki            ClickHouse
        │                    │                │                  │
        └────────────────────┴────────────────┴──────────────────┘
                                   ▼
                          Grafana (unified) + admin panel
```

**AI-specific observability (the differentiator):** every span carries `tokens_in`, `tokens_out`, `orchestration_tokens`, `usd_cost`, `provider`, `model_version`, `cache_hit`, `node_id`, `eval_score`. This means:
- **Cost is traceable to the node** — you see exactly which agent/tool spent the money.
- **The trace is the eval substrate** — the eval system samples traces; the router trains on them.
- **Semantic monitoring** — not just "is it up" but "is answer quality regressing" via online LLM-judge sampling.

**Golden signals per SLO:** latency (p50/p95/p99 per model), error rate (per provider), saturation (tokens-in-flight vs capacity), cost-per-request drift, quality-score drift.


---

# PART M — EVALUATION & OPTIMIZATION (entirely missing from v1 — the most important gap)

**You cannot run an orchestrator in production without continuously measuring quality.** Routing decisions, model manifests, and prompt changes all silently affect answer quality. Without measurement you're flying blind. This subsystem is as important as serving.

```
┌──────────────────────────────────────────────────────────────────────┐
│ EVAL & OPTIMIZATION PLATFORM                                          │
│                                                                        │
│  1. OFFLINE EVAL      2. SHADOW TRAFFIC     3. ONLINE JUDGE            │
│  golden datasets      mirror % of live      sample live traces,       │
│  + LLM-judge +        traffic to new        LLM-judge scores +        │
│  ground-truth tasks   manifest, compare     user feedback signals     │
│  (regression gate)    (no user impact)      (drift detection)         │
│         │                    │                     │                   │
│         └────────────────────┼─────────────────────┘                   │
│                              ▼                                          │
│  4. REPLAY ENGINE: re-run any stored IR against recorded provider      │
│     outputs → deterministic reproduction for debugging + A/B          │
│                              │                                          │
│                              ▼                                          │
│  5. OPTIMIZER: bandit/RL router trained on (request class → outcome)   │
│     to maximize quality-per-dollar; feeds learned routing (G.3)       │
└──────────────────────────────────────────────────────────────────────┘
```

- **Regression gate in CI/CD:** no manifest or prompt ships if it regresses the golden eval set. This is the quality equivalent of unit tests.
- **Shadow traffic:** new manifests run on mirrored real traffic with zero user impact; promote only if metrics win.
- **Online LLM-judge:** samples a % of prod responses, scores them, alerts on quality drift (catches provider-side model changes that silently degrade you).
- **Replay:** the IR + recorded outputs make any bug reproducible — turns "it was flaky once" into a deterministic test case.

**Cost optimization loop:** the optimizer continuously answers "can a cheaper worker/topology hit the same quality?" and proposes manifest changes. Over time OneZox gets cheaper *and* better automatically.

---

# PART N — KUBERNETES, DEPLOYMENT, MULTI-REGION, DR

## N.1 Kubernetes Architecture

```
Per REGION:  multiple CELLS.  Each CELL = 1..N Kubernetes namespaces (or vclusters).

┌─────────────────────────────────────────────────────────────────────┐
│ REGION (e.g. us-east-1)                                              │
│                                                                       │
│  ┌──────────── CELL 1 (vcluster) ────────────┐   ┌── CELL 2 ──┐      │
│  │ node pools:                                │   │    ...      │      │
│  │  · edge (Rust, burstable, HPA on conn)     │   │            │      │
│  │  · dataplane (Python, HPA on tokens/queue) │   └────────────┘      │
│  │  · memory-service (Python, HPA on q-depth) │                      │
│  │  · chat-service (Go, HPA on write/read RPS) │                      │
│  │  · provider-gw (Go)                         │                      │
│  │  · gpu-inference (A100/H100, MIG, KEDA)     │                      │
│  │  · gpu-embed-rerank                          │                      │
│  │  · sandbox (bare-metal-ish, Firecracker)     │                      │
│  │  · stateful (operators: Cockroach, Qdrant,   │                      │
│  │    ScyllaDB, Redpanda, Redis, Temporal,      │                      │
│  │    OpenSearch)                               │                      │
│  │  service mesh: Cilium (eBPF) + mTLS          │                      │
│  └──────────────────────────────────────────────┘                    │
│                                                                       │
│  Platform: Karpenter (node autoscale) · Argo CD (GitOps) ·           │
│  Argo Rollouts (canary) · KEDA (event autoscale) · Cilium (CNI+mesh) │
└─────────────────────────────────────────────────────────────────────┘
```

**Key K8s decisions & why:**
- **Cilium (eBPF) for CNI + service mesh** over Istio: far lower latency (kernel-level), mTLS, network policy, observability — Istio's sidecar overhead is a real tax at this QPS.
- **Karpenter** over cluster-autoscaler: fast, bin-packing, spot-aware node provisioning — matches spiky LLM load and cuts cost.
- **KEDA** for token-aware autoscaling: scales on custom metrics (tokens-in-flight, provider headroom, queue depth), *not* CPU/RPS. This is the correct scaling signal for LLM workloads.
- **Argo CD + Argo Rollouts**: GitOps (declarative, auditable) + automated canary/blue-green with metric-based auto-rollback.
- **vcluster per cell**: strong isolation without a full cluster per cell's ops cost.
- **GPU nodes with MIG** (Multi-Instance GPU): partition an H100 into slices for small models (embed/rerank/router) → high utilization, no stranded GPU.

## N.2 Scaling Strategy

| Component | Scale signal | Mechanism |
|---|---|---|
| Edge gateway | active connections, CPU | HPA + Karpenter |
| Data plane | tokens-in-flight, queue depth | KEDA custom metric |
| Provider GW | outbound concurrency vs provider caps | KEDA + global governor |
| Memory Service | retrieve/write queue depth | KEDA; Qdrant shards + Postgres scale independently |
| Chat Service | read/write RPS | HPA; **ScyllaDB scales by adding nodes** (partitions rebalance), Redis hot cache absorbs reads |
| GPU inference | tokens-in-flight, batch queue | KEDA + MIG bin-packing |
| Sandbox | pending tool-calls | KEDA, pre-warmed microVM pool |
| Stateful | storage/throughput | operator-driven, planned |
| **Whole cell** | tenant count / saturation | **add cells** (linear scale-out) |

**Chat scales independently of the website:** because chat history lives in the Chat Service on ScyllaDB (not the website's OLTP store), a chat write storm adds Scylla nodes and never contends with account/billing queries — the website stays fast regardless of chat volume. Memory scales independently the same way: the Memory Service's Qdrant + Postgres grow on their own axis without touching the data-plane brain.

**The core scaling philosophy:** scale *out by cells*, not *up by fleets*. Cells cap blast radius and give linear, predictable capacity growth to billions of requests.

## N.3 High Availability & Fault Tolerance

- **Active-active multi-region.** Anycast routes to nearest healthy region; a full region loss sheds to others.
- **Graceful degradation ladder:** provider down → fallback provider → cheaper model → cached/semantic response → clear error. Never a hard 500 if any lower rung can serve.
- **Circuit breakers** everywhere outbound (providers, tools, internal deps).
- **Backpressure + load shedding** at admission control — protect SLOs for admitted requests over accepting all.
- **Idempotency + Temporal durability** → crashes resume, never double-charge.
- **Bulkheads:** per-provider, per-tenant concurrency pools so one noisy tenant/provider can't starve others.

## N.4 Disaster Recovery (tiered RTO/RPO — entirely new)

| Data class | RPO | RTO | Mechanism |
|---|---|---|---|
| Billing / usage / manifests | ≈0 | < 5 min | CockroachDB multi-region synchronous replication |
| Chat history (Chat Service) | seconds | < 5 min | ScyllaDB RF≥3 across AZs/regions (quorum write = no loss on node death); cold tier already durable in S3 |
| Memory text (Memory Service) | ≈0 | < 5 min | Postgres source-of-truth replicated; embeddings in Qdrant rebuildable from text |
| Workflow state | seconds | < 5 min | FoundationDB replication + Temoral multi-cluster |
| Vector/search indexes | minutes–hours | < 1 hr | rebuildable from object store (source of truth) + snapshots |
| Caches | n/a | instant | ephemeral, rebuild on demand |
| Traces/analytics | minutes | < 1 hr | Redpanda replication + ClickHouse backups |
| Object store | ≈0 | instant | cross-region replication, versioned |

- **Backups tested by restore drills** (a backup you haven't restored is a hope, not a backup).
- **Chaos engineering** (regular game-days): kill a provider, kill a cell, kill a region — verify the degradation ladder and failover actually work.
- **Runbooks + automated failover** for each failure class.

## N.5 CI/CD

```
git push ──► CI: build · unit · buf breaking-change · lint · SAST ·
             container scan · IR/plan tests · EVAL REGRESSION GATE
        ──► sign artifacts (Sigstore) · SBOM
        ──► Argo CD sync (GitOps) to staging cell
        ──► shadow traffic + smoke + eval on staging
        ──► Argo Rollouts canary to prod cell (1→10→50→100%)
             with automated metric-based rollback
```

**The eval regression gate is the standout:** code that passes tests but *degrades answer quality* is blocked — a quality bar most platforms lack.

---

# PART O — SECURITY (defense in depth)

| Layer | Control |
|---|---|
| Edge | WAF, DDoS (anycast), TLS 1.3, mTLS for enterprise, API-key + JWT/OIDC |
| AuthZ | fine-grained RBAC/ABAC, per-key scopes, tenant isolation enforced at every query |
| Secrets | Vault, dynamic short-lived creds; **provider keys never leave control plane** — execution plane gets scoped, expiring tokens |
| Sandbox | Firecracker VM isolation, seccomp, default-deny egress via allow-list proxy (blocks exfil + SSRF) |
| Data | encryption at rest (per-tenant keys, envelope encryption) + in transit; tenant-scoped vector/memory queries (cross-tenant leak = P0) |
| Prompt security | prompt-injection detection on tool/retrieval inputs; untrusted content is sandboxed from instructions |
| Supply chain | signed images (Sigstore), SBOM, dependency scanning, admission controllers (only signed images run) |
| Network | Cilium network policies, zero-trust between services, no implicit trust |
| Compliance | SOC2 / ISO27001 / GDPR / data-residency via geo-partitioning; full audit log (immutable, in ClickHouse + object store) |
| PII | detection + redaction pipeline before logging/tracing; configurable per-tenant data-retention + right-to-delete |

**Zero-trust principle:** every service authenticates every call (mTLS), least-privilege everywhere, secrets are short-lived, and the highest-risk component (the sandbox running arbitrary code) is the most isolated.


---

# PART P — FULL REQUEST LIFECYCLE (sequence diagram)

Example: 750K-token repo audit to `onezox-ultra`, streaming.

**Where the Chat Service and Memory Service enter the flow** (both are called by the stateless engine for a *relevant slice*, never the whole store):

1. On entry, the request carries identity (`org_id/user_id/project_id/conversation_id`) — attached by the edge (API keys) or website (JWT).
2. Before planning, the Data Plane asks the **Chat Service** for the *bounded recent slice* of the conversation (Redis hot → ScyllaDB partition on miss) and asks the **Memory Service** for *relevant memories only* (tenant-filtered hybrid search). Both return slices, not full histories.
3. Those slices feed the Context Manager / Planner alongside the corpus.
4. After synthesis, the engine **submits any new memories asynchronously** to the Memory Service over NATS (embed + index off the hot path). The **website** persists the new turn via the **Chat Service** (append to conversation). Neither write blocks the streamed response.

```
Client        Edge(Rust)     DataPlane(Py)   Scheduler   ProviderGW(Go)  Sandbox   Providers
  │  (before planning: DataPlane → Chat Service for recent slice,              │
  │   DataPlane → Memory Service for relevant memories — both tenant-filtered) │
  │  POST /v1/chat  │             │              │             │            │          │
  ├───────────────►│             │              │             │            │          │
  │                │ authN, key,  │              │             │            │          │
  │                │ ratelimit,   │              │             │            │          │
  │                │ admit, norm, │              │             │            │          │
  │                │ meter start  │              │             │            │          │
  │                ├──gRPC───────►│              │             │            │          │
  │                │              │ Context Mgr: │             │            │          │
  │                │              │ large corpus→│             │            │          │
  │                │              │ async ingest │             │            │          │
  │                │              │ (queue) +    │             │            │          │
  │                │              │ retrieve top │             │            │          │
  │                │              │ Planner:     │             │            │          │
  │                │              │ classify→    │             │            │          │
  │                │              │ deliberate→  │             │            │          │
  │                │              │ emit IR DAG  │             │            │          │
  │                │              │ (cost-gated) │             │            │          │
  │                │              ├─────────────►│ admit? place│            │          │
  │                │              │              │ roles→models│            │          │
  │                │              │              │ (health,    │            │          │
  │                │              │              │  cache-loc) │            │          │
  │                │              │              ├─Temporal───►│ agent A/B/C│          │
  │                │              │              │  activities │ parallel   │          │
  │                │              │              │             ├───────────────────────►│
  │                │              │              │             │ (prefix-cache-aware)   │
  │                │              │              │             │◄───────────────────────┤
  │                │              │              │             │ stream deltas          │
  │                │              │              │             │ tool call?             │
  │                │              │              │             ├──────────►│ Firecracker│
  │                │              │              │             │           │ run tests  │
  │                │              │              │             │◄──────────┤ result→    │
  │                │              │              │             │           │ emitting   │
  │                │              │              │             │           │ agent      │
  │                │              │ Aggregator:  │             │            │          │
  │                │              │ claims+prov, │             │            │          │
  │                │              │ verify(adver-│             │            │          │
  │                │              │ sarial),drop │             │            │          │
  │                │              │ unsupported  │             │            │          │
  │                │              │ Synthesizer  │             │            │          │
  │                │◄─stream──────┤ token deltas │             │            │          │
  │◄─SSE deltas────┤              │              │             │            │          │
  │                │ meter final: │              │             │            │          │
  │                │ in+orch+cache│              │             │            │          │
  │                │ tokens→bus   │              │             │            │          │
  │                │ →ClickHouse  │              │             │            │          │
  │  (post-response, async: engine → Memory Service submit new memories (NATS);│
  │   website → Chat Service append new turn — neither blocks the stream)      │
```

**Failure handling inline at every hop:** provider timeout → circuit breaker → scheduler reroutes to fallback (Temporal retry, idempotent); sandbox crash → isolated, agent gets error, workflow continues; data-plane pod dies mid-request → Temporal resumes on another pod from last activity; cell saturated → admission control sheds/queues; region down → anycast reroutes.

---

# PART Q — PROJECT / FOLDER STRUCTURE

Monorepo (Bazel or Nx for cross-language builds; buf for protobuf governance).

```
onezox/
├── proto/                         # single source of truth for all contracts
│   ├── gateway/ dataplane/ provider/ admin/ eval/
│   └── buf.yaml buf.gen.yaml      # codegen + breaking-change CI
├── services/
│   ├── edge-gateway/              # RUST — public ingress, streaming relay
│   │   ├── src/{auth,ratelimit,admission,normalize,stream,meter}
│   ├── data-plane/                # PYTHON — the brain
│   │   ├── context/{ingest,parse,chunk,dedupe,store}
│   │   ├── retrieval/{lexical,dense,structural,rerank,pack}
│   │   ├── planner/{classifier,templates,deliberate,critic,ir}
│   │   ├── scheduler/{admit,place,dispatch,backpressure}
│   │   ├── working_memory/        # in-request only (Redis/in-proc); NOT durable memory
│   │   ├── aggregator/{claims,verify,synthesize}
│   │   └── workflows/             # Temporal workflow + activity defs
│   ├── memory-service/            # PYTHON — single owner of AI memory
│   │   ├── retrieve/ write/ update/ delete/ decay/
│   │   ├── embeddings/            # pluggable via Provider Gateway
│   │   ├── tenant_isolation/      # org/user/project filter (single chokepoint)
│   │   └── stores/{postgres_text_of_truth, qdrant_embeddings, graph}
│   ├── chat-service/              # GO — single owner of chat history
│   │   ├── conversations/ messages/ attachments/ pagination/
│   │   ├── retention/ archiving/ deletion/
│   │   └── stores/{scylladb_primary, redis_hot_cache, s3_cold_archive}
│   ├── provider-gateway/          # GO — provider abstraction
│   │   ├── adapters/{openai,anthropic,google,azure,bedrock,vllm}
│   │   ├── quota/ breaker/ coalesce/ stream/ prefixcache/
│   ├── sandbox-supervisor/        # RUST — Firecracker microVM mgmt
│   ├── gpu-inference/             # vLLM/SGLang deploy + serving configs
│   ├── embed-rerank/              # GPU embedding + cross-encoder services
│   ├── control-plane/             # registry, rollout, vault-integ, policy
│   │   ├── registry/ rollout/ quota/ policy/ pricing/
│   ├── eval-platform/             # PYTHON — offline/shadow/judge/replay/optimizer
│   └── admin-api/                 # gRPC + GraphQL gateway
├── admin-panel/                   # TYPESCRIPT — Next.js RSC
│   ├── app/{dashboard,model-studio,traces,cost,keys,providers,playground,audit}
│   └── components/ lib/
├── platform/                      # infra as code
│   ├── terraform/{regions,cells,networking,gpu,dr}
│   ├── helm/ argocd/ karpenter/ keda/ cilium/
│   └── operators/                 # Cockroach, Qdrant, ScyllaDB, Redpanda, Temporal
├── libs/                          # shared: tokenizer(Rust/PyO3), otel, ir-schema
├── data/                          # dbt models, eval datasets, migrations
├── runbooks/                      # DR, failover, on-call, game-day scripts
└── ci/                            # pipelines, eval-gate, sigstore, sbom
```

**Why monorepo:** atomic cross-service changes (change a proto, update all consumers in one PR), unified CI, one dependency graph. Buf enforces that no proto change breaks a consumer.

---

# PART R — ADMIN PANEL ARCHITECTURE

```
Next.js (RSC) ──GraphQL──► Admin API (gRPC) ──► control plane + ClickHouse (reads)
                                              ──► Redpanda (live event stream via WS)
```

Modules:
- **Dashboard** — live RPS, p50/95/99 latency, error/quality drift, spend today, provider health. (ClickHouse near-real-time + WS live tail.)
- **Model Studio** — the **create-model wizard** → emits a *manifest*, runs **cost + latency simulation on a replay set**, stages a **canary rollout** with live promote/rollback controls. Recommender explains every choice (agent count, worker mix, topology, budgets) with live cost/latency estimates. This is your headline feature, now safe (versioned, canaried) instead of a raw DB edit.
- **Trace Explorer** — per-request agent waterfall: every node, the context it saw, tool calls, tokens, cost, eval score; one-click **replay**.
- **Cost Center** — spend by model/provider/tenant/role, orchestration-token breakdown, optimizer suggestions ("switch role:summarizer to self-hosted → save 40%").
- **Provider Console** — keys (Vault-backed), health, quota headroom, failover order, live circuit-breaker state.
- **Eval Center** — golden sets, shadow-traffic results, quality-drift alerts, A/B outcomes, regression-gate history.
- **Tenants & Keys** — orgs, RBAC, per-key rate limits/scopes, data-residency settings.
- **Audit** — immutable log of every control-plane change.

---

# PART S — COST & PERFORMANCE OPTIMIZATION (consolidated)

**Cost levers (ranked by impact):**
1. **Prefix/prompt-cache-aware routing** — turn repeated prefills into cache hits (biggest single lever).
2. **Self-host supporting models** (router/rerank/embed/summarize) — stop paying frontier prices for high-volume small tasks.
3. **Semantic response cache** — near-duplicate requests skip execution entirely.
4. **Right-size topology per request** — the optimizer downgrades over-provisioned plans (don't run 3 agents where 1 wins).
5. **Spot GPUs + Karpenter bin-packing + MIG** — cut GPU cost, no stranded capacity.
6. **Request coalescing** — dedup identical concurrent provider calls.
7. **Budget gating** — kill/simplify plans that exceed per-request $ budget before spending.

**Performance levers:**
1. **Rust hot path** — predictable p99 streaming (no GC).
2. **Fast-path bypass** — simple requests skip planning + Temporal entirely.
3. **Parallel IR branches** — independent agents run concurrently.
4. **Continuous batching (vLLM)** on self-hosted models.
5. **Speculative retrieval** — start retrieval while the planner is still planning.
6. **Deadline propagation** — a slow node can't blow the whole SLO.
7. **eBPF mesh (Cilium)** — kernel-level service-to-service, no sidecar tax.

---

# PART T — WHY THIS IS 10/10 (the summary defense)

| Dimension | v1 | v2 | The win |
|---|---|---|---|
| Plane separation | one engine | control / data / execution | blast-radius isolation, independent scale |
| Orchestration | "custom loop" | typed IR + planner/scheduler + Temporal | cacheable, replayable, durable, observable |
| Provider layer | litellm (in-proc) | Provider Gateway service | fleet-wide quota, circuit breaking, cache routing |
| Model config | live DB row | versioned manifest + canary | safe rollout, instant rollback |
| Scaling | HPA on RPS | token-aware KEDA + cells | correct signal, linear scale-out |
| Memory | Redis blob | dedicated **Memory Service** owning 3-tier working/episodic/semantic | single owner, stateless engine, async writes, tenant-isolated |
| Chat history | in website DB | dedicated **Chat Service** on ScyllaDB + Redis + S3 | scales independently of website, partition-by-conversation, tiered retention |
| Retrieval | list of channels | hybrid + GPU rerank + contextual + knapsack | actual quality |
| Eval | none | offline/shadow/judge/replay/optimizer | quality you can trust + improve |
| DR | none | tiered RTO/RPO + chaos-tested | survives region loss |
| Security | "gVisor mentioned" | zero-trust + Firecracker + scoped creds | enterprise-grade |
| Observability | admin panel | OTel-native, cost/quality per span | traces power eval + routing |
| Languages | Go+Python | Rust/Go/Python by bottleneck | right tool per layer |

**The core thesis:** a world-class AI orchestration platform is defined less by *calling models well* and more by everything *around* that call — plane separation, a typed executable plan, durable execution, real memory, hybrid retrieval, continuous evaluation, safe rollout, deep observability, and multi-region resilience. v2 builds all of it.

---

# PART U — BUILD SEQUENCING (so a team can actually start)

You don't build all of this on day one — you build it in the right order so every stage is production-real, not a toy.

1. **Foundation:** monorepo + proto/buf + Terraform (one region, one cell) + Cockroach + Redis + object store + OTel from line one.
2. **Vertical slice:** Rust edge → Python data plane (fast path only, template DAG) → Provider Gateway → one frontier model, streamed + metered. *Shippable.*
3. **Registry + manifests + admin Model Studio** (versioned, canary via Argo Rollouts).
4. **Deliberate path:** IR + planner + scheduler + Temporal + multi-agent + aggregator/verifier.
5. **Chat Service + Memory Service + context/retrieval:** stand up the **Chat Service** (ScyllaDB + Redis + S3, website routes all chat ops to it) and the **Memory Service** (Postgres text-of-truth + Qdrant, engine retrieves relevant slices + submits async); ingest workers, hybrid retrieval + GPU rerank, 3-tier memory (owned by Memory Service), cache tiers.
6. **Execution plane hardening:** Firecracker sandbox, self-hosted GPU (vLLM) for support models.
7. **Eval platform:** golden sets → regression gate → shadow → online judge → replay → optimizer.
8. **Observability + cost center + trace explorer** fully wired.
9. **Multi-cell → multi-region active-active + DR drills + chaos.**
10. **Learned router, semantic cache, on-prem/enterprise packaging, SDKs + docs + playground.**

Each stage ends in a deployable, observable, tested increment. Stage 2 alone is a real product; every stage after widens capability without a rewrite — because the plane separation and typed contracts were there from the start.
