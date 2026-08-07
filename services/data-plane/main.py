"""data-plane — Phase-03 Step H: real metering, the EC2 gate.

Composes every module built in Steps C-G into one real request path:
admission (scheduler.admit) -> complexity classification
(planner.classifier) -> template instantiation (planner.templates) ->
working-memory write (working_memory) -> role->model binding
(scheduler.place, live provider health) -> single-worker dispatch and
streaming fan-in (aggregator) -> working-memory cleanup -> metering
(request_log + usage_event, on every path, success or failure).

Structured logging on the SUCCESS path, not just errors, has been built
in since Step B — this is the exact gap Phase-01's edge-gateway (Step
J3) and Phase-02's provider-gateway (Step N3) both had to retrofit after
the fact.

Preliminary deployment — alongside dataplane-stub, no cutover yet, that's
Step J, same mesh-identity-safe sequencing Phase-01/02 both used.
"""

import asyncio
import importlib
import json
import logging
import os
import sys
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import aetcd
import asyncpg
import grpc
import httpx
from fastapi import FastAPI, Response
from fastapi.responses import PlainTextResponse
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.propagate import extract
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest
from redis.asyncio.cluster import RedisCluster

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "generated"))
import aggregator  # noqa: E402
import identity  # noqa: E402
import model_registry  # noqa: E402
import request_log  # noqa: E402
import usage_event  # noqa: E402
import vault_client  # noqa: E402
import working_memory  # noqa: E402
from dataplane.v1 import dataplane_pb2, dataplane_pb2_grpc  # noqa: E402
from planner import classifier, templates  # noqa: E402
from provider.v1 import provider_pb2, provider_pb2_grpc  # noqa: E402
from scheduler import admit, place  # noqa: E402

SERVICE = "data-plane"
GRPC_PORT = int(os.environ.get("GRPC_PORT", "50051"))

# Phase-04 Step Q: STATIC_MODEL_REF is gone — model_registry.Cache
# (etcd-fed, independently signature-verified) resolves a real worker_ref
# per request now. DEFAULT_MODEL_REF is only what request.model falls
# back to when a caller leaves it empty — "fake" so the same image still
# points at provider-fake for cluster-side testing without a code change,
# same purpose STATIC_MODEL_REF's own env-var default used to serve, just
# now a model_ref the registry resolves rather than a literal worker_ref.
DEFAULT_MODEL_REF = os.environ.get("DEFAULT_MODEL_REF", "fake")

# Untuned placeholder fleet-wide in-flight cap — same "not tuned against
# real data" framing already used for provider-gateway's quota/breaker
# thresholds and this service's own classifier/template bounds.
ADMISSION_LIMIT = int(os.environ.get("ADMISSION_LIMIT", "100"))

# Boot-time smoke test (Step G): imports every module the real Submit
# flow depends on and fails FAST and LOUD at startup if any is missing
# from the image — turns exactly the class of bug Step F caught (the
# Dockerfile silently never copying planner/scheduler/working_memory/
# request_log, undetected until something finally tried to import them)
# into an immediate, unmissable crash instead of a future confusing
# runtime failure the first time a real request needed one of them.
_REQUIRED_MODULES = (
    "planner.classifier",
    "planner.templates",
    "scheduler.admit",
    "scheduler.place",
    "working_memory",
    "request_log",
    "usage_event",
    "identity",
    "aggregator",
    "vault_client",
    "model_registry",
)


def verify_modules_present() -> None:
    for module_name in _REQUIRED_MODULES:
        importlib.import_module(module_name)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "service": SERVICE,
            "message": record.getMessage(),
        }
        return json.dumps(payload)


def setup_logging() -> logging.Logger:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logger = logging.getLogger(SERVICE)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    return logger


log = setup_logging()

resource = Resource.create({SERVICE_NAME: SERVICE})
provider_tp = TracerProvider(resource=resource)
otlp_endpoint = os.environ.get(
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "http://otel-collector-opentelemetry-collector.default.svc.cluster.local:4317",
)
provider_tp.add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True))
)
trace.set_tracer_provider(provider_tp)
tracer = trace.get_tracer(SERVICE)

boot_counter = Counter("data_plane_boot_total", "Number of successful boot sequences")

# Phase-05 Step N: the SLO signal control-plane's own AnalysisTemplate
# queries (services/control-plane/internal/analysis/analysisrun.go's own
# queryFor, committed at Step M ahead of this metric existing) —
# data_plane_submit_total{model_ref,canary,status}. Labeled, replacing the
# old unlabeled data_plane_submit_total counter (same metric NAME, so
# Step M's own already-written PromQL query needs no change; strictly
# richer than what it replaces since "sum across all labels" reproduces
# the old plain count exactly).
#
# canary/status are not decorative: canary is the SAME is_canary value
# resolve() itself returned for THIS request (model_registry.ResolvedWorker,
# Step N) — not re-derived or guessed at the call site — and status is
# incremented on BOTH the success path and every real-dispatch failure
# path (provider_unavailable, provider_fallback, provider_stream_error),
# not success only. A success-only metric would make a regression
# invisible to the exact AnalysisTemplate this labels feed — the same
# "success-path-only" gap Phase-01's edge-gateway (Step J3) and Phase-02's
# provider-gateway (Step N3) both had to retroactively fix for structured
# LOGGING; this is that same discipline applied to a metric instead.
submit_outcome_counter = Counter(
    "data_plane_submit_total",
    "Submit outcomes by resolved model_ref, canary/stable path, and status",
    ["model_ref", "canary", "status"],
)


def _record_submit_outcome(model_ref: str, is_canary: bool, status: str) -> None:
    """The ONE place this metric is ever incremented — every call site
    below passes the SAME model_ref/is_canary resolve() itself returned
    for that request, never a re-derived guess."""
    submit_outcome_counter.labels(
        model_ref=model_ref, canary="true" if is_canary else "false", status=status
    ).inc()

pg_pool: asyncpg.Pool | None = None
redis_client: RedisCluster | None = None
provider_channel: grpc.aio.Channel | None = None
provider_stub: provider_pb2_grpc.ProviderServiceStub | None = None
grpc_server: grpc.aio.Server | None = None
vault_http_client: httpx.AsyncClient | None = None
etcd_client: aetcd.Client | None = None
registry_cache: model_registry.Cache | None = None
registry_watch_task: asyncio.Task | None = None


async def _finish_request(
    span: trace.Span,
    request_id: str,
    org_id: str,
    status: str,
    start: float,
    *,
    usage: aggregator.Usage | None,
    model_ref: str | None,
) -> None:
    """Metering, Step H: writes request_log on EVERY path (Phase-03.txt's
    own words: "a request can fail before any provider usage exists at
    all, and still needs a log row"). Writes usage_event too, but ONLY
    when model_ref is not None — that's this function's signal that a
    provider dispatch was actually attempted (bind_via_provider_health
    succeeded and aggregator.relay was invoked). Admission/classifier/
    pre-dispatch-health rejections never reach a provider at all, so
    there is structurally nothing to bill; manufacturing an all-NULL
    usage_event row for them would falsely imply "maybe tokens were
    spent" about a call that provably never happened — the opposite
    failure mode from the silent-zero this step exists to rule out.

    tokens_in/tokens_out pass through exactly what `usage` reports
    (None when the provider's final delta didn't set that field, most
    notably provider-gateway's own best-effort synthetic delta on an
    upstream error) — never coerced to zero. orch_tokens is written as a
    real, known 0: the fast path structurally never invokes an LLM
    planner (Steps C/D's own AST-based no-model-call proof), so this
    isn't an estimate. usd_cost stays NULL — no pricing/rate source
    exists anywhere yet; that belongs to the model registry (Phase-04,
    Dependencies.txt F10), and fabricating a rate here would be the same
    kind of "looks right, isn't" mistake this step exists to catch for
    tokens.

    The span attributes below are set from the SAME local variables in
    the SAME call, right alongside the usage_event write — span and
    usage_event cannot independently drift by construction, which is
    what Phase-03.txt's "token/cost fields on the span match usage_event"
    testing requirement is asking to prove. A field is OMITTED from the
    span (not set to a sentinel) exactly when its usage_event column is
    NULL — presence semantics applied to spans the same way Step A
    applied them to the wire.
    """
    latency_ms = int((time.monotonic() - start) * 1000)
    span.set_attribute("status", status)
    await request_log.write(pg_pool, request_id, org_id, "Submit", status, latency_ms)

    if model_ref is None:
        return

    tokens_in = usage.input_tokens if usage is not None else None
    tokens_out = usage.output_tokens if usage is not None else None
    orch_tokens = 0
    usd_cost = None

    await usage_event.write(
        pg_pool, org_id, request_id, tokens_in, tokens_out, orch_tokens, usd_cost, model_ref
    )
    span.set_attribute("model_ref", model_ref)
    span.set_attribute("orch_tokens", orch_tokens)
    if tokens_in is not None:
        span.set_attribute("tokens_in", tokens_in)
    if tokens_out is not None:
        span.set_attribute("tokens_out", tokens_out)


class DataplaneServicer(dataplane_pb2_grpc.DataplaneServiceServicer):
    async def Submit(self, request, context):
        request_id = request.request_id
        org_id = request.identity.org_id
        start = time.monotonic()

        # Step I: Part O's "no call proceeds without it" gate, checked
        # before anything else — including the success-path log line
        # below, admission, and every other stage. No request_log/
        # usage_event row is ever written for this path (not even under
        # a placeholder org_id): with no real tenant identified, there is
        # nothing to durably attribute a row to, and a default/fallback
        # tenant would itself be the cross-tenant leak this step exists
        # to rule out.
        try:
            identity.validate(org_id)
        except identity.MissingIdentity:
            log.warning(
                f"rejected: missing identity request_id={request_id} "
                f"kind={dataplane_pb2.RequestKind.Name(request.kind)}"
            )
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "request missing required org_id"
            )
            return

        log.info(
            "request received "
            f"request_id={request_id} org_id={org_id} "
            f"kind={dataplane_pb2.RequestKind.Name(request.kind)} model={request.model}"
        )

        # Step L finding: without extracting the incoming traceparent from
        # the gRPC call's own metadata, this span starts as a fresh root
        # every time — genuinely exported, genuinely correct-looking on
        # its own, but disconnected from edge-gateway's trace. Three
        # services each exporting their own well-formed spans LOOKED like
        # tracing worked until someone actually counted trace IDs across
        # the full slice and found three, not one. dict() over
        # invocation_metadata()'s (key, value) tuples is the carrier
        # shape opentelemetry.propagate.extract expects; gRPC metadata
        # keys are already lowercase ASCII (HTTP/2 requirement), matching
        # W3C traceparent's own casing.
        parent_context = extract(dict(context.invocation_metadata()))
        with tracer.start_as_current_span(
            "data_plane.submit", context=parent_context
        ) as span:
            span.set_attribute("request_id", request_id)
            span.set_attribute("org_id", org_id)

            try:
                await admit.admit(redis_client, ADMISSION_LIMIT)
            except admit.Shed as e:
                log.info(
                    f"admission shed request_id={request_id} org_id={org_id} inflight={e.inflight}"
                )
                await _finish_request(
                    span, request_id, org_id, "admission_shed", start,
                    usage=None, model_ref=None,
                )
                await context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, str(e))
                return

            try:
                # Complexity classifier (Step C) — rules-based, no model
                # call. NeedsDeliberatePath marks the exact Phase-06 seam.
                try:
                    classifier.classify(
                        classifier.ClassifierInput(
                            kind=request.kind, message_count=len(request.messages)
                        )
                    )
                except classifier.NeedsDeliberatePath as e:
                    log.info(
                        f"needs deliberate path request_id={request_id} org_id={org_id} "
                        f"reason={e.reason}"
                    )
                    await _finish_request(
                        span, request_id, org_id, "unsupported", start,
                        usage=None, model_ref=None,
                    )
                    await context.abort(grpc.StatusCode.UNIMPLEMENTED, str(e))
                    return

                # Template DAG (Step D) — mechanical instantiation, no
                # per-request reasoning about the shape.
                template_req = templates.TemplateRequest(
                    request_id=request_id,
                    messages=tuple(
                        templates.Message(role=m.role, content=m.content) for m in request.messages
                    ),
                    max_tokens=request.max_tokens if request.HasField("max_tokens") else None,
                    temperature=request.temperature if request.HasField("temperature") else None,
                )
                dag = templates.instantiate(template_req)

                # Working memory (Step F) — request-scoped, TTL'd; written
                # before dispatch, explicitly deleted on the clean path
                # below (the TTL is the safety net for every other path,
                # not a substitute for this).
                wm_messages = [{"role": m.role, "content": m.content} for m in request.messages]
                await working_memory.write(redis_client, request_id, {"messages": wm_messages})

                # Phase-04 Step Q: model_ref -> worker_ref via the
                # registry cache (hot-path safe — resolve() is a plain
                # in-memory dict read, no network call per request), THEN
                # the SAME live-health gate Step E already established.
                # request.model empty falls back to DEFAULT_MODEL_REF, the
                # direct replacement for STATIC_MODEL_REF's own env-var
                # default.
                model_ref = request.model or DEFAULT_MODEL_REF
                try:
                    resolved = registry_cache.resolve(model_ref, request_id)
                except model_registry.ManifestNotFound as e:
                    log.info(
                        f"model not found request_id={request_id} org_id={org_id} "
                        f"model_ref={model_ref}"
                    )
                    await _finish_request(
                        span, request_id, org_id, "model_not_found", start,
                        usage=None, model_ref=None,
                    )
                    await context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(e))
                    return
                # is_canary is fixed at THIS point, from resolve()'s own
                # decision — never re-derived below. bind_via_provider_health
                # may still reroute worker_ref to a different provider on
                # an unhealthy target (a lower-level concern, Step E), but
                # that does not change which cohort (canary vs stable) this
                # request belongs to for SLO purposes.
                is_canary = resolved.is_canary

                try:
                    worker_ref = await place.bind_via_provider_health(
                        provider_stub, resolved.worker_ref
                    )
                except place.ProviderUnavailable as e:
                    log.info(
                        f"provider unavailable request_id={request_id} org_id={org_id} "
                        f"reason={e.reason}"
                    )
                    _record_submit_outcome(model_ref, is_canary, "error")
                    await _finish_request(
                        span, request_id, org_id, "provider_unavailable", start,
                        usage=None, model_ref=None,
                    )
                    await context.abort(grpc.StatusCode.UNAVAILABLE, str(e))
                    return

                # Single-worker dispatch + streaming fan-in (Step G),
                # capturing real usage off the final delta as it passes
                # through (Step H) — never fabricated, never assumed.
                #
                # Step J finding: the final response is held back and
                # yielded LAST, only after working_memory cleanup and
                # metering have already run — not yielded inline as each
                # item arrives, the way every earlier item is. A real
                # gRPC client (edge-gateway's own stream.rs: "stops at
                # the first is_final chunk") legitimately stops polling
                # the instant it receives the final application-level
                # message; nothing requires it to poll once more just to
                # observe the stream close. The old ordering (yield
                # every item inline, run cleanup once the `async for`
                # loop itself exits) silently depended on exactly that
                # extra poll to resume this generator past its last
                # yield — grpcurl happens to drain to the true stream
                # end, which is exactly why this never surfaced in Steps
                # G/H/I's own live verification, all done directly
                # against grpcurl. The moment a real client (edge-
                # gateway) was wired in at this step, it stopped polling
                # right after the final chunk, the generator was
                # effectively abandoned mid-cleanup: working_memory.
                # delete() had already run by then, but usage_event/
                # request_log never got written — a genuine,
                # previously-undetected EC2-breaking bug, caught only
                # because this step verifies against the real slice, not
                # grpcurl.
                captured_usage: aggregator.Usage | None = None
                final_response = None
                try:
                    stream = aggregator.relay(provider_stub, request_id, worker_ref, dag.worker)
                    async for response, usage in stream:
                        if usage is not None:
                            captured_usage = usage
                            final_response = response
                            break
                        yield response
                except aggregator.ProviderFallback as e:
                    log.info(
                        f"provider fallback request_id={request_id} org_id={org_id} "
                        f"reason={e.reason}"
                    )
                    # provider-gateway declined before ever calling the
                    # adapter (breaker/quota check, Step G's own relay.go
                    # reading) — no dispatch happened, so unlike the
                    # stream-error branch below, this one stays
                    # model_ref=None too: nothing was attempted, nothing
                    # to bill. Still a real canary-cohort OUTCOME though
                    # (the resolved worker_ref was rejected before ever
                    # streaming) — recorded as "error" for SLO purposes
                    # even though nothing is billed.
                    _record_submit_outcome(model_ref, is_canary, "error")
                    await _finish_request(
                        span, request_id, org_id, "provider_fallback", start,
                        usage=None, model_ref=None,
                    )
                    await context.abort(grpc.StatusCode.UNAVAILABLE, str(e))
                    return
                except aggregator.ProviderStreamError as e:
                    log.warning(
                        f"provider stream error request_id={request_id} org_id={org_id} error={e}"
                    )
                    # Unlike every branch above, a real dispatch to the
                    # provider was genuinely attempted here — real tokens
                    # may have been spent before the connection broke.
                    # This is the one failure path that still gets a
                    # usage_event row: honestly incomplete (captured_usage
                    # is whatever the best-effort final delta reported,
                    # which is None/None unless the provider itself
                    # managed to report something before dying), never a
                    # silent zero pretending the call was free.
                    _record_submit_outcome(model_ref, is_canary, "error")
                    await _finish_request(
                        span, request_id, org_id, "provider_stream_error", start,
                        usage=captured_usage, model_ref=worker_ref,
                    )
                    await context.abort(grpc.StatusCode.UNAVAILABLE, str(e))
                    return

                await working_memory.delete(redis_client, request_id)
                _record_submit_outcome(model_ref, is_canary, "ok")
                await _finish_request(
                    span, request_id, org_id, "ok", start,
                    usage=captured_usage, model_ref=worker_ref,
                )
                if final_response is not None:
                    yield final_response
            finally:
                await admit.release(redis_client)

        latency_ms = int((time.monotonic() - start) * 1000)
        # The success-path completion log Phase-01/02 both had to
        # retrofit — built in from the start (Step B) and now exercised
        # by a real assembled request, not just a placeholder.
        log.info(
            f"request completed request_id={request_id} org_id={org_id} latency_ms={latency_ms}"
        )


async def write_health_probe(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO health_probe (service) VALUES ($1)", SERVICE)
    log.info("wrote health_probe row")


async def set_and_get_redis_key(client: RedisCluster) -> None:
    key = f"{SERVICE}:boot"
    value = datetime.now(UTC).isoformat()
    await client.set(key, value)
    got = await client.get(key)
    log.info(f"redis set/get round-trip verified key={key} value={got}")


async def check_provider_gateway(stub: provider_pb2_grpc.ProviderServiceStub) -> None:
    # Empty request = "report on every known provider" (provider.proto's
    # own doc comment) — a real, cheap, read-only RPC (Step A7) that proves
    # this gRPC channel actually works, the same way dataplane-stub's own
    # Cockroach insert / Redis set-get proved those connections in
    # Phase-00, rather than just opening a channel and never using it.
    resp = await stub.ProviderHealth(provider_pb2.ProviderHealthRequest())
    statuses = [
        f"{s.provider}:{provider_pb2.BreakerState.Name(s.breaker_state)}:{s.quota_headroom:.2f}"
        for s in resp.statuses
    ]
    log.info(f"provider-gateway ProviderHealth reachable statuses={statuses}")


async def init_model_registry() -> tuple[httpx.AsyncClient, aetcd.Client, model_registry.Cache]:
    """Phase-04 Step Q boot sequence for the registry cache: Vault client
    (data-plane's OWN, independent of control-plane's — see vault_client's
    own module doc for why), etcd client, then a BLOCKING initial
    sync_once() before this function returns — the same "prove it before
    yielding readiness" discipline check_provider_gateway/
    set_and_get_redis_key already use for their own dependencies. The
    background watch_forever() task is spawned by the caller, not here,
    since it needs to keep running past this function's own return.
    """
    vault_addr = os.environ.get("VAULT_ADDR", "http://vault-active.default.svc.cluster.local:8200")
    vault_k8s_role = os.environ.get("VAULT_K8S_ROLE", "data-plane")
    http_client = httpx.AsyncClient(timeout=10.0)
    vault = vault_client.Client(vault_addr, vault_k8s_role, http_client)

    etcd_host, _, etcd_port = os.environ.get(
        "ETCD_ENDPOINT", "etcd.default.svc.cluster.local:2379"
    ).partition(":")
    etcd = aetcd.Client(host=etcd_host, port=int(etcd_port) if etcd_port else 2379)
    await etcd.connect()

    cache = model_registry.Cache(etcd, vault, log)
    await cache.sync_once()
    log.info("model_registry: initial sync complete")

    return http_client, etcd, cache


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pg_pool, redis_client, provider_channel, provider_stub, grpc_server
    global vault_http_client, etcd_client, registry_cache, registry_watch_task

    with tracer.start_as_current_span("data_plane.boot"):
        log.info("starting boot sequence")

        verify_modules_present()
        log.info(f"boot: all required modules present: {list(_REQUIRED_MODULES)}")

        pg_host = os.environ.get("COCKROACH_HOST", "onezox-crdb-public.default.svc.cluster.local")
        pg_pool = await asyncpg.create_pool(
            host=pg_host, port=26257, user="root", database="defaultdb", ssl=False,
        )
        await write_health_probe(pg_pool)

        redis_host = os.environ.get(
            "REDIS_HOST", "redis-cluster-headless.default.svc.cluster.local"
        )
        redis_client = RedisCluster(host=redis_host, port=6379, decode_responses=True)
        await set_and_get_redis_key(redis_client)

        provider_gateway_host = os.environ.get(
            "PROVIDER_GATEWAY_HOST", "provider-gateway.default.svc.cluster.local:50051"
        )
        provider_channel = grpc.aio.insecure_channel(provider_gateway_host)
        provider_stub = provider_pb2_grpc.ProviderServiceStub(provider_channel)
        await check_provider_gateway(provider_stub)

        vault_http_client, etcd_client, registry_cache = await init_model_registry()

        boot_counter.inc()
        log.info("boot sequence complete")

    # Spawned AFTER the span above closes — this loop runs for the rest of
    # the process's life, unlike everything inside data_plane.boot which
    # completes once.
    registry_watch_task = asyncio.create_task(registry_cache.watch_forever())

    grpc_server = grpc.aio.server()
    dataplane_pb2_grpc.add_DataplaneServiceServicer_to_server(DataplaneServicer(), grpc_server)
    grpc_server.add_insecure_port(f"[::]:{GRPC_PORT}")
    await grpc_server.start()
    log.info(f"gRPC DataplaneService listening on :{GRPC_PORT}")

    yield

    await grpc_server.stop(grace=5)
    if registry_watch_task:
        registry_watch_task.cancel()
    if provider_channel:
        await provider_channel.close()
    if etcd_client:
        await etcd_client.close()
    if vault_http_client:
        await vault_http_client.aclose()
    if pg_pool:
        await pg_pool.close()
    if redis_client:
        await redis_client.aclose()


app = FastAPI(lifespan=lifespan)


@app.get("/healthz")
async def healthz():
    return PlainTextResponse("ok")


@app.get("/readyz")
async def readyz():
    pg_ok = False
    redis_ok = False
    try:
        async with pg_pool.acquire() as conn:
            await conn.execute("SELECT 1")
        pg_ok = True
    except Exception:
        log.exception("readyz: postgres check failed")

    try:
        await redis_client.ping()
        redis_ok = True
    except Exception:
        log.exception("readyz: redis check failed")

    if pg_ok and redis_ok:
        return PlainTextResponse("ready")
    return PlainTextResponse("not ready", status_code=503)


@app.get("/metrics")
async def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
