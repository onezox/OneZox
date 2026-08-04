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

import importlib
import json
import logging
import os
import sys
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import asyncpg
import grpc
from fastapi import FastAPI, Response
from fastapi.responses import PlainTextResponse
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest
from redis.asyncio.cluster import RedisCluster

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "generated"))
import aggregator  # noqa: E402
import request_log  # noqa: E402
import usage_event  # noqa: E402
import working_memory  # noqa: E402
from dataplane.v1 import dataplane_pb2, dataplane_pb2_grpc  # noqa: E402
from planner import classifier, templates  # noqa: E402
from provider.v1 import provider_pb2, provider_pb2_grpc  # noqa: E402
from scheduler import admit, place  # noqa: E402

SERVICE = "data-plane"
GRPC_PORT = int(os.environ.get("GRPC_PORT", "50051"))

# Model source is STATIC this phase (Dependencies.txt F10: the signed
# registry replaces this in Phase-04) — one worker_ref, read from env so
# the same image can be pointed at provider-fake for every step's own
# testing (this one included) and the real Anthropic model for Step K's
# one real call, without a code change between the two.
STATIC_MODEL_REF = os.environ.get("STATIC_MODEL_REF", "anthropic:claude-haiku-4-5-20251001")

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
    "aggregator",
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
submit_counter = Counter("data_plane_submit_total", "Number of Submit RPCs served")

pg_pool: asyncpg.Pool | None = None
redis_client: RedisCluster | None = None
provider_channel: grpc.aio.Channel | None = None
provider_stub: provider_pb2_grpc.ProviderServiceStub | None = None
grpc_server: grpc.aio.Server | None = None


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

        log.info(
            "request received "
            f"request_id={request_id} org_id={org_id} "
            f"kind={dataplane_pb2.RequestKind.Name(request.kind)} model={request.model}"
        )

        with tracer.start_as_current_span("data_plane.submit") as span:
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

                # Scheduler bind (Step E) — the one static worker_ref,
                # gated on live provider health.
                try:
                    worker_ref = await place.bind_via_provider_health(
                        provider_stub, STATIC_MODEL_REF
                    )
                except place.ProviderUnavailable as e:
                    log.info(
                        f"provider unavailable request_id={request_id} org_id={org_id} "
                        f"reason={e.reason}"
                    )
                    await _finish_request(
                        span, request_id, org_id, "provider_unavailable", start,
                        usage=None, model_ref=None,
                    )
                    await context.abort(grpc.StatusCode.UNAVAILABLE, str(e))
                    return

                # Single-worker dispatch + streaming fan-in (Step G),
                # capturing real usage off the final delta as it passes
                # through (Step H) — never fabricated, never assumed.
                captured_usage: aggregator.Usage | None = None
                try:
                    stream = aggregator.relay(provider_stub, request_id, worker_ref, dag.worker)
                    async for response, usage in stream:
                        yield response
                        if usage is not None:
                            captured_usage = usage
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
                    # to bill.
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
                    await _finish_request(
                        span, request_id, org_id, "provider_stream_error", start,
                        usage=captured_usage, model_ref=worker_ref,
                    )
                    await context.abort(grpc.StatusCode.UNAVAILABLE, str(e))
                    return

                await working_memory.delete(redis_client, request_id)
                submit_counter.inc()
                await _finish_request(
                    span, request_id, org_id, "ok", start,
                    usage=captured_usage, model_ref=worker_ref,
                )
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pg_pool, redis_client, provider_channel, provider_stub, grpc_server

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

        boot_counter.inc()
        log.info("boot sequence complete")

    grpc_server = grpc.aio.server()
    dataplane_pb2_grpc.add_DataplaneServiceServicer_to_server(DataplaneServicer(), grpc_server)
    grpc_server.add_insecure_port(f"[::]:{GRPC_PORT}")
    await grpc_server.start()
    log.info(f"gRPC DataplaneService listening on :{GRPC_PORT}")

    yield

    await grpc_server.stop(grace=5)
    if provider_channel:
        await provider_channel.close()
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
