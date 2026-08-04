"""data-plane — Phase-03 Step B: skeleton for the real data plane, replacing
the Phase-00/01 dataplane-stub.

This is a skeleton ONLY: DataplaneService.Submit does not run the
complexity classifier, execute a template DAG, or bind a role to a model
via the scheduler — those are Steps C/D/E, each its own commit. What Submit
does here is the same thing dataplane-stub's own "stub" has always done
(Phase-00/01): complete the RPC successfully with a fixed, clearly-marked
placeholder response, so the wire contract and the logging/telemetry
surface around it are real and verified before any real logic sits behind
them.

Structured logging on the SUCCESS path, not just errors, is built in from
day one — this is the exact gap Phase-01's edge-gateway (Step J3) and
Phase-02's provider-gateway (Step N3) both had to retrofit after the fact.
Submit logs once on request receipt and once on successful completion;
"successful" here means "the RPC completed and responded," not "produced a
real answer" — that distinction matters because this skeleton's only
answer IS the placeholder, and pretending otherwise would defeat the point
of building the logging discipline honestly.

Preliminary deployment (Step B), alongside dataplane-stub — no cutover yet,
that's Step J, same mesh-identity-safe sequencing Phase-01/02 both used.
"""

import json
import logging
import os
import sys
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
from dataplane.v1 import dataplane_pb2, dataplane_pb2_grpc  # noqa: E402
from provider.v1 import provider_pb2, provider_pb2_grpc  # noqa: E402

SERVICE = "data-plane"
GRPC_PORT = int(os.environ.get("GRPC_PORT", "50051"))


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

# Clearly marked as a skeleton placeholder, not a real answer — Steps C/D/E
# replace this with the actual classify -> template DAG -> scheduler bind
# -> provider dispatch flow. Matches dataplane-stub's own established
# "canned, self-identifying content" convention (Phase-00/01) rather than
# returning a bare UNIMPLEMENTED status: the RPC itself, the logging around
# it, and the wire contract all need to be real and verifiable now, even
# though the business logic behind the answer isn't yet.
_PLACEHOLDER_CONTENT = (
    "data-plane skeleton (Phase-03 Step B) -- fast path not yet implemented, "
    "classifier/template DAG/scheduler arrive in Steps C-E."
)


class DataplaneServicer(dataplane_pb2_grpc.DataplaneServiceServicer):
    async def Submit(self, request, context):
        log.info(
            "request received "
            f"request_id={request.request_id} org_id={request.identity.org_id} "
            f"kind={dataplane_pb2.RequestKind.Name(request.kind)} model={request.model}"
        )
        with tracer.start_as_current_span("data_plane.submit") as span:
            span.set_attribute("request_id", request.request_id)
            span.set_attribute("org_id", request.identity.org_id)

            yield dataplane_pb2.SubmitResponse(
                request_id=request.request_id,
                content=_PLACEHOLDER_CONTENT,
                finish_reason="not_implemented",
                is_final=True,
            )
            submit_counter.inc()

        # The success-path completion log Phase-01/02 both had to retrofit
        # — built in here from the start. "Completed" describes the RPC's
        # own lifecycle (received, responded, done), not the quality of the
        # answer, which is intentionally a placeholder at this step.
        log.info(
            f"request completed request_id={request.request_id} org_id={request.identity.org_id}"
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
