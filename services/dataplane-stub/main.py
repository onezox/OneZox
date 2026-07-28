"""dataplane-stub — Phase-00 throwaway health stub (Python).

Proves the toolchain, mesh, and telemetry work end-to-end: on boot it
connects to CockroachDB, Redis, and MinIO, emits a trace span covering the
whole sequence, and exposes /healthz, /readyz, and /metrics.

Phase-01 adds a minimal DataplaneService.Submit gRPC shim (see
proto/dataplane/v1/dataplane.proto): it does not call a model, it streams a
fixed placeholder response so edge-gateway has a real downstream to forward
and relay over SSE. This is a contract requirement of Phase-01 (edge needs a
Submit endpoint to call), not a reopening of Phase-00 — the health-check
surface and its verification are unchanged.

Replaced by the real data plane in Phase-03.
"""

import asyncio
import io
import json
import logging
import os
import sys
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import asyncpg
import grpc
from redis.asyncio.cluster import RedisCluster
from fastapi import FastAPI, Response
from fastapi.responses import PlainTextResponse
from minio import Minio
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "generated"))
from dataplane.v1 import dataplane_pb2, dataplane_pb2_grpc  # noqa: E402

SERVICE = "dataplane-stub"
TENANT = "onezox-dev"
GRPC_PORT = int(os.environ.get("GRPC_PORT", "50051"))


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
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
provider = TracerProvider(resource=resource)
otlp_endpoint = os.environ.get(
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "http://otel-collector-opentelemetry-collector.default.svc.cluster.local:4317",
)
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(SERVICE)

boot_counter = Counter("dataplane_stub_boot_total", "Number of successful boot sequences")
submit_counter = Counter("dataplane_stub_submit_total", "Number of Submit RPCs served")

pg_pool: asyncpg.Pool | None = None
redis_client: RedisCluster | None = None
grpc_server: grpc.aio.Server | None = None

# Fixed placeholder stream: dataplane-stub does not call a model (Phase-03
# does). This just proves edge-gateway's SSE relay carries real multi-chunk
# streamed content end to end.
_PLACEHOLDER_WORDS = ["Hello", "from", "the", "Phase-01", "dataplane-stub", "shim."]


class DataplaneServicer(dataplane_pb2_grpc.DataplaneServiceServicer):
    async def Submit(self, request, context):
        with tracer.start_as_current_span("dataplane_stub.submit") as span:
            span.set_attribute("request_id", request.request_id)
            span.set_attribute("model", request.model)
            log.info(
                f"submit request_id={request.request_id} "
                f"org_id={request.identity.org_id} model={request.model} kind={request.kind}"
            )

            for word in _PLACEHOLDER_WORDS:
                yield dataplane_pb2.SubmitResponse(
                    request_id=request.request_id,
                    content=f"{word} ",
                    is_final=False,
                )
                await asyncio.sleep(0.05)

            yield dataplane_pb2.SubmitResponse(
                request_id=request.request_id,
                finish_reason="stop",
                is_final=True,
            )
            submit_counter.inc()


async def write_health_probe(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO health_probe (service) VALUES ($1)", SERVICE)
    log.info("wrote health_probe row")


async def set_and_get_redis_key(client: RedisCluster) -> None:
    key = f"{TENANT}:{SERVICE}:boot"
    value = datetime.now(timezone.utc).isoformat()
    await client.set(key, value)
    got = await client.get(key)
    log.info(f"redis set/get round-trip verified key={key} value={got}")


def upload_test_object() -> None:
    endpoint = os.environ.get("MINIO_ENDPOINT", "minio.default.svc.cluster.local:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "onezox-admin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "onezox-local-dev-only")
    bucket = os.environ.get("MINIO_BUCKET", "onezox-artifacts")

    client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)
    key = f"health-checks/{SERVICE}-{int(time.time())}.txt"
    body = f"OneZox Phase-00 health check from {SERVICE} at {datetime.now(timezone.utc).isoformat()}".encode()
    client.put_object(bucket, key, io.BytesIO(body), length=len(body))
    log.info(f"uploaded test object to MinIO bucket={bucket} key={key}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pg_pool, redis_client, grpc_server

    with tracer.start_as_current_span("dataplane_stub.boot"):
        log.info("starting boot sequence")

        pg_host = os.environ.get("COCKROACH_HOST", "onezox-crdb-public.default.svc.cluster.local")
        pg_pool = await asyncpg.create_pool(
            host=pg_host, port=26257, user="root", database="defaultdb", ssl=False,
        )
        await write_health_probe(pg_pool)

        redis_host = os.environ.get("REDIS_HOST", "redis-cluster-headless.default.svc.cluster.local")
        redis_client = RedisCluster(host=redis_host, port=6379, decode_responses=True)
        await set_and_get_redis_key(redis_client)

        try:
            upload_test_object()
        except Exception:
            log.exception("failed to upload test object to MinIO")

        boot_counter.inc()
        log.info("boot sequence complete")

    grpc_server = grpc.aio.server()
    dataplane_pb2_grpc.add_DataplaneServiceServicer_to_server(DataplaneServicer(), grpc_server)
    grpc_server.add_insecure_port(f"[::]:{GRPC_PORT}")
    await grpc_server.start()
    log.info(f"gRPC DataplaneService listening on :{GRPC_PORT}")

    yield

    await grpc_server.stop(grace=5)
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
