// edge-stub — Phase-00 throwaway health stub (Rust).
// Proves the toolchain, mesh, and telemetry work end-to-end: on boot it
// connects to CockroachDB, Redis, and MinIO, emits a trace span covering
// the whole sequence, and exposes /healthz, /readyz, and /metrics.
// Replaced by the real edge-gateway in Phase-01.

use axum::{routing::get, Router, response::IntoResponse, http::StatusCode, extract::State};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use prometheus::{Registry, IntCounter, TextEncoder, Encoder};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, error, instrument};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

const SERVICE_NAME: &str = "edge-stub";
const TENANT: &str = "onezox-dev";

struct AppState {
    pg: Arc<Mutex<tokio_postgres::Client>>,
    redis: redis::Client,
    boot_ok: IntCounter,
    registry: Registry,
}

fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn init_telemetry() -> opentelemetry_sdk::trace::SdkTracerProvider {
    let otlp_endpoint = env(
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "http://otel-collector-opentelemetry-collector.default.svc.cluster.local:4317",
    );

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(otlp_endpoint)
        .build()
        .expect("failed to build OTLP exporter");

    let resource = Resource::builder()
        .with_service_name(SERVICE_NAME)
        .build();

    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();

    let tracer = provider.tracer(SERVICE_NAME);
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    let fmt_layer = tracing_subscriber::fmt::layer().json();

    tracing_subscriber::registry()
        .with(otel_layer)
        .with(fmt_layer)
        .init();

    provider
}

#[instrument(skip(pg_client))]
async fn write_health_probe(pg_client: &tokio_postgres::Client) -> Result<(), tokio_postgres::Error> {
    pg_client
        .execute("INSERT INTO health_probe (service) VALUES ($1)", &[&SERVICE_NAME])
        .await?;
    info!(table = "health_probe", "wrote health_probe row");
    Ok(())
}

#[instrument(skip(client))]
async fn set_and_get_redis_key(client: &redis::Client) -> redis::RedisResult<()> {
    let mut conn = client.get_multiplexed_async_connection().await?;
    let key = format!("{TENANT}:{SERVICE_NAME}:boot");
    let value = chrono::Utc::now().to_rfc3339();
    redis::cmd("SET").arg(&key).arg(&value).query_async::<()>(&mut conn).await?;
    let got: String = redis::cmd("GET").arg(&key).query_async(&mut conn).await?;
    info!(key = %key, value = %got, "redis set/get round-trip verified");
    Ok(())
}

#[instrument]
async fn upload_test_object() -> Result<(), s3::error::S3Error> {
    let endpoint = env("MINIO_ENDPOINT", "http://minio.default.svc.cluster.local:9000");
    let access_key = env("MINIO_ACCESS_KEY", "onezox-admin");
    let secret_key = env("MINIO_SECRET_KEY", "onezox-local-dev-only");
    let bucket_name = env("MINIO_BUCKET", "onezox-artifacts");

    let region = s3::Region::Custom { region: "us-east-1".to_string(), endpoint };
    let credentials = s3::creds::Credentials::new(Some(&access_key), Some(&secret_key), None, None, None)?;
    let bucket = s3::Bucket::new(&bucket_name, region, credentials)?.with_path_style();

    let key = format!("health-checks/{SERVICE_NAME}-{}.txt", chrono::Utc::now().timestamp());
    let body = format!("OneZox Phase-00 health check from {SERVICE_NAME} at {}", chrono::Utc::now().to_rfc3339());
    bucket.put_object(&key, body.as_bytes()).await?;
    info!(bucket = %bucket_name, key = %key, "uploaded test object to MinIO");
    Ok(())
}

async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn readyz(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let pg = state.pg.lock().await;
    let pg_ok = pg.simple_query("SELECT 1").await.is_ok();
    drop(pg);

    let redis_ok = match state.redis.get_multiplexed_async_connection().await {
        Ok(mut conn) => redis::cmd("PING").query_async::<String>(&mut conn).await.is_ok(),
        Err(_) => false,
    };

    if pg_ok && redis_ok {
        (StatusCode::OK, "ready").into_response()
    } else {
        error!(pg_ok, redis_ok, "readiness check failed");
        (StatusCode::SERVICE_UNAVAILABLE, "not ready").into_response()
    }
}

async fn metrics(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let metric_families = state.registry.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    (StatusCode::OK, [("content-type", "text/plain; version=0.0.4")], buffer)
}

#[tokio::main]
async fn main() {
    let provider = init_telemetry();

    let boot_span = tracing::info_span!("edge_stub.boot");
    let _enter = boot_span.enter();
    info!(service = SERVICE_NAME, "starting boot sequence");

    let pg_host = env("COCKROACH_HOST", "onezox-crdb-public.default.svc.cluster.local");
    let pg_conninfo = format!("host={pg_host} port=26257 user=root dbname=defaultdb sslmode=disable");
    let (pg_client, pg_connection) = tokio_postgres::connect(&pg_conninfo, tokio_postgres::NoTls)
        .await
        .expect("failed to connect to CockroachDB");
    tokio::spawn(async move {
        if let Err(e) = pg_connection.await {
            error!(error = %e, "postgres connection task ended with error");
        }
    });
    write_health_probe(&pg_client).await.expect("failed to write health_probe row");

    let redis_host = env("REDIS_HOST", "redis-cluster-headless.default.svc.cluster.local");
    let redis_client = redis::Client::open(format!("redis://{redis_host}:6379")).expect("invalid redis url");
    set_and_get_redis_key(&redis_client).await.expect("failed redis set/get");

    if let Err(e) = upload_test_object().await {
        error!(error = ?e, "failed to upload test object to MinIO");
    }

    let registry = Registry::new();
    let boot_ok = IntCounter::new("edge_stub_boot_total", "Number of successful boot sequences").unwrap();
    registry.register(Box::new(boot_ok.clone())).unwrap();
    boot_ok.inc();

    info!("boot sequence complete");
    drop(_enter);

    let state = Arc::new(AppState { pg: Arc::new(Mutex::new(pg_client)), redis: redis_client, boot_ok, registry });

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .with_state(state);

    let port = env("PORT", "8080");
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await.unwrap();
    info!(port = %port, "edge-stub listening");
    axum::serve(listener, app).await.unwrap();

    provider.shutdown().ok();
}
