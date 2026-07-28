//! edge-gateway — Phase-01: replaces the Phase-00 edge-stub with the real
//! public ingress. Built incrementally per CLAUDE.md: this step wires the
//! four OpenAI-compatible ingress routes (Part K, src/ingress.rs) behind
//! authentication (Part O, src/auth), rate limiting (src/ratelimit), and
//! admission control (src/admission), composed in src/pipeline.rs.
//! normalize, meter, and the SSE relay to the data plane (per
//! Phase-01.txt's src/{normalize,stream,meter} folder structure) land in
//! the next Phase-01 step.

mod admission;
mod auth;
mod ingress;
mod meter;
mod normalize;
mod pb;
mod pipeline;
mod ratelimit;
mod state;

use std::sync::Arc;

use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use admission::store::RedisAdmissionGauge;
use auth::store::{CockroachApiKeyStore, build_pool};
use ratelimit::store::{CockroachRateLimitPolicyStore, RedisRateLimitCounter, build_redis_client};
use state::AppState;

const SERVICE_NAME: &str = "edge-gateway";

fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Adapted from edge-stub's already-verified Phase-00 pattern (Phase-00's
/// own Problem #15 in Phase-00-Complete.txt). Two things that pattern
/// specifically exists to avoid:
/// - Without the EnvFilter excluding TRACE/DEBUG, tracing_opentelemetry
///   bridges h2/tonic's own internal per-frame spans (used by the OTLP
///   exporter itself) into Tempo alongside real application spans —
///   confirmed there by searching Tempo and getting transport-noise
///   instead of the intended span.
/// - Spans wrapping async code with multiple .await points must be entered
///   via `.instrument()`, never a manual `span.enter()` guard held across
///   an await — that breaks OTel span-context propagation. meter.rs's
///   `RequestMeter::span()` is designed to be used with `.instrument()` in
///   Step E3/E4 for exactly this reason.
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

    let resource = Resource::builder().with_service_name(SERVICE_NAME).build();

    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();

    let tracer = provider.tracer(SERVICE_NAME);
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    let fmt_layer = tracing_subscriber::fmt::layer().json();

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(filter)
        .with(otel_layer)
        .with(fmt_layer)
        .init();

    provider
}

async fn readyz(State(state): State<AppState>) -> impl IntoResponse {
    if state.api_key_store.ping().await.is_ok() {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready")
    }
}

#[tokio::main]
async fn main() {
    let provider = init_telemetry();

    let pg_host = env("COCKROACH_HOST", "onezox-crdb-public.default.svc.cluster.local");
    let pool = build_pool(&pg_host);

    let redis_host = env("REDIS_HOST", "redis-cluster-headless.default.svc.cluster.local");
    let redis_client = build_redis_client(&redis_host);

    // No real JWT issuer exists yet in Phase-01 (see auth/jwt.rs) — this is
    // a placeholder default so the service boots without a K8s Secret
    // during local dev; production deployment sets JWT_HMAC_SECRET.
    let jwt_secret = env("JWT_HMAC_SECRET", "phase01-dev-only-placeholder-secret");

    // Placeholder concurrency thresholds for Phase-01's single local cell
    // (Part D). Not tuned against real capacity data — that's a later-phase
    // concern once there's real load/latency data to size against.
    let admission_soft_limit = env_u64("ADMISSION_SOFT_LIMIT", 100);
    let admission_hard_limit = env_u64("ADMISSION_HARD_LIMIT", 200);

    let state = AppState {
        api_key_store: Arc::new(CockroachApiKeyStore::new(pool.clone())),
        jwt_secret: Arc::from(jwt_secret.into_bytes().into_boxed_slice()),
        rate_limit_counter: Arc::new(RedisRateLimitCounter::new(redis_client.clone())),
        rate_limit_policy_store: Arc::new(CockroachRateLimitPolicyStore::new(pool)),
        admission_gauge: Arc::new(RedisAdmissionGauge::new(redis_client)),
        admission_soft_limit,
        admission_hard_limit,
    };

    let app = ingress::router()
        .route("/healthz", get(|| async { StatusCode::OK }))
        .route("/readyz", get(readyz))
        .with_state(state);

    let port = env("PORT", "8080");
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .unwrap();
    tracing::info!(port = %port, "edge-gateway listening");
    axum::serve(listener, app).await.unwrap();

    provider.shutdown().ok();
}
