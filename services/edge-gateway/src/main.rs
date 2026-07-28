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
mod normalize;
mod pb;
mod pipeline;
mod ratelimit;
mod state;

use std::sync::Arc;

use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use admission::store::RedisAdmissionGauge;
use auth::store::{CockroachApiKeyStore, build_pool};
use ratelimit::store::{CockroachRateLimitPolicyStore, RedisRateLimitCounter, build_redis_client};
use state::AppState;

fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn init_logging() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().json())
        .init();
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
    init_logging();

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
}
