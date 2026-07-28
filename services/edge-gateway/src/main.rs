//! edge-gateway — Phase-01: replaces the Phase-00 edge-stub with the real
//! public ingress. Built incrementally per CLAUDE.md: this step wires the
//! four OpenAI-compatible ingress routes (Part K, src/ingress.rs) behind
//! API-key authentication (Part O, src/auth). ratelimit, admission,
//! normalize, meter, and the SSE relay to the data plane (per Phase-01.txt's
//! src/{ratelimit,admission,normalize,stream,meter} folder structure) land
//! in later Phase-01 steps.

mod auth;
mod ingress;
mod state;

use std::sync::Arc;

use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use auth::store::{CockroachApiKeyStore, build_pool};
use state::AppState;

fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn init_logging() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().json())
        .init();
}

async fn readyz(State(state): State<AppState<CockroachApiKeyStore>>) -> impl IntoResponse {
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
    // No real JWT issuer exists yet in Phase-01 (see auth/jwt.rs) — this is
    // a placeholder default so the service boots without a K8s Secret
    // during local dev; production deployment sets JWT_HMAC_SECRET.
    let jwt_secret = env("JWT_HMAC_SECRET", "phase01-dev-only-placeholder-secret");
    let state = AppState {
        api_key_store: Arc::new(CockroachApiKeyStore::new(pool)),
        jwt_secret: Arc::from(jwt_secret.into_bytes().into_boxed_slice()),
    };

    let app = ingress::router::<CockroachApiKeyStore>()
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
