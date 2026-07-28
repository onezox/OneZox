//! edge-gateway — Phase-01: replaces the Phase-00 edge-stub with the real
//! public ingress. Built incrementally per CLAUDE.md: this step wires the
//! four OpenAI-compatible ingress routes only (Part K, src/ingress.rs).
//! auth, ratelimit, admission, normalize, meter, and the SSE relay to the
//! data plane (src/{auth,ratelimit,admission,normalize,stream,meter}, per
//! Phase-01.txt's folder structure) land in later Phase-01 steps.

mod ingress;

use axum::{http::StatusCode, routing::get};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

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

#[tokio::main]
async fn main() {
    init_logging();

    // Liveness only for now — "is the process up." /readyz is added once
    // there's a real dependency to check (CockroachDB in Step C, Redis in
    // Step D); a readyz that checks nothing would be misleading.
    let app = ingress::router().route("/healthz", get(|| async { StatusCode::OK }));

    let port = env("PORT", "8080");
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .unwrap();
    tracing::info!(port = %port, "edge-gateway listening");
    axum::serve(listener, app).await.unwrap();
}
