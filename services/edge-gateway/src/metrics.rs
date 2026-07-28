//! Operational Prometheus metrics (Part L, Phase-01 Step J2): request
//! count, latency histogram, and the in-flight gauge — distinct from
//! meter.rs's per-request OTel span fields (tokens/cost/finish_reason),
//! which stay Phase-01 placeholders. These are real, correctly-computed
//! values from the moment they're recorded; nothing here is a placeholder.
//!
//! Exposed the same way the Phase-00 Go stubs already do (provider-stub's
//! `promauto`/`promhttp` pattern) — a `/metrics` text endpoint Prometheus
//! scrapes directly (platform/operators/prometheus/prometheus-values.yaml's
//! `onezox-stubs` job), not routed through the OTel Collector.
//!
//! `edge_gateway_inflight_requests` deliberately reuses the exact same
//! lifecycle as admission::AdmissionGuard's Redis-backed gauge (increment
//! on admit, decrement on Drop) rather than inventing a second, independent
//! in-flight tracking mechanism — Steps H3 and I already verified that
//! lifecycle correctly bounds and drains under disconnect and sustained
//! load; a second, separately-implemented counter could silently drift
//! from it and nobody would notice until the two disagreed.

use std::time::Instant;

use axum::extract::{MatchedPath, Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

use crate::state::AppState;

pub const REQUESTS_TOTAL: &str = "edge_gateway_requests_total";
pub const REQUEST_DURATION_SECONDS: &str = "edge_gateway_request_duration_seconds";
pub const INFLIGHT_REQUESTS: &str = "edge_gateway_inflight_requests";

/// Installs the global metrics recorder. Must run once, before any
/// `metrics::counter!`/`histogram!`/`gauge!` call anywhere else in the
/// process (main.rs calls this before building the router). Returns the
/// handle the `/metrics` route renders on each scrape.
///
/// Explicit bucket boundaries (seconds), not the crate's default: without
/// `set_buckets`, metrics-exporter-prometheus renders histogram! calls as
/// Prometheus *summaries* (client-side sliding-window quantiles) instead
/// of true histograms — a different metric type with different semantics
/// (no server-side aggregation across instances). Phase-01.txt/EC4 asks
/// for a latency histogram specifically, so this sets real bucket
/// boundaries to get the real thing.
pub fn install() -> PrometheusHandle {
    PrometheusBuilder::new()
        .set_buckets(&[
            0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        ])
        .expect("bucket list is non-empty")
        .install_recorder()
        .expect("failed to install the Prometheus metrics recorder")
}

/// Records `edge_gateway_requests_total` and
/// `edge_gateway_request_duration_seconds` for every request, labeled by
/// method/route/status. Uses `MatchedPath` (the route pattern, e.g.
/// "/v1/chat/completions"), not the literal request path, so metrics
/// stay low-cardinality even once routes ever carry path params. Applied
/// as `route_layer` in main.rs so it only wraps matched routes and
/// `MatchedPath` is guaranteed to be present — 404s never reach it and
/// so aren't counted, same as how Phase-00's Go stubs' promhttp default
/// metrics work.
///
/// Measures wall time until the handler returns a `Response`, not until a
/// streamed SSE body fully drains — for `/v1/chat/completions` this is
/// "time to first byte of the stream", not total stream duration. That's
/// the standard meaning of an HTTP server's request-duration metric and
/// is a different, complementary signal from `INFLIGHT_REQUESTS` (which
/// does stay elevated for the SSE stream's full lifetime, mirroring
/// admission::AdmissionGuard).
pub async fn track(
    State(_state): State<AppState>,
    matched_path: MatchedPath,
    req: Request,
    next: Next,
) -> Response {
    let method = req.method().to_string();
    let route = matched_path.as_str().to_string();
    let start = Instant::now();

    let response = next.run(req).await;

    let status = response.status().as_u16().to_string();
    let elapsed = start.elapsed().as_secs_f64();

    metrics::counter!(
        REQUESTS_TOTAL,
        "method" => method.clone(),
        "route" => route.clone(),
        "status" => status,
    )
    .increment(1);

    metrics::histogram!(
        REQUEST_DURATION_SECONDS,
        "method" => method,
        "route" => route,
    )
    .record(elapsed);

    response.into_response()
}
