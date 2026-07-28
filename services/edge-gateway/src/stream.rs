//! SSE streaming relay with backpressure (Part K, Phase-01 Step E4):
//! converts the gRPC SubmitResponse stream from dataplane-stub into
//! Server-Sent Events for the client.
//!
//! Split into two layers:
//! - `to_chunks`: pure transformation, generic over any
//!   `Stream<Item = Result<SubmitResponse, Status>>` rather than tied to a
//!   real `tonic::Streaming` — hermetically unit-testable with a fake
//!   in-memory stream, same rationale as auth/ratelimit/admission's
//!   trait-backed stores.
//! - `relay`: the thin axum-specific wrapper (SSE JSON serialization, plus
//!   holding the `RequestMeter` and `AdmissionGuard` for the stream's full
//!   lifetime) — low-risk glue, verified live in Step E5 rather than unit
//!   tested.
//!
//! Backpressure is inherent to axum's `Sse` response: the underlying
//! `Stream` is only polled for its next item when the client's connection
//! can accept more bytes (hyper's flow control on the write side) — this
//! relay does no manual buffering, so a slow client naturally throttles how
//! fast dataplane-stub is asked for the next chunk.
//!
//! "Disconnect: client drop mid-stream frees resources cleanly"
//! (Phase-01.txt testing requirement) needs no explicit handler here:
//! dropping the SSE response's stream (which axum/hyper does automatically
//! when the client's connection closes) drops everything the stream owns —
//! the gRPC `Streaming` handle (closing the call to dataplane-stub) and,
//! once `relay` holds them for the stream's duration, the `AdmissionGuard`
//! (decrementing the in-flight gauge) too. Rust's ownership model gives
//! this for free; there is deliberately no disconnect callback to write.

use std::convert::Infallible;

use axum::response::sse::{Event, Sse};
use futures_util::{Stream, StreamExt};
use tonic::Status;
use tracing::Instrument;

use crate::admission::AdmissionGuard;
use crate::ingress::{ChatCompletionChunk, ChatCompletionChunkChoice, ChatCompletionChunkDelta};
use crate::meter::RequestMeter;
use crate::pb::dataplane::v1::SubmitResponse;

/// Transforms upstream deltas into OpenAI-compatible chunks. Stops at the
/// first `is_final` chunk, the upstream ending (`None`), or an upstream
/// error — whichever comes first. On an upstream error, synthesizes one
/// final chunk with `finish_reason: "error"` so the caller always has a
/// uniform way to observe how the stream ended, without a separate
/// side-channel value; on a well-behaved stream, the real `is_final` delta
/// already carries `finish_reason` (dataplane-stub always sends "stop").
pub fn to_chunks<S>(
    upstream: S,
    request_id: String,
    model: String,
) -> impl Stream<Item = ChatCompletionChunk>
where
    S: Stream<Item = Result<SubmitResponse, Status>> + Send + 'static,
{
    async_stream::stream! {
        futures_util::pin_mut!(upstream);
        loop {
            match upstream.next().await {
                Some(Ok(delta)) => {
                    let is_final = delta.is_final;
                    let chunk = ChatCompletionChunk {
                        id: request_id.clone(),
                        model: model.clone(),
                        choices: vec![ChatCompletionChunkChoice {
                            index: 0,
                            delta: ChatCompletionChunkDelta { role: None, content: delta.content },
                            finish_reason: delta.finish_reason,
                        }],
                    };
                    yield chunk;
                    if is_final {
                        break;
                    }
                }
                Some(Err(status)) => {
                    tracing::warn!(
                        error = %status,
                        request_id = %request_id,
                        "upstream gRPC stream error"
                    );
                    yield ChatCompletionChunk {
                        id: request_id.clone(),
                        model: model.clone(),
                        choices: vec![ChatCompletionChunkChoice {
                            index: 0,
                            delta: ChatCompletionChunkDelta { role: None, content: None },
                            finish_reason: Some("error".to_string()),
                        }],
                    };
                    break;
                }
                None => break,
            }
        }
    }
}

/// Wraps `to_chunks` as an axum SSE response, serializing each chunk to a
/// `data:` event and finishing the meter span with whatever
/// `finish_reason` the last chunk carried ("unknown" if the upstream
/// stream ended without ever sending one — an ill-behaved upstream, not
/// expected from dataplane-stub, but a valid state to have a fallback for
/// rather than panicking). Holds `meter` and `admission_guard` until the
/// stream itself ends or is dropped, so `AdmissionGuard`'s in-flight gauge
/// stays accurate for the request's entire streamed duration, not just its
/// initial admission (see admission::AdmissionGuard's doc comment).
///
/// Each `.next()` call is individually `.instrument()`-ed with a dedicated
/// `edge_gateway.relay` span (Step J — parented under the meter's root
/// span, not the root span itself, so relay shows up as its own stage in
/// the trace tree rather than folding into the request span) rather than
/// entered with a manual guard once: `tracing`'s `Instrument` trait only
/// covers `Future`, not `Stream` (there's no blanket "instrument every
/// poll" wrapper for a whole stream), and a manual `span.enter()` guard
/// held across the loop's `.await` points would be exactly the async-span
/// pitfall documented in main.rs's `init_telemetry` and this module's own
/// doc comment. Instrumenting the `Future` returned by each individual
/// `.next()` call gets the same result — the span is entered/exited around
/// each poll, never held across an await — at the granularity
/// `tracing::Instrument` actually supports.
pub fn relay<S>(
    request_id: String,
    model: String,
    upstream: S,
    meter: RequestMeter,
    admission_guard: AdmissionGuard,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>>
where
    S: Stream<Item = Result<SubmitResponse, Status>> + Send + 'static,
{
    let span = tracing::info_span!(parent: meter.span(), "edge_gateway.relay");
    let log_request_id = request_id.clone();
    let chunks = to_chunks(upstream, request_id, model);

    let sse_stream = async_stream::stream! {
        futures_util::pin_mut!(chunks);
        let mut last_finish_reason = "unknown".to_string();
        while let Some(chunk) = chunks.next().instrument(span.clone()).await {
            if let Some(reason) = &chunk.choices[0].finish_reason {
                last_finish_reason = reason.clone();
            }
            let json = serde_json::to_string(&chunk).unwrap_or_default();
            yield Ok(Event::default().data(json));
        }
        // EC4/Step J3: a structured log line for the request, not just a
        // span — Tempo (traces) and Prometheus (metrics) both already
        // observe every request; Loki previously didn't, since nothing in
        // the happy path called tracing::info!/warn! at all (only error
        // branches did). Explicit fields here, not relying on the fmt
        // JSON formatter's span-field embedding, so this line is
        // self-contained regardless of formatter config.
        tracing::info!(
            request_id = %log_request_id,
            finish_reason = %last_finish_reason,
            "edge_gateway request completed"
        );
        meter.finish(&last_finish_reason);
        drop(admission_guard);
    };

    Sse::new(sse_stream)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;

    /// Builds a `RequestMeter` wrapping a standalone root span, matching
    /// the shape pipeline.rs's `Admitted` extractor produces in the real
    /// request path (see meter.rs's own `test_span` helper) — these tests
    /// exercise `relay()` directly, without going through the extractor.
    fn test_meter() -> RequestMeter {
        let span = tracing::info_span!(
            "edge_gateway.request",
            request_id = "req-1",
            org_id = "org-1",
            model = "onezox-ultra",
            tokens_in = crate::meter::PLACEHOLDER_TOKENS_IN,
            tokens_out = crate::meter::PLACEHOLDER_TOKENS_OUT,
            usd_cost = crate::meter::PLACEHOLDER_USD_COST,
            finish_reason = tracing::field::Empty,
        );
        RequestMeter::new(span)
    }

    fn delta(content: &str, is_final: bool, finish_reason: Option<&str>) -> SubmitResponse {
        SubmitResponse {
            request_id: "req-1".to_string(),
            content: Some(content.to_string()),
            finish_reason: finish_reason.map(|s| s.to_string()),
            is_final,
        }
    }

    #[tokio::test]
    async fn a_well_behaved_stream_yields_every_chunk_ending_with_finish_reason() {
        let upstream = stream::iter(vec![
            Ok(delta("Hello ", false, None)),
            Ok(delta("world", false, None)),
            Ok(delta("", true, Some("stop"))),
        ]);

        let chunks: Vec<_> =
            to_chunks(upstream, "req-1".to_string(), "onezox-ultra".to_string())
                .collect()
                .await;

        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].choices[0].delta.content, Some("Hello ".to_string()));
        assert_eq!(chunks[1].choices[0].delta.content, Some("world".to_string()));
        assert_eq!(chunks[2].choices[0].finish_reason, Some("stop".to_string()));
        assert!(chunks.iter().all(|c| c.id == "req-1" && c.model == "onezox-ultra"));
    }

    #[tokio::test]
    async fn stops_at_the_first_is_final_chunk_even_if_more_items_would_follow() {
        let upstream = stream::iter(vec![
            Ok(delta("a", false, None)),
            Ok(delta("b", true, Some("stop"))),
            Ok(delta("c (should never be reached)", false, None)),
        ]);

        let chunks: Vec<_> =
            to_chunks(upstream, "req-1".to_string(), "m".to_string()).collect().await;

        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[1].choices[0].delta.content, Some("b".to_string()));
    }

    #[tokio::test]
    async fn an_upstream_error_mid_stream_yields_a_synthetic_error_chunk() {
        let upstream = stream::iter(vec![
            Ok(delta("partial", false, None)),
            Err(Status::unavailable("dataplane-stub connection lost")),
        ]);

        let chunks: Vec<_> =
            to_chunks(upstream, "req-1".to_string(), "m".to_string()).collect().await;

        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].choices[0].delta.content, Some("partial".to_string()));
        assert_eq!(chunks[1].choices[0].finish_reason, Some("error".to_string()));
        assert_eq!(chunks[1].choices[0].delta.content, None);
    }

    #[tokio::test]
    async fn an_upstream_that_ends_without_a_final_delta_just_stops() {
        // Ill-behaved upstream (never sends is_final) — to_chunks doesn't
        // synthesize a finish_reason in this case (only on a real Err);
        // relay()'s "unknown" fallback (not unit-tested here, thin glue) is
        // what covers this specific case for the actual SSE output.
        let upstream = stream::iter(vec![Ok(delta("only chunk", false, None))]);

        let chunks: Vec<_> =
            to_chunks(upstream, "req-1".to_string(), "m".to_string()).collect().await;

        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].choices[0].finish_reason, None);
    }

    #[tokio::test]
    async fn an_empty_upstream_yields_nothing() {
        let upstream = stream::iter(Vec::<Result<SubmitResponse, Status>>::new());
        let chunks: Vec<_> =
            to_chunks(upstream, "req-1".to_string(), "m".to_string()).collect().await;
        assert!(chunks.is_empty());
    }

    #[tokio::test]
    async fn relay_serializes_chunks_and_holds_the_guard_until_the_stream_fully_drains() {
        // Exercises `relay()` itself, not just `to_chunks`: SSE JSON
        // serialization and the meter/admission-guard lifecycle.
        // axum::response::sse::Event has no public data getter, so this
        // drains the actual HTTP response body rather than inspecting
        // typed Event values.
        use crate::admission::{self, FakeAdmissionGauge};
        use axum::response::IntoResponse;
        use http_body_util::BodyExt;
        use std::sync::Arc;

        let gauge = Arc::new(FakeAdmissionGauge::new());
        let admission_guard = admission::admit(gauge.clone(), 10, 20).await.unwrap();
        assert_eq!(gauge.current(), 1);

        let meter = test_meter();

        let upstream = stream::iter(vec![
            Ok(delta("hi ", false, None)),
            Ok(delta("", true, Some("stop"))),
        ]);

        let sse = relay(
            "req-1".to_string(),
            "onezox-ultra".to_string(),
            upstream,
            meter,
            admission_guard,
        );
        let response = sse.into_response();
        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let body_text = String::from_utf8(body_bytes.to_vec()).unwrap();

        assert!(body_text.contains(r#""content":"hi "#));
        assert!(body_text.contains(r#""finish_reason":"stop""#));

        // The guard drops only once the stream (and thus the response
        // body) fully drains — Drop spawns the decrement rather than
        // awaiting it inline, so yield to let that task actually run.
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(gauge.current(), 0);
    }

    #[tokio::test]
    async fn client_disconnect_mid_stream_still_frees_resources() {
        // Phase-01.txt testing requirement: "Disconnect: client drop
        // mid-stream frees resources cleanly." Simulates dataplane-stub
        // still mid-stream (upstream never ends) when the client goes
        // away: reads exactly one chunk, then drops the response body
        // without draining it — exactly what axum/hyper does when the
        // client's connection closes early. Nothing here calls any
        // explicit cleanup function; Drop is the entire mechanism under
        // test (see this module's doc comment).
        use crate::admission::{self, FakeAdmissionGauge};
        use axum::response::IntoResponse;
        use http_body_util::BodyExt;
        use std::sync::Arc;

        let gauge = Arc::new(FakeAdmissionGauge::new());
        let admission_guard = admission::admit(gauge.clone(), 10, 20).await.unwrap();
        assert_eq!(gauge.current(), 1);

        let meter = test_meter();

        let upstream = stream::iter(vec![Ok(delta("first", false, None))])
            .chain(stream::pending::<Result<SubmitResponse, Status>>());

        let sse = relay(
            "req-1".to_string(),
            "onezox-ultra".to_string(),
            upstream,
            meter,
            admission_guard,
        );
        let mut body = sse.into_response().into_body();

        let _first_frame = body.frame().await;
        drop(body);

        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(gauge.current(), 0);
    }
}
