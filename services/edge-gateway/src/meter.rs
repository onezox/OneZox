//! Metering (Part L, Phase-01 Steps E2/J): token/cost fields on the
//! request-scoped span. Phase-01 doesn't call a model yet (Phase-03
//! does) — token/cost values are fixed placeholders (0), not real usage;
//! Part L's full field set (orchestration_tokens, cache_hit, node_id,
//! eval_score, ...) is out of scope until later phases actually produce
//! those signals.
//!
//! Step J correction: the request span itself is now created early, in
//! pipeline.rs's `Admitted` extractor (before auth even runs), not here —
//! EC4's "all edge telemetry visible" requires every pipeline stage
//! (auth, ratelimit, admission, normalize, meter, submit, relay) to
//! produce a span correctly parented under ONE request-scoped root; that's
//! only possible if the root exists before the earliest stage runs. Part
//! P's "meter start" (right before the gRPC Submit call) now means
//! "the token/cost fields are attached to the already-open root span
//! here", not "the span itself is created here". `RequestMeter` wraps
//! that already-created span.
//!
//! `RequestMeter` is deliberately NOT closed on Drop (unlike
//! admission::AdmissionGuard): closing needs the real final token/cost
//! values in hand, which only Step E4's relay loop has once the stream
//! actually ends. A self-closing-on-drop design would have to guess those
//! values (they're not known until the caller decides the request is
//! done) and would silently emit stale placeholders if a caller forgot to
//! call `finish` explicitly — an explicit, visible call is the honest
//! alternative.

use tracing::Span;

pub const PLACEHOLDER_TOKENS_IN: i64 = 0;
pub const PLACEHOLDER_TOKENS_OUT: i64 = 0;
pub const PLACEHOLDER_USD_COST: f64 = 0.0;

pub struct RequestMeter {
    span: Span,
}

impl RequestMeter {
    /// Wraps an already-created request span as the meter. The span is
    /// expected to have already declared `tokens_in`/`tokens_out`/
    /// `usd_cost`/`finish_reason` fields (pipeline.rs's root span does,
    /// at creation, via `tracing::field::Empty`/the placeholder
    /// constants above) — `finish` below only records final values into
    /// fields that already exist.
    pub fn new(span: Span) -> Self {
        Self { span }
    }

    /// A handle to enter/instrument the Submit+relay work with this span
    /// (Step E3/E4) — `RequestMeter` itself isn't a `Future`, so callers
    /// use `tracing::Instrument` against this directly:
    /// `some_future.instrument(meter.span().clone())`.
    pub fn span(&self) -> &Span {
        &self.span
    }

    /// Records the final (still placeholder, Phase-01) token/cost fields
    /// and the outcome, then closes the span. Phase-01.txt: "On
    /// completion: edge emits final meter span (tokens in/out placeholders
    /// until real model in Phase-03)".
    pub fn finish(self, finish_reason: &str) {
        self.span.record("tokens_in", PLACEHOLDER_TOKENS_IN);
        self.span.record("tokens_out", PLACEHOLDER_TOKENS_OUT);
        self.span.record("usd_cost", PLACEHOLDER_USD_COST);
        self.span.record("finish_reason", finish_reason);
        // The span exports (to the OTel Collector -> Tempo) when `self.span`
        // drops here, closing it.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::layer::SubscriberExt;

    fn test_span() -> Span {
        tracing::info_span!(
            "edge_gateway.request",
            request_id = "req-1",
            org_id = "org-1",
            model = "onezox-ultra",
            tokens_in = PLACEHOLDER_TOKENS_IN,
            tokens_out = PLACEHOLDER_TOKENS_OUT,
            usd_cost = PLACEHOLDER_USD_COST,
            finish_reason = tracing::field::Empty,
        )
    }

    /// Not a "does Tempo receive this" test (that's a live check, since it
    /// needs a real OTel Collector) — this only proves finish() doesn't
    /// panic and that a span is genuinely closed, running against a no-op
    /// subscriber so it has no external dependency.
    #[test]
    fn finish_does_not_panic_with_no_subscriber_installed() {
        let meter = RequestMeter::new(test_span());
        meter.finish("stop");
    }

    #[test]
    fn span_is_observable_while_open() {
        let subscriber = tracing_subscriber::registry().with(tracing_subscriber::fmt::layer());
        tracing::subscriber::with_default(subscriber, || {
            let meter = RequestMeter::new(test_span());
            assert!(!meter.span().is_disabled());
            meter.finish("stop");
        });
    }
}
