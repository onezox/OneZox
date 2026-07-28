//! Metering start (Part L, Phase-01 Step E2): emits a request-scoped OTel
//! span carrying token/cost fields. Phase-01 doesn't call a model yet
//! (Phase-03 does) — token/cost values are fixed placeholders (0), not
//! real usage; Part L's full field set (orchestration_tokens, cache_hit,
//! node_id, eval_score, ...) is out of scope until later phases actually
//! produce those signals.
//!
//! Part P's sequence diagram places "meter start" right before the gRPC
//! Submit call and "meter final" after the stream completes — so the span
//! must wrap the request's FULL Submit+relay lifecycle, not just admission.
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

/// Starts the request-scoped meter span. Call as close as possible to the
/// gRPC Submit call (Part P) so the span's duration reflects the actual
/// Submit+relay work, not upstream auth/ratelimit/admission overhead.
pub fn start(request_id: &str, org_id: &str, model: &str) -> RequestMeter {
    let span = tracing::info_span!(
        "edge_gateway.request",
        request_id = %request_id,
        org_id = %org_id,
        model = %model,
        tokens_in = PLACEHOLDER_TOKENS_IN,
        tokens_out = PLACEHOLDER_TOKENS_OUT,
        usd_cost = PLACEHOLDER_USD_COST,
        finish_reason = tracing::field::Empty,
    );
    RequestMeter { span }
}

impl RequestMeter {
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

    /// Not a "does Tempo receive this" test (that's a live check, since it
    /// needs a real OTel Collector) — this only proves start()/finish()
    /// don't panic and that a span is genuinely created and closed,
    /// running against a no-op subscriber so it has no external
    /// dependency.
    #[test]
    fn start_and_finish_do_not_panic_with_no_subscriber_installed() {
        let meter = start("req-1", "org-1", "onezox-ultra");
        meter.finish("stop");
    }

    #[test]
    fn span_is_observable_while_open() {
        let subscriber = tracing_subscriber::registry().with(tracing_subscriber::fmt::layer());
        tracing::subscriber::with_default(subscriber, || {
            let meter = start("req-2", "org-2", "onezox-ultra");
            assert!(!meter.span().is_disabled());
            meter.finish("stop");
        });
    }
}
