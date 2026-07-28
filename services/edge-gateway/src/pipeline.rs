//! Composes the auth -> ratelimit -> admission pipeline (Phase-01.txt) into
//! a single axum extractor, so each stage's own module (auth, ratelimit,
//! admission) stays independently cohesive and unit-tested in isolation,
//! while a real request still flows through all of them, in order, before
//! reaching a handler.
//!
//! Step J: this is also where the request-scoped root span
//! (`edge_gateway.request`) is created — as early as possible, before auth
//! even runs, since EC4 requires auth/ratelimit/admission themselves (not
//! just normalize/meter/submit/relay, which come later in
//! ingress.rs/stream.rs) to appear as child spans under one root. Each
//! stage below gets its own child span via explicit `parent: &root_span`
//! syntax rather than relying on tracing's implicit "current span"
//! tracking — the same implicit-context approach silently broke span
//! parenting once already in this codebase (Phase-00's edge-stub, and
//! again in Step E5's `RequestMeter::span()`), so stages here are entered
//! explicitly via `.instrument()` instead.

use axum::Json;
use axum::extract::FromRequestParts;
use axum::http::{StatusCode, request::Parts};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use tracing::Instrument;
use uuid::Uuid;

use crate::admission::{self, AdmissionError, AdmissionGuard};
use crate::auth::Identity;
use crate::ratelimit::{self, RateLimitError};
use crate::state::AppState;

#[derive(Serialize)]
struct ErrorBody {
    error: ErrorDetail,
}

#[derive(Serialize)]
struct ErrorDetail {
    message: &'static str,
    #[serde(rename = "type")]
    kind: &'static str,
}

fn rate_limited() -> Response {
    (
        StatusCode::TOO_MANY_REQUESTS,
        Json(ErrorBody {
            error: ErrorDetail { message: "rate limit exceeded", kind: "rate_limited" },
        }),
    )
        .into_response()
}

fn shed() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(ErrorBody {
            error: ErrorDetail { message: "cell at capacity, request shed", kind: "shed" },
        }),
    )
        .into_response()
}

/// The request-scoped identity carried alongside the root span: `request_id`
/// is generated here (not in normalize.rs, per Step J) since the root span
/// needs it at creation, before auth has even resolved an `Identity`.
pub struct RequestContext {
    pub request_id: String,
    pub root_span: tracing::Span,
}

/// Admits a request: resolves identity (auth), enforces the org's rate
/// limit, then applies admission control. Use exactly like `Identity` as a
/// handler parameter — this IS the identity, just also guaranteed to have
/// passed rate limiting and admission. Holds the `AdmissionGuard` for as
/// long as the handler (and the whole streamed response) is alive; it
/// decrements the in-flight gauge on drop. Also carries the `RequestContext`
/// (request_id + root span) so downstream stages (normalize, meter, submit,
/// relay) can keep parenting their own child spans under the same root.
pub struct Admitted(pub Identity, pub AdmissionGuard, pub RequestContext);

impl FromRequestParts<AppState> for Admitted {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let request_id = Uuid::new_v4().to_string();
        let root_span = tracing::info_span!(
            "edge_gateway.request",
            request_id = %request_id,
            org_id = tracing::field::Empty,
            model = tracing::field::Empty,
            tokens_in = crate::meter::PLACEHOLDER_TOKENS_IN,
            tokens_out = crate::meter::PLACEHOLDER_TOKENS_OUT,
            usd_cost = crate::meter::PLACEHOLDER_USD_COST,
            finish_reason = tracing::field::Empty,
        );

        let auth_span = tracing::info_span!(parent: &root_span, "edge_gateway.auth");
        let identity = Identity::from_request_parts(parts, state)
            .instrument(auth_span)
            .await
            .map_err(IntoResponse::into_response)?;
        root_span.record("org_id", identity.org_id.to_string().as_str());

        let ratelimit_span = tracing::info_span!(parent: &root_span, "edge_gateway.ratelimit");
        let rl_result = async {
            let policy = state.rate_limit_policy_store.policy_for(identity.org_id).await;
            ratelimit::enforce(state.rate_limit_counter.as_ref(), identity.org_id, &policy).await
        }
        .instrument(ratelimit_span)
        .await;
        match rl_result {
            Ok(()) => {}
            Err(RateLimitError::Exceeded) => return Err(rate_limited()),
            // enforce() already fails open on a store error — unreachable
            // in practice, kept only so the match is exhaustive.
            Err(RateLimitError::Store(_)) => {}
        }

        let admission_span = tracing::info_span!(parent: &root_span, "edge_gateway.admission");
        // admit() fails open on a gauge-store error (see its doc comment) —
        // the only Err it actually returns is a genuine Shed decision.
        let guard = async {
            admission::admit(
                state.admission_gauge.clone(),
                state.admission_soft_limit,
                state.admission_hard_limit,
            )
            .await
        }
        .instrument(admission_span)
        .await
        .map_err(|_: AdmissionError| shed())?;

        Ok(Admitted(identity, guard, RequestContext { request_id, root_span }))
    }
}
