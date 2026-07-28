//! Composes the auth -> ratelimit -> admission pipeline (Phase-01.txt) into
//! a single axum extractor, so each stage's own module (auth, ratelimit,
//! admission) stays independently cohesive and unit-tested in isolation,
//! while a real request still flows through all of them, in order, before
//! reaching a handler.

use axum::Json;
use axum::extract::FromRequestParts;
use axum::http::{StatusCode, request::Parts};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

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

/// Admits a request: resolves identity (auth), enforces the org's rate
/// limit, then applies admission control. Use exactly like `Identity` as a
/// handler parameter — this IS the identity, just also guaranteed to have
/// passed rate limiting and admission. Holds the `AdmissionGuard` for as
/// long as the handler (and, once Step E's SSE relay exists, the whole
/// streamed response) is alive; it decrements the in-flight gauge on drop.
pub struct Admitted(pub Identity, pub AdmissionGuard);

impl FromRequestParts<AppState> for Admitted {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let identity = Identity::from_request_parts(parts, state)
            .await
            .map_err(IntoResponse::into_response)?;

        let policy = state.rate_limit_policy_store.policy_for(identity.org_id).await;
        match ratelimit::enforce(state.rate_limit_counter.as_ref(), identity.org_id, &policy).await
        {
            Ok(()) => {}
            Err(RateLimitError::Exceeded) => return Err(rate_limited()),
            // enforce() already fails open on a store error — unreachable
            // in practice, kept only so the match is exhaustive.
            Err(RateLimitError::Store(_)) => {}
        }

        // admit() fails open on a gauge-store error (see its doc comment) —
        // the only Err it actually returns is a genuine Shed decision.
        let guard = admission::admit(
            state.admission_gauge.clone(),
            state.admission_soft_limit,
            state.admission_hard_limit,
        )
        .await
        .map_err(|_: AdmissionError| shed())?;

        Ok(Admitted(identity, guard))
    }
}
