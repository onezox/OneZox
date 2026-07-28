//! Composes the auth -> ratelimit (-> admission, Step D2) pipeline
//! (Phase-01.txt) into a single axum extractor, so each stage's own module
//! (auth, ratelimit, ...) stays independently cohesive and unit-tested in
//! isolation, while a real request still flows through all of them, in
//! order, before reaching a handler.

use axum::Json;
use axum::extract::FromRequestParts;
use axum::http::{StatusCode, request::Parts};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

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

/// Admits a request: resolves identity (auth), then enforces the org's
/// rate limit. Use exactly like `Identity` as a handler parameter — this
/// IS the identity, just also guaranteed to have passed rate limiting.
pub struct Admitted(pub Identity);

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
            Ok(()) => Ok(Admitted(identity)),
            Err(RateLimitError::Exceeded) => Err(rate_limited()),
            // enforce() already fails open on a store error — this arm is
            // unreachable in practice, kept only so the match is exhaustive.
            Err(RateLimitError::Store(_)) => Ok(Admitted(identity)),
        }
    }
}
