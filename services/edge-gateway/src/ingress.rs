//! OpenAI-compatible ingress surface (Part K): routes exist, bodies are
//! structurally validated via serde's `Deserialize` — axum's `Json`
//! extractor rejects malformed/missing-field bodies with 422
//! automatically. chat.completions and responses (Step E5) are fully
//! wired: auth -> ratelimit -> admission -> normalize -> meter -> Submit
//! -> SSE relay. /v1/embeddings stays a 501 placeholder deliberately —
//! Phase-01.txt's APIS CREATED section is explicit that it's "wired to
//! real embed in Phase-10", the only one of the four routes with that
//! caveat.
//!
//! Hand-written serde structs here, not the prost-generated proto/gateway
//! types: protobuf's canonical JSON mapping renders field names in
//! lowerCamelCase by default, which would break OpenAI wire compatibility
//! (the real API uses snake_case, e.g. "max_tokens"). These structs mirror
//! proto/gateway/v1/gateway.proto's field names/types by convention; that
//! proto file remains the cross-language schema of record. The generated
//! Rust types from it are used downstream instead, in the gRPC call to the
//! data plane, where binary wire format applies and this concern doesn't
//! exist.

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use tracing::Instrument;

use crate::auth::Identity;
use crate::pipeline::Admitted;
use crate::state::AppState;
use crate::{dataplane_client, meter, normalize, stream};

#[derive(Debug, Deserialize, Serialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Deserialize)]
pub struct ChatCompletionRequest {
    pub model: String,
    pub messages: Vec<ChatMessage>,
    #[serde(default)]
    pub stream: bool,
    pub max_tokens: Option<i32>,
    pub temperature: Option<f32>,
}

#[derive(Debug, Deserialize)]
pub struct ResponsesRequest {
    pub model: String,
    pub input: String,
    #[serde(default)]
    pub stream: bool,
}

#[derive(Debug, Deserialize)]
pub struct EmbeddingsRequest {
    pub model: String,
    pub input: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct Model {
    pub id: String,
    pub owned_by: String,
}

#[derive(Debug, Serialize)]
pub struct ModelsListResponse {
    pub data: Vec<Model>,
}

/// SSE chunk shape (Step E4): mirrors proto/gateway/v1/gateway.proto's
/// ChatCompletionChunk field names by the same hand-written-not-generated
/// convention as the request structs above (see module doc).
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ChatCompletionChunkDelta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ChatCompletionChunkChoice {
    pub index: i32,
    pub delta: ChatCompletionChunkDelta,
    pub finish_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ChatCompletionChunk {
    pub id: String,
    pub model: String,
    pub choices: Vec<ChatCompletionChunkChoice>,
}

#[derive(Debug, Serialize)]
struct NotWiredError {
    error: NotWiredErrorDetail,
}

#[derive(Debug, Serialize)]
struct NotWiredErrorDetail {
    message: String,
    #[serde(rename = "type")]
    kind: String,
}

fn not_wired(stage: String) -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(NotWiredError {
            error: NotWiredErrorDetail {
                message: format!(
                    "{stage} accepted a structurally valid request but downstream is not wired yet (Phase-10)"
                ),
                kind: "not_implemented".to_string(),
            },
        }),
    )
        .into_response()
}

fn bad_gateway(request_id: &str) -> Response {
    (
        StatusCode::BAD_GATEWAY,
        Json(NotWiredError {
            error: NotWiredErrorDetail {
                message: format!("request {request_id}: downstream (dataplane-stub) unavailable"),
                kind: "bad_gateway".to_string(),
            },
        }),
    )
        .into_response()
}

/// Shared tail of the auth -> ratelimit -> admission -> normalize pipeline
/// (Phase-01.txt) for chat.completions and responses: starts metering,
/// calls dataplane-stub's Submit, and either relays the stream (Step E4)
/// or — if Submit itself fails, e.g. dataplane-stub unreachable — finishes
/// the meter span and drops the admission guard explicitly (relay()'s
/// Drop-based cleanup never starts in this branch, since relay() is never
/// called), then returns 502.
async fn submit_and_relay(
    state: &AppState,
    identity: &Identity,
    admission_guard: crate::admission::AdmissionGuard,
    submit_req: crate::pb::dataplane::v1::SubmitRequest,
) -> Response {
    let request_id = submit_req.request_id.clone();
    let model = submit_req.model.clone();
    let meter = meter::start(&request_id, &identity.org_id.to_string(), &model);
    let span = meter.span().clone();

    // .instrument(), not a manual enter() guard: this await crosses the
    // whole RPC round-trip — see stream::relay's doc comment for why that
    // distinction matters for async spans.
    let submit_result = dataplane_client::submit(state.dataplane_channel.clone(), submit_req)
        .instrument(span)
        .await;

    match submit_result {
        Ok(upstream) => stream::relay(request_id, model, upstream, meter, admission_guard).into_response(),
        Err(status) => {
            tracing::warn!(
                error = %status,
                request_id = %request_id,
                "dataplane-stub Submit call failed"
            );
            meter.finish("error");
            drop(admission_guard);
            bad_gateway(&request_id)
        }
    }
}

// Every handler takes `Admitted(identity, guard)` (even where identity
// isn't used, e.g. models): this is the enforcement point for "no request
// proceeds without a resolved org_id", rate limiting, and admission
// (Phase-01.txt, Security Implementation + FEATURES). `Admitted` must be
// extracted before any body-consuming extractor (axum requires the last
// argument to be the one that consumes the request body).

async fn chat_completions(
    State(state): State<AppState>,
    Admitted(identity, admission_guard): Admitted,
    Json(req): Json<ChatCompletionRequest>,
) -> Response {
    let submit_req = normalize::normalize_chat_completions(&identity, &req);
    submit_and_relay(&state, &identity, admission_guard, submit_req).await
}

async fn responses(
    State(state): State<AppState>,
    Admitted(identity, admission_guard): Admitted,
    Json(req): Json<ResponsesRequest>,
) -> Response {
    let submit_req = normalize::normalize_responses(&identity, &req);
    submit_and_relay(&state, &identity, admission_guard, submit_req).await
}

async fn embeddings(
    Admitted(identity, _guard): Admitted,
    Json(req): Json<EmbeddingsRequest>,
) -> impl IntoResponse {
    not_wired(format!(
        "embeddings org_id={} model={}",
        identity.org_id, req.model
    ))
}

async fn models(Admitted(_identity, _guard): Admitted) -> impl IntoResponse {
    // Phase-01.txt: "served from a static list until Phase-04 registry".
    Json(ModelsListResponse {
        data: vec![Model {
            id: "onezox-ultra".to_string(),
            owned_by: "onezox".to_string(),
        }],
    })
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/chat/completions", post(chat_completions))
        .route("/v1/responses", post(responses))
        .route("/v1/embeddings", post(embeddings))
        .route("/v1/models", get(models))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admission::{AdmissionGauge, FakeAdmissionGauge};
    use crate::auth::FakeApiKeyStore;
    use crate::ratelimit::{FakeRateLimitCounter, FixedRateLimitPolicyStore, RateLimitPolicy};
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use std::sync::Arc;
    use tower::ServiceExt;
    use uuid::Uuid;

    const TEST_KEY: &str = "oz_test_ingress_key";
    // High enough that no ingress test trips it incidentally; ratelimit's
    // and admission's own denial behavior is unit-tested directly in their
    // own modules.
    const GENEROUS_RPM: u32 = 1000;
    const GENEROUS_ADMISSION_LIMIT: u64 = 1000;

    /// A router wired to fake, in-memory stores (auth + ratelimit +
    /// admission) with one pre-seeded valid key — hermetic, no live
    /// CockroachDB/Redis needed for these route-level tests (the real
    /// store/counter implementations are verified separately against the
    /// live cluster).
    fn test_app() -> (Router, Uuid) {
        let org_id = Uuid::new_v4();
        let store = FakeApiKeyStore::new();
        store.insert(TEST_KEY, org_id, None);
        let state = AppState {
            api_key_store: Arc::new(store),
            jwt_secret: Arc::from(b"test-secret".as_slice()),
            rate_limit_counter: Arc::new(FakeRateLimitCounter::new()),
            rate_limit_policy_store: Arc::new(FixedRateLimitPolicyStore(RateLimitPolicy {
                rpm: GENEROUS_RPM,
            })),
            admission_gauge: Arc::new(FakeAdmissionGauge::new()),
            admission_soft_limit: GENEROUS_ADMISSION_LIMIT,
            admission_hard_limit: GENEROUS_ADMISSION_LIMIT,
            dataplane_channel: unreachable_dataplane_channel(),
        };
        (router().with_state(state), org_id)
    }

    /// A channel to a port nothing listens on. `connect_lazy` never fails
    /// at construction (see dataplane_client.rs) — the actual connection
    /// attempt only happens on first use, so this is safe and fast to
    /// build in every test even though most tests never call the routes
    /// that would use it.
    fn unreachable_dataplane_channel() -> tonic::transport::Channel {
        crate::dataplane_client::connect_lazy("http://127.0.0.1:1").unwrap()
    }

    fn authed(req: axum::http::request::Builder) -> axum::http::request::Builder {
        req.header("authorization", format!("Bearer {TEST_KEY}"))
    }

    async fn body_json(response: axum::response::Response) -> serde_json::Value {
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn chat_completions_reaches_the_real_downstream_call_once_authenticated_and_valid() {
        // The real Submit() call to dataplane-stub is now wired (Step E5);
        // this test's fake AppState has no reachable dataplane-stub, so a
        // structurally valid, authenticated request gets 502 — proving it
        // got all the way through auth/ratelimit/admission/normalize and
        // genuinely attempted the downstream call, rather than short-
        // circuiting on validation (that's a distinct, earlier failure
        // mode covered by the "rejects" tests below, all still 422/401).
        // The actual happy-path SSE relay is verified live against a real
        // dataplane-stub (dataplane_client.rs's #[ignore] test, and this
        // step's own live end-to-end check).
        let (app, _org_id) = test_app();
        let body = serde_json::json!({
            "model": "onezox-ultra",
            "messages": [{"role": "user", "content": "hi"}],
        });
        let response = app
            .oneshot(
                authed(Request::post("/v1/chat/completions"))
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn chat_completions_rejects_a_missing_required_field() {
        let (app, _org_id) = test_app();
        // Missing "model".
        let body = serde_json::json!({
            "messages": [{"role": "user", "content": "hi"}],
        });
        let response = app
            .oneshot(
                authed(Request::post("/v1/chat/completions"))
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn chat_completions_rejects_wrong_field_types() {
        let (app, _org_id) = test_app();
        // "messages" should be an array, not a string.
        let body = serde_json::json!({
            "model": "onezox-ultra",
            "messages": "not-an-array",
        });
        let response = app
            .oneshot(
                authed(Request::post("/v1/chat/completions"))
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn chat_completions_rejects_no_credential_before_even_checking_the_body() {
        let (app, _org_id) = test_app();
        // Well-formed body, but no Authorization header at all.
        let body = serde_json::json!({
            "model": "onezox-ultra",
            "messages": [{"role": "user", "content": "hi"}],
        });
        let response = app
            .oneshot(
                Request::post("/v1/chat/completions")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn chat_completions_rejects_an_unknown_credential() {
        let (app, _org_id) = test_app();
        let body = serde_json::json!({
            "model": "onezox-ultra",
            "messages": [{"role": "user", "content": "hi"}],
        });
        let response = app
            .oneshot(
                Request::post("/v1/chat/completions")
                    .header("authorization", "Bearer oz_test_totally_wrong")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn models_authenticates_via_jwt_too() {
        // Proves the Identity extractor's shape-based dispatch (Step C2)
        // actually reaches jwt::verify_jwt through the real route, not just
        // the pure function in isolation (auth/jwt.rs's own unit tests).
        let (app, _org_id) = test_app();
        let org_id = Uuid::new_v4();
        let claims = serde_json::json!({
            "org_id": org_id,
            "exp": (chrono::Utc::now().timestamp() + 3600),
        });
        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret(b"test-secret"),
        )
        .unwrap();

        let response = app
            .oneshot(
                Request::get("/v1/models")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn responses_reaches_the_real_downstream_call_once_authenticated_and_valid() {
        // Same reasoning as chat_completions' equivalent test above.
        let (app, _org_id) = test_app();
        let body = serde_json::json!({"model": "onezox-ultra", "input": "hi"});
        let response = app
            .oneshot(
                authed(Request::post("/v1/responses"))
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn embeddings_rejects_wrong_field_types() {
        let (app, _org_id) = test_app();
        // "input" should be an array of strings, not a bare string.
        let body = serde_json::json!({"model": "onezox-ultra", "input": "not-an-array"});
        let response = app
            .oneshot(
                authed(Request::post("/v1/embeddings"))
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn models_requires_authentication_too() {
        let (app, _org_id) = test_app();
        let response = app
            .oneshot(Request::get("/v1/models").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn models_returns_the_static_list_when_authenticated() {
        let (app, _org_id) = test_app();
        let response = app
            .oneshot(
                authed(Request::get("/v1/models"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let json = body_json(response).await;
        assert!(!json["data"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_request_over_the_rate_limit_is_rejected_through_the_real_route() {
        // Proves pipeline.rs's `Admitted` extractor actually reaches
        // ratelimit::enforce through a real route (ratelimit/mod.rs's own
        // tests cover the threshold logic itself in isolation).
        let org_id = Uuid::new_v4();
        let store = FakeApiKeyStore::new();
        store.insert(TEST_KEY, org_id, None);
        let state = AppState {
            api_key_store: Arc::new(store),
            jwt_secret: Arc::from(b"test-secret".as_slice()),
            rate_limit_counter: Arc::new(FakeRateLimitCounter::new()),
            rate_limit_policy_store: Arc::new(FixedRateLimitPolicyStore(RateLimitPolicy {
                rpm: 1,
            })),
            admission_gauge: Arc::new(FakeAdmissionGauge::new()),
            admission_soft_limit: GENEROUS_ADMISSION_LIMIT,
            admission_hard_limit: GENEROUS_ADMISSION_LIMIT,
            dataplane_channel: unreachable_dataplane_channel(),
        };
        let app = router().with_state(state);

        let first = app
            .clone()
            .oneshot(
                authed(Request::get("/v1/models"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);

        let second = app
            .oneshot(
                authed(Request::get("/v1/models"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn a_request_over_the_admission_hard_limit_is_shed_through_the_real_route() {
        // Proves pipeline.rs's `Admitted` extractor actually reaches
        // admission::admit through a real route (admission/mod.rs's own
        // tests cover the threshold logic itself in isolation). Pre-fills
        // the gauge directly rather than relying on sequential requests'
        // guard-drop timing (async/spawned on Drop, not deterministic
        // within a test) — this way is race-free.
        let org_id = Uuid::new_v4();
        let store = FakeApiKeyStore::new();
        store.insert(TEST_KEY, org_id, None);
        let gauge = Arc::new(FakeAdmissionGauge::new());
        gauge.increment("admission:cell-local:inflight").await.unwrap();
        let state = AppState {
            api_key_store: Arc::new(store),
            jwt_secret: Arc::from(b"test-secret".as_slice()),
            rate_limit_counter: Arc::new(FakeRateLimitCounter::new()),
            rate_limit_policy_store: Arc::new(FixedRateLimitPolicyStore(RateLimitPolicy {
                rpm: GENEROUS_RPM,
            })),
            admission_gauge: gauge,
            admission_soft_limit: 0,
            admission_hard_limit: 1,
            dataplane_channel: unreachable_dataplane_channel(),
        };
        let app = router().with_state(state);

        let response = app
            .oneshot(
                authed(Request::get("/v1/models"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
