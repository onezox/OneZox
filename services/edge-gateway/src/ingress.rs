//! OpenAI-compatible ingress surface (Part K, Phase-01 Step B): routes
//! exist, bodies are structurally validated via serde's `Deserialize` —
//! axum's `Json` extractor rejects malformed/missing-field bodies with 422
//! automatically. Semantic validation and the actual conversion to the
//! internal proto contract is the normalize module's job (Phase-01 Step E),
//! not wired here yet; valid requests currently get a 501 placeholder.
//!
//! Hand-written serde structs here, not the prost-generated proto/gateway
//! types: protobuf's canonical JSON mapping renders field names in
//! lowerCamelCase by default, which would break OpenAI wire compatibility
//! (the real API uses snake_case, e.g. "max_tokens"). These structs mirror
//! proto/gateway/v1/gateway.proto's field names/types by convention; that
//! proto file remains the cross-language schema of record. The generated
//! Rust types from it are used downstream instead, in the gRPC call to the
//! data plane (Phase-01 Step E), where binary wire format applies and this
//! concern doesn't exist.

use axum::{
    Json, Router,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};

use crate::pipeline::Admitted;
use crate::state::AppState;

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

fn not_wired(stage: String) -> impl IntoResponse {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(NotWiredError {
            error: NotWiredErrorDetail {
                message: format!(
                    "{stage} accepted a structurally valid request but downstream is not wired yet (Phase-01, later step)"
                ),
                kind: "not_implemented".to_string(),
            },
        }),
    )
}

// Every handler takes `Admitted(identity, _guard)` (even where identity
// isn't used yet, e.g. models): this is the enforcement point for "no
// request proceeds without a resolved org_id", rate limiting, and admission
// (Phase-01.txt, Security Implementation + FEATURES). `_guard` isn't read
// directly by any handler yet — it exists to be held for the handler's
// full lifetime, decrementing the in-flight gauge on drop; once Step E's
// SSE relay exists, it moves into the stream's task instead of dropping
// immediately. `Admitted` must be extracted before any body-consuming
// extractor (axum requires the last argument to be the one that consumes
// the request body).

async fn chat_completions(
    Admitted(identity, _guard): Admitted,
    Json(req): Json<ChatCompletionRequest>,
) -> impl IntoResponse {
    not_wired(format!(
        "chat.completions org_id={} model={}",
        identity.org_id, req.model
    ))
}

async fn responses(
    Admitted(identity, _guard): Admitted,
    Json(req): Json<ResponsesRequest>,
) -> impl IntoResponse {
    not_wired(format!(
        "responses org_id={} model={}",
        identity.org_id, req.model
    ))
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
        };
        (router().with_state(state), org_id)
    }

    fn authed(req: axum::http::request::Builder) -> axum::http::request::Builder {
        req.header("authorization", format!("Bearer {TEST_KEY}"))
    }

    async fn body_json(response: axum::response::Response) -> serde_json::Value {
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn chat_completions_accepts_a_well_formed_authenticated_body() {
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
        // 501: authenticated + structurally valid, downstream not wired yet
        // (Step E).
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
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
    async fn responses_accepts_a_well_formed_authenticated_body() {
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
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
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
