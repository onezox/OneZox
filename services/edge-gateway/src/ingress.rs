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

async fn chat_completions(Json(req): Json<ChatCompletionRequest>) -> impl IntoResponse {
    not_wired(format!("chat.completions model={}", req.model))
}

async fn responses(Json(req): Json<ResponsesRequest>) -> impl IntoResponse {
    not_wired(format!("responses model={}", req.model))
}

async fn embeddings(Json(req): Json<EmbeddingsRequest>) -> impl IntoResponse {
    not_wired(format!("embeddings model={}", req.model))
}

async fn models() -> impl IntoResponse {
    // Phase-01.txt: "served from a static list until Phase-04 registry".
    Json(ModelsListResponse {
        data: vec![Model {
            id: "onezox-ultra".to_string(),
            owned_by: "onezox".to_string(),
        }],
    })
}

pub fn router() -> Router {
    Router::new()
        .route("/v1/chat/completions", post(chat_completions))
        .route("/v1/responses", post(responses))
        .route("/v1/embeddings", post(embeddings))
        .route("/v1/models", get(models))
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    async fn body_json(response: axum::response::Response) -> serde_json::Value {
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn chat_completions_accepts_a_well_formed_body() {
        let app = router();
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
        // 501: structurally valid, downstream not wired yet (Step E).
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[tokio::test]
    async fn chat_completions_rejects_a_missing_required_field() {
        let app = router();
        // Missing "model".
        let body = serde_json::json!({
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
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn chat_completions_rejects_wrong_field_types() {
        let app = router();
        // "messages" should be an array, not a string.
        let body = serde_json::json!({
            "model": "onezox-ultra",
            "messages": "not-an-array",
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
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn responses_accepts_a_well_formed_body() {
        let app = router();
        let body = serde_json::json!({"model": "onezox-ultra", "input": "hi"});
        let response = app
            .oneshot(
                Request::post("/v1/responses")
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
        let app = router();
        // "input" should be an array of strings, not a bare string.
        let body = serde_json::json!({"model": "onezox-ultra", "input": "not-an-array"});
        let response = app
            .oneshot(
                Request::post("/v1/embeddings")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn models_returns_the_static_list() {
        let app = router();
        let response = app
            .oneshot(Request::get("/v1/models").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let json = body_json(response).await;
        assert!(json["data"].as_array().unwrap().len() >= 1);
    }
}
