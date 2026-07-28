//! Request normalization to the internal proto contract (Part K, Phase-01
//! Step E1): converts the ingress HTTP layer's parsed request structs
//! (ingress.rs) into proto/dataplane's `SubmitRequest` — the message
//! edge-gateway's gRPC client (Step E3) sends to dataplane-stub.
//!
//! No "bad payload" branch here: by the time a request reaches normalize,
//! it has already passed serde/axum's structural validation (ingress.rs)
//! and identity resolution (auth). This is a pure, infallible mapping.

use uuid::Uuid;

use crate::auth::Identity;
use crate::ingress::{ChatCompletionRequest, EmbeddingsRequest, ResponsesRequest};
use crate::pb::dataplane::v1::{Identity as PbIdentity, RequestKind, SubmitRequest};
use crate::pb::gateway::v1::ChatMessage as PbChatMessage;

fn pb_identity(identity: &Identity) -> PbIdentity {
    PbIdentity {
        org_id: identity.org_id.to_string(),
        user_id: identity.user_id.map(|u| u.to_string()).unwrap_or_default(),
        project_id: identity.project_id.map(|u| u.to_string()).unwrap_or_default(),
        conversation_id: identity
            .conversation_id
            .map(|u| u.to_string())
            .unwrap_or_default(),
    }
}

fn new_request_id() -> String {
    Uuid::new_v4().to_string()
}

pub fn normalize_chat_completions(
    identity: &Identity,
    req: &ChatCompletionRequest,
) -> SubmitRequest {
    SubmitRequest {
        request_id: new_request_id(),
        identity: Some(pb_identity(identity)),
        kind: RequestKind::ChatCompletion as i32,
        model: req.model.clone(),
        messages: req
            .messages
            .iter()
            .map(|m| PbChatMessage { role: m.role.clone(), content: m.content.clone() })
            .collect(),
        stream: req.stream,
        max_tokens: req.max_tokens,
        temperature: req.temperature,
    }
}

pub fn normalize_responses(identity: &Identity, req: &ResponsesRequest) -> SubmitRequest {
    SubmitRequest {
        request_id: new_request_id(),
        identity: Some(pb_identity(identity)),
        kind: RequestKind::Responses as i32,
        model: req.model.clone(),
        messages: vec![PbChatMessage { role: "user".to_string(), content: req.input.clone() }],
        stream: req.stream,
        max_tokens: None,
        temperature: None,
    }
}

pub fn normalize_embeddings(identity: &Identity, req: &EmbeddingsRequest) -> SubmitRequest {
    SubmitRequest {
        request_id: new_request_id(),
        identity: Some(pb_identity(identity)),
        kind: RequestKind::Embeddings as i32,
        model: req.model.clone(),
        messages: req
            .input
            .iter()
            .map(|text| PbChatMessage { role: "user".to_string(), content: text.clone() })
            .collect(),
        stream: false,
        max_tokens: None,
        temperature: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingress::ChatMessage;

    fn test_identity() -> Identity {
        Identity {
            org_id: Uuid::new_v4(),
            user_id: None,
            project_id: None,
            conversation_id: None,
        }
    }

    #[test]
    fn chat_completions_maps_every_field() {
        let identity = test_identity();
        let req = ChatCompletionRequest {
            model: "onezox-ultra".to_string(),
            messages: vec![
                ChatMessage { role: "system".to_string(), content: "be terse".to_string() },
                ChatMessage { role: "user".to_string(), content: "hi".to_string() },
            ],
            stream: true,
            max_tokens: Some(256),
            temperature: Some(0.7),
        };

        let normalized = normalize_chat_completions(&identity, &req);

        assert_eq!(normalized.kind, RequestKind::ChatCompletion as i32);
        assert_eq!(normalized.model, "onezox-ultra");
        assert!(normalized.stream);
        assert_eq!(normalized.max_tokens, Some(256));
        assert_eq!(normalized.temperature, Some(0.7));
        assert_eq!(normalized.messages.len(), 2);
        assert_eq!(normalized.messages[0].role, "system");
        assert_eq!(normalized.messages[1].content, "hi");
        assert_eq!(normalized.identity.unwrap().org_id, identity.org_id.to_string());
        assert!(!normalized.request_id.is_empty());
    }

    #[test]
    fn two_normalizations_get_different_request_ids() {
        let identity = test_identity();
        let req = ChatCompletionRequest {
            model: "onezox-ultra".to_string(),
            messages: vec![],
            stream: false,
            max_tokens: None,
            temperature: None,
        };
        let a = normalize_chat_completions(&identity, &req);
        let b = normalize_chat_completions(&identity, &req);
        assert_ne!(a.request_id, b.request_id);
    }

    #[test]
    fn identity_with_only_org_id_leaves_the_rest_empty_not_absent() {
        // Identity's proto fields are plain `string`, not `optional string`
        // (see dataplane.proto) — absence is represented as "", not by
        // omitting the field, since api_keys-only auth (Step C1) never
        // resolves user_id/project_id/conversation_id.
        let identity = test_identity();
        let req = ChatCompletionRequest {
            model: "m".to_string(),
            messages: vec![],
            stream: false,
            max_tokens: None,
            temperature: None,
        };
        let pb = normalize_chat_completions(&identity, &req).identity.unwrap();
        assert_eq!(pb.user_id, "");
        assert_eq!(pb.project_id, "");
        assert_eq!(pb.conversation_id, "");
    }

    #[test]
    fn identity_with_all_fields_populated_carries_all_of_them() {
        let identity = Identity {
            org_id: Uuid::new_v4(),
            user_id: Some(Uuid::new_v4()),
            project_id: Some(Uuid::new_v4()),
            conversation_id: Some(Uuid::new_v4()),
        };
        let req = ChatCompletionRequest {
            model: "m".to_string(),
            messages: vec![],
            stream: false,
            max_tokens: None,
            temperature: None,
        };
        let pb = normalize_chat_completions(&identity, &req).identity.unwrap();
        assert_eq!(pb.org_id, identity.org_id.to_string());
        assert_eq!(pb.user_id, identity.user_id.unwrap().to_string());
        assert_eq!(pb.project_id, identity.project_id.unwrap().to_string());
        assert_eq!(pb.conversation_id, identity.conversation_id.unwrap().to_string());
    }

    #[test]
    fn responses_wraps_input_as_a_single_user_message() {
        let identity = test_identity();
        let req = ResponsesRequest {
            model: "onezox-ultra".to_string(),
            input: "summarize this repo".to_string(),
            stream: true,
        };

        let normalized = normalize_responses(&identity, &req);

        assert_eq!(normalized.kind, RequestKind::Responses as i32);
        assert_eq!(normalized.messages.len(), 1);
        assert_eq!(normalized.messages[0].role, "user");
        assert_eq!(normalized.messages[0].content, "summarize this repo");
        assert!(normalized.stream);
    }

    #[test]
    fn embeddings_maps_each_input_string_to_its_own_message() {
        let identity = test_identity();
        let req = EmbeddingsRequest {
            model: "onezox-embed".to_string(),
            input: vec!["a".to_string(), "b".to_string(), "c".to_string()],
        };

        let normalized = normalize_embeddings(&identity, &req);

        assert_eq!(normalized.kind, RequestKind::Embeddings as i32);
        assert_eq!(normalized.messages.len(), 3);
        assert_eq!(normalized.messages[0].content, "a");
        assert_eq!(normalized.messages[2].content, "c");
        assert!(normalized.messages.iter().all(|m| m.role == "user"));
        // Embeddings requests aren't streamed, regardless of what the
        // ingress body might otherwise imply (EmbeddingsRequest has no
        // stream field at all — nothing to carry through).
        assert!(!normalized.stream);
    }
}
