//! gRPC client to dataplane-stub (Part K.2, Phase-01 Step E3): the
//! Submit(SubmitRequest) -> stream(SubmitResponse) call edge-gateway makes
//! to the "Temporary downstream" (Phase-01.txt) until Phase-03 replaces
//! dataplane-stub with the real data plane.
//!
//! Stores a `Channel`, not a `DataplaneServiceClient`, in AppState: tonic's
//! generated client needs `&mut self` per call, but `Channel` itself is
//! Send+Sync+Clone and multiplexes over HTTP/2 internally — the idiomatic
//! pattern is a fresh (cheap) client wrapper per call from a shared
//! channel, not one client instance shared across concurrent requests.

use tonic::Status;
use tonic::transport::Channel;

use crate::pb::dataplane::v1::dataplane_service_client::DataplaneServiceClient;
use crate::pb::dataplane::v1::{SubmitRequest, SubmitResponse};

/// Builds a channel to dataplane-stub without blocking or failing if it
/// isn't reachable yet — matches the rest of edge-gateway's boot behavior
/// (the CockroachDB pool and Redis client also don't require every
/// downstream to be healthy just to start listening; readiness is
/// /readyz's job, not main()'s).
pub fn connect_lazy(endpoint: &str) -> Result<Channel, tonic::codegen::http::uri::InvalidUri> {
    Ok(Channel::from_shared(endpoint.to_string())?.connect_lazy())
}

/// Calls Submit and returns the response stream. Fresh `DataplaneServiceClient`
/// per call (see module doc) — cheap, since `channel` itself is what's
/// actually shared/reused.
pub async fn submit(
    channel: Channel,
    request: SubmitRequest,
) -> Result<tonic::Streaming<SubmitResponse>, Status> {
    let mut client = DataplaneServiceClient::new(channel);
    let response = client.submit(request).await?;
    Ok(response.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::dataplane::v1::{Identity, RequestKind};

    /// Live check against a real dataplane-stub — not run by default
    /// `cargo test` (Phase-01.txt's testing requirements don't list the
    /// gRPC client as its own unit-test category; this is Step E3's
    /// "verified it can call Submit and receive the stream in isolation").
    /// Requires `kubectl port-forward -n default svc/dataplane-stub
    /// 50051:50051` first, then:
    /// `cargo test -p edge-gateway -- --ignored dataplane_client`.
    #[tokio::test]
    #[ignore]
    async fn submit_streams_the_placeholder_response_from_a_real_dataplane_stub() {
        let channel = connect_lazy("http://localhost:50051").unwrap();
        let request = SubmitRequest {
            request_id: "live-e3-test".to_string(),
            identity: Some(Identity {
                org_id: "test-org".to_string(),
                user_id: String::new(),
                project_id: String::new(),
                conversation_id: String::new(),
            }),
            kind: RequestKind::ChatCompletion as i32,
            model: "test-model".to_string(),
            messages: vec![],
            stream: true,
            max_tokens: None,
            temperature: None,
        };

        let mut stream = submit(channel, request).await.unwrap();
        let mut chunks = Vec::new();
        while let Some(chunk) = stream.message().await.unwrap() {
            chunks.push(chunk);
        }

        assert!(!chunks.is_empty());
        let last = chunks.last().unwrap();
        assert!(last.is_final);
        assert_eq!(last.finish_reason.as_deref(), Some("stop"));
    }
}
