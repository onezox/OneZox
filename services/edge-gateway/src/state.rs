//! Shared axum state. Non-generic, deliberately: `ApiKeyStore` and
//! `RateLimitCounter` are `#[async_trait]` (dyn-compatible), stored as
//! `Arc<dyn Trait>` — see auth/mod.rs's module doc for why a generic type
//! parameter per pipeline stage doesn't scale as more stages (admission,
//! ...) get added.

use std::sync::Arc;

use tonic::transport::Channel;

use crate::admission::AdmissionGauge;
use crate::auth::ApiKeyStore;
use crate::ratelimit::{RateLimitCounter, RateLimitPolicyStore};

#[derive(Clone)]
pub struct AppState {
    pub api_key_store: Arc<dyn ApiKeyStore>,
    /// HS256 shared secret for the JWT verification hook (Step C2). No real
    /// issuer exists yet in Phase-01 — see auth/jwt.rs's doc comment.
    pub jwt_secret: Arc<[u8]>,
    pub rate_limit_counter: Arc<dyn RateLimitCounter>,
    pub rate_limit_policy_store: Arc<dyn RateLimitPolicyStore>,
    pub admission_gauge: Arc<dyn AdmissionGauge>,
    pub admission_soft_limit: u64,
    pub admission_hard_limit: u64,
    /// gRPC channel to dataplane-stub (Step E3/E5). `Channel` is itself
    /// Send+Sync+Clone (see dataplane_client.rs's doc comment), so it
    /// doesn't need an `Arc` wrapper here.
    pub dataplane_channel: Channel,
}
