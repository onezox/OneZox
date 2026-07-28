//! Shared axum state. Generic over `ApiKeyStore` rather than hardcoding the
//! CockroachDB-backed implementation, so route-level tests (ingress.rs) can
//! run against `auth::FakeApiKeyStore` and stay hermetic — only the real
//! binary (main.rs) instantiates the CockroachDB-backed store.

use std::sync::Arc;

use crate::auth::ApiKeyStore;

pub struct AppState<S: ApiKeyStore> {
    pub api_key_store: Arc<S>,
    /// HS256 shared secret for the JWT verification hook (Step C2). No real
    /// issuer exists yet in Phase-01 — see auth/jwt.rs's doc comment.
    pub jwt_secret: Arc<[u8]>,
}

// Not `#[derive(Clone)]`: that would require `S: Clone`, but `Arc<S>` is
// Clone regardless of whether `S` is — deriving here would force every
// `ApiKeyStore` implementation (including CockroachApiKeyStore, which holds
// a connection pool with no reason to be `Clone` itself) to add a bound it
// doesn't need.
impl<S: ApiKeyStore> Clone for AppState<S> {
    fn clone(&self) -> Self {
        Self {
            api_key_store: Arc::clone(&self.api_key_store),
            jwt_secret: Arc::clone(&self.jwt_secret),
        }
    }
}
