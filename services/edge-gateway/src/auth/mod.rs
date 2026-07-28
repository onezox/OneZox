//! Authentication (Part O, Part P.1, Phase-01 Step C): resolves an identity
//! from an incoming request and attaches it to the request context. "No
//! request proceeds without a resolved org_id" (Phase-01.txt, Security
//! Implementation) — everything else on `Identity` is best-effort in
//! Phase-01, populated only by whichever path (API key or JWT) can supply
//! it.
//!
//! The DB-backed API-key lookup lives behind the `ApiKeyStore` trait
//! (store.rs) so `authenticate_api_key` below is pure/DB-free and directly
//! unit-testable with a fake in-memory store — Phase-01.txt's testing
//! requirement is explicitly "Unit: auth (valid/invalid/expired key + JWT)".
//! A thin CockroachDB-backed implementation is verified separately against
//! the live cluster (store.rs).
//!
//! `ApiKeyStore` uses `#[async_trait]` (not a native async fn in a trait)
//! specifically so it's `dyn`-compatible: `AppState` (state.rs) holds
//! `Arc<dyn ApiKeyStore>` rather than being generic over the store type.
//! Phase-01's pipeline (auth -> ratelimit -> admission -> ...) composes
//! several independently-swappable stores/counters into one shared state;
//! a generic type parameter per stage doesn't scale — it would have to
//! thread through every handler signature and grow with each new stage.

pub mod jwt;
pub mod store;

use async_trait::async_trait;
use axum::Json;
use axum::extract::FromRequestParts;
use axum::http::{StatusCode, header, request::Parts};
use chrono::{DateTime, Utc};
use serde::Serialize;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::state::AppState;

/// Resolved from an API key or a JWT. `org_id` is the only field Phase-01
/// guarantees; the rest are attached as each auth path can supply them —
/// see Part P.1: "the request carries identity (org_id/user_id/project_id/
/// conversation_id) — attached by the edge (API keys) or website (JWT)".
/// API keys are org-scoped only in this phase's schema (api_keys has no
/// user_id/project_id/conversation_id columns); those fill in once JWT
/// (Step C2) and later phases (Chat/Memory Service) supply them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    pub org_id: Uuid,
    pub user_id: Option<Uuid>,
    pub project_id: Option<Uuid>,
    pub conversation_id: Option<Uuid>,
}

/// Deliberately a single flat variant set with no "which check failed"
/// detail surfaced to callers beyond this enum — the HTTP layer (Step C's
/// axum wiring) maps every variant to the same generic 401, so a caller
/// probing with different bad keys can't distinguish "unknown key" from
/// "revoked key" from "expired token" (Phase-01.txt Security: "unauth
/// rejected", no oracle for key enumeration).
#[derive(Debug, PartialEq, Eq)]
pub enum AuthError {
    MissingCredential,
    InvalidCredential,
    Revoked,
    Expired,
    Store(String),
}

/// A single row's worth of what the API-key lookup needs. Deliberately not
/// the full `api_keys` schema (no `hash` field here — the store looks up BY
/// hash, it doesn't need to return it).
#[derive(Debug, Clone)]
pub struct ApiKeyRow {
    pub org_id: Uuid,
    pub revoked_at: Option<DateTime<Utc>>,
}

/// Looks up a pre-hashed API key. Implemented by a real CockroachDB-backed
/// store (store.rs) in production and by an in-memory fake in tests.
#[async_trait]
pub trait ApiKeyStore: Send + Sync {
    async fn lookup_by_hash(&self, hash: &str) -> Result<Option<ApiKeyRow>, AuthError>;

    /// Health check for /readyz. Part of the trait (not a CockroachDB-only
    /// inherent method) so main.rs's readyz handler can call it through
    /// `Arc<dyn ApiKeyStore>` without knowing the concrete store type.
    async fn ping(&self) -> Result<(), AuthError>;
}

/// SHA-256 hex digest of a raw API key — the same convention used by
/// data/seed/seed-test-tenant.sh to populate `api_keys.hash`. Never log or
/// return the raw key itself; only ever pass it through this function.
pub fn hash_api_key(raw_key: &str) -> String {
    let digest = Sha256::digest(raw_key.as_bytes());
    hex::encode(digest)
}

/// Pure authentication logic: hash the raw key, look it up, check it isn't
/// revoked, resolve an `Identity`. No I/O beyond what `store` performs.
pub async fn authenticate_api_key(
    store: &dyn ApiKeyStore,
    raw_key: &str,
) -> Result<Identity, AuthError> {
    if raw_key.is_empty() {
        return Err(AuthError::MissingCredential);
    }

    let hash = hash_api_key(raw_key);
    let row = store.lookup_by_hash(&hash).await?;

    match row {
        None => Err(AuthError::InvalidCredential),
        Some(ApiKeyRow { revoked_at: Some(_), .. }) => Err(AuthError::Revoked),
        Some(ApiKeyRow { org_id, revoked_at: None }) => Ok(Identity {
            org_id,
            user_id: None,
            project_id: None,
            conversation_id: None,
        }),
    }
}

impl From<AuthError> for String {
    fn from(err: AuthError) -> Self {
        format!("{err:?}")
    }
}

#[derive(Serialize)]
pub struct AuthErrorBody {
    error: AuthErrorDetail,
}

#[derive(Serialize)]
struct AuthErrorDetail {
    message: &'static str,
    #[serde(rename = "type")]
    kind: &'static str,
}

/// Same generic body for every rejection reason (see `AuthError`'s doc
/// comment: no oracle for which specific check failed).
fn unauthorized() -> (StatusCode, Json<AuthErrorBody>) {
    (
        StatusCode::UNAUTHORIZED,
        Json(AuthErrorBody {
            error: AuthErrorDetail {
                message: "invalid or missing credentials",
                kind: "unauthorized",
            },
        }),
    )
}

/// Lets any axum handler take `identity: Identity` as a parameter: extracts
/// `Authorization: Bearer <token>`, dispatches to whichever verification
/// path matches the token's shape (Part P.1: "resolves ...  from API key
/// or JWT"), and rejects with a generic 401 on any failure. This is the
/// enforcement point for "no request proceeds without a resolved org_id"
/// (Phase-01.txt, Security Implementation).
impl FromRequestParts<AppState> for Identity {
    type Rejection = (StatusCode, Json<AuthErrorBody>);

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let token = parts
            .headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .unwrap_or("");

        let result = if jwt::looks_like_jwt(token) {
            jwt::verify_jwt(token, &state.jwt_secret)
        } else {
            authenticate_api_key(state.api_key_store.as_ref(), token).await
        };

        result.map_err(|_| unauthorized())
    }
}

/// In-memory `ApiKeyStore`, used by this module's own unit tests and
/// reused (cfg(test)-only, so it never ships) by ingress.rs's route-level
/// tests so those stay hermetic too instead of needing a live CockroachDB.
#[cfg(test)]
pub struct FakeApiKeyStore {
    rows: std::sync::Mutex<std::collections::HashMap<String, ApiKeyRow>>,
}

#[cfg(test)]
impl FakeApiKeyStore {
    pub fn new() -> Self {
        Self { rows: std::sync::Mutex::new(std::collections::HashMap::new()) }
    }

    pub fn insert(&self, raw_key: &str, org_id: Uuid, revoked_at: Option<DateTime<Utc>>) {
        self.rows
            .lock()
            .unwrap()
            .insert(hash_api_key(raw_key), ApiKeyRow { org_id, revoked_at });
    }
}

#[cfg(test)]
#[async_trait]
impl ApiKeyStore for FakeApiKeyStore {
    async fn lookup_by_hash(&self, hash: &str) -> Result<Option<ApiKeyRow>, AuthError> {
        Ok(self.rows.lock().unwrap().get(hash).cloned())
    }

    async fn ping(&self) -> Result<(), AuthError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn valid_key_resolves_the_identity() {
        let store = FakeApiKeyStore::new();
        let org_id = Uuid::new_v4();
        store.insert("oz_test_good", org_id, None);

        let identity = authenticate_api_key(&store, "oz_test_good").await.unwrap();
        assert_eq!(identity.org_id, org_id);
        assert_eq!(identity.user_id, None);
    }

    #[tokio::test]
    async fn unknown_key_is_rejected() {
        let store = FakeApiKeyStore::new();
        store.insert("oz_test_good", Uuid::new_v4(), None);

        let err = authenticate_api_key(&store, "oz_test_totally_wrong").await.unwrap_err();
        assert_eq!(err, AuthError::InvalidCredential);
    }

    #[tokio::test]
    async fn empty_key_is_rejected_without_a_store_round_trip() {
        let store = FakeApiKeyStore::new();
        let err = authenticate_api_key(&store, "").await.unwrap_err();
        assert_eq!(err, AuthError::MissingCredential);
    }

    #[tokio::test]
    async fn revoked_key_is_rejected() {
        let store = FakeApiKeyStore::new();
        let org_id = Uuid::new_v4();
        store.insert("oz_test_revoked", org_id, Some(Utc::now()));

        let err = authenticate_api_key(&store, "oz_test_revoked").await.unwrap_err();
        assert_eq!(err, AuthError::Revoked);
    }

    #[tokio::test]
    async fn two_different_keys_hash_differently_and_resolve_different_orgs() {
        let store = FakeApiKeyStore::new();
        let org_a = Uuid::new_v4();
        let org_b = Uuid::new_v4();
        store.insert("oz_test_key_a", org_a, None);
        store.insert("oz_test_key_b", org_b, None);

        let identity_a = authenticate_api_key(&store, "oz_test_key_a").await.unwrap();
        let identity_b = authenticate_api_key(&store, "oz_test_key_b").await.unwrap();
        assert_eq!(identity_a.org_id, org_a);
        assert_eq!(identity_b.org_id, org_b);
        assert_ne!(identity_a.org_id, identity_b.org_id);
    }

    #[test]
    fn hash_is_sha256_hex_matching_the_seed_script_convention() {
        // Known SHA-256("test") value, same as verified against CockroachDB's
        // own sha256() and the host's sha256sum during Step A5.
        assert_eq!(
            hash_api_key("test"),
            "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
        );
    }
}
