//! vault_client — Phase-04 Step R: edge-gateway's OWN Vault access,
//! entirely independent of control-plane's (Go) and data-plane's
//! (Python) — same reasoning as data-plane's own vault_client module doc:
//! model_registry's cache must verify a manifest's signature ITSELF
//! before trusting it, never because "it came from etcd" or "another
//! service already checked it." A compromised etcd or a bad publish must
//! not be able to make edge-gateway route on an invalid manifest.
//!
//! Kubernetes auth, not a static Vault token: edge-gateway authenticates
//! using its own pod's projected ServiceAccount token, validated via
//! Vault's Kubernetes auth method against the bound role
//! scripts/vault-setup-edge-gateway.sh configures.
//!
//! VERIFY ONLY: edge-gateway's Vault policy (edge-gateway-manifest-verify)
//! grants transit/verify on model-manifest-signing but NOT transit/sign —
//! same asymmetric capability data-plane's own policy already established
//! (Step Q): even a fully compromised edge-gateway pod cannot forge a new
//! signed manifest, only check existing signatures.

use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde_json::json;
use tokio::sync::Mutex;

const SA_TOKEN_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/token";

/// Renewal buffer mirrors data-plane's own vault_client.Client — re-login
/// before the cached token's TTL actually expires, not exactly at it.
const RENEWAL_BUFFER: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub enum VaultError {
    /// A genuine call failure (network, auth, bad key name, token file
    /// unreadable) — distinct from a well-formed verify call that
    /// legitimately answers false.
    Call(String),
}

impl std::fmt::Display for VaultError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VaultError::Call(msg) => write!(f, "{msg}"),
        }
    }
}

struct TokenState {
    token: String,
    expires_at: Instant,
}

pub struct VaultClient {
    addr: String,
    k8s_role: String,
    http: reqwest::Client,
    token: Mutex<Option<TokenState>>,
}

impl VaultClient {
    pub fn new(addr: String, k8s_role: String, http: reqwest::Client) -> Self {
        Self { addr, k8s_role, http, token: Mutex::new(None) }
    }

    async fn login(&self) -> Result<TokenState, VaultError> {
        let jwt = tokio::fs::read_to_string(SA_TOKEN_PATH)
            .await
            .map_err(|e| VaultError::Call(format!("reading service account token: {e}")))?;

        let resp = self
            .http
            .post(format!("{}/v1/auth/kubernetes/login", self.addr))
            .json(&json!({"role": self.k8s_role, "jwt": jwt.trim()}))
            .send()
            .await
            .map_err(|e| VaultError::Call(format!("calling vault kubernetes login: {e}")))?;

        let status = resp.status();
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| VaultError::Call(format!("decoding login response: {e}")))?;

        let client_token = body
            .get("auth")
            .and_then(|a| a.get("client_token"))
            .and_then(|t| t.as_str());

        match client_token {
            Some(token) if status.is_success() => {
                let lease_secs = body["auth"]["lease_duration"].as_u64().unwrap_or(0);
                let ttl = Duration::from_secs(lease_secs).saturating_sub(RENEWAL_BUFFER);
                Ok(TokenState { token: token.to_string(), expires_at: Instant::now() + ttl })
            }
            _ => Err(VaultError::Call(format!(
                "vault kubernetes login failed (status {status}): {:?}",
                body.get("errors")
            ))),
        }
    }

    async fn ensure_token(&self) -> Result<String, VaultError> {
        let mut guard = self.token.lock().await;
        if let Some(state) = guard.as_ref() {
            if Instant::now() < state.expires_at {
                return Ok(state.token.clone());
            }
        }
        let state = self.login().await?;
        let token = state.token.clone();
        *guard = Some(state);
        Ok(token)
    }

    /// Verifies signature over payload under key_name via Vault Transit.
    /// Returns Ok(false) for a well-formed request that legitimately
    /// doesn't verify (tampered/unsigned/garbage signature) — a definite
    /// negative answer, not a call failure. Returns Err only for a
    /// genuine call failure.
    pub async fn verify(
        &self,
        key_name: &str,
        payload: &[u8],
        signature: &str,
    ) -> Result<bool, VaultError> {
        let token = self.ensure_token().await?;

        let resp = self
            .http
            .post(format!("{}/v1/transit/verify/{key_name}", self.addr))
            .header("X-Vault-Token", token)
            .json(&json!({"input": BASE64.encode(payload), "signature": signature}))
            .send()
            .await
            .map_err(|e| VaultError::Call(format!("calling transit verify: {e}")))?;

        let status = resp.status();
        if status.as_u16() == 400 {
            // Vault's own invalid-signature response — same "well-formed
            // request, definite no" treatment the Go and Python
            // vault_client implementations both give this exact status.
            return Ok(false);
        }

        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| VaultError::Call(format!("decoding verify response: {e}")))?;

        if !status.is_success() {
            return Err(VaultError::Call(format!(
                "transit verify failed (status {status}): {:?}",
                body.get("errors")
            )));
        }

        Ok(body["data"]["valid"].as_bool().unwrap_or(false))
    }
}
