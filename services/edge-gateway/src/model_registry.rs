//! model_registry — Phase-04 Step R: edge-gateway's first real etcd-watch
//! integration. Mirrors data-plane's own model_registry package
//! (services/data-plane/model_registry/__init__.py) field-for-field and
//! behavior-for-behavior — same three guarantees, proven the same way:
//!
//! 1. INDEPENDENT verification. Every manifest is signature-checked HERE,
//!    against edge-gateway's own Vault Transit client (vault_client.rs),
//!    before it is ever trusted — never because "it came from etcd" or
//!    "another service already checked it."
//!
//! 2. BYTE-EXACTNESS. `signed_payload` must byte-match control-plane's own
//!    Go `signedPayload` (services/control-plane/internal/registry/
//!    registry.go) and data-plane's own Python `signed_payload` — same
//!    "|"-delimited field order, same UTF-8 encoding. spec_json is parsed
//!    with serde_json ONLY after its signature verifies.
//!
//! 3. HOT-PATH SAFETY. `resolve()` is a plain, synchronous, in-memory
//!    `HashMap` read (std::sync::RwLock, not tokio's async one — there is
//!    no I/O on this path). A verification failure on an incoming update
//!    never evicts an already-trusted manifest (last-known-good). An
//!    unresolvable model_ref returns the typed `ManifestError::NotFound`.
//!
//! `EtcdReader` is a trait (not a direct dependency on `etcd_client::Client`)
//! for the same reason data-plane's own `EtcdReader`/`SignatureVerifier`
//! are Python `Protocol`s: it lets `Cache`'s verification/last-known-good
//! logic be unit-tested hermetically against a `FakeEtcdReader`, without a
//! live etcd — `EtcdClientReader` (bottom of this file) is the real
//! `etcd_client`-backed implementation main.rs constructs.
//!
//! `watch_forever()` never panics on its own — any watch disruption
//! triggers a full `sync_once()` re-sync (not a resume-from-revision)
//! before re-establishing the watch. This is what makes the "missed
//! window" case correct: a manifest that changed WHILE disconnected is
//! caught by the full re-list, not lost because the new watch only sees
//! future events.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::stream::BoxStream;
use serde::Deserialize;
use tracing::{info, warn};

const ETCD_PREFIX: &str = "/onezox/";
const MANIFESTS_PREFIX: &str = "/onezox/manifests/";
const ACTIVE_PREFIX: &str = "/onezox/active/";
const SIGNING_KEY_NAME: &str = "model-manifest-signing";

/// Re-sync (not resume-from-revision) after any watch failure — the
/// simple, unambiguously-correct recovery, same choice data-plane's own
/// Cache made.
const RECONNECT_BACKOFF: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestError {
    NotFound(String),
}

/// MUST match control-plane's own Go signedPayload and data-plane's own
/// Python signed_payload exactly.
pub fn signed_payload(version_id: &str, model_ref: &str, spec_json: &str) -> Vec<u8> {
    format!("{version_id}|{model_ref}|{spec_json}").into_bytes()
}

#[async_trait]
pub trait SignatureVerifier: Send + Sync {
    async fn verify(&self, key_name: &str, payload: &[u8], signature: &str) -> Result<bool, String>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EtcdEventKind {
    Put,
    Delete,
}

#[derive(Debug, Clone)]
pub struct EtcdEvent {
    pub kind: EtcdEventKind,
    pub key: String,
    pub value: Vec<u8>,
}

/// Decouples Cache from etcd_client's own concrete Client type — see the
/// module doc for why (testability, same reasoning as data-plane's own
/// Python Protocol pair).
#[async_trait]
pub trait EtcdReader: Send + Sync {
    async fn get_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, String>;
    /// Returns a stream of EVENT BATCHES (one Vec per underlying watch
    /// message, which can itself carry multiple events) — matches
    /// etcd_client::WatchStream's own message-at-a-time shape.
    async fn watch_prefix(
        &self,
        prefix: &str,
    ) -> Result<BoxStream<'static, Result<Vec<EtcdEvent>, String>>, String>;
}

#[derive(Debug, Clone, Deserialize)]
struct ManifestEnvelope {
    version_id: String,
    model_ref: String,
    spec_json: String,
    signature: String,
    #[serde(default)]
    #[allow(dead_code)]
    created_by: String,
    #[serde(default)]
    #[allow(dead_code)]
    created_at: String,
    #[serde(default)]
    #[allow(dead_code)]
    status: String,
}

#[derive(Default)]
struct CacheState {
    /// (model_ref, version_id) -> manifest, ONLY entries that verified.
    manifests: HashMap<(String, String), ManifestEnvelope>,
    /// model_ref -> version_id, the active pointer.
    active: HashMap<String, String>,
}

pub struct Cache {
    etcd: Arc<dyn EtcdReader>,
    verifier: Arc<dyn SignatureVerifier>,
    state: RwLock<CacheState>,
}

impl Cache {
    pub fn new(etcd: Arc<dyn EtcdReader>, verifier: Arc<dyn SignatureVerifier>) -> Self {
        Self { etcd, verifier, state: RwLock::new(CacheState::default()) }
    }

    /// Returns false for BOTH a well-formed "no" from the verifier AND a
    /// genuine call failure (Vault unreachable, missing/misconfigured
    /// role, network error) — a bad-update caller treats these
    /// identically: refuse to trust, log, move on. Crashing the whole
    /// cache over a transient Vault problem would be exactly the
    /// hot-path-safety violation this module exists to rule out — the
    /// same live-caught bug data-plane's own Cache._verify had to fix
    /// (services/data-plane/model_registry/__init__.py) before its own
    /// first successful deploy.
    async fn verify(&self, m: &ManifestEnvelope) -> bool {
        let payload = signed_payload(&m.version_id, &m.model_ref, &m.spec_json);
        match self.verifier.verify(SIGNING_KEY_NAME, &payload, &m.signature).await {
            Ok(valid) => valid,
            Err(e) => {
                warn!(
                    model_ref = %m.model_ref,
                    version_id = %m.version_id,
                    error = %e,
                    "model_registry: verifier call failed (treated as verification failure, not a crash)"
                );
                false
            }
        }
    }

    async fn handle_manifest_kv(&self, model_ref: &str, version_id: &str, value: &[u8]) {
        let envelope: ManifestEnvelope = match serde_json::from_slice(value) {
            Ok(e) => e,
            Err(e) => {
                warn!(
                    model_ref = %model_ref, version_id = %version_id, error = %e,
                    "model_registry: malformed manifest envelope, ignoring"
                );
                return;
            }
        };

        if !self.verify(&envelope).await {
            // Last-known-good: a failed verification NEVER overwrites or
            // removes whatever this (model_ref, version_id) already held
            // — it just refuses to let the bad update in.
            warn!(
                model_ref = %model_ref, version_id = %version_id,
                "model_registry: signature verification FAILED, refusing to trust"
            );
            return;
        }

        info!(model_ref = %model_ref, version_id = %version_id, "model_registry: verified and cached");
        self.state
            .write()
            .expect("model_registry cache lock poisoned")
            .manifests
            .insert((model_ref.to_string(), version_id.to_string()), envelope);
    }

    fn handle_active_kv(&self, model_ref: &str, version_id: &str) {
        self.state
            .write()
            .expect("model_registry cache lock poisoned")
            .active
            .insert(model_ref.to_string(), version_id.to_string());
    }

    fn handle_delete(&self, key: &str) {
        let mut state = self.state.write().expect("model_registry cache lock poisoned");
        if let Some(rest) = key.strip_prefix(MANIFESTS_PREFIX) {
            if let Some((model_ref, version_id)) = rest.split_once('/') {
                state.manifests.remove(&(model_ref.to_string(), version_id.to_string()));
            }
        } else if let Some(model_ref) = key.strip_prefix(ACTIVE_PREFIX) {
            state.active.remove(model_ref);
        }
    }

    async fn dispatch_kv(&self, key: &str, value: &[u8]) {
        if let Some(rest) = key.strip_prefix(MANIFESTS_PREFIX) {
            if let Some((model_ref, version_id)) = rest.split_once('/') {
                if !model_ref.is_empty() && !version_id.is_empty() {
                    self.handle_manifest_kv(model_ref, version_id, value).await;
                }
            }
        } else if let Some(model_ref) = key.strip_prefix(ACTIVE_PREFIX) {
            if !model_ref.is_empty() {
                self.handle_active_kv(model_ref, &String::from_utf8_lossy(value));
            }
        }
    }

    /// Full re-list from etcd — the initial load, and also what a
    /// reconnect after a watch failure falls back to.
    pub async fn sync_once(&self) -> Result<(), String> {
        let kvs = self.etcd.get_prefix(ETCD_PREFIX).await?;
        for (key, value) in kvs {
            self.dispatch_kv(&key, &value).await;
        }
        Ok(())
    }

    /// Long-running background task (main.rs spawns this once at startup,
    /// after sync_once() has already populated the cache). Never panics
    /// on its own — a watch disruption triggers a full sync_once()
    /// re-sync and a fresh watch, forever, until the process exits.
    pub async fn watch_forever(&self) {
        use futures_util::StreamExt;

        loop {
            match self.etcd.watch_prefix(ETCD_PREFIX).await {
                Ok(mut stream) => loop {
                    match stream.next().await {
                        Some(Ok(events)) => {
                            for event in events {
                                match event.kind {
                                    EtcdEventKind::Delete => self.handle_delete(&event.key),
                                    EtcdEventKind::Put => self.dispatch_kv(&event.key, &event.value).await,
                                }
                            }
                        }
                        Some(Err(e)) => {
                            warn!(error = %e, "model_registry: watch stream errored, re-syncing");
                            break;
                        }
                        None => {
                            warn!("model_registry: watch stream ended, re-syncing");
                            break;
                        }
                    }
                },
                Err(e) => {
                    warn!(error = %e, "model_registry: failed to establish watch, re-syncing");
                }
            }

            tokio::time::sleep(RECONNECT_BACKOFF).await;
            if let Err(e) = self.sync_once().await {
                warn!(error = %e, "model_registry: re-sync after watch disruption also failed");
            }
        }
    }

    /// Hot-path lookup: pure in-memory reads, no I/O. Returns the typed
    /// ManifestError::NotFound rather than ever panicking or returning a
    /// placeholder.
    pub fn resolve(&self, model_ref: &str) -> Result<String, ManifestError> {
        let state = self.state.read().expect("model_registry cache lock poisoned");
        let version_id = state
            .active
            .get(model_ref)
            .ok_or_else(|| ManifestError::NotFound(model_ref.to_string()))?;
        let m = state
            .manifests
            .get(&(model_ref.to_string(), version_id.clone()))
            .ok_or_else(|| ManifestError::NotFound(model_ref.to_string()))?;

        let spec: serde_json::Value = serde_json::from_str(&m.spec_json)
            .map_err(|_| ManifestError::NotFound(model_ref.to_string()))?;
        spec.get("worker_ref")
            .and_then(|w| w.as_str())
            .map(String::from)
            .ok_or_else(|| ManifestError::NotFound(model_ref.to_string()))
    }
}

// ---------------------------------------------------------------------
// Real implementations: etcd_client-backed EtcdReader, VaultClient-backed
// SignatureVerifier.
// ---------------------------------------------------------------------

#[async_trait]
impl SignatureVerifier for crate::vault_client::VaultClient {
    async fn verify(&self, key_name: &str, payload: &[u8], signature: &str) -> Result<bool, String> {
        crate::vault_client::VaultClient::verify(self, key_name, payload, signature)
            .await
            .map_err(|e| e.to_string())
    }
}

/// The real, etcd_client-backed EtcdReader. Wraps the client in an async
/// mutex only for the brief `get`/`watch` calls themselves — once a watch
/// is established, the returned WatchStream is owned entirely by the
/// async_stream generator below, not held against the shared client lock,
/// so a long-running watch never blocks a concurrent get_prefix (e.g. from
/// a reconnect's own sync_once()) sharing the same underlying connection.
pub struct EtcdClientReader {
    client: tokio::sync::Mutex<etcd_client::Client>,
}

impl EtcdClientReader {
    pub fn new(client: etcd_client::Client) -> Self {
        Self { client: tokio::sync::Mutex::new(client) }
    }
}

#[async_trait]
impl EtcdReader for EtcdClientReader {
    async fn get_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, String> {
        let mut client = self.client.lock().await;
        let resp = client
            .get(prefix.as_bytes().to_vec(), Some(etcd_client::GetOptions::new().with_prefix()))
            .await
            .map_err(|e| e.to_string())?;
        Ok(resp
            .kvs()
            .iter()
            .map(|kv| (kv.key_str().unwrap_or_default().to_string(), kv.value().to_vec()))
            .collect())
    }

    async fn watch_prefix(
        &self,
        prefix: &str,
    ) -> Result<BoxStream<'static, Result<Vec<EtcdEvent>, String>>, String> {
        let mut stream = {
            let mut client = self.client.lock().await;
            client
                .watch(prefix.as_bytes().to_vec(), Some(etcd_client::WatchOptions::new().with_prefix()))
                .await
                .map_err(|e| e.to_string())?
        };

        let s = async_stream::stream! {
            loop {
                match stream.message().await {
                    Ok(Some(resp)) => {
                        let events: Vec<EtcdEvent> = resp
                            .events()
                            .iter()
                            .filter_map(|event| {
                                let kv = event.kv()?;
                                let key = kv.key_str().unwrap_or_default().to_string();
                                let kind = match event.event_type() {
                                    etcd_client::EventType::Put => EtcdEventKind::Put,
                                    etcd_client::EventType::Delete => EtcdEventKind::Delete,
                                };
                                Some(EtcdEvent { kind, key, value: kv.value().to_vec() })
                            })
                            .collect();
                        yield Ok(events);
                    }
                    Ok(None) => {
                        yield Err("watch stream ended".to_string());
                        return;
                    }
                    Err(e) => {
                        yield Err(e.to_string());
                        return;
                    }
                }
            }
        };
        Ok(Box::pin(s))
    }
}

/// A Cache backed by no-op EtcdReader/SignatureVerifier — for OTHER
/// modules' tests (e.g. ingress.rs) that need a valid `AppState` but don't
/// exercise registry behavior at all. `#[cfg(test)]`, not shipped in the
/// release binary. This is deliberately separate from this module's own
/// `tests::FakeEtcdReader`/`FakeVerifier` (private to `mod tests`, used
/// for THIS module's own adversarial verification tests) — those
/// genuinely verify; this one exists purely to satisfy AppState's type in
/// unrelated tests.
#[cfg(test)]
pub(crate) fn empty_cache_for_test() -> Cache {
    struct NoopReader;
    #[async_trait]
    impl EtcdReader for NoopReader {
        async fn get_prefix(&self, _prefix: &str) -> Result<Vec<(String, Vec<u8>)>, String> {
            Ok(vec![])
        }
        async fn watch_prefix(
            &self,
            _prefix: &str,
        ) -> Result<BoxStream<'static, Result<Vec<EtcdEvent>, String>>, String> {
            Ok(Box::pin(futures_util::stream::empty()))
        }
    }
    struct NoopVerifier;
    #[async_trait]
    impl SignatureVerifier for NoopVerifier {
        async fn verify(&self, _key_name: &str, _payload: &[u8], _signature: &str) -> Result<bool, String> {
            Ok(false)
        }
    }
    Cache::new(Arc::new(NoopReader), Arc::new(NoopVerifier))
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::sync::Mutex as StdMutex;

    /// Real HMAC-ish (SHA256-based) verifier, not a stub that always
    /// answers true — Verify genuinely recomputes and compares, so the
    /// adversarial tests below exercise real rejection logic. Mirrors
    /// control-plane's own Go FakeSigner and data-plane's own Python
    /// FakeVerifier exactly.
    struct FakeVerifier;

    impl FakeVerifier {
        fn sign(payload: &[u8]) -> String {
            let mut hasher = Sha256::new();
            hasher.update(b"fake-test-signing-key");
            hasher.update(payload);
            format!("fake:v1:{:x}", hasher.finalize())
        }
    }

    #[async_trait]
    impl SignatureVerifier for FakeVerifier {
        async fn verify(&self, _key_name: &str, payload: &[u8], signature: &str) -> Result<bool, String> {
            Ok(Self::sign(payload) == signature)
        }
    }

    struct RaisingVerifier;

    #[async_trait]
    impl SignatureVerifier for RaisingVerifier {
        async fn verify(&self, _key_name: &str, _payload: &[u8], _signature: &str) -> Result<bool, String> {
            Err("vault kubernetes login failed (status 400): invalid role name".to_string())
        }
    }

    /// In-memory EtcdReader for unit tests — get_prefix only (mirrors
    /// data-plane's own FakeEtcdReader: the live watch path is proven
    /// against the real etcd cluster, not faked here).
    #[derive(Default)]
    struct FakeEtcdReader {
        store: StdMutex<HashMap<String, Vec<u8>>>,
    }

    impl FakeEtcdReader {
        fn put(&self, key: &str, value: Vec<u8>) {
            self.store.lock().unwrap().insert(key.to_string(), value);
        }
    }

    #[async_trait]
    impl EtcdReader for FakeEtcdReader {
        async fn get_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, String> {
            Ok(self
                .store
                .lock()
                .unwrap()
                .iter()
                .filter(|(k, _)| k.starts_with(prefix))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect())
        }

        async fn watch_prefix(
            &self,
            _prefix: &str,
        ) -> Result<BoxStream<'static, Result<Vec<EtcdEvent>, String>>, String> {
            // Not exercised by these unit tests (same as data-plane's own
            // FakeEtcdReader.watch_prefix) — an empty, never-yielding
            // stream is enough to satisfy the trait.
            Ok(Box::pin(futures_util::stream::empty()))
        }
    }

    fn envelope(version_id: &str, model_ref: &str, spec_json: &str, signature: &str) -> Vec<u8> {
        serde_json::json!({
            "version_id": version_id,
            "model_ref": model_ref,
            "spec_json": spec_json,
            "signature": signature,
            "created_by": "test",
            "created_at": "2026-01-01T00:00:00Z",
            "status": "published",
        })
        .to_string()
        .into_bytes()
    }

    fn put_manifest(etcd: &FakeEtcdReader, version_id: &str, model_ref: &str, spec_json: &str, signature: &str) {
        etcd.put(
            &format!("/onezox/manifests/{model_ref}/{version_id}"),
            envelope(version_id, model_ref, spec_json, signature),
        );
        etcd.put(&format!("/onezox/active/{model_ref}"), version_id.as_bytes().to_vec());
    }

    #[tokio::test]
    async fn resolve_valid_manifest() {
        let etcd = Arc::new(FakeEtcdReader::default());
        let (version_id, model_ref) = ("v1", "openai");
        let spec_json = r#"{"worker_ref":"openai:gpt-4o-mini"}"#;
        let signature = FakeVerifier::sign(&signed_payload(version_id, model_ref, spec_json));
        put_manifest(&etcd, version_id, model_ref, spec_json, &signature);

        let cache = Cache::new(etcd, Arc::new(FakeVerifier));
        cache.sync_once().await.unwrap();

        assert_eq!(cache.resolve("openai").unwrap(), "openai:gpt-4o-mini");
    }

    #[tokio::test]
    async fn resolve_unknown_model_ref_raises_typed_error() {
        let cache = Cache::new(Arc::new(FakeEtcdReader::default()), Arc::new(FakeVerifier));
        cache.sync_once().await.unwrap();

        assert_eq!(cache.resolve("does-not-exist"), Err(ManifestError::NotFound("does-not-exist".to_string())));
    }

    #[tokio::test]
    async fn tampered_manifest_rejected() {
        let etcd = Arc::new(FakeEtcdReader::default());
        let (version_id, model_ref) = ("v1", "openai");
        let original_spec = r#"{"worker_ref":"openai:gpt-4o-mini"}"#;
        let signature = FakeVerifier::sign(&signed_payload(version_id, model_ref, original_spec));

        // Tampered: the etcd VALUE carries different spec_json than what
        // was actually signed.
        let tampered_spec = r#"{"worker_ref":"openai:gpt-4o-EXPENSIVE"}"#;
        put_manifest(&etcd, version_id, model_ref, tampered_spec, &signature);

        let cache = Cache::new(etcd, Arc::new(FakeVerifier));
        cache.sync_once().await.unwrap();

        assert!(cache.resolve("openai").is_err(), "tampered manifest must never resolve");
    }

    #[tokio::test]
    async fn unsigned_manifest_rejected() {
        let etcd = Arc::new(FakeEtcdReader::default());
        let (version_id, model_ref) = ("v1", "openai");
        put_manifest(&etcd, version_id, model_ref, r#"{"worker_ref":"openai:gpt-4o-mini"}"#, "");

        let cache = Cache::new(etcd, Arc::new(FakeVerifier));
        cache.sync_once().await.unwrap();

        assert!(cache.resolve("openai").is_err(), "unsigned manifest must never resolve");
    }

    #[tokio::test]
    async fn garbage_signature_rejected() {
        let etcd = Arc::new(FakeEtcdReader::default());
        let (version_id, model_ref) = ("v1", "openai");
        put_manifest(
            &etcd,
            version_id,
            model_ref,
            r#"{"worker_ref":"openai:gpt-4o-mini"}"#,
            "fake:v1:not-a-real-signature",
        );

        let cache = Cache::new(etcd, Arc::new(FakeVerifier));
        cache.sync_once().await.unwrap();

        assert!(cache.resolve("openai").is_err(), "garbage signature must never resolve");
    }

    #[tokio::test]
    async fn verifier_call_failure_does_not_crash_sync() {
        // Regression test mirroring data-plane's own
        // test_verifier_call_failure_does_not_crash_sync: a genuine
        // Vault call failure must degrade to "not trusted," never panic.
        let etcd = Arc::new(FakeEtcdReader::default());
        put_manifest(&etcd, "v1", "openai", r#"{"worker_ref":"openai:gpt-4o-mini"}"#, "some-signature");

        let cache = Cache::new(etcd, Arc::new(RaisingVerifier));
        cache.sync_once().await.unwrap(); // must not panic

        assert!(cache.resolve("openai").is_err());
    }

    #[tokio::test]
    async fn last_known_good_survives_a_bad_update() {
        let etcd = Arc::new(FakeEtcdReader::default());
        let (version_id, model_ref) = ("v1", "openai");
        let spec_json = r#"{"worker_ref":"openai:gpt-4o-mini"}"#;
        let signature = FakeVerifier::sign(&signed_payload(version_id, model_ref, spec_json));
        put_manifest(&etcd, version_id, model_ref, spec_json, &signature);

        let cache = Cache::new(Arc::clone(&etcd) as Arc<dyn EtcdReader>, Arc::new(FakeVerifier));
        cache.sync_once().await.unwrap();
        assert_eq!(cache.resolve("openai").unwrap(), "openai:gpt-4o-mini");

        // Simulate a bad direct write overwriting the SAME key with
        // tampered content (empty signature this time), then exercise the
        // incremental-update path directly (what watch_forever would call
        // per event).
        cache
            .handle_manifest_kv(model_ref, version_id, &envelope(version_id, model_ref, spec_json, ""))
            .await;

        assert_eq!(cache.resolve("openai").unwrap(), "openai:gpt-4o-mini", "last-known-good must survive");
    }
}
