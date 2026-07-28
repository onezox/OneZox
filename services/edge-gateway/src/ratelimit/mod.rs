//! Rate limiting (Part K, Phase-01 Step D1): Redis-backed, tenant-scoped
//! fixed-window request counters, keyed `ratelimit:{org_id}:{window}` per
//! Phase-01.txt's DATABASE TABLES section (TTL = window).
//!
//! TPM (tokens per minute) is part of rate_limit_policy's schema but NOT
//! enforced here: Phase-01 has no real token count before a request
//! completes (meter emits placeholder token/cost fields until Phase-03's
//! real model call) — there's nothing meaningful to check TPM against
//! pre-request. RPM (request count) is the only limit Phase-01 can
//! actually enforce; TPM enforcement is deferred to whichever phase gives
//! the edge a real pre-request token estimate.
//!
//! Composed into the same request-admission step as auth (see
//! src/pipeline.rs) rather than a separate axum middleware layer —
//! Phase-01.txt's pipeline (auth -> ratelimit -> admission -> ...) is
//! naturally expressed as sequential checks after identity is resolved.

pub mod store;

use async_trait::async_trait;
use uuid::Uuid;

#[derive(Debug, Clone, Copy)]
pub struct RateLimitPolicy {
    pub rpm: u32,
}

/// Applied when no `rate_limit_policy` row exists for an org — Phase-01.txt
/// doesn't specify a default, so this keeps unconfigured tenants usable
/// (matching CLAUDE.md's spirit: don't invent extra gating not called for)
/// without being effectively unlimited.
pub const DEFAULT_RPM: u32 = 60;

#[derive(Debug, PartialEq, Eq)]
pub enum RateLimitError {
    Exceeded,
    Store(String),
}

/// Atomically increments the counter for `key`. Implemented by a real
/// Redis-backed counter (store.rs) in production and by an in-memory fake
/// in tests.
#[async_trait]
pub trait RateLimitCounter: Send + Sync {
    /// Sets `ttl_secs` only on the key's first increment in a window (so a
    /// later request in the same window never resets when it decays) and
    /// returns the new count.
    async fn increment(&self, key: &str, ttl_secs: u64) -> Result<u64, RateLimitError>;
}

/// Resolves an org's policy. Implemented by a real CockroachDB-backed
/// lookup (store.rs) and by a fixed-value fake in tests — trait-abstracted
/// for the same reason as `RateLimitCounter`: without it, route-level
/// tests going through the composed pipeline (pipeline.rs) would need a
/// real, reachable CockroachDB just to resolve a policy, which risks a
/// hang (not just slowness) if the test pool points at an unreachable
/// host.
#[async_trait]
pub trait RateLimitPolicyStore: Send + Sync {
    async fn policy_for(&self, org_id: Uuid) -> RateLimitPolicy;
}

/// The current fixed window, in whole minutes since the epoch — matches
/// rate_limit_policy.rpm's "requests per minute" semantics.
fn current_window() -> i64 {
    chrono::Utc::now().timestamp() / 60
}

const WINDOW_SECS: u64 = 60;

/// Pure rate-limit logic: increments this org's counter for the current
/// window and compares against its policy. Fails OPEN on a counter-store
/// error (e.g. Redis unreachable) — deliberately different from auth,
/// which fails closed. Rate limiting is DoS protection (Phase-01.txt,
/// Security Implementation), not a security boundary; a Redis outage
/// should degrade it, not take down all traffic.
pub async fn enforce(
    counter: &dyn RateLimitCounter,
    org_id: Uuid,
    policy: &RateLimitPolicy,
) -> Result<(), RateLimitError> {
    let key = format!("ratelimit:{org_id}:{}", current_window());
    match counter.increment(&key, WINDOW_SECS).await {
        Ok(count) if count <= policy.rpm as u64 => Ok(()),
        Ok(_) => Err(RateLimitError::Exceeded),
        Err(RateLimitError::Store(msg)) => {
            tracing::warn!(
                org_id = %org_id,
                error = %msg,
                "rate limit counter store failed; failing open"
            );
            Ok(())
        }
        Err(e) => Err(e),
    }
}

#[cfg(test)]
pub struct FakeRateLimitCounter {
    counts: std::sync::Mutex<std::collections::HashMap<String, u64>>,
}

#[cfg(test)]
impl FakeRateLimitCounter {
    pub fn new() -> Self {
        Self { counts: std::sync::Mutex::new(std::collections::HashMap::new()) }
    }
}

#[cfg(test)]
#[async_trait]
impl RateLimitCounter for FakeRateLimitCounter {
    async fn increment(&self, key: &str, _ttl_secs: u64) -> Result<u64, RateLimitError> {
        let mut counts = self.counts.lock().unwrap();
        let entry = counts.entry(key.to_string()).or_insert(0);
        *entry += 1;
        Ok(*entry)
    }
}

#[cfg(test)]
pub struct FailingRateLimitCounter;

#[cfg(test)]
#[async_trait]
impl RateLimitCounter for FailingRateLimitCounter {
    async fn increment(&self, _key: &str, _ttl_secs: u64) -> Result<u64, RateLimitError> {
        Err(RateLimitError::Store("simulated outage".to_string()))
    }
}

/// Always returns the same fixed policy, regardless of org_id — used by
/// route-level tests (ingress.rs) that don't care about per-org policy
/// variation, only that the pipeline actually calls through to a policy
/// store.
#[cfg(test)]
pub struct FixedRateLimitPolicyStore(pub RateLimitPolicy);

#[cfg(test)]
#[async_trait]
impl RateLimitPolicyStore for FixedRateLimitPolicyStore {
    async fn policy_for(&self, _org_id: Uuid) -> RateLimitPolicy {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn requests_within_the_limit_are_allowed() {
        let counter = FakeRateLimitCounter::new();
        let org_id = Uuid::new_v4();
        let policy = RateLimitPolicy { rpm: 3 };
        for _ in 0..3 {
            assert!(enforce(&counter, org_id, &policy).await.is_ok());
        }
    }

    #[tokio::test]
    async fn the_request_exceeding_the_limit_is_denied() {
        let counter = FakeRateLimitCounter::new();
        let org_id = Uuid::new_v4();
        let policy = RateLimitPolicy { rpm: 3 };
        for _ in 0..3 {
            enforce(&counter, org_id, &policy).await.unwrap();
        }
        let err = enforce(&counter, org_id, &policy).await.unwrap_err();
        assert_eq!(err, RateLimitError::Exceeded);
    }

    #[tokio::test]
    async fn different_orgs_have_independent_windows() {
        let counter = FakeRateLimitCounter::new();
        let org_a = Uuid::new_v4();
        let org_b = Uuid::new_v4();
        let policy = RateLimitPolicy { rpm: 1 };
        assert!(enforce(&counter, org_a, &policy).await.is_ok());
        // org_b's own window is untouched by org_a's request.
        assert!(enforce(&counter, org_b, &policy).await.is_ok());
        // org_a is now at its limit.
        assert!(enforce(&counter, org_a, &policy).await.is_err());
    }

    #[tokio::test]
    async fn zero_rpm_denies_everything() {
        let counter = FakeRateLimitCounter::new();
        let org_id = Uuid::new_v4();
        let policy = RateLimitPolicy { rpm: 0 };
        assert!(enforce(&counter, org_id, &policy).await.is_err());
    }

    #[tokio::test]
    async fn a_counter_store_failure_fails_open() {
        let counter = FailingRateLimitCounter;
        let policy = RateLimitPolicy { rpm: 0 }; // would deny everything if it evaluated the count
        assert!(enforce(&counter, Uuid::new_v4(), &policy).await.is_ok());
    }
}
