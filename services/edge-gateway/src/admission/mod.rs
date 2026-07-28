//! Admission control (Part D, Phase-01.txt): accept/queue/shed based on a
//! per-cell in-flight request gauge, keyed `admission:{cell}:inflight` per
//! Phase-01.txt's DATABASE TABLES section.
//!
//! Phase-01 runs a single local cell (Part D: cells are a multi-region
//! scaling unit) — `CELL_ID` is a fixed constant, not read from config,
//! since there's exactly one to configure.
//!
//! "Queue" is a real decision state (a third band above the soft limit),
//! but Phase-01 doesn't actually hold/delay a queued request — there's no
//! backpressure signal to queue against until Step E's real streaming
//! relay exists. A Queue decision currently proceeds exactly like Accept;
//! only Shed rejects. This gets real queueing semantics once there's a
//! downstream stream to apply backpressure against.

pub mod store;

use std::sync::Arc;

use async_trait::async_trait;

pub const CELL_ID: &str = "cell-local";

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum AdmissionDecision {
    Accept,
    Queue,
    Shed,
}

#[derive(Debug, PartialEq, Eq)]
pub enum AdmissionError {
    Shed,
    Store(String),
}

/// In-flight gauge. Implemented by a real Redis-backed gauge (store.rs) in
/// production and by an in-memory fake in tests.
#[async_trait]
pub trait AdmissionGauge: Send + Sync {
    async fn increment(&self, key: &str) -> Result<u64, AdmissionError>;
    async fn decrement(&self, key: &str) -> Result<(), AdmissionError>;
}

/// Pure decision logic: no I/O, given a snapshot of the current in-flight
/// count and the two thresholds.
pub fn decide(inflight: u64, soft_limit: u64, hard_limit: u64) -> AdmissionDecision {
    if inflight <= soft_limit {
        AdmissionDecision::Accept
    } else if inflight <= hard_limit {
        AdmissionDecision::Queue
    } else {
        AdmissionDecision::Shed
    }
}

/// RAII guard: decrements the gauge whenever it's dropped — immediately
/// for Phase-01's current placeholder handlers, or (once Step E's SSE
/// relay holds one for a request's full duration, by moving it into the
/// stream's task) whenever the stream actually ends. Drop can't be async,
/// so it detaches a task to perform the decrement rather than blocking.
///
/// Known limitation, not solved here: if the process crashes mid-request
/// (not a clean shutdown), the decrement never runs and the gauge drifts
/// upward permanently. Real systems solve this with a heartbeat/reconcile
/// loop; out of scope for Phase-01's single local cell.
pub struct AdmissionGuard {
    gauge: Arc<dyn AdmissionGauge>,
    key: String,
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        let gauge = Arc::clone(&self.gauge);
        let key = self.key.clone();
        tokio::spawn(async move {
            if let Err(e) = gauge.decrement(&key).await {
                tracing::warn!(error = ?e, key = %key, "admission gauge decrement failed");
            }
        });
    }
}

/// Increments the cell's in-flight gauge and applies the accept/queue/shed
/// decision. Returns a guard that decrements the gauge on drop for Accept
/// and Queue; for Shed, decrements immediately (the request was never
/// admitted) and returns an error.
///
/// Fails OPEN on a gauge-store error (e.g. Redis unreachable) — same
/// reasoning as ratelimit::enforce: admission is DoS protection
/// (Phase-01.txt, Security Implementation groups it with rate limiting
/// under that heading), not a security boundary, and a Redis outage isn't
/// evidence of real overload. The only way `admit` returns `Err` is a
/// genuine Shed decision from `decide()` below.
pub async fn admit(
    gauge: Arc<dyn AdmissionGauge>,
    soft_limit: u64,
    hard_limit: u64,
) -> Result<AdmissionGuard, AdmissionError> {
    let key = format!("admission:{CELL_ID}:inflight");

    let inflight = match gauge.increment(&key).await {
        Ok(n) => n,
        Err(e) => {
            tracing::warn!(error = ?e, key = %key, "admission gauge increment failed; failing open");
            return Ok(AdmissionGuard { gauge, key });
        }
    };

    match decide(inflight, soft_limit, hard_limit) {
        AdmissionDecision::Shed => {
            if let Err(e) = gauge.decrement(&key).await {
                tracing::warn!(error = ?e, key = %key, "admission gauge decrement (after shed) failed");
            }
            Err(AdmissionError::Shed)
        }
        AdmissionDecision::Accept | AdmissionDecision::Queue => Ok(AdmissionGuard { gauge, key }),
    }
}

#[cfg(test)]
pub struct FakeAdmissionGauge {
    count: std::sync::atomic::AtomicU64,
}

#[cfg(test)]
impl FakeAdmissionGauge {
    pub fn new() -> Self {
        Self { count: std::sync::atomic::AtomicU64::new(0) }
    }

    pub fn current(&self) -> u64 {
        self.count.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(test)]
#[async_trait]
impl AdmissionGauge for FakeAdmissionGauge {
    async fn increment(&self, _key: &str) -> Result<u64, AdmissionError> {
        Ok(self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1)
    }

    async fn decrement(&self, _key: &str) -> Result<(), AdmissionError> {
        self.count.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

#[cfg(test)]
pub struct FailingAdmissionGauge;

#[cfg(test)]
#[async_trait]
impl AdmissionGauge for FailingAdmissionGauge {
    async fn increment(&self, _key: &str) -> Result<u64, AdmissionError> {
        Err(AdmissionError::Store("simulated outage".to_string()))
    }

    async fn decrement(&self, _key: &str) -> Result<(), AdmissionError> {
        Err(AdmissionError::Store("simulated outage".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decide_accepts_at_and_below_the_soft_limit() {
        assert_eq!(decide(0, 10, 20), AdmissionDecision::Accept);
        assert_eq!(decide(10, 10, 20), AdmissionDecision::Accept);
    }

    #[test]
    fn decide_queues_between_soft_and_hard_limit() {
        assert_eq!(decide(11, 10, 20), AdmissionDecision::Queue);
        assert_eq!(decide(20, 10, 20), AdmissionDecision::Queue);
    }

    #[test]
    fn decide_sheds_above_the_hard_limit() {
        assert_eq!(decide(21, 10, 20), AdmissionDecision::Shed);
    }

    #[tokio::test]
    async fn admit_succeeds_and_increments_the_gauge_within_limits() {
        let gauge = Arc::new(FakeAdmissionGauge::new());
        let guard = admit(gauge.clone(), 10, 20).await.unwrap();
        assert_eq!(gauge.current(), 1);
        drop(guard);
    }

    #[tokio::test]
    async fn admit_sheds_and_leaves_the_gauge_unchanged_beyond_the_hard_limit() {
        let gauge = Arc::new(FakeAdmissionGauge::new());
        // Fill up to the hard limit.
        let mut guards = Vec::new();
        for _ in 0..5 {
            guards.push(admit(gauge.clone(), 2, 5).await.unwrap());
        }
        assert_eq!(gauge.current(), 5);

        let result = admit(gauge.clone(), 2, 5).await;
        assert!(matches!(result, Err(AdmissionError::Shed)));
        // The shed attempt's own increment was reversed — gauge unchanged.
        assert_eq!(gauge.current(), 5);
    }

    #[tokio::test]
    async fn a_gauge_store_failure_fails_open() {
        let gauge = Arc::new(FailingAdmissionGauge);
        // soft_limit=0 would shed everything if the increment had actually
        // evaluated against real thresholds; failing open should skip that
        // entirely and admit.
        assert!(admit(gauge, 0, 0).await.is_ok());
    }

    #[tokio::test]
    async fn dropping_the_guard_decrements_the_gauge() {
        let gauge = Arc::new(FakeAdmissionGauge::new());
        let guard = admit(gauge.clone(), 10, 20).await.unwrap();
        assert_eq!(gauge.current(), 1);

        drop(guard);
        // Drop spawns the decrement rather than awaiting it inline (Drop
        // can't be async) — yield so the spawned task actually runs before
        // asserting.
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(gauge.current(), 0);
    }
}
