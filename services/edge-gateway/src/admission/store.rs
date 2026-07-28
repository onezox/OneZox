//! Redis-backed `AdmissionGauge` — the `admission:{cell}:inflight` key
//! convention from Phase-01.txt. Thin by design, same rationale as
//! auth/store.rs and ratelimit/store.rs: the interesting logic (thresholds,
//! guard lifetime) lives in mod.rs and is unit-tested against a fake; this
//! file's job is the actual round-trip, verified separately against the
//! live cluster.
//!
//! No TTL on the gauge key (unlike ratelimit's windowed counters): a live
//! in-flight count has no natural window to expire on. See mod.rs's
//! `AdmissionGuard` doc comment for the known crash-drift limitation this
//! implies.

use async_trait::async_trait;
use redis::AsyncCommands;
use redis::cluster::ClusterClient;

use super::{AdmissionError, AdmissionGauge};

pub struct RedisAdmissionGauge {
    client: ClusterClient,
}

impl RedisAdmissionGauge {
    pub fn new(client: ClusterClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl AdmissionGauge for RedisAdmissionGauge {
    async fn increment(&self, key: &str) -> Result<u64, AdmissionError> {
        let mut conn = self
            .client
            .get_async_connection()
            .await
            .map_err(|e| AdmissionError::Store(format!("redis connection failed: {e}")))?;
        conn.incr(key, 1)
            .await
            .map_err(|e| AdmissionError::Store(format!("redis INCR failed: {e}")))
    }

    async fn decrement(&self, key: &str) -> Result<(), AdmissionError> {
        let mut conn = self
            .client
            .get_async_connection()
            .await
            .map_err(|e| AdmissionError::Store(format!("redis connection failed: {e}")))?;
        let _: i64 = conn
            .decr(key, 1)
            .await
            .map_err(|e| AdmissionError::Store(format!("redis DECR failed: {e}")))?;
        Ok(())
    }
}
