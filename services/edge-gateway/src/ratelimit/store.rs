//! Redis-backed `RateLimitCounter` and the `rate_limit_policy` lookup.
//! Thin by design, same rationale as auth/store.rs: the interesting logic
//! (window arithmetic, threshold comparison, fail-open behavior) lives in
//! mod.rs and is unit-tested against fakes; this file's job is the actual
//! round-trips, verified separately against the live cluster.

use async_trait::async_trait;
use deadpool_postgres::Pool;
use redis::AsyncCommands;
use redis::cluster::ClusterClient;
use uuid::Uuid;

use super::{DEFAULT_RPM, RateLimitCounter, RateLimitError, RateLimitPolicy, RateLimitPolicyStore};

pub fn build_redis_client(host: &str) -> ClusterClient {
    ClusterClient::new(vec![format!("redis://{host}:6379")]).expect("invalid redis cluster config")
}

pub struct RedisRateLimitCounter {
    client: ClusterClient,
}

impl RedisRateLimitCounter {
    pub fn new(client: ClusterClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl RateLimitCounter for RedisRateLimitCounter {
    async fn increment(&self, key: &str, ttl_secs: u64) -> Result<u64, RateLimitError> {
        let mut conn = self
            .client
            .get_async_connection()
            .await
            .map_err(|e| RateLimitError::Store(format!("redis connection failed: {e}")))?;

        // `SET key 1 NX EX ttl` atomically creates the counter with its TTL
        // on the first request in a window; a later request in the same
        // window only INCRs, never touching that TTL. This avoids the
        // correctness bug of INCR-then-separately-EXPIRE: a crash between
        // those two calls would leave a counter with no TTL that never
        // resets, permanently rate-limiting that org at whatever count it
        // last reached.
        let created: Option<String> = redis::cmd("SET")
            .arg(key)
            .arg(1)
            .arg("NX")
            .arg("EX")
            .arg(ttl_secs)
            .query_async(&mut conn)
            .await
            .map_err(|e| RateLimitError::Store(format!("redis SET NX failed: {e}")))?;

        if created.is_some() {
            return Ok(1);
        }

        conn.incr(key, 1)
            .await
            .map_err(|e| RateLimitError::Store(format!("redis INCR failed: {e}")))
    }
}

pub struct CockroachRateLimitPolicyStore {
    pool: Pool,
}

impl CockroachRateLimitPolicyStore {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl RateLimitPolicyStore for CockroachRateLimitPolicyStore {
    async fn policy_for(&self, org_id: Uuid) -> RateLimitPolicy {
        // No row (unconfigured tenant) and any store error both fall back
        // to DEFAULT_RPM — same fail-open spirit as the counter itself
        // (ratelimit/mod.rs's `enforce`): a policy-lookup hiccup shouldn't
        // block traffic.
        let rpm = lookup_rpm(&self.pool, org_id).await;
        RateLimitPolicy { rpm: rpm.unwrap_or(DEFAULT_RPM) }
    }
}

async fn lookup_rpm(pool: &Pool, org_id: Uuid) -> Option<u32> {
    let conn = pool.get().await.ok()?;
    let row = conn
        .query_opt(
            "SELECT rpm FROM rate_limit_policy WHERE org_id = $1",
            &[&org_id],
        )
        .await
        .ok()??;
    let rpm: i32 = row.get("rpm");
    Some(rpm as u32)
}
