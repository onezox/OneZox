//! CockroachDB-backed `ApiKeyStore`. Thin by design: `authenticate_api_key`
//! (mod.rs) holds all the actual logic and is unit-tested against a fake
//! store; this file's job is just the SQL round-trip, verified separately
//! against the live cluster (see Phase-01-Progress.txt / commit message for
//! how — not an automated `cargo test`, since that would make the unit test
//! suite depend on cluster availability).

use async_trait::async_trait;
use deadpool_postgres::{Config, Pool, Runtime};
use tokio_postgres::NoTls;

use super::{ApiKeyRow, ApiKeyStore, AuthError};

/// Connects as the least-privilege `edge_gateway` role (migration 0023),
/// never as root — root is a CockroachDB superuser and bypasses
/// GRANT/REVOKE entirely, which is the same reasoning control-plane's
/// and admin-api's own roles already document. That role holds SELECT on
/// api_keys and rate_limit_policy and nothing else: this service issues
/// no INSERT/UPDATE/DELETE anywhere, so it is granted none, and it
/// cannot read or touch model_active, audit_log or any registry table.
///
/// Env-overridable with the same COCKROACH_USER name control-plane and
/// admin-api use, so all four services are configured identically.
pub fn build_pool(host: &str) -> Pool {
    let mut cfg = Config::new();
    cfg.host = Some(host.to_string());
    cfg.port = Some(26257);
    cfg.user = Some(std::env::var("COCKROACH_USER").unwrap_or_else(|_| "edge_gateway".to_string()));
    cfg.dbname = Some("defaultdb".to_string());
    cfg.pool = Some(deadpool_postgres::PoolConfig::new(16));
    cfg.create_pool(Some(Runtime::Tokio1), NoTls)
        .expect("failed to build CockroachDB connection pool")
}

pub struct CockroachApiKeyStore {
    pool: Pool,
}

impl CockroachApiKeyStore {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ApiKeyStore for CockroachApiKeyStore {
    async fn lookup_by_hash(&self, hash: &str) -> Result<Option<ApiKeyRow>, AuthError> {
        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| AuthError::Store(format!("pool checkout failed: {e}")))?;

        let row = conn
            .query_opt(
                "SELECT org_id, revoked_at FROM api_keys WHERE hash = $1",
                &[&hash],
            )
            .await
            .map_err(|e| AuthError::Store(format!("query failed: {e}")))?;

        Ok(row.map(|r| ApiKeyRow {
            org_id: r.get("org_id"),
            revoked_at: r.get("revoked_at"),
        }))
    }

    async fn ping(&self) -> Result<(), AuthError> {
        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| AuthError::Store(format!("pool checkout failed: {e}")))?;
        conn.simple_query("SELECT 1")
            .await
            .map_err(|e| AuthError::Store(format!("ping failed: {e}")))?;
        Ok(())
    }
}
