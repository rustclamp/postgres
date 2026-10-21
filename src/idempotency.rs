//! PostgreSQL store for rustclamp-http's `Idempotency-Key` support (feature `idempotency`).

use std::time::Duration;

use rustclamp_http::{
    Claim, IdempotencyError, IdempotencyKey, IdempotencyRecord, IdempotencyStore, StoreFuture,
    StoredResponse,
};
use sqlx::postgres::PgPool;

use crate::Migration;

/// One key's row; SQLx takes only literal SQL, so this is spliced with `concat!`.
macro_rules! key_match {
    () => {
        "principal = $1 AND idempotency_key = $2 AND method = $3 AND path = $4"
    };
}

/// The table [`PgIdempotencyStore`] uses, as a migration at the `version` the
/// application picks for it in its own plan.
pub const fn idempotency_migration(version: i64) -> Migration {
    Migration {
        version,
        description: "rustclamp idempotency keys",
        sql: "CREATE TABLE rustclamp_idempotency_keys (
            principal TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            method TEXT NOT NULL,
            path TEXT NOT NULL,
            fingerprint BYTEA NOT NULL,
            status SMALLINT,
            header_names TEXT[],
            header_values BYTEA[],
            body BYTEA,
            expires_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (principal, idempotency_key, method, path)
        );
        CREATE INDEX rustclamp_idempotency_keys_expires_at
            ON rustclamp_idempotency_keys (expires_at);",
    }
}

/// [`IdempotencyStore`] shared by every instance on one database.
///
/// The claim is an `INSERT ... ON CONFLICT DO NOTHING`, so of two concurrent
/// requests with one key exactly one runs the handler. Expiry uses the
/// database clock. Expired rows are replaced on claim; call
/// [`Self::delete_expired`] from a scheduled job to keep the table small.
#[derive(Clone, Debug)]
pub struct PgIdempotencyStore {
    pool: PgPool,
}

impl PgIdempotencyStore {
    /// Uses `pool`, migrated with [`idempotency_migration`].
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Deletes expired keys and returns how many went.
    pub async fn delete_expired(&self) -> Result<u64, sqlx::Error> {
        sqlx::query("DELETE FROM rustclamp_idempotency_keys WHERE expires_at <= now()")
            .execute(&self.pool)
            .await
            .map(|done| done.rows_affected())
    }

    async fn claim_once(
        &self,
        key: &IdempotencyKey,
        fingerprint: [u8; 32],
        ttl: Duration,
    ) -> Result<Option<Claim>, sqlx::Error> {
        sqlx::query(concat!(
            "DELETE FROM rustclamp_idempotency_keys WHERE ",
            key_match!(),
            " AND expires_at <= now()"
        ))
        .bind(&key.principal)
        .bind(&key.key)
        .bind(&key.method)
        .bind(&key.path)
        .execute(&self.pool)
        .await?;
        let inserted = sqlx::query(
            "INSERT INTO rustclamp_idempotency_keys
                 (principal, idempotency_key, method, path, fingerprint, expires_at)
             VALUES ($1, $2, $3, $4, $5, now() + $6 * interval '1 second')
             ON CONFLICT DO NOTHING",
        )
        .bind(&key.principal)
        .bind(&key.key)
        .bind(&key.method)
        .bind(&key.path)
        .bind(&fingerprint[..])
        .bind(ttl.as_secs_f64())
        .execute(&self.pool)
        .await?;
        if inserted.rows_affected() == 1 {
            return Ok(Some(Claim::Claimed));
        }
        type Row = (
            Vec<u8>,
            Option<i16>,
            Option<Vec<String>>,
            Option<Vec<Vec<u8>>>,
            Option<Vec<u8>>,
        );
        let row: Option<Row> = sqlx::query_as(concat!(
            "SELECT fingerprint, status, header_names, header_values, body
             FROM rustclamp_idempotency_keys WHERE ",
            key_match!(),
            " AND expires_at > now()"
        ))
        .bind(&key.principal)
        .bind(&key.key)
        .bind(&key.method)
        .bind(&key.path)
        .fetch_optional(&self.pool)
        .await?;
        // None: the holder released or expired between the two statements.
        Ok(row.map(|(stored, status, names, values, body)| {
            let response = status.map(|status| StoredResponse {
                status: u16::try_from(status).unwrap_or(500),
                headers: names
                    .unwrap_or_default()
                    .into_iter()
                    .zip(values.unwrap_or_default())
                    .collect(),
                body: body.unwrap_or_default(),
            });
            Claim::Existing(IdempotencyRecord {
                // A wrong length never matches a SHA-256, so it answers 422.
                fingerprint: stored.try_into().unwrap_or([0; 32]),
                response,
            })
        }))
    }
}

impl IdempotencyStore for PgIdempotencyStore {
    fn claim<'a>(
        &'a self,
        key: &'a IdempotencyKey,
        fingerprint: [u8; 32],
        ttl: Duration,
    ) -> StoreFuture<'a, Claim> {
        Box::pin(async move {
            for _ in 0..3 {
                if let Some(claim) = self.claim_once(key, fingerprint, ttl).await? {
                    return Ok(claim);
                }
            }
            Err(IdempotencyError::from(
                "idempotency key kept changing hands",
            ))
        })
    }

    fn complete<'a>(
        &'a self,
        key: &'a IdempotencyKey,
        response: &'a StoredResponse,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            let (names, values): (Vec<&str>, Vec<&[u8]>) = response
                .headers
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_slice()))
                .unzip();
            sqlx::query(concat!(
                "UPDATE rustclamp_idempotency_keys
                 SET status = $5, header_names = $6, header_values = $7, body = $8
                 WHERE ",
                key_match!(),
                ""
            ))
            .bind(&key.principal)
            .bind(&key.key)
            .bind(&key.method)
            .bind(&key.path)
            .bind(i16::try_from(response.status)?)
            .bind(names)
            .bind(values)
            .bind(&response.body)
            .execute(&self.pool)
            .await?;
            Ok(())
        })
    }

    fn release<'a>(&'a self, key: &'a IdempotencyKey) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            sqlx::query(concat!(
                "DELETE FROM rustclamp_idempotency_keys WHERE ",
                key_match!(),
                " AND status IS NULL"
            ))
            .bind(&key.principal)
            .bind(&key.key)
            .bind(&key.method)
            .bind(&key.path)
            .execute(&self.pool)
            .await?;
            Ok(())
        })
    }
}
