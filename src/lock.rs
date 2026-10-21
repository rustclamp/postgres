//! Session advisory locks held on a dedicated pooled connection.

use sqlx::Postgres;
use sqlx::pool::PoolConnection;

use crate::Database;

/// Key of a Postgres advisory lock.
///
/// An `i64` is used as is. A string is hashed with 64-bit FNV-1a over its
/// UTF-8 bytes (offset basis `0xcbf29ce484222325`, prime `0x100000001b3`), and
/// the result is reinterpreted as `i64`. The hash is fixed by this crate, so
/// every instance and version computes the same key for the same name; it is
/// not Postgres' `hashtext`, which is not guaranteed stable across releases.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct LockKey(pub i64);

impl From<i64> for LockKey {
    fn from(key: i64) -> Self {
        Self(key)
    }
}

impl From<&str> for LockKey {
    fn from(name: &str) -> Self {
        let hash = name.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
        });
        Self(i64::from_ne_bytes(hash.to_ne_bytes()))
    }
}

/// A held session advisory lock and the connection that holds it.
///
/// Call [`LockGuard::release`] to unlock and return the connection to the
/// pool. Dropping the guard instead closes the connection, which ends the
/// session and frees the lock; a connection still holding the lock never goes
/// back to the pool. [`Database::close`] waits for the guard, so release or
/// drop it (and any leader lock holding one) before closing the pool.
pub struct LockGuard {
    connection: Option<PoolConnection<Postgres>>,
    key: LockKey,
}

impl LockGuard {
    /// The key this guard holds.
    pub const fn key(&self) -> LockKey {
        self.key
    }

    /// Checks that the holding connection is still alive, and so still holds
    /// the lock. After a `false`, drop the guard and acquire again.
    pub async fn is_held(&mut self) -> bool {
        let Some(connection) = self.connection.as_mut() else {
            return false;
        };
        sqlx::query("SELECT 1")
            .execute(&mut **connection)
            .await
            .is_ok()
    }

    /// Unlocks and returns the connection to the pool. If the unlock fails,
    /// or Postgres reports the lock was not held, the connection is closed
    /// instead and the error (if any) returned.
    pub async fn release(mut self) -> Result<(), sqlx::Error> {
        let Some(mut connection) = self.connection.take() else {
            return Ok(());
        };
        let unlocked: Result<(bool,), _> = sqlx::query_as("SELECT pg_advisory_unlock($1)")
            .bind(self.key.0)
            .fetch_one(&mut *connection)
            .await;
        match unlocked {
            Ok((true,)) => Ok(()),
            Ok((false,)) => connection.close().await,
            Err(error) => {
                connection.close_on_drop();
                Err(error)
            }
        }
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.as_mut() {
            connection.close_on_drop();
        }
    }
}

impl<Q: rustclamp_core::Qualifier> Database<Q> {
    /// Tries to take the session advisory lock `key` without waiting, on a
    /// connection taken from the pool for as long as the guard lives.
    /// `Ok(None)` means another session holds it, and the connection goes
    /// back to the pool. Locks are re-entrant per session only, so a second
    /// `try_lock` in the same process, on another connection, returns `None`.
    pub async fn try_lock(
        &self,
        key: impl Into<LockKey>,
    ) -> Result<Option<LockGuard>, sqlx::Error> {
        let key = key.into();
        let mut connection = self.pool().acquire().await?;
        let (locked,): (bool,) = sqlx::query_as("SELECT pg_try_advisory_lock($1)")
            .bind(key.0)
            .fetch_one(&mut *connection)
            .await?;
        Ok(locked.then(|| LockGuard {
            connection: Some(connection),
            key,
        }))
    }
}

#[cfg(feature = "scheduler")]
pub use leader::AdvisoryLeader;

#[cfg(feature = "scheduler")]
mod leader {
    use std::sync::{Mutex, PoisonError};

    use rustclamp_scheduler::{LeaderFuture, LeaderLock};

    use super::{LockGuard, LockKey};
    use crate::Database;

    /// [`LeaderLock`] on a session advisory lock, for
    /// `Scheduler::leader_only`. Built by [`Database::leader_lock`].
    ///
    /// Each [`LeaderLock::hold`] pings the held connection. If it died (a
    /// restart, a killed backend, a network cut), the guard is dropped and the
    /// lock acquired again, which succeeds only if no other instance took it.
    /// A ping on a hung network waits for the pool's socket to fail.
    pub struct AdvisoryLeader<Q> {
        database: Database<Q>,
        key: LockKey,
        guard: Mutex<Option<LockGuard>>,
    }

    impl<Q: rustclamp_core::Qualifier> Database<Q> {
        /// A scheduler leader lock on advisory lock `key`.
        pub fn leader_lock(&self, key: impl Into<LockKey>) -> AdvisoryLeader<Q> {
            AdvisoryLeader {
                database: self.clone(),
                key: key.into(),
                guard: Mutex::new(None),
            }
        }
    }

    impl<Q: rustclamp_core::Qualifier + Send + Sync> LeaderLock for AdvisoryLeader<Q> {
        fn hold(&self) -> LeaderFuture<'_> {
            Box::pin(async move {
                // ponytail: take the guard out across the await; a concurrent
                // hold() sees none and its try_lock fails, i.e. a spurious false.
                let held = self
                    .guard
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take();
                let mut guard = None;
                if let Some(mut held) = held
                    && held.is_held().await
                {
                    guard = Some(held);
                }
                if guard.is_none() {
                    guard = self.database.try_lock(self.key).await.ok().flatten();
                }
                let leader = guard.is_some();
                *self.guard.lock().unwrap_or_else(PoisonError::into_inner) = guard;
                leader
            })
        }
    }
}
