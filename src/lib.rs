//! Typed SQLx PostgreSQL pools, ownership, health, and migrations.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::fmt;
use std::marker::PhantomData;
use std::time::Duration;

use sqlx::Postgres;
use sqlx::postgres::{PgPool, PgPoolOptions};

/// SQLx remains accessible for adapter-specific queries and types.
pub use sqlx;

/// Primary database qualifier.
pub struct Primary;

impl rustclamp_core::Qualifier for Primary {
    const ID: rustclamp_core::QualifierId =
        rustclamp_core::QualifierId::new("rustclamp.postgres.primary");
}

/// Analytics database qualifier.
pub struct Analytics;

impl rustclamp_core::Qualifier for Analytics {
    const ID: rustclamp_core::QualifierId =
        rustclamp_core::QualifierId::new("rustclamp.postgres.analytics");
}

/// States which party closes a PostgreSQL pool.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Ownership {
    /// This wrapper is responsible for closing the pool.
    Managed,
    /// The caller retains responsibility for closing the pool.
    External,
}

/// PostgreSQL pool identified by a compile-time qualifier.
pub struct Database<Q> {
    pool: PgPool,
    ownership: Ownership,
    qualifier: PhantomData<Q>,
}

impl<Q> Clone for Database<Q> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            ownership: self.ownership,
            qualifier: PhantomData,
        }
    }
}

impl<Q: rustclamp_core::Qualifier> Database<Q> {
    /// Wraps an already-created pool with an explicit lifecycle owner.
    pub fn from_pool(pool: PgPool, ownership: Ownership) -> Self {
        Self {
            pool,
            ownership,
            qualifier: PhantomData,
        }
    }

    /// Returns the pool for infrastructure adapters that need SQLx APIs.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Returns who is responsible for closing the pool.
    pub const fn ownership(&self) -> Ownership {
        self.ownership
    }

    /// Returns the stable database qualifier used in diagnostics and composition.
    pub const fn qualifier(&self) -> rustclamp_core::QualifierId {
        Q::ID
    }

    /// Returns a driver transaction kept local to one execution scope.
    pub async fn begin(&self) -> Result<sqlx::Transaction<'static, Postgres>, sqlx::Error> {
        self.pool.begin().await
    }

    /// Probes the selected database without retaining a connection.
    pub async fn health(&self) -> Result<(), sqlx::Error> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map(|_| ())
    }

    /// Closes a managed pool, or drops only this wrapper for an external pool.
    pub async fn close(self) {
        if self.ownership == Ownership::Managed {
            self.pool.close().await;
        }
    }
}

impl<Q: rustclamp_core::Qualifier> Database<Q> {
    /// Connects and owns a new pool.
    pub async fn connect_managed(url: &str, max_connections: u32) -> Result<Self, sqlx::Error> {
        Self::connect_managed_with_timeout(url, max_connections, Duration::from_secs(5)).await
    }

    /// Connects and owns a pool with an explicit connection timeout.
    pub async fn connect_managed_with_timeout(
        url: &str,
        max_connections: u32,
        connect_timeout: Duration,
    ) -> Result<Self, sqlx::Error> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(connect_timeout)
            .connect(url)
            .await?;
        Ok(Self::from_pool(pool, Ownership::Managed))
    }
}

/// Qualified configuration for one pool instance.
pub struct PoolConfig<Q> {
    url: String,
    max_connections: u32,
    connect_timeout: Duration,
    qualifier: PhantomData<Q>,
}

impl<Q> PoolConfig<Q> {
    /// Creates a pool configuration without logging or formatting the URL.
    pub fn new(url: impl Into<String>, max_connections: u32) -> Self {
        Self {
            url: url.into(),
            max_connections,
            connect_timeout: Duration::from_secs(5),
            qualifier: PhantomData,
        }
    }

    /// Sets the bound for establishing or acquiring a connection.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }
}

impl<Q: rustclamp_core::Qualifier> PoolConfig<Q> {
    /// Connects a managed pool for this qualifier.
    pub async fn connect(self) -> Result<Database<Q>, sqlx::Error> {
        Database::connect_managed_with_timeout(
            &self.url,
            self.max_connections,
            self.connect_timeout,
        )
        .await
    }

    /// Returns the stable qualifier identity without exposing the URL.
    pub const fn qualifier(&self) -> rustclamp_core::QualifierId {
        Q::ID
    }
}

/// One explicit SQL migration declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Migration {
    /// Monotonically ordered version assigned by the application.
    pub version: i64,
    /// Human-readable migration summary.
    pub description: &'static str,
    /// SQL applied when this version is absent.
    pub sql: &'static str,
}

/// Validated target for migrations belonging to one database qualifier.
pub struct Migrations<Q> {
    migrations: Vec<Migration>,
    qualifier: PhantomData<Q>,
}

impl<Q> Migrations<Q> {
    /// Validates unique positive versions and returns migrations in version order.
    pub fn new(mut migrations: Vec<Migration>) -> Result<Self, MigrationPlanError> {
        migrations.sort_by_key(|migration| migration.version);
        for pair in migrations.windows(2) {
            if pair[0].version <= 0 || pair[0].version == pair[1].version {
                return Err(MigrationPlanError::InvalidOrDuplicateVersion(
                    pair[1].version,
                ));
            }
        }
        if migrations
            .first()
            .is_some_and(|migration| migration.version <= 0)
        {
            return Err(MigrationPlanError::InvalidOrDuplicateVersion(
                migrations[0].version,
            ));
        }
        Ok(Self {
            migrations,
            qualifier: PhantomData,
        })
    }

    /// Returns ordered migration declarations.
    pub fn entries(&self) -> &[Migration] {
        &self.migrations
    }

    /// Applies unapplied versions. Migration execution is explicit; pools never migrate on connect.
    pub async fn run(&self, pool: &PgPool) -> Result<(), sqlx::Error> {
        sqlx::query("CREATE TABLE IF NOT EXISTS rustclamp_migrations (version BIGINT PRIMARY KEY, description TEXT NOT NULL)")
            .execute(pool).await?;
        for migration in &self.migrations {
            let applied: Option<(i64,)> =
                sqlx::query_as("SELECT version FROM rustclamp_migrations WHERE version = $1")
                    .bind(migration.version)
                    .fetch_optional(pool)
                    .await?;
            if applied.is_some() {
                continue;
            }
            let mut transaction = pool.begin().await?;
            sqlx::raw_sql(migration.sql)
                .execute(&mut *transaction)
                .await?;
            sqlx::query("INSERT INTO rustclamp_migrations (version, description) VALUES ($1, $2)")
                .bind(migration.version)
                .bind(migration.description)
                .execute(&mut *transaction)
                .await?;
            transaction.commit().await?;
        }
        Ok(())
    }
}

/// Migration declaration validation error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MigrationPlanError {
    /// Migration versions must be positive and unique.
    InvalidOrDuplicateVersion(i64),
}

impl fmt::Display for MigrationPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidOrDuplicateVersion(version) => write!(
                formatter,
                "invalid or duplicate migration version {version}"
            ),
        }
    }
}

impl std::error::Error for MigrationPlanError {}
