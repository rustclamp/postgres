//! Explicit integration coverage against a real local PostgreSQL service.
use rustclamp_postgres::{Database, Migration, Migrations, Ownership, Primary};
use sqlx::postgres::PgPoolOptions;

#[tokio::test]
#[ignore = "requires the local Phase 6 PostgreSQL service fixture"]
async fn real_postgres_migration_health_and_transaction_rollback() {
    let url = std::env::var("RUSTCLAMP_TEST_DATABASE_URL").expect("fixture sets test URL");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .unwrap();
    let database = Database::<Primary>::from_pool(pool, Ownership::Managed);
    let migrations = Migrations::<Primary>::new(vec![Migration {
        version: 6001,
        description: "phase 6 integration users",
        sql: "CREATE TABLE IF NOT EXISTS rustclamp_p6_smoke_users (id BIGINT PRIMARY KEY, name TEXT NOT NULL)",
    }]).unwrap();
    migrations.run(database.pool()).await.unwrap();
    database.health().await.unwrap();

    let mut transaction = database.begin().await.unwrap();
    rustclamp_postgres::sqlx::query("INSERT INTO rustclamp_p6_smoke_users (id, name) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name")
        .bind(6001_i64).bind("rolled back").execute(&mut *transaction).await.unwrap();
    transaction.rollback().await.unwrap();
    let count: (i64,) = rustclamp_postgres::sqlx::query_as(
        "SELECT count(*) FROM rustclamp_p6_smoke_users WHERE id = $1",
    )
    .bind(6001_i64)
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(count.0, 0);
    database.close().await;
}

#[tokio::test]
#[ignore = "requires the local Phase 6 PostgreSQL service fixture"]
async fn failed_migration_rolls_back_and_remains_retryable() {
    let url = std::env::var("RUSTCLAMP_TEST_DATABASE_URL").expect("fixture sets test URL");
    let database = Database::<Primary>::connect_managed(&url, 2).await.unwrap();
    let version = 6099_i64;
    let plan = Migrations::<Primary>::new(vec![Migration {
        version,
        description: "intentional failure fixture",
        sql: "CREATE TABLE rustclamp_p6_broken (id BIGINT); SELECT * FROM rustclamp_p6_missing",
    }])
    .unwrap();
    assert!(plan.run(database.pool()).await.is_err());
    let applied: Option<(i64,)> =
        sqlx::query_as("SELECT version FROM rustclamp_migrations WHERE version = $1")
            .bind(version)
            .fetch_optional(database.pool())
            .await
            .unwrap();
    assert!(applied.is_none());
    let table: Option<(String,)> = sqlx::query_as(
        "SELECT table_name FROM information_schema.tables WHERE table_name = 'rustclamp_p6_broken'",
    )
    .fetch_optional(database.pool())
    .await
    .unwrap();
    assert!(table.is_none(), "DDL from failed migration must roll back");
    database.close().await;
}
