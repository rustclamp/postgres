//! Pool ownership, qualifier, and migration planning tests.
use rustclamp_core::Qualifier;
use rustclamp_postgres::{
    Analytics, Database, Migration, Migrations, Ownership, PoolConfig, Primary,
};
use sqlx::postgres::PgPoolOptions;

#[test]
fn migration_target_orders_versions_and_rejects_duplicates() {
    let plan = Migrations::<Primary>::new(vec![
        Migration {
            version: 2,
            description: "second",
            sql: "SELECT 2",
        },
        Migration {
            version: 1,
            description: "first",
            sql: "SELECT 1",
        },
    ])
    .unwrap();
    assert_eq!(plan.entries()[0].version, 1);
    assert!(
        Migrations::<Analytics>::new(vec![
            Migration {
                version: 1,
                description: "first",
                sql: "SELECT 1"
            },
            Migration {
                version: 1,
                description: "duplicate",
                sql: "SELECT 2"
            },
        ])
        .is_err()
    );
}

#[tokio::test]
async fn managed_close_and_external_pool_drop_follow_declared_ownership() {
    let managed_pool = PgPoolOptions::new()
        .connect_lazy("postgres://user:pass@localhost/test")
        .unwrap();
    let managed = Database::<Primary>::from_pool(managed_pool, Ownership::Managed);
    let managed_observer = managed.clone();
    managed.close().await;
    assert!(managed_observer.pool().is_closed());

    let external_pool = PgPoolOptions::new()
        .connect_lazy("postgres://user:pass@localhost/test")
        .unwrap();
    let external_observer = external_pool.clone();
    let external = Database::<Primary>::from_pool(external_pool, Ownership::External);
    assert_eq!(external.qualifier(), Primary::ID);
    assert_eq!(
        PoolConfig::<Analytics>::new("postgres://example", 2).qualifier(),
        Analytics::ID
    );
    external.close().await;
    assert!(!external_observer.is_closed());
    external_observer.close().await;
}

#[tokio::test]
async fn cloned_pools_share_application_lifetime_and_distinct_pools_are_isolated() {
    let application_pool = PgPoolOptions::new()
        .connect_lazy("postgres://user:pass@localhost/application")
        .unwrap();
    let application = Database::<Primary>::from_pool(application_pool, Ownership::Managed);
    let second_module = application.clone();
    assert_eq!(application.pool().size(), second_module.pool().size());
    application.close().await;
    assert!(second_module.pool().is_closed());

    let first_process_pool = PgPoolOptions::new()
        .connect_lazy("postgres://user:pass@localhost/process-one")
        .unwrap();
    let second_process_pool = PgPoolOptions::new()
        .connect_lazy("postgres://user:pass@localhost/process-two")
        .unwrap();
    let first_process = Database::<Primary>::from_pool(first_process_pool, Ownership::Managed);
    let second_process = Database::<Analytics>::from_pool(second_process_pool, Ownership::Managed);
    assert_ne!(first_process.qualifier(), second_process.qualifier());
    first_process.close().await;
    assert!(!second_process.pool().is_closed());
    second_process.close().await;
}

#[tokio::test]
async fn invalid_selected_configuration_fails_before_connection() {
    let invalid = PoolConfig::<Primary>::new("not a postgres url", 5)
        .connect()
        .await;
    assert!(invalid.is_err());
}

#[tokio::test]
async fn connection_failure_respects_the_selected_timeout() {
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        Database::<Primary>::connect_managed_with_timeout(
            "postgres://user:pass@127.0.0.1:1/test",
            1,
            std::time::Duration::from_millis(100),
        ),
    )
    .await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_err());
}
