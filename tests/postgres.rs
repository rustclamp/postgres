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

async fn lock_database(max_connections: u32) -> Database<Primary> {
    let url = std::env::var("RUSTCLAMP_TEST_DATABASE_URL").expect("fixture sets test URL");
    Database::<Primary>::connect_managed(&url, max_connections)
        .await
        .unwrap()
}

/// Ends the session holding advisory lock `key`, as a server restart or a cut connection would,
/// and waits (up to 5 s) for it to exit.
async fn kill_holder(database: &Database<Primary>, key: i64) {
    let killed: (bool,) = sqlx::query_as(
        "SELECT pg_terminate_backend(pid, 5000) FROM pg_locks WHERE locktype = 'advisory' AND granted \
         AND ((classid::bigint << 32) | objid::bigint) = $1",
    )
    .bind(key)
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert!(killed.0);
}

#[tokio::test]
#[ignore = "requires the local Phase 6 PostgreSQL service fixture"]
async fn advisory_lock_has_one_holder_and_release_or_drop_frees_it() {
    let (a, b) = (lock_database(2).await, lock_database(2).await);
    let key = 7001_i64;

    let held = a.try_lock(key).await.unwrap().expect("free lock");
    assert!(b.try_lock(key).await.unwrap().is_none());
    // Per session, not per process: another connection of the same pool is refused too.
    assert!(a.try_lock(key).await.unwrap().is_none());
    held.release().await.unwrap();

    let held = b.try_lock(key).await.unwrap().expect("released lock");
    drop(held);
    // Drop closes the holding connection; the server frees the lock once the session ends.
    let mut retaken = None;
    for _ in 0..50 {
        retaken = a.try_lock(key).await.unwrap();
        if retaken.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    retaken.expect("dropped lock").release().await.unwrap();
    a.close().await;
    b.close().await;
}

#[tokio::test]
#[ignore = "requires the local Phase 6 PostgreSQL service fixture"]
async fn killed_holder_connection_lets_another_session_take_over() {
    let (a, b) = (lock_database(2).await, lock_database(2).await);
    let key = rustclamp_postgres::LockKey::from("rustclamp.test.takeover");

    let mut held = a.try_lock(key).await.unwrap().expect("free lock");
    assert!(held.is_held().await);
    assert!(b.try_lock(key).await.unwrap().is_none());
    kill_holder(&b, key.0).await;
    assert!(!held.is_held().await);
    drop(held);
    b.try_lock(key)
        .await
        .unwrap()
        .expect("lock of a killed session")
        .release()
        .await
        .unwrap();
    a.close().await;
    b.close().await;
}

#[cfg(feature = "scheduler")]
#[tokio::test]
#[ignore = "requires the local Phase 6 PostgreSQL service fixture"]
async fn leader_only_scheduler_runs_on_one_instance_and_fails_over() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, UNIX_EPOCH};

    use rustclamp_core::{ContributionTarget, ManualClock, ModuleId};
    use rustclamp_scheduler::{JobDeclaration, MisfirePolicy, Scheduler, SchedulerTarget};

    const KEY: i64 = 7003;
    const INTERVAL: Duration = Duration::from_secs(60);
    fn instance(database: &Database<Primary>, runs: &Arc<AtomicUsize>) -> Scheduler {
        let runs = Arc::clone(runs);
        SchedulerTarget
            .build(&[(
                ModuleId::new("test.postgres.leader"),
                JobDeclaration::new("report", INTERVAL, MisfirePolicy::Skip, move || {
                    runs.fetch_add(1, Ordering::SeqCst);
                    async { Ok(()) }
                }),
            )])
            .unwrap()
            .leader_only(database.leader_lock(KEY))
    }

    let (db_a, db_b) = (lock_database(2).await, lock_database(2).await);
    let (runs_a, runs_b) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let (a, b) = (instance(&db_a, &runs_a), instance(&db_b, &runs_b));
    let clock = ManualClock::new(UNIX_EPOCH);

    for _ in 0..3 {
        let (ra, rb) = (a.tick(&clock).await, b.tick(&clock).await);
        assert_eq!((ra.invoked, rb.invoked, rb.not_leader), (1, 0, 1));
        clock.advance(INTERVAL);
    }
    assert_eq!(
        (runs_a.load(Ordering::SeqCst), runs_b.load(Ordering::SeqCst)),
        (3, 0)
    );

    // The leader's connection dies: it stops, the standby takes over on its next due tick.
    kill_holder(&db_b, KEY).await;
    let rb = b.tick(&clock).await;
    let ra = a.tick(&clock).await;
    assert_eq!((rb.invoked, ra.invoked, ra.not_leader), (1, 0, 1));
    assert_eq!(
        (runs_a.load(Ordering::SeqCst), runs_b.load(Ordering::SeqCst)),
        (3, 1)
    );
    // Pool::close waits for every connection, including the one a held lock keeps.
    drop((a, b));
    db_a.close().await;
    db_b.close().await;
}
