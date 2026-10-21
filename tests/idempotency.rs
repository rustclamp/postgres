//! `PgIdempotencyStore` against a real PostgreSQL (`tests/integration/run.sh`).
#![cfg(feature = "idempotency")]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, SystemTime};

use rustclamp_http::ecosystem::Router;
use rustclamp_http::ecosystem::body::Body;
use rustclamp_http::ecosystem::http::{Request, StatusCode};
use rustclamp_http::ecosystem::routing::post;
use rustclamp_http::{
    Claim, Idempotency, IdempotencyKey, IdempotencyStore, StoredResponse, with_idempotency,
};
use rustclamp_postgres::{Migrations, PgIdempotencyStore, Primary, idempotency_migration};
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;

/// Tests in this binary share one database; the first migrates it.
static MIGRATE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn store() -> PgIdempotencyStore {
    let url = std::env::var("RUSTCLAMP_TEST_DATABASE_URL").expect("fixture sets test URL");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .unwrap();
    let _migrating = MIGRATE.lock().await;
    Migrations::<Primary>::new(vec![idempotency_migration(6101)])
        .unwrap()
        .run(&pool)
        .await
        .unwrap();
    PgIdempotencyStore::new(pool)
}

/// A key no earlier run used.
fn key(name: &str) -> IdempotencyKey {
    let nanos = SystemTime::UNIX_EPOCH.elapsed().unwrap().as_nanos();
    IdempotencyKey {
        principal: "alice".into(),
        key: format!("{name}-{nanos}"),
        method: "POST".into(),
        path: "/orders".into(),
    }
}

#[tokio::test]
#[ignore = "requires the local PostgreSQL service fixture"]
async fn claims_once_stores_replays_releases_and_expires() {
    let store = store().await;
    let ttl = Duration::from_secs(60);
    let order = key("order");
    assert_eq!(
        store.claim(&order, [1; 32], ttl).await.unwrap(),
        Claim::Claimed
    );
    let Claim::Existing(record) = store.claim(&order, [1; 32], ttl).await.unwrap() else {
        panic!("second claim must see the first");
    };
    assert_eq!(record.fingerprint, [1; 32]);
    assert_eq!(record.response, None, "in progress");

    let response = StoredResponse {
        status: 201,
        headers: vec![
            ("content-type".into(), b"application/json".to_vec()),
            ("location".into(), b"/orders/1".to_vec()),
        ],
        body: b"{\"id\":1}".to_vec(),
    };
    store.complete(&order, &response).await.unwrap();
    // Releasing a completed key keeps it.
    store.release(&order).await.unwrap();
    let Claim::Existing(record) = store.claim(&order, [2; 32], ttl).await.unwrap() else {
        panic!("completed key must stay claimed");
    };
    assert_eq!(record.fingerprint, [1; 32]);
    assert_eq!(record.response, Some(response));

    // An in-progress key is released.
    let failed = key("failed");
    store.claim(&failed, [1; 32], ttl).await.unwrap();
    store.release(&failed).await.unwrap();
    assert_eq!(
        store.claim(&failed, [1; 32], ttl).await.unwrap(),
        Claim::Claimed
    );

    // An expired key is claimable again, and cleanup removes expired rows.
    let short = key("short");
    let brief = Duration::from_millis(50);
    store.claim(&short, [1; 32], brief).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        store.claim(&short, [1; 32], brief).await.unwrap(),
        Claim::Claimed
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(store.delete_expired().await.unwrap() >= 1);
    assert_eq!(
        store.claim(&short, [1; 32], ttl).await.unwrap(),
        Claim::Claimed
    );
}

#[tokio::test]
#[ignore = "requires the local PostgreSQL service fixture"]
async fn two_concurrent_identical_requests_run_the_handler_once() {
    let store = Arc::new(store().await);
    let runs = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&runs);
    let orders = Router::new().route(
        "/orders",
        post(move || {
            let counter = Arc::clone(&counter);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(200)).await;
                (StatusCode::CREATED, "created")
            }
        }),
    );
    let router = with_idempotency(orders, Idempotency::new(store));
    let id = key("http").key;
    let request = || {
        Request::post("/orders")
            .header("idempotency-key", id.as_str())
            .body(Body::from("pay 10"))
            .unwrap()
    };
    let (first, second) = tokio::join!(
        router.clone().oneshot(request()),
        router.clone().oneshot(request())
    );
    let mut statuses = [first.unwrap().status(), second.unwrap().status()];
    statuses.sort();
    assert_eq!(statuses, [StatusCode::CREATED, StatusCode::CONFLICT]);
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    let replay = router.oneshot(request()).await.unwrap();
    assert_eq!(replay.status(), StatusCode::CREATED);
    assert_eq!(replay.headers()["idempotent-replayed"], "true");
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}
